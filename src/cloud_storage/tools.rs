use aws_sdk_s3::{
    operation::get_object::GetObjectOutput,
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart},
    Client,
};
use aws_smithy_types::byte_stream::Length;
use color_eyre::Report;
use log::{error, info};
use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};
use tokio::{sync::Semaphore, task::JoinSet, time::sleep};

use crate::cloud_storage::{error::AwsStorageError, path_conversions::normalize_path};

#[derive(Debug)]
pub struct UploadTaskInfo {
    pub client: Client,
    pub bucket: String,
    pub local_path: PathBuf,
    pub s3_key: String,
    pub size: u64,
    pub chunk_size: u64,
    pub max_chunks: u64,
    pub retries: usize,
    pub chunk_retries: usize,
    pub chunk_workers: usize,
    pub check_size: bool,
}

#[derive(Debug, Clone)]
pub struct UploadInfo {
    pub client: Client,
    pub bucket: String,
    pub local_path: PathBuf,
    pub key: String,
    pub size: u64,
    pub check_size: bool,
}

#[derive(Debug, Clone)]
pub struct UploadInfoExtra {
    pub chunk_size: u64,
    pub max_chunks: u64,
    pub chunk_retries: usize,
    pub chunk_workers: usize,
}

pub async fn get_object(
    client: &Client,
    bucket_name: &str,
    key: &str,
) -> Result<GetObjectOutput, AwsStorageError> {
    Ok(client
        .get_object()
        .bucket(bucket_name)
        .key(key)
        .send()
        .await?)
}

pub async fn list_keys(
    client: &Client,
    bucket: &str,
    prefix: &str,
) -> Result<HashMap<String, i64>, AwsStorageError> {
    let mut stream = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .into_paginator()
        .send();

    let mut files: HashMap<String, i64> = HashMap::new();
    while let Some(objects) = stream.next().await.transpose()? {
        for obj in objects.contents().iter().cloned() {
            if let Some(f_name) = &obj.key {
                let f_size = obj.size().unwrap_or(0);
                files.insert(f_name.clone(), f_size);
            }
        }
    }

    Ok(files)
}

pub async fn spawn_upload_task(task_info: UploadTaskInfo) -> Result<(), AwsStorageError> {
    let UploadTaskInfo {
        client,
        bucket,
        local_path,
        s3_key,
        size,
        chunk_size,
        max_chunks,
        retries,
        chunk_retries,
        chunk_workers,
        check_size,
    } = task_info;

    let upload_info = UploadInfo {
        client,
        bucket,
        local_path,
        key: s3_key,
        size,
        check_size,
    };
    let upload_info_extra = UploadInfoExtra {
        chunk_size,
        max_chunks,
        chunk_retries,
        chunk_workers,
    };

    if size < chunk_size {
        retry(move || upload_object(upload_info.clone()), retries).await
    } else {
        retry(
            move || upload_object_multipart(upload_info.clone(), upload_info_extra.clone()),
            retries,
        )
        .await
    }
}

/// Retry an async operation `op` up to `retries` times.
async fn retry<F, Fut, T, E>(mut op: F, retries: usize) -> Result<T, E>
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<T, E>> + Send,
    T: Send,
    E: std::fmt::Debug + Send,
{
    let mut attempts = 0;
    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(e) => {
                attempts += 1;
                if attempts > retries {
                    return Err(e);
                }
                error!(
                    "Retry {}/{} failed: {:?}. Retrying...",
                    attempts, retries, e
                );
                sleep(Duration::from_secs(1 << attempts)).await;
            }
        }
    }
}

/// If file is small
async fn upload_object(upload_info: UploadInfo) -> Result<(), AwsStorageError> {
    info!(
        "Uploading file: {}",
        normalize_path(&upload_info.local_path)
    );
    let body = ByteStream::from_path(&upload_info.local_path)
        .await
        .map_err(AwsStorageError::ByteSreamError)?;

    upload_info
        .client
        .put_object()
        .bucket(&upload_info.bucket)
        .key(&upload_info.key)
        .body(body)
        .send()
        .await
        .map_err(AwsStorageError::PutObjectError)?;

    let result = upload_info
        .client
        .get_object()
        .bucket(upload_info.bucket)
        .key(upload_info.key)
        .send()
        .await?;

    if upload_info.check_size {
        let uploaded_size = result.content_length().unwrap_or(0) as u64;
        if uploaded_size != upload_info.size {
            return Err(AwsStorageError::UnexpectedError(Report::msg(format!(
                "Size mismatch after upload. Expected {}, got {}",
                upload_info.size, uploaded_size
            ))));
        }
    }

    info!("Uploaded file: {}", normalize_path(&upload_info.local_path));
    Ok(())
}

/// If file is big, upload it by chunks and chunks in parallel
async fn upload_object_multipart(
    upload_info: UploadInfo,
    upload_info_extra: UploadInfoExtra,
) -> Result<(), AwsStorageError> {
    info!(
        "Uploading file: {}",
        normalize_path(&upload_info.local_path)
    );

    if upload_info.size == 0 {
        return Err(AwsStorageError::UnexpectedError(Report::msg(format!(
            "File is empty: {}",
            normalize_path(&upload_info.local_path)
        ))));
    }

    let chunk_count = upload_info.size.div_ceil(upload_info_extra.chunk_size); // (upload_info.size + upload_info_extra.chunk_size - 1) / upload_info_extra.chunk_size;
    if chunk_count > upload_info_extra.max_chunks {
        return Err(AwsStorageError::UnexpectedError(Report::msg(format!(
            "Too many chunks for {}: {}. Increase chunk size or max_chunks.",
            normalize_path(&upload_info.local_path),
            chunk_count
        ))));
    }

    let multipart_upload_res = upload_info
        .client
        .create_multipart_upload()
        .bucket(&upload_info.bucket)
        .key(&upload_info.key)
        .send()
        .await?;

    let upload_id = multipart_upload_res.upload_id().ok_or_else(|| {
        AwsStorageError::UnexpectedError(Report::msg(format!(
            "No upload ID returned for file: {}",
            normalize_path(&upload_info.local_path)
        )))
    })?;

    let path = Arc::new(upload_info.local_path);
    let semaphore = Arc::new(Semaphore::new(upload_info_extra.chunk_workers));
    let mut tasks = JoinSet::new();

    for part_index in 0..chunk_count {
        let client = upload_info.client.clone();
        let bucket = upload_info.bucket.clone();
        let key = upload_info.key.clone();
        let upload_id = upload_id.to_string();
        let path = Arc::clone(&path);
        let permit = Arc::clone(&semaphore).acquire_owned().await.map_err(|e| {
            AwsStorageError::UnexpectedError(Report::msg(format!("Can't acquire semaphore: {e}")))
        })?;

        tasks.spawn(async move {
            let _permit = permit;
            let offset = part_index * upload_info_extra.chunk_size;
            let this_chunk_size =
                std::cmp::min(upload_info_extra.chunk_size, upload_info.size - offset);
            let part_number = (part_index + 1) as i32;

            let mut last_err = None;

            for attempt in 1..=upload_info_extra.chunk_retries {
                let stream_result = ByteStream::read_from()
                    .path(&*path)
                    .offset(offset)
                    .length(Length::Exact(this_chunk_size))
                    .build()
                    .await;

                let stream = match stream_result {
                    Ok(s) => s,
                    Err(e) => {
                        last_err = Some(AwsStorageError::UnexpectedError(Report::msg(format!(
                            "ByteStream error: {e}"
                        ))));
                        continue;
                    }
                };

                let result = client
                    .upload_part()
                    .bucket(&bucket)
                    .key(&key)
                    .upload_id(&upload_id)
                    .part_number(part_number)
                    .body(stream)
                    .send()
                    .await;

                match result {
                    Ok(upload_part) => {
                        let e_tag = upload_part.e_tag.ok_or_else(|| {
                            AwsStorageError::UnexpectedError(Report::msg(format!(
                                "Missing ETag for part {part_number}"
                            )))
                        })?;

                        return Ok(CompletedPart::builder()
                            .e_tag(e_tag)
                            .part_number(part_number)
                            .build());
                    }
                    Err(e) => {
                        last_err = Some(AwsStorageError::UnexpectedError(Report::msg(format!(
                            "Failed to upload part {part_number}, attempt {attempt}: {e}"
                        ))));
                        tokio::time::sleep(std::time::Duration::from_millis(300 * attempt as u64))
                            .await;
                    }
                }
            }

            Err(last_err.unwrap_or_else(|| {
                AwsStorageError::UnexpectedError(Report::msg(format!(
                    "Part {part_number} failed with unknown error"
                )))
            }))
        });
    }

    let mut completed_parts = Vec::with_capacity(chunk_count as usize);
    while let Some(result) = tasks.join_next().await {
        let res: CompletedPart = result
            .map_err(|e| AwsStorageError::UnexpectedError(Report::msg(e)))?
            .map_err(|e| AwsStorageError::UnexpectedError(Report::msg(e)))?;
        completed_parts.push(res);
    }

    completed_parts.sort_by_key(|part| part.part_number());
    let completed_upload = CompletedMultipartUpload::builder()
        .set_parts(Some(completed_parts))
        .build();

    upload_info
        .client
        .complete_multipart_upload()
        .bucket(&upload_info.bucket)
        .key(&upload_info.key)
        .upload_id(upload_id)
        .multipart_upload(completed_upload)
        .send()
        .await?;

    let result = upload_info
        .client
        .get_object()
        .bucket(upload_info.bucket)
        .key(upload_info.key)
        .send()
        .await?;

    if upload_info.check_size {
        let uploaded_size = result.content_length().unwrap_or(0) as u64;
        if uploaded_size != upload_info.size {
            return Err(AwsStorageError::UnexpectedError(Report::msg(format!(
                "Size mismatch after upload. Expected {}, got {}",
                upload_info.size, uploaded_size
            ))));
        }
    }

    info!("Uploaded file: {}", normalize_path(path.as_ref()));
    Ok(())
}
