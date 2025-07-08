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

use crate::{
    cloud_storage::{error::AwsStorageError, path_conversions::normalize_path},
    utils::constants::{CHUNKS_MAX_WORKERS, CHUNK_RETRIES, RETRIES},
};

#[derive(Debug)]
pub struct UploadTaskInfo {
    pub client: Client,
    pub bucket: String,
    pub local_path: PathBuf,
    pub s3_key: String,
    pub size: u64,
    pub chunk_size: u64,
    pub max_chunks: u64,
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
    } = task_info;

    if size < chunk_size {
        retry(
            move || {
                upload_object(
                    client.clone(),
                    bucket.clone(),
                    local_path.clone(),
                    s3_key.clone(),
                    size,
                )
            },
            RETRIES,
        )
        .await
    } else {
        retry(
            move || {
                upload_object_multipart(
                    client.clone(),
                    bucket.clone(),
                    local_path.clone(),
                    s3_key.clone(),
                    size,
                    chunk_size,
                    max_chunks,
                )
            },
            RETRIES,
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
async fn upload_object(
    client: Client,
    bucket: String,
    file_path: PathBuf,
    key: String,
    file_size: u64,
) -> Result<(), AwsStorageError> {
    info!("Uploading file: {}", normalize_path(&file_path));
    let body = ByteStream::from_path(&file_path)
        .await
        .map_err(AwsStorageError::ByteSreamError)?;

    client
        .put_object()
        .bucket(&bucket)
        .key(&key)
        .body(body)
        .send()
        .await
        .map_err(AwsStorageError::PutObjectError)?;

    let result = client.get_object().bucket(bucket).key(key).send().await?; //#TODO think about adding a parameter because can be expensive
    let uploaded_size = result.content_length().unwrap_or(0) as u64;
    if uploaded_size != file_size {
        return Err(AwsStorageError::UnexpectedError(Report::msg(format!(
            "Size mismatch after upload. Expected {}, got {}",
            file_size, uploaded_size
        ))));
    }
    info!("Uploaded file: {}", normalize_path(file_path));
    Ok(())
}

/// If file is big, upload it by chunks and chunks in parallel
async fn upload_object_multipart(
    client: Client,
    bucket: String,
    file_path: PathBuf,
    key: String,
    file_size: u64,
    chunk_size: u64,
    max_chunks: u64,
) -> Result<(), AwsStorageError> {
    info!("Uploading file: {}", normalize_path(&file_path));

    if file_size == 0 {
        return Err(AwsStorageError::UnexpectedError(Report::msg(format!(
            "File is empty: {}",
            normalize_path(&file_path)
        ))));
    }

    let chunk_count = (file_size + chunk_size - 1) / chunk_size;
    if chunk_count > max_chunks {
        return Err(AwsStorageError::UnexpectedError(Report::msg(format!(
            "Too many chunks for {}: {}. Increase chunk size or max_chunks.",
            normalize_path(&file_path),
            chunk_count
        ))));
    }

    let multipart_upload_res = client
        .create_multipart_upload()
        .bucket(&bucket)
        .key(&key)
        .send()
        .await?;

    let upload_id = multipart_upload_res.upload_id().ok_or_else(|| {
        AwsStorageError::UnexpectedError(Report::msg(format!(
            "No upload ID returned for file: {}",
            normalize_path(&file_path)
        )))
    })?;

    let path = Arc::new(file_path);
    let semaphore = Arc::new(Semaphore::new(CHUNKS_MAX_WORKERS));
    let mut tasks = JoinSet::new();

    for part_index in 0..chunk_count {
        let client = client.clone();
        let bucket = bucket.clone();
        let key = key.clone();
        let upload_id = upload_id.to_string();
        let path = Arc::clone(&path);
        let permit = Arc::clone(&semaphore).acquire_owned().await.map_err(|e| {
            AwsStorageError::UnexpectedError(Report::msg(format!("Can't acquire semaphore: {e}")))
        })?;

        tasks.spawn(async move {
            let _permit = permit;
            let offset = part_index * chunk_size;
            let this_chunk_size = std::cmp::min(chunk_size, file_size - offset);
            let part_number = (part_index + 1) as i32;

            let mut last_err = None;

            for attempt in 1..=CHUNK_RETRIES { // #TODO chunk retry add in config file
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
                        tokio::time::sleep(std::time::Duration::from_millis(300 * attempt)).await;
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

    client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(&key)
        .upload_id(upload_id)
        .multipart_upload(completed_upload)
        .send()
        .await?;

    let result = client.get_object().bucket(bucket).key(key).send().await?; //#TODO think about adding a parameter because can be expensive
    let uploaded_size = result.content_length().unwrap_or(0) as u64;
    if uploaded_size != file_size {
        return Err(AwsStorageError::UnexpectedError(Report::msg(format!(
            "Size mismatch after upload. Expected {}, got {}",
            file_size, uploaded_size
        ))));
    }
    info!("Uploaded file: {}", normalize_path(path.as_ref()));
    Ok(())
}

// #TODO use this fn when network is slow, add parameter in config slow_network = true
pub async fn upload_object_multipart_v1(
    client: Client,
    bucket_name: String,
    file_name: PathBuf,
    key: String,
    file_size: u64,
    chunk_size: u64,
    max_chunks: u64,
) -> Result<(), AwsStorageError> {
    info!("Uploading file: {}", normalize_path(&file_name));

    let multipart_upload_res = client
        .create_multipart_upload()
        .bucket(&bucket_name)
        .key(&key)
        .send()
        .await
        .map_err(AwsStorageError::CreateMultipartError)?;

    let upload_id = multipart_upload_res.upload_id().unwrap_or_default();
    let mut chunk_count = (file_size / chunk_size) + 1;
    let mut size_of_last_chunk = file_size % chunk_size;

    if size_of_last_chunk == 0 {
        size_of_last_chunk = chunk_size;
        chunk_count -= 1;
    }
    if file_size == 0 {
        return Err(AwsStorageError::UnexpectedError(Report::msg(format!(
            "Bad file size for: {}",
            normalize_path(file_name)
        ))));
    }
    if chunk_count > max_chunks {
        return Err(AwsStorageError::UnexpectedError(Report::msg(format!(
            "Too many chunks file: {}. Try increasing your chunk size",
            normalize_path(file_name)
        ))));
    }

    let mut upload_parts: Vec<CompletedPart> = Vec::new();
    for chunk_index in 0..chunk_count {
        let this_chunk = if chunk_count - 1 == chunk_index {
            size_of_last_chunk
        } else {
            chunk_size
        };
        let stream = ByteStream::read_from()
            .path(&file_name)
            .offset(chunk_index * chunk_size)
            .length(Length::Exact(this_chunk))
            .build()
            .await
            .map_err(AwsStorageError::ByteSreamError)?;

        let part_number = (chunk_index as i32) + 1;
        let upload_part_res = client
            .upload_part()
            .key(&key)
            .bucket(&bucket_name)
            .upload_id(upload_id)
            .body(stream)
            .part_number(part_number)
            .send()
            .await
            .map_err(AwsStorageError::UploadPartError)?;

        upload_parts.push(
            CompletedPart::builder()
                .e_tag(upload_part_res.e_tag.unwrap_or_default())
                .part_number(part_number)
                .build(),
        );
    }

    let completed_multipart_upload = CompletedMultipartUpload::builder()
        .set_parts(Some(upload_parts))
        .build();

    let _complete_multipart_upload_res = client
        .complete_multipart_upload()
        .bucket(&bucket_name)
        .key(&key)
        .multipart_upload(completed_multipart_upload)
        .upload_id(upload_id)
        .send()
        .await
        .map_err(AwsStorageError::CompleteMultipartError)?;

    let data: GetObjectOutput = get_object(&client, &bucket_name, &key).await?;
    let data_length = data.content_length().unwrap_or(0) as u64;
    if file_size != data_length {
        return Err(AwsStorageError::UnexpectedError(Report::msg(
            "Source and target data sizes after upload don't match",
        )));
    }
    info!("Uploaded file: {}", normalize_path(file_name));
    Ok(())
}
