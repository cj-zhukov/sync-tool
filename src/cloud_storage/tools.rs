use aws_sdk_s3::{
    operation::get_object::GetObjectOutput,
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart},
    Client,
};
use aws_smithy_types::byte_stream::Length;
use color_eyre::Report;
use log::{error, info};
use std::{collections::HashMap, path::Path, time::Duration};
use tokio::time::sleep;

use crate::{
    cloud_storage::error::AwsStorageError,
    utils::{config::AppConfig, constants::RETRIES},
    SyncToolError,
};

#[derive(Debug)]
pub struct UploadTaskInfo {
    pub client: Client,
    pub bucket: String,
    pub local_path: String,
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
    client: Client,
    bucket: String,
    prefix: String,
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

pub fn dif_calc(
    config: &AppConfig,
    source: &HashMap<String, i64>,
    target: &HashMap<String, i64>,
) -> Option<HashMap<String, i64>> {
    let mut dif: HashMap<String, i64> = HashMap::new();
    for (k, v) in source.iter() {
        let source_f_name = k
            .strip_prefix(&config.source)
            .unwrap_or("no_file_name")
            .to_string();
        let source_f_size = *v;
        let target_f_name = format!("{}{}", &config.target, &source_f_name);
        match target.get(&target_f_name) {
            Some(target_f_size) => {
                if source_f_size != *target_f_size {
                    dif.insert(source_f_name.clone(), source_f_size);
                }
            }
            None => {
                dif.insert(source_f_name.clone(), source_f_size);
            }
        }
    }

    if dif.is_empty() {
        None
    } else {
        Some(dif)
    }
}

pub async fn spawn_upload_task(task_info: UploadTaskInfo) -> Result<(), SyncToolError> {
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
    bucket_name: String,
    file_name: String,
    key: String,
    file_size: u64,
) -> Result<(), SyncToolError> {
    info!("Uploading file: {}", file_name);
    let body = ByteStream::from_path(Path::new(&file_name))
        .await
        .map_err(AwsStorageError::ByteSreamError)?;

    client
        .put_object()
        .bucket(&bucket_name)
        .key(&key)
        .body(body)
        .send()
        .await
        .map_err(AwsStorageError::PutObjectError)?;

    let data = get_object(&client, &bucket_name, &key).await?;
    let data_length = data.content_length().unwrap_or(0) as u64;
    if file_size != data_length {
        return Err(SyncToolError::UnexpectedError(Report::msg(
            "Source and target data sizes after upload don't match",
        )));
    }
    info!("Uploaded file: {}", file_name);
    Ok(())
}

/// If file is too big
async fn upload_object_multipart(
    client: Client,
    bucket_name: String,
    file_name: String,
    key: String,
    file_size: u64,
    chunk_size: u64,
    max_chunks: u64,
) -> Result<(), SyncToolError> {
    info!("Uploading file: {}", file_name);

    let multipart_upload_res = client
        .create_multipart_upload()
        .bucket(&bucket_name)
        .key(&key)
        .send()
        .await
        .map_err(AwsStorageError::CreateMultipartError)?;

    let upload_id = multipart_upload_res.upload_id().unwrap_or_default();
    let path = Path::new(&file_name);
    let mut chunk_count = (file_size / chunk_size) + 1;
    let mut size_of_last_chunk = file_size % chunk_size;

    if size_of_last_chunk == 0 {
        size_of_last_chunk = chunk_size;
        chunk_count -= 1;
    }
    if file_size == 0 {
        return Err(SyncToolError::UnexpectedError(Report::msg(format!(
            "Bad file size for: {}",
            file_name
        ))));
    }
    if chunk_count > max_chunks {
        return Err(SyncToolError::UnexpectedError(Report::msg(format!(
            "Too many chunks file: {}. Try increasing your chunk size",
            file_name
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
            .path(path)
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

    let data: GetObjectOutput = get_object(&client, &bucket_name, &key).await?; //#TODO think about adding a parameter because can be expensive
    let data_length = data.content_length().unwrap_or(0) as u64;
    if file_size != data_length {
        return Err(SyncToolError::UnexpectedError(Report::msg(
            "Source and target data sizes after upload don't match",
        )));
    }
    info!("Uploaded file: {}", file_name);
    Ok(())
}
