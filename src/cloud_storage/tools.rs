use aws_sdk_s3::{
    operation::get_object::GetObjectOutput,
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart},
    Client,
};
use aws_smithy_types::byte_stream::Length;
use color_eyre::Report;
use indicatif::ProgressBar;
use log::{error, info};
use std::{collections::HashMap, path::Path, sync::{atomic::{AtomicU64, Ordering}, Arc}, time::Duration};
use tokio::{task::JoinSet, time::sleep};

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

pub async fn list_keys_stream(
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

/// Based on file size run upload
pub async fn spawn_upload_task(
    tasks: &mut JoinSet<Result<(), AwsStorageError>>,
    task_info: UploadTaskInfo,
    max_workers: usize,
) -> Result<(), SyncToolError> {
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
        tasks.spawn(async move {
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
        });
    } else {
        tasks.spawn(async move {
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
        });
    }

    // If we hit the max worker limit, wait for any one task to finish before continuing
    if tasks.len() >= max_workers {
        if let Some(res) = tasks.join_next().await {
            if let Err(e) = res {
                error!("Upload task failed: {}", e);
            }
        }
    }
    Ok(())
}

pub async fn spawn_upload_task2(
    tasks: &mut JoinSet<Result<(), AwsStorageError>>,
    task_info: UploadTaskInfo,
    max_workers: usize,
    uploaded_files: Arc<AtomicU64>,
    uploaded_bytes: Arc<AtomicU64>,
    pb: Arc<ProgressBar>,
) -> Result<(), SyncToolError> {
    let UploadTaskInfo {
        client,
        bucket,
        local_path,
        s3_key,
        size,
        chunk_size,
        max_chunks,
    } = task_info;

    let update_progress = move || {
        uploaded_files.fetch_add(1, Ordering::Relaxed);
        uploaded_bytes.fetch_add(size, Ordering::Relaxed);
        let count = uploaded_files.load(Ordering::Relaxed);
        let bytes = uploaded_bytes.load(Ordering::Relaxed) as f64 / (1024.0 * 1024.0);
        pb.set_message(format!("Uploaded: {count} files | {bytes:.2} MB"));
    };

    let task = async move {
        let result = if size < chunk_size {
            retry(move || {
                upload_object(
                    client.clone(),
                    bucket.clone(),
                    local_path.clone(),
                    s3_key.clone(),
                    size,
                )
            }, RETRIES).await
        } else {
            retry(move || {
                upload_object_multipart(
                    client.clone(),
                    bucket.clone(),
                    local_path.clone(),
                    s3_key.clone(),
                    size,
                    chunk_size,
                    max_chunks,
                )
            }, RETRIES).await
        };

        if result.is_ok() {
            update_progress();
        }

        result
    };

    tasks.spawn(task);

    // If we hit the max worker limit, wait for any one task to finish before continuing
    if tasks.len() >= max_workers {
        if let Some(res) = tasks.join_next().await {
            if let Err(e) = res {
                error!("Upload task failed: {}", e);
            }
        }
    }
    Ok(())
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
) -> Result<(), AwsStorageError> {
    let body = ByteStream::from_path(Path::new(&file_name)).await?;
    info!("Uploading file: {}", file_name);

    client
        .put_object()
        .bucket(&bucket_name)
        .key(&key)
        .body(body)
        .send()
        .await?;

    info!("Uploaded file: {}", file_name);

    let data = get_object(&client, &bucket_name, &key).await?;
    let data_length = data.content_length().unwrap_or(0) as u64;
    if file_size != data_length {
        return Err(AwsStorageError::UnexpectedError(Report::msg(
            "Source and target data sizes after upload don't match",
        )));
    }

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
) -> Result<(), AwsStorageError> {
    info!("Uploading file: {}", file_name);

    let multipart_upload_res = client
        .create_multipart_upload()
        .bucket(&bucket_name)
        .key(&key)
        .send()
        .await?;

    let upload_id = multipart_upload_res.upload_id().unwrap_or_default();
    let path = Path::new(&file_name);
    let mut chunk_count = (file_size / chunk_size) + 1;
    let mut size_of_last_chunk = file_size % chunk_size;

    if size_of_last_chunk == 0 {
        size_of_last_chunk = chunk_size;
        chunk_count -= 1;
    }
    if file_size == 0 {
        return Err(AwsStorageError::UnexpectedError(Report::msg(format!(
            "Bad file size for: {}",
            file_name
        ))));
    }
    if chunk_count > max_chunks {
        return Err(AwsStorageError::UnexpectedError(Report::msg(format!(
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
            .await?;

        let part_number = (chunk_index as i32) + 1;
        let upload_part_res = client
            .upload_part()
            .key(&key)
            .bucket(&bucket_name)
            .upload_id(upload_id)
            .body(stream)
            .part_number(part_number)
            .send()
            .await?;

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
        .await?;

    info!("Uploaded file: {}", file_name);

    let data: GetObjectOutput = get_object(&client, &bucket_name, &key).await?; //#TODO think about adding a parameter because can be expensive
    let data_length = data.content_length().unwrap_or(0) as u64;
    if file_size != data_length {
        return Err(AwsStorageError::UnexpectedError(Report::msg(
            "Source and target data sizes after upload don't match",
        )));
    }
    Ok(())
}
