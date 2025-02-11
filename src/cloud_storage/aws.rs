use async_walkdir::{Filtering, WalkDir};
use aws_sdk_s3::{
    operation::get_object::GetObjectOutput,
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart},
    Client,
};
use aws_smithy_types::byte_stream::Length;
use color_eyre::Report;
use std::{collections::HashMap, path::Path};
use tokio::task::JoinSet;
use tokio_stream::StreamExt;

use crate::{
    domain::CloudStorage,
    utils::{
        config::AppConfig, constants::FILES_TO_IGNORE, error::UtilsError, tools::{files_walker, sanitize_file_path}
    },
    SyncToolError,
};

use super::error::AwsStorageError;

pub struct AwsStorage {
    client: Client,
}

impl AwsStorage {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

#[async_trait::async_trait]
impl CloudStorage for AwsStorage {
    /// Calculate and show dif
    async fn dif(&self, config: &AppConfig) -> Result<(), SyncToolError> {
        let source_task = tokio::spawn(files_walker(config.source.clone()));
        let target_task = tokio::spawn(list_keys_stream(
            self.client.clone(),
            config.bucket.clone(),
            config.target.clone(),
        ));
        let (source, target) = (source_task.await.map_err(|e| UtilsError::TokioJoinError(e))??, target_task.await.map_err(|e| UtilsError::TokioJoinError(e))??);

        let dif = dif_calc(&config, &source, &target);
        match dif {
            Some(dif) => println!("dif found: {:?}", dif),
            None => println!("no dif found"),
        };
        Ok(())
    }

    /// Show files from source and target folders
    async fn show(&self, config: &AppConfig) -> Result<(), SyncToolError> {
        let source_task = tokio::spawn(files_walker(config.source.clone()));
        let target_task = tokio::spawn(list_keys_stream(
            self.client.clone(),
            config.bucket.clone(),
            config.target.clone(),
        ));
        let (source, target) = (source_task.await.map_err(|e| UtilsError::TokioJoinError(e))??, target_task.await.map_err(|e| UtilsError::TokioJoinError(e))??);
        println!("source: {:?}", source);
        println!("target: {:?}", target);
        Ok(())
    }

    /// Upload files from source to target without checking file name and size
    async fn upload(&self, config: &AppConfig) -> Result<(), SyncToolError> {
        let mut entries = WalkDir::new(&config.source).filter(|entry| async move {
            if let Some(true) = entry
                .path()
                .file_name()
                .map(|file| FILES_TO_IGNORE.iter().any(|f| file.to_string_lossy() == *f))
            {
                return Filtering::IgnoreDir;
            }
            Filtering::Continue
        });

        let mut tasks = JoinSet::new();
        let chunk_size = config.chunk_size * 1024 * 1024; // MB

        while let Some(entry) = entries.next().await.transpose().map_err(|e| UtilsError::IOError(e))? {
            if let Ok(file) = entry.file_type().await {
                if file.is_file() {
                    let file_name = entry.path().to_string_lossy().to_string();
                    let file_name = sanitize_file_path(&file_name); // sanitize file_name for windows only onces here
                    let file_size = entry.metadata().await.map_err(|e| UtilsError::IOError(e))?.len();
                    let file_name = Path::new(&file_name).strip_prefix(&config.source).map_err(|e| UtilsError::StripPrefixError(e))?;
                    let file_name = file_name.to_string_lossy().to_string();
                    let key = format!("{}{}", &config.target, &file_name);
                    let f_name = format!("{}{}", &config.source, &file_name);

                    if file_size < chunk_size as u64 {
                        tasks.spawn(upload_object(
                            self.client.clone(),
                            config.bucket.clone(),
                            f_name,
                            key,
                            file_size,
                        ))
                    } else {
                        tasks.spawn(upload_object_multipart(
                            self.client.clone(),
                            config.bucket.clone(),
                            f_name,
                            key,
                            file_size,
                            chunk_size as u64,
                            config.max_chunks as u64,
                        ))
                    };

                    if tasks.len() == config.workers {
                        if let Some(res) = tasks.join_next().await {
                            match res {
                                Ok(res) => {
                                    if let Err(e) = res {
                                        println!("could not upload object: {}", e);
                                    }
                                }
                                Err(e) => println!("failed running task: {}", e),
                            }
                        }
                    }
                }
            }
        }
        while let Some(res) = tasks.join_next().await {
            match res {
                Ok(res) => {
                    if let Err(e) = res {
                        println!("could not upload object: {}", e);
                    }
                }
                Err(e) => println!("could not run task: {}", e),
            }
        }
        Ok(())
    }

    /// Sync files from source to target with checking file name and size
    async fn sync(&self, config: &AppConfig) -> Result<(), SyncToolError> {
        let target = list_keys_stream(
            self.client.clone(),
            config.bucket.clone(),
            config.target.clone(),
        )
        .await?;

        let mut entries = WalkDir::new(&config.source).filter(|entry| async move {
            if let Some(true) = entry
                .path()
                .file_name()
                .map(|file| FILES_TO_IGNORE.iter().any(|f| file.to_string_lossy() == *f))
            {
                return Filtering::IgnoreDir;
            }
            Filtering::Continue
        });

        let mut tasks = JoinSet::new();
        let chunk_size = config.chunk_size * 1024 * 1024; // MiB

        while let Some(entry) = entries.next().await.transpose().map_err(|e| UtilsError::IOError(e))? {
            if let Ok(file) = entry.file_type().await {
                if file.is_file() {
                    let file_name = entry.path().to_string_lossy().to_string();
                    let file_name = sanitize_file_path(&file_name); // sanitize file_name for windows only onces here
                    let source_file_size = entry.metadata().await.map_err(|e| UtilsError::IOError(e))?.len();
                    let source_file_name = Path::new(&file_name).strip_prefix(&config.source).map_err(|e| UtilsError::StripPrefixError(e))?;
                    let source_file_name = source_file_name.to_string_lossy().to_string();
                    let target_file_name = format!("{}{}", &config.target, &source_file_name);
                    let target_file_size = target.get(&target_file_name).unwrap_or(&0);

                    if source_file_size != *target_file_size as u64 {
                        // dif found
                        if source_file_size < chunk_size as u64 {
                            tasks.spawn(upload_object(
                                self.client.clone(),
                                config.bucket.clone(),
                                file_name,
                                target_file_name,
                                source_file_size,
                            ))
                        } else {
                            tasks.spawn(upload_object_multipart(
                                self.client.clone(),
                                config.bucket.clone(),
                                file_name,
                                target_file_name,
                                source_file_size,
                                chunk_size as u64,
                                config.max_chunks as u64,
                            ))
                        };

                        if tasks.len() == config.workers {
                            if let Some(res) = tasks.join_next().await {
                                match res {
                                    Ok(res) => {
                                        if let Err(e) = res {
                                            println!("could not upload object: {}", e);
                                        }
                                    }
                                    Err(e) => println!("failed running task: {}", e),
                                }
                            }
                        }
                    }
                }
            }
        }
        while let Some(res) = tasks.join_next().await {
            match res {
                Ok(res) => {
                    if let Err(e) = res {
                        println!("could not upload object: {}", e);
                    }
                }
                Err(e) => println!("could not run task: {}", e),
            }
        }
        Ok(())
    }
}

fn dif_calc(
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

async fn get_object(
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

async fn list_keys_stream(
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

async fn upload_object(
    client: Client,
    bucket_name: String,
    file_name: String,
    key: String,
    file_size: u64,
) -> Result<(), AwsStorageError> {
    let body = ByteStream::from_path(Path::new(&file_name)).await?;
    println!("Uploading file: {}", file_name);

    client
        .put_object()
        .bucket(&bucket_name)
        .key(&key)
        .body(body)
        .send()
        .await?;

    println!("Uploaded file: {}", file_name);

    let data = get_object(&client, &bucket_name, &key).await?;
    let data_length = data.content_length().unwrap_or(0) as u64;
    if file_size != data_length {
        return Err(AwsStorageError::UnexpectedError(Report::msg(
            "Source and target data sizes after upload don't match",
        )));
    }

    Ok(())
}

async fn upload_object_multipart(
    client: Client,
    bucket_name: String,
    file_name: String,
    key: String,
    file_size: u64,
    chunk_size: u64,
    max_chunks: u64,
) -> Result<(), AwsStorageError> {
    println!("Uploading file: {}", file_name);

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

    println!("Uploaded file: {}", file_name);

    let data: GetObjectOutput = get_object(&client, &bucket_name, &key).await?;
    let data_length = data.content_length().unwrap_or(0) as u64;
    if file_size != data_length {
        return Err(AwsStorageError::UnexpectedError(Report::msg(
            "Source and target data sizes after upload don't match",
        )));
    }
    Ok(())
}
