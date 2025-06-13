use std::path::Path;
use std::sync::Arc;

use async_walkdir::{Filtering, WalkDir};
use aws_sdk_s3::Client;
use log::{error, info};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_stream::StreamExt;

use crate::{
    cloud_storage::{
        path_conversions::{make_s3_key, normalize_path},
        tools::{list_keys, spawn_upload_task, UploadTaskInfo},
    },
    domain::CloudStorage,
    utils::{config::AppConfig, constants::FILES_TO_IGNORE, error::UtilsError},
    SyncToolError,
};

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
    /// Calculate and show dif (dry-run)
    async fn dif(&self, config: &AppConfig) -> Result<(), SyncToolError> {
        let target = list_keys(&self.client, &config.bucket, &config.target).await?;

        let mut entries = WalkDir::new(&config.source).filter(|entry| async move {
            if let Some(true) = entry.path().file_name().map(|f| {
                FILES_TO_IGNORE
                    .iter()
                    .any(|ignore| f.to_string_lossy() == *ignore)
            }) {
                return Filtering::IgnoreDir;
            }
            Filtering::Continue
        });

        let mut files_to_upload = Vec::new();

        while let Some(entry) = entries
            .next()
            .await
            .transpose()
            .map_err(UtilsError::IOError)?
        {
            if entry
                .file_type()
                .await
                .map_err(UtilsError::IOError)?
                .is_file()
            {
                let full_path = entry.path();
                let source_file_size = entry.metadata().await.map_err(UtilsError::IOError)?.len();
                let target_file_name =
                    make_s3_key(Path::new(&config.source), &full_path, &config.target)?;
                let target_file_size = target.get(&target_file_name).unwrap_or(&0);
                if source_file_size != *target_file_size as u64 {
                    files_to_upload.push((normalize_path(full_path), source_file_size));
                }
            }
        }

        if files_to_upload.is_empty() {
            info!("All files are already synced. No uploads needed.");
        } else {
            for file in files_to_upload.iter() {
                let file_name = &file.0;
                let bytes_mb = file.1 as f64 / (1024.0 * 1024.0);
                info!("Found file to upload: {file_name} | {bytes_mb:.2} MB");
            }
            let files_to_upload_count = files_to_upload.len();
            let bytes_to_upload = files_to_upload.iter().fold(0, |acc, file| acc +  file.1);
            let bytes_to_upload = bytes_to_upload as f64 / (1024.0 * 1024.0);
            info!("Found to upload: {files_to_upload_count} files | {bytes_to_upload:.2} MB");
        }
        Ok(())
    }

    /// Upload files from source to target without checking file name and size,
    /// so target files if exist will be overwritten
    async fn upload(&self, config: &AppConfig) -> Result<(), SyncToolError> {
        let mut entries = WalkDir::new(&config.source).filter(|entry| async move {
            if let Some(true) = entry.path().file_name().map(|f| {
                FILES_TO_IGNORE
                    .iter()
                    .any(|ignore| f.to_string_lossy() == *ignore)
            }) {
                return Filtering::IgnoreDir;
            }
            Filtering::Continue
        });

        let mut tasks = JoinSet::new();
        let chunk_size = config.chunk_size * 1024 * 1024; // MiB
        let sem = Arc::new(Semaphore::new(config.workers));

        while let Some(entry) = entries
            .next()
            .await
            .transpose()
            .map_err(UtilsError::IOError)?
        {
            if entry
                .file_type()
                .await
                .map_err(UtilsError::IOError)?
                .is_file()
            {
                let full_path = entry.path();
                let source_file_size = entry.metadata().await.map_err(UtilsError::IOError)?.len();
                let target_file_name =
                    make_s3_key(Path::new(&config.source), &full_path, &config.target)?;

                let task_info = UploadTaskInfo {
                    client: self.client.clone(),
                    bucket: config.bucket.clone(),
                    local_path: full_path.clone(),
                    s3_key: target_file_name,
                    size: source_file_size,
                    chunk_size: chunk_size as u64,
                    max_chunks: config.max_chunks as u64,
                };

                let permit = Arc::clone(&sem)
                    .acquire_owned()
                    .await
                    .map_err(|e| SyncToolError::UnexpectedError(e.into()))?;

                tasks.spawn(async move {
                    let _permit = permit;
                    match spawn_upload_task(task_info).await {
                        Ok(_) => Ok(source_file_size),
                        Err(e) => Err(e),
                    }
                    .map(|bytes| (normalize_path(&full_path), bytes))
                    .map_err(|err| (normalize_path(&full_path), err))
                });
            }
        }

        let mut failed_uploads: Vec<String> = Vec::new();
        let mut uploaded_files_count: u64 = 0;
        let mut uploaded_bytes_total: u64 = 0;

        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(upload_result) => match upload_result {
                    Ok((_file_name, bytes_uploaded)) => {
                        uploaded_files_count += 1;
                        uploaded_bytes_total += bytes_uploaded;
                    }
                    Err((file_name, error)) => {
                        error!("Failed to upload: {} : {:?}", file_name, error);
                        failed_uploads.push(file_name);
                    }
                },

                Err(join_error) => {
                    error!("A task failed unexpectedly: {:?}", join_error);
                }
            }
        }

        if uploaded_files_count > 0 {
            let bytes_mb = uploaded_bytes_total as f64 / (1024.0 * 1024.0);
            info!("Uploaded: {uploaded_files_count} files | {bytes_mb:.2} MB");
        }

        if !failed_uploads.is_empty() {
            error!("Failed to upload: {} files", failed_uploads.len());
            for file in failed_uploads.iter() {
                error!("Failed to upload file: {}", file);
            }
        } else if uploaded_files_count == 0 {
            info!("All files are already synced. No uploads needed.");
        }
        Ok(())
    }

    /// Sync files between source and target using file name and size
    async fn sync(&self, config: &AppConfig) -> Result<(), SyncToolError> {
        let target = list_keys(&self.client, &config.bucket, &config.target).await?;

        let mut entries = WalkDir::new(&config.source).filter(|entry| async move {
            if let Some(true) = entry.path().file_name().map(|f| {
                FILES_TO_IGNORE
                    .iter()
                    .any(|ignore| f.to_string_lossy() == *ignore)
            }) {
                return Filtering::IgnoreDir;
            }
            Filtering::Continue
        });

        let mut tasks = JoinSet::new();
        let chunk_size = config.chunk_size * 1024 * 1024; // MiB
        let sem = Arc::new(Semaphore::new(config.workers));

        while let Some(entry) = entries
            .next()
            .await
            .transpose()
            .map_err(UtilsError::IOError)?
        {
            if entry
                .file_type()
                .await
                .map_err(UtilsError::IOError)?
                .is_file()
            {
                let full_path = entry.path();
                let source_file_size = entry.metadata().await.map_err(UtilsError::IOError)?.len();
                let target_file_name =
                    make_s3_key(Path::new(&config.source), &full_path, &config.target)?;
                let target_file_size = target.get(&target_file_name).unwrap_or(&0);

                if source_file_size != *target_file_size as u64 {
                    let task_info = UploadTaskInfo {
                        client: self.client.clone(),
                        bucket: config.bucket.clone(),
                        local_path: full_path.clone(),
                        s3_key: target_file_name,
                        size: source_file_size,
                        chunk_size: chunk_size as u64,
                        max_chunks: config.max_chunks as u64,
                    };

                    let permit = Arc::clone(&sem)
                        .acquire_owned()
                        .await
                        .map_err(|e| SyncToolError::UnexpectedError(e.into()))?;

                    tasks.spawn(async move {
                        let _permit = permit;
                        match spawn_upload_task(task_info).await {
                            Ok(_) => Ok(source_file_size),
                            Err(e) => Err(e),
                        }
                        .map(|bytes| (normalize_path(&full_path), bytes))
                        .map_err(|err| (normalize_path(&full_path), err))
                    });
                }
            }
        }

        let mut failed_uploads: Vec<String> = Vec::new();
        let mut uploaded_files_count: u64 = 0;
        let mut uploaded_bytes_total: u64 = 0;

        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(upload_result) => match upload_result {
                    Ok((_file_name, bytes_uploaded)) => {
                        uploaded_files_count += 1;
                        uploaded_bytes_total += bytes_uploaded;
                    }
                    Err((file_name, error)) => {
                        error!("Failed to upload: {} : {:?}", file_name, error);
                        failed_uploads.push(file_name);
                    }
                },

                Err(join_error) => {
                    error!("A task failed unexpectedly: {:?}", join_error);
                }
            }
        }

        if uploaded_files_count > 0 {
            let bytes_mb = uploaded_bytes_total as f64 / (1024.0 * 1024.0);
            info!("Uploaded: {uploaded_files_count} files | {bytes_mb:.2} MB");
        }

        if !failed_uploads.is_empty() {
            error!("Failed to upload: {} files", failed_uploads.len());
            for file in failed_uploads.iter() {
                error!("Failed to upload file: {}", file);
            }
        } else if uploaded_files_count == 0 {
            info!("All files are already synced. No uploads needed.");
        }
        Ok(())
    }
}
