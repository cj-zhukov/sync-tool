use std::path::Path;
use std::sync::Arc;

use async_walkdir::{Filtering, WalkDir};
use aws_sdk_s3::Client;
use log::{error, info};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_stream::StreamExt;

use crate::{
    cloud_storage::tools::{dif_calc, list_keys, spawn_upload_task, UploadTaskInfo},
    domain::CloudStorage,
    utils::{
        config::AppConfig,
        constants::FILES_TO_IGNORE,
        error::UtilsError,
        tools::{files_walker, sanitize_file_path},
    },
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
    /// Calculate and show dif
    async fn dif(&self, config: &AppConfig) -> Result<(), SyncToolError> {
        let source_task = tokio::spawn(files_walker(config.source.clone()));
        let target_task = tokio::spawn(list_keys(
            self.client.clone(),
            config.bucket.clone(),
            config.target.clone(),
        ));
        let (source, target) = (
            source_task.await.map_err(UtilsError::TokioJoinError)??,
            target_task.await.map_err(UtilsError::TokioJoinError)??,
        );

        let dif = dif_calc(config, &source, &target);
        match dif {
            Some(dif) => info!("dif found: {:?}", dif),
            None => info!("no dif found"),
        };
        Ok(())
    }

    /// Show files from source and target folders
    async fn show(&self, config: &AppConfig) -> Result<(), SyncToolError> {
        let source_task = tokio::spawn(files_walker(config.source.clone()));
        let target_task = tokio::spawn(list_keys(
            self.client.clone(),
            config.bucket.clone(),
            config.target.clone(),
        ));
        let (source, target) = (
            source_task.await.map_err(UtilsError::TokioJoinError)??,
            target_task.await.map_err(UtilsError::TokioJoinError)??,
        );
        info!("source: {:?}", source);
        info!("target: {:?}", target);
        Ok(())
    }

    /// Upload files from source to target without checking file name and size
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
                let file_name = entry.path().to_string_lossy().to_string();
                let file_name = sanitize_file_path(&file_name); // sanitize file_name for windows only onces here
                let source_file_size = entry.metadata().await.map_err(UtilsError::IOError)?.len();
                let source_file_name = Path::new(&file_name)
                    .strip_prefix(&config.source)
                    .map_err(UtilsError::StripPrefixError)?;
                let source_file_name = source_file_name.to_string_lossy().to_string();
                let target_file_name = format!("{}{}", &config.target, &source_file_name);

                let task_info = UploadTaskInfo {
                    client: self.client.clone(),
                    bucket: config.bucket.clone(),
                    local_path: file_name.clone(),
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
                    .map(|bytes| (file_name.clone(), bytes))
                    .map_err(|err| (file_name, err))
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

        if !failed_uploads.is_empty() {
            error!("{} files failed to upload:", failed_uploads.len());
            for file in failed_uploads.iter() {
                error!(" - {}", file);
            }
        } else {
            let bytes_mb = uploaded_bytes_total as f64 / (1024.0 * 1024.0);
            info!("Uploaded: {uploaded_files_count} files | {bytes_mb:.2} MB");
        }
        Ok(())
    }

    /// Sync files from source to target with checking file name and size
    async fn sync(&self, config: &AppConfig) -> Result<(), SyncToolError> {
        let target = list_keys(
            self.client.clone(),
            config.bucket.clone(),
            config.target.clone(),
        )
        .await?;

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
                let file_name = entry.path().to_string_lossy().to_string();
                let file_name = sanitize_file_path(&file_name); // sanitize file_name for windows only onces here
                let source_file_size = entry.metadata().await.map_err(UtilsError::IOError)?.len();
                let source_file_name = Path::new(&file_name)
                    .strip_prefix(&config.source)
                    .map_err(UtilsError::StripPrefixError)?;
                let source_file_name = source_file_name.to_string_lossy().to_string();
                let target_file_name = format!("{}{}", &config.target, &source_file_name);
                let target_file_size = target.get(&target_file_name).unwrap_or(&0);

                if source_file_size != *target_file_size as u64 {
                    let task_info = UploadTaskInfo {
                        client: self.client.clone(),
                        bucket: config.bucket.clone(),
                        local_path: file_name.clone(),
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
                        .map(|bytes| (file_name.clone(), bytes))
                        .map_err(|err| (file_name, err))
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

        let bytes_mb = uploaded_bytes_total as f64 / (1024.0 * 1024.0);
        info!("Uploaded: {uploaded_files_count} files | {bytes_mb:.2} MB");
        if !failed_uploads.is_empty() {
            error!("Failed to upload: {} files", failed_uploads.len());
            for file in failed_uploads.iter() {
                error!("Failed to upload file: {}", file);
            }
        } 
        Ok(())
    }
}
