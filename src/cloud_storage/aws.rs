use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use async_walkdir::{Filtering, WalkDir};
use aws_sdk_s3::Client;
use log::{error, info};
use indicatif::{ProgressBar, ProgressStyle};
use std::path::Path;
use tokio::task::JoinSet;
use tokio_stream::StreamExt;

use crate::cloud_storage::error::AwsStorageError;
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
            source_task
                .await
                .map_err(|e| UtilsError::TokioJoinError(e))??,
            target_task
                .await
                .map_err(|e| UtilsError::TokioJoinError(e))??,
        );

        let dif = dif_calc(&config, &source, &target);
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
            source_task
                .await
                .map_err(|e| UtilsError::TokioJoinError(e))??,
            target_task
                .await
                .map_err(|e| UtilsError::TokioJoinError(e))??,
        );
        info!("source: {:?}", source);
        info!("target: {:?}", target);
        Ok(())
    }

    /// Upload files from source to target without checking file name and size
    async fn upload(&self, config: &AppConfig) -> Result<(), SyncToolError> {
        let mut entries = WalkDir::new(&config.source).filter(|entry| async move {
            entry
                .path()
                .file_name()
                .map(|file| FILES_TO_IGNORE.iter().any(|f| file.to_string_lossy() == *f))
                .unwrap_or(false)
                .then_some(Filtering::IgnoreDir)
                .unwrap_or(Filtering::Continue)
        });

        let uploaded_files = Arc::new(AtomicU64::new(0));
        let uploaded_bytes = Arc::new(AtomicU64::new(0));
        let pb = Arc::new(ProgressBar::new_spinner());
        pb.set_style(ProgressStyle::with_template(
            "{spinner:.green} [{elapsed}] {msg}",
        ).map_err(|e| AwsStorageError::TemplateError(e))?);
        pb.enable_steady_tick(Duration::from_millis(100));
        let mut tasks = JoinSet::new();
        let chunk_size = config.chunk_size * 1024 * 1024; // MB

        while let Some(entry) = entries
            .next()
            .await
            .transpose()
            .map_err(|e| UtilsError::IOError(e))?
        {
            if entry
                .file_type()
                .await
                .map_err(|e| UtilsError::IOError(e))?
                .is_file()
            {
                let file_name = entry.path().to_string_lossy().to_string();
                let file_name = sanitize_file_path(&file_name); // sanitize file_name for windows only onces here
                let file_size = entry
                    .metadata()
                    .await
                    .map_err(|e| UtilsError::IOError(e))?
                    .len();
                let file_name = Path::new(&file_name)
                    .strip_prefix(&config.source)
                    .map_err(|e| UtilsError::StripPrefixError(e))?;
                let file_name = file_name.to_string_lossy().to_string();
                let key = format!("{}{}", &config.target, &file_name);
                let f_name = format!("{}{}", &config.source, &file_name);

                let task_info = UploadTaskInfo {
                    client: self.client.clone(),
                    bucket: config.bucket.clone(),
                    local_path: f_name,
                    s3_key: key,
                    size: file_size,
                    chunk_size: chunk_size as u64,
                    max_chunks: config.max_chunks as u64,
                };

                let uploaded_files = Arc::clone(&uploaded_files);
                let uploaded_bytes = Arc::clone(&uploaded_bytes);
                let pb = Arc::clone(&pb);

                if let Err(e) = spawn_upload_task(&mut tasks, task_info, config.workers, uploaded_files, uploaded_bytes, pb).await {
                    error!("Failed to spawn upload task: {:?}", e);
                }
            }
        }

        while let Some(res) = tasks.join_next().await {
            match res {
                Ok(res) => {
                    if let Err(e) = res {
                        error!("Upload task failed: {:?}", e);
                    }
                }
                Err(e) => error!("Task panicked or was cancelled: {:?}", e),
            }
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
            entry
                .path()
                .file_name()
                .map(|file| FILES_TO_IGNORE.iter().any(|f| file.to_string_lossy() == *f))
                .unwrap_or(false)
                .then_some(Filtering::IgnoreDir)
                .unwrap_or(Filtering::Continue)
        });

        let uploaded_files = Arc::new(AtomicU64::new(0));
        let uploaded_bytes = Arc::new(AtomicU64::new(0));
        let pb = Arc::new(ProgressBar::new_spinner());
        pb.set_style(ProgressStyle::with_template(
            "{spinner:.green} [{elapsed}] {msg}",
        ).map_err(|e| AwsStorageError::TemplateError(e))?);
        pb.enable_steady_tick(Duration::from_millis(100));
        let mut tasks = JoinSet::new();
        let chunk_size = config.chunk_size * 1024 * 1024; // MiB

        while let Some(entry) = entries
            .next()
            .await
            .transpose()
            .map_err(|e| UtilsError::IOError(e))?
        {
            if entry
                .file_type()
                .await
                .map_err(|e| UtilsError::IOError(e))?
                .is_file()
            {
                let file_name = entry.path().to_string_lossy().to_string();
                let file_name = sanitize_file_path(&file_name); // sanitize file_name for windows only onces here
                let source_file_size = entry
                    .metadata()
                    .await
                    .map_err(|e| UtilsError::IOError(e))?
                    .len();
                let source_file_name = Path::new(&file_name)
                    .strip_prefix(&config.source)
                    .map_err(|e| UtilsError::StripPrefixError(e))?;
                let source_file_name = source_file_name.to_string_lossy().to_string();
                let target_file_name = format!("{}{}", &config.target, &source_file_name);
                let target_file_size = target.get(&target_file_name).unwrap_or(&0);

                if source_file_size != *target_file_size as u64 {
                    let task_info = UploadTaskInfo {
                        client: self.client.clone(),
                        bucket: config.bucket.clone(),
                        local_path: file_name,
                        s3_key: target_file_name,
                        size: source_file_size,
                        chunk_size: chunk_size as u64,
                        max_chunks: config.max_chunks as u64,
                    };

                    let uploaded_files = Arc::clone(&uploaded_files);
                    let uploaded_bytes = Arc::clone(&uploaded_bytes);
                    let pb = Arc::clone(&pb);

                    if let Err(e) = spawn_upload_task(&mut tasks, task_info, config.workers, uploaded_files, uploaded_bytes, pb).await {
                        error!("Failed to spawn upload task: {:?}", e);
                    }
                }
            }
        }

        while let Some(res) = tasks.join_next().await {
            match res {
                Ok(res) => {
                    if let Err(e) = res {
                        error!("Upload task failed: {:?}", e);
                    }
                }
                Err(e) => error!("Task panicked or was cancelled: {:?}", e),
            }
        }
        Ok(())
    }
}
