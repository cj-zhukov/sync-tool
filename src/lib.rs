pub mod config;
pub use config::Config;

pub mod error;
pub use error::{Result, Error};

use std::{collections::HashMap, path::Path, ffi::OsStr};

use aws_sdk_s3::{
    Client,
    types::{CompletedMultipartUpload, CompletedPart},
    operation::{create_multipart_upload::CreateMultipartUploadOutput, get_object::GetObjectOutput},
};
use aws_config::{BehaviorVersion, Region};
use aws_smithy_types::byte_stream::{ByteStream, Length};
use tokio::{fs, task::JoinSet};
use async_walkdir::{Filtering, WalkDir};
use async_recursion::async_recursion;
use tokio_stream::StreamExt;

pub const AWS_MAX_RETRIES: u32 = 10;
pub const CONFIG_NAME: &str = "sync-tool.json";
pub const DEFAULT_MODE: &str = "dif";

pub enum Mode {
    Dif,    // check dif in file name and size
    Upload, // upload files without checking target
    Sync, // check file name and size and upload
    Show, // print source and target files
}

impl Mode {
    pub fn new(mode: &str) -> Option<Self> {
        match mode {
            "dif" | "Dif" | "DIF" => Some(Self::Dif),
            "upload" | "Upload" | "UPLOAD" => Some(Self::Upload),
            "sync" | "Sync" | "SYNC" => Some(Self::Sync),
            "show" | "Show" | "SHOW" => Some(Self::Show),
            _ => { 
                println!("unknown mode provided: {} valid: dif, upload, sync, show", mode);
                None 
            }
        }
    }

    pub fn value(&self) -> &str {
        match *self {
            Self::Dif => "dif",
            Self::Upload => "upload",
            Self::Sync => "sync",
            Self::Show => "show",
        } 
    }
}

pub async fn get_aws_client(region: &str) -> Client {
    let config = aws_config::defaults(BehaviorVersion::v2023_11_09())
        .region(Region::new(region.to_string()))
        .load()
        .await;

    Client::from_conf(
        aws_sdk_s3::config::Builder::from(&config)
            .retry_config(aws_config::retry::RetryConfig::standard()
            .with_max_attempts(AWS_MAX_RETRIES))
            .build()
    )
}

async fn get_object(client: &Client, bucket_name: &str, key: &str) -> Result<GetObjectOutput> {
    Ok(client
        .get_object()
        .bucket(bucket_name)
        .key(key)
        .send()
        .await?
    )
}

pub async fn dif(client: Client, config: Config) -> Result<()> {
	let source_task = tokio::spawn(files_walker(config.source.clone()));
    let target_task = tokio::spawn(list_keys_stream(client.clone(), config.bucket.clone(), config.target.clone()));
    let (source, target) = (source_task.await??, target_task.await??);

    let dif = dif_calc(&config, &source, &target);
    match dif {
        Some(dif) => println!("dif found: {:?}", dif),
        None => println!("no dif found"),
    };

    Ok(())
}

pub fn dif_calc(config: &Config, source: &HashMap<String, i64>, target: &HashMap<String, i64>) -> Option<HashMap<String, i64>> {
    let mut dif: HashMap<String, i64> = HashMap::new();
    for (k, v) in source.iter() {
        let source_f_name = k.strip_prefix(&config.source).unwrap_or("no_file_name").to_string();
        let source_f_size = *v;
        let target_f_name = format!("{}{}", &config.target, &source_f_name);
        match target.get(&target_f_name) {
            Some(target_f_size) => {
                if  source_f_size != *target_f_size {
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

pub async fn files_walker<P>(path: P) -> Result<HashMap<String, i64>>
where P: AsRef<Path> + std::marker::Send + std::marker::Sync + std::fmt::Debug,  
    {
    #[async_recursion]
    async fn files_walker_inner<P>(path: P, files: &mut HashMap<String, i64>) -> Result<()> 
    where P: AsRef<Path> + std::marker::Send + std::marker::Sync + std::fmt::Debug,
    {
        let mut entries = fs::read_dir(&path).await?;
        while let Some(entry) = entries.next_entry().await? {
            if entry.path().is_file() {
                if entry.path().file_name().unwrap_or(OsStr::new("no_file_name")).to_string_lossy().starts_with(".DS_Store") {
                    continue;
                }
                let file_name = entry.path().to_string_lossy().to_string();             
                let file_name = sanitize_file_path(&file_name); // sanitize file_name for windows only onces here
                let file_size = entry.metadata().await?.len() as i64;
                files.insert(file_name, file_size);
            } else if entry.path().is_dir() && !entry.file_name().to_string_lossy().starts_with(".DS_Store") {
                files_walker_inner(entry.path(), files).await?;
            }
        }

        Ok(())
    }

    let mut files: HashMap<String, i64> = HashMap::new();
    files_walker_inner(path, &mut files).await?;

    Ok(files)
}

pub async fn list_keys_stream(client: Client, bucket: String, prefix: String) -> Result<HashMap<String, i64>> {
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

pub async fn show(client: Client, config: Config) -> Result<()> {
    let source_task = tokio::spawn(files_walker(config.source));
    let target_task = tokio::spawn(list_keys_stream(client, config.bucket, config.target));
    let (source, target) = (source_task.await??, target_task.await??);
    println!("source: {:?}", source);
    println!("target: {:?}", target);

    Ok(())
}

pub async fn upload(client: Client, config: Config) -> Result<()> {
    let mut entries = WalkDir::new(&config.source).filter(|entry| async move {
        if let Some(true) = entry
            .path()
            .file_name()
            .map(|f| f.to_string_lossy().starts_with(".DS_Store")) {
                return Filtering::IgnoreDir;
        }
        Filtering::Continue
    });

    let mut tasks = JoinSet::new();
    let mut outputs = Vec::new();
    let chunk_size = config.chunk_size * 1024 * 1024; // MB
    while let Some(entry) = entries.next().await.transpose()? {
        if let Ok(file) = entry.file_type().await {
            if file.is_file() {
                let file_name = entry.path().to_string_lossy().to_string();             
                let file_name = sanitize_file_path(&file_name); // sanitize file_name for windows only onces here
                let file_size = entry.metadata().await?.len();
                let file_name = Path::new(&file_name).strip_prefix(&config.source)?;
                let file_name = file_name.to_string_lossy().to_string();
                let key = format!("{}{}", &config.target, &file_name);
                let f_name = format!("{}{}", &config.source, &file_name);
    
                if file_size < chunk_size as u64 {
                    tasks.spawn(upload_object(client.clone(), config.bucket.clone(), f_name, key, file_size))
                } else {
                    tasks.spawn(upload_object_multipart(client.clone(), config.bucket.clone(), f_name, key, file_size, chunk_size as u64, config.max_chunks as u64))
                };
    
                if tasks.len() == config.workers {
                    outputs.push(tasks.join_next().await);
                }
            }
        }        
    }
    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(res) => match res {
                Ok(_) => (),
                Err(e) => println!("could not upload object: {}", e),
            }
            Err(e) => println!("could not run task: {}", e),
        }
    }

    Ok(())
}

pub async fn sync(client: Client, config: Config) -> Result<()> {
    let target = list_keys_stream(client.clone(), config.bucket.clone(), config.target.clone()).await?;

    let mut entries = WalkDir::new(&config.source).filter(|entry| async move {
        if let Some(true) = entry
            .path()
            .file_name()
            .map(|f| f.to_string_lossy().starts_with(".DS_Store")) {
                return Filtering::IgnoreDir;
        }
        Filtering::Continue
    });

    let mut tasks = JoinSet::new();
    let mut outputs = Vec::new();
    let chunk_size = config.chunk_size * 1024 * 1024; // MiB
    while let Some(entry) = entries.next().await.transpose()? {
        if let Ok(file) = entry.file_type().await {
            if file.is_file() {
                let file_name = entry.path().to_string_lossy().to_string();             
                let file_name = sanitize_file_path(&file_name); // sanitize file_name for windows only onces here
                let source_file_size = entry.metadata().await?.len();
                let source_file_name = Path::new(&file_name).strip_prefix(&config.source)?;
                let source_file_name = source_file_name.to_string_lossy().to_string();
                let target_file_name = format!("{}{}", &config.target, &source_file_name);
                let target_file_size = target.get(&target_file_name).unwrap_or(&0);

                if source_file_size != *target_file_size as u64 {
                    // dif found 
                    if source_file_size < chunk_size as u64 {
                        tasks.spawn(upload_object(client.clone(), config.bucket.clone(), file_name, target_file_name, source_file_size))
                    } else {
                        tasks.spawn(upload_object_multipart(client.clone(), config.bucket.clone(), file_name, target_file_name, source_file_size, chunk_size as u64, config.max_chunks as u64))
                    };
        
                    if tasks.len() == config.workers {
                        outputs.push(tasks.join_next().await);
                    }
                }
            }
        }        
    }
    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(res) => match res {
                Ok(_) => (),
                Err(e) => println!("could not upload object: {}", e),
            }
            Err(e) => println!("could not run task: {}", e),
        }
    }

    Ok(())
}

async fn upload_object(client: Client, bucket_name: String, file_name: String, key: String, file_size: u64) -> Result<()> {
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

    let data: GetObjectOutput = get_object(&client, &bucket_name, &key).await?;
    let data_length = data.content_length().unwrap_or(0) as u64;
    if file_size == data_length {
        // println!("Data lengths match");
    } else {
        return Err(Error::Custom("Failed checking data size after upload".into()));
    }

    Ok(())
}

async fn upload_object_multipart(client: Client, bucket_name: String, file_name: String, key: String, file_size: u64, chunk_size: u64, max_chunks: u64) -> Result<()> {
    println!("Uploading file: {}", file_name);

    let multipart_upload_res: CreateMultipartUploadOutput = client
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
        return Err(Error::Custom(format!("Bad file size for: {}", file_name)));
    }
    if chunk_count > max_chunks {
        return Err(Error::Custom(format!("Too many chunks file: {}. Try increasing your chunk size", file_name)));
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

    let completed_multipart_upload: CompletedMultipartUpload = CompletedMultipartUpload::builder()
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
    if file_size == data_length {
        // println!("Data lengths match.");
    } else {
        return Err(Error::Custom("Failed checking data size after upload".into()));
    }

    Ok(())
}

fn sanitize_file_path(path: &str) -> String {
    path
        .replace('\\', "/")
}