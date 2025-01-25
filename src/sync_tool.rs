use std::{collections::HashMap, ffi::OsStr, path::Path};

use crate::utils::{
    aws::{list_keys_stream, upload_object, upload_object_multipart},
    config::Config,
    tools::sanitize_file_path,
};
use crate::Result;

use async_recursion::async_recursion;
use async_walkdir::{Filtering, WalkDir};
use aws_sdk_s3::Client;
use tokio::{fs, task::JoinSet};
use tokio_stream::StreamExt;

pub enum Mode {
    Dif,    // check dif in file name and size
    Upload, // upload files without checking target
    Sync,   // check file name and size and upload
    Show,   // print source and target files
}

impl Mode {
    pub fn new(mode: &str) -> Option<Self> {
        match mode {
            "dif" | "Dif" | "DIF" => Some(Self::Dif),
            "upload" | "Upload" | "UPLOAD" => Some(Self::Upload),
            "sync" | "Sync" | "SYNC" => Some(Self::Sync),
            "show" | "Show" | "SHOW" => Some(Self::Show),
            _ => {
                println!(
                    "unknown mode provided: {} valid: dif, upload, sync, show",
                    mode
                );
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

pub async fn dif(client: Client, config: Config) -> Result<()> {
    let source_task = tokio::spawn(files_walker(config.source.clone()));
    let target_task = tokio::spawn(list_keys_stream(
        client.clone(),
        config.bucket.clone(),
        config.target.clone(),
    ));
    let (source, target) = (source_task.await??, target_task.await??);

    let dif = dif_calc(&config, &source, &target);
    match dif {
        Some(dif) => println!("dif found: {:?}", dif),
        None => println!("no dif found"),
    };

    Ok(())
}

pub fn dif_calc(
    config: &Config,
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

pub async fn files_walker<P>(path: P) -> Result<HashMap<String, i64>>
where
    P: AsRef<Path> + std::marker::Send + std::marker::Sync + std::fmt::Debug,
{
    #[async_recursion]
    async fn files_walker_inner<P>(path: P, files: &mut HashMap<String, i64>) -> Result<()>
    where
        P: AsRef<Path> + std::marker::Send + std::marker::Sync + std::fmt::Debug,
    {
        let mut entries = fs::read_dir(&path).await?;
        while let Some(entry) = entries.next_entry().await? {
            if entry.path().is_file() {
                if entry
                    .path()
                    .file_name()
                    .unwrap_or(OsStr::new("no_file_name"))
                    .to_string_lossy()
                    .starts_with(".DS_Store")
                {
                    continue;
                }
                let file_name = entry.path().to_string_lossy().to_string();
                let file_name = sanitize_file_path(&file_name); // sanitize file_name for windows only onces here
                let file_size = entry.metadata().await?.len() as i64;
                files.insert(file_name, file_size);
            } else if entry.path().is_dir()
                && !entry.file_name().to_string_lossy().starts_with(".DS_Store")
            {
                files_walker_inner(entry.path(), files).await?;
            }
        }

        Ok(())
    }

    let mut files: HashMap<String, i64> = HashMap::new();
    files_walker_inner(path, &mut files).await?;

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
            .map(|f| f.to_string_lossy().starts_with(".DS_Store"))
        {
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
                    tasks.spawn(upload_object(
                        client.clone(),
                        config.bucket.clone(),
                        f_name,
                        key,
                        file_size,
                    ))
                } else {
                    tasks.spawn(upload_object_multipart(
                        client.clone(),
                        config.bucket.clone(),
                        f_name,
                        key,
                        file_size,
                        chunk_size as u64,
                        config.max_chunks as u64,
                    ))
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
            },
            Err(e) => println!("could not run task: {}", e),
        }
    }

    Ok(())
}

pub async fn sync(client: Client, config: Config) -> Result<()> {
    let target =
        list_keys_stream(client.clone(), config.bucket.clone(), config.target.clone()).await?;

    let mut entries = WalkDir::new(&config.source).filter(|entry| async move {
        if let Some(true) = entry
            .path()
            .file_name()
            .map(|f| f.to_string_lossy().starts_with(".DS_Store"))
        {
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
                        tasks.spawn(upload_object(
                            client.clone(),
                            config.bucket.clone(),
                            file_name,
                            target_file_name,
                            source_file_size,
                        ))
                    } else {
                        tasks.spawn(upload_object_multipart(
                            client.clone(),
                            config.bucket.clone(),
                            file_name,
                            target_file_name,
                            source_file_size,
                            chunk_size as u64,
                            config.max_chunks as u64,
                        ))
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
            },
            Err(e) => println!("could not run task: {}", e),
        }
    }

    Ok(())
}
