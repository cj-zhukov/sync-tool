use std::fmt::Debug;

use anyhow::Context;
use tokio::fs;
use serde::Deserialize;
use serde_json;

#[derive(Deserialize, Debug)]
pub struct Config {
    pub bucket: String,     // AWS S3 bucket name
    pub region: String,     // target bucket region
    pub source: String,     // path to source local folder
    pub target: String,     // path to target folder in AWS S3
    pub workers: usize,     // how much files upload in async
    pub chunk_size: usize,  // if file is less, then will be uploaded by chunks
    pub max_chunks: usize,  // for big files
}

impl Config {
    pub async fn new(file_path: &str) -> anyhow::Result<Config>  {
        let contents = fs::read_to_string(file_path).await.context(format!("could not read file: {}", file_path))?;
        let config: Config = serde_json::from_str(contents.as_str()).context("could not get config")?;

        Ok(config)
    }
}

impl std::fmt::Display for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Config info: bucket={} region={} source={} target={} workers={} chunk_size={} max_chunks={}\n", 
        self.bucket,
        self.region,
        self.source,
        self.target,
        self.workers,
        self.chunk_size,
        self.max_chunks,
        )
    }
 }