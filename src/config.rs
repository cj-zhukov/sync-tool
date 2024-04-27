use crate::Result;

use std::fmt::Debug;

use tokio::fs;
use serde::Deserialize;
use serde_json;

#[derive(Deserialize, Debug)]
pub struct Config {
    pub bucket: String,
    pub region: String,
    pub source: String,
    pub target: String,
    pub workers: usize,
    pub chunk_size: usize, 
    pub max_chunks: usize,
}

impl Config {
    pub async fn new(file_path: &str) -> Result<Config>  {
        let contents = fs::read_to_string(file_path).await?;
        let config: Config = serde_json::from_str(contents.as_str())?;

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