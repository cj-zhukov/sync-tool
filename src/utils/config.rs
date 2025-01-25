use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub struct AppConfig {
    pub bucket: String,
    pub region: String,
    pub source: String,
    pub target: String,
    pub workers: usize,
    pub chunk_size: usize,
    pub max_chunks: usize,
}
