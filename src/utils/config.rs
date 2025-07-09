use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub struct AppConfig {
    pub bucket: String,                       // target bucket for upload
    pub region: String,                       // region for upload
    pub source: String,                       // data source path
    pub target: String,                       // data source target, will be created if not exists
    pub workers: usize,                       // count files for uploading in parallel
    pub chunk_size: usize, // if less then whole file will be uploaded, else by chunk
    pub max_chunks: usize, // count of chunks for file, describes max file to be uploaded
    pub files_to_ignore: Option<Vec<String>>, // these files will be ignored for uploading
    pub retries: usize,    // how many times try to reupload file
    pub chunk_retries: usize, // how many times try to reupload chunk per file
    pub chunk_workers: usize, // how many chunks of file to upload at once per file
}
