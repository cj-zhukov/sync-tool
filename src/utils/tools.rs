use std::{collections::HashMap, ffi::OsStr, path::Path};

use async_recursion::async_recursion;
use tokio::fs;

use crate::{utils::constants::FILES_TO_IGNORE, utils::error::UtilsError};

/// Prepare Windows path
pub fn sanitize_file_path(path: &str) -> String {
    path.replace('\\', "/")
}

/// Get local files names and size
pub async fn files_walker<P>(path: P) -> Result<HashMap<String, i64>, UtilsError>
where
    P: AsRef<Path> + std::marker::Send + std::marker::Sync + std::fmt::Debug,
{
    #[async_recursion]
    async fn files_walker_inner<P>(
        path: P,
        files: &mut HashMap<String, i64>,
    ) -> Result<(), UtilsError>
    where
        P: AsRef<Path> + std::marker::Send + std::marker::Sync + std::fmt::Debug,
    {
        let mut entries = fs::read_dir(&path).await?;
        while let Some(entry) = entries.next_entry().await? {
            if entry.path().is_file() {
                if FILES_TO_IGNORE.iter().any(|f| {
                    entry
                        .path()
                        .file_name()
                        .unwrap_or(OsStr::new("no_file_name"))
                        .to_string_lossy()
                        == *f
                }) {
                    continue;
                }

                let file_name = entry.path().to_string_lossy().to_string();
                let file_name = sanitize_file_path(&file_name); // sanitize file_name for windows only onces here
                let file_size = entry.metadata().await?.len() as i64;
                files.insert(file_name, file_size);
            } else if entry.path().is_dir()
            // && !entry.file_name().to_string_lossy().starts_with(".DS_Store") // #TODO seperate files and folders to ignore?
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
