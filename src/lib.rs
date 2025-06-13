pub mod cloud_storage;
pub mod domain;
pub mod error;
pub mod utils;

pub use error::SyncToolError;

use log::error;

/// Mode has 3 options:
/// dif - dry-run, calculate and show dif
/// upload - simple upload files without checking target file names and sizes
/// sync - smart check file name and size and upload
pub enum Mode {
    Dif,
    Upload,
    Sync,
}

impl AsRef<str> for Mode {
    fn as_ref(&self) -> &str {
        match *self {
            Self::Dif => "dif",
            Self::Upload => "upload",
            Self::Sync => "sync",
        }
    }
}

impl Mode {
    pub fn new(mode: &str) -> Option<Self> {
        match mode {
            "dif" | "Dif" | "DIF" => Some(Self::Dif),
            "upload" | "Upload" | "UPLOAD" => Some(Self::Upload),
            "sync" | "Sync" | "SYNC" => Some(Self::Sync),
            _ => {
                error!("unknown mode provided: {} valid: dif, upload, sync", mode);
                None
            }
        }
    }
}
