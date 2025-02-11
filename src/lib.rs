pub mod cloud_storage;
pub mod domain;
pub mod error;
pub mod utils;

pub use error::SyncToolError;

/// Mode has 4 options:
/// dif - calculate and show dif in file name and size
/// upload - simple upload files without checking target file names and sizes
/// sync - smart check file name and size and upload
/// show - print source and target files
pub enum Mode {
    Dif,
    Upload,
    Sync,
    Show,
}

impl AsRef<str> for Mode {
    fn as_ref(&self) -> &str {
        match *self {
            Self::Dif => "dif",
            Self::Upload => "upload",
            Self::Sync => "sync",
            Self::Show => "show",
        }
    }
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
}
