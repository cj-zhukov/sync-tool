use color_eyre::eyre::Report;
use thiserror::Error;

use crate::cloud_storage::error::AwsStorageError;
use crate::utils::error::UtilsError;

#[derive(Error, Debug)]
pub enum SyncToolError {
    #[error("aws storage error")]
    AwsStorageError(#[from] AwsStorageError),

    #[error("utils error")]
    UtilsError(#[from] UtilsError),

    #[error("unexpected error")]
    UnexpectedError(#[source] Report),
}
