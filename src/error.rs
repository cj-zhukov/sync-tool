use color_eyre::eyre::Report;
use thiserror::Error;

use crate::cloud_storage::error::AwsStorageError;
use crate::cloud_storage::path_conversions::PathConversionsError;
use crate::utils::error::UtilsError;

#[derive(Error, Debug)]
pub enum SyncToolError {
    #[error("aws storage error")]
    AwsStorageError(#[from] AwsStorageError),

    #[error("path conversions error")]
    PathConversionsError(#[from] PathConversionsError),

    #[error("utils error")]
    UtilsError(#[from] UtilsError),

    #[error("unexpected error")]
    UnexpectedError(#[source] Report),
}
