use color_eyre::eyre::Report;
use std::io::Error as IOError;
use thiserror::Error;
use tokio::sync::AcquireError;
use tokio::task::JoinError;

#[derive(Error, Debug)]
pub enum UtilsError {
    #[error("io error")]
    IOError(#[from] IOError),

    #[error("tokio join error")]
    TokioJoinError(#[from] JoinError),

    #[error("tokio acquire semaphore error")]
    AcquireError(#[from] AcquireError),

    #[error("unexpected error")]
    UnexpectedError(#[source] Report),
}
