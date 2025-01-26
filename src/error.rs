use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadError;
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadError;
use aws_sdk_s3::operation::delete_object::DeleteObjectError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_sdk_s3::operation::upload_part::UploadPartError;
use aws_smithy_types::byte_stream::error::Error as AwsSmithyError;
use color_eyre::eyre::Report;
use std::io::Error as IOError;
use std::num::ParseIntError;
use std::path::StripPrefixError;
use thiserror::Error;
use tokio::task::JoinError;
use tokio::sync::AcquireError;

pub type Result<T> = core::result::Result<T, SyncToolError>;

#[derive(Error, Debug)]
pub enum SyncToolError {
    #[error("parse int error")]
    ParseIntError(#[from] ParseIntError),

    #[error("strip prefix error")]
    StripPrefixError(#[from] StripPrefixError),

    #[error("io error")]
    IOError(#[from] IOError),

    #[error("config parse error")]
    ConfigParseError(#[from] serde_json::Error),

    #[error("aws sdk s3 error")]
    AwsSdkS3Error(#[from] aws_sdk_s3::Error),

    #[error("delete object aws sdk error")]
    DeleteObjectError(#[from] SdkError<DeleteObjectError>),

    #[error("list object aws sdk error")]
    ListObjectError(#[from] SdkError<ListObjectsV2Error>),

    #[error("get object aws sdk error")]
    GetObjectError(#[from] SdkError<GetObjectError>),

    #[error("put object aws sdk error")]
    PutObjectError(#[from] SdkError<PutObjectError>),

    #[error("create multipart object aws sdk error")]
    CreateMultipartError(#[from] SdkError<CreateMultipartUploadError>),

    #[error("complete multipart object aws sdk error")]
    CompleteMultipartError(#[from] SdkError<CompleteMultipartUploadError>),

    #[error("upload part object aws sdk error")]
    UploadPartError(#[from] SdkError<UploadPartError>),

    #[error("byte stream aws smithy error")]
    ByteSreamError(#[from] AwsSmithyError),

    #[error("tokio join error")]
    TokioJoinError(#[from] JoinError),

    #[error("tokio acquire semaphore error")]
    AcquireError(#[from] AcquireError),

    #[error("unexpected error")]
    UnexpectedError(#[source] Report),
}
