pub type Result<T> = core::result::Result<T, Error>;

use thiserror::Error;
use std::io::Error as IOError;
use std::num::ParseIntError;
use std::path::StripPrefixError;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
use aws_sdk_s3::operation::delete_object::DeleteObjectError;
use aws_sdk_s3::operation::upload_part::UploadPartError;
use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadError;
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_smithy_types::byte_stream::error::Error as AwsSmithyError;
use tokio::task::JoinError;

#[derive(Error, Debug)]
pub enum Error {
    #[error("custom error: `{0}`")]
    Custom(String),

    #[error("parse int error: `{0}`")]
    ParseIntError(#[from] ParseIntError),

    #[error("strip prefix error: `{0}`")]
    StripPrefixError(#[from] StripPrefixError),

    #[error("io error: `{0}`")]
    IOError(#[from] IOError),

    #[error("config parse error: `{0}`")]
    ConfigParseError(#[from] serde_json::Error),

    #[error("aws_sdk_s3 error: `{0}`")]
    AwsSdkS3Error(#[from] aws_sdk_s3::Error),

    #[error("delete object error: {0}")]
    DeleteObjectError(#[from] SdkError<DeleteObjectError>),

    #[error("listing object error: {0}")]
    ListObjectError(#[from] SdkError<ListObjectsV2Error>),

    #[error("get object error: {0}")]
    GetObjectError(#[from] SdkError<GetObjectError>),

    #[error("put object error: {0}")]
    PutObjectError(#[from] SdkError<PutObjectError>),

    #[error("create multipart object error: {0}")]
    CreateMultipartError(#[from] SdkError<CreateMultipartUploadError>),

    #[error("complit multipart object error: {0}")]
    ComplitMultipartError(#[from] SdkError<CompleteMultipartUploadError>),

    #[error("upload part object error: {0}")]
    UploadPartError(#[from] SdkError<UploadPartError>),

    #[error("byte stream error: {0}")]
    ByteSreamError(#[from] AwsSmithyError),

    #[error("tokio join error: {0}")]
    TokioJoinError(#[from] JoinError),
}