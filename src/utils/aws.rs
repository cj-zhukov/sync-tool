use std::{collections::HashMap, path::Path};

use crate::{Error, Result};
use crate::utils::constants::*;

use aws_config::{BehaviorVersion, Region};
use aws_smithy_types::byte_stream::{ByteStream, Length};
use aws_sdk_s3::{
    Client,
    types::{CompletedMultipartUpload, CompletedPart},
    operation::{create_multipart_upload::CreateMultipartUploadOutput, get_object::GetObjectOutput},
};

pub async fn get_aws_client(region: &str) -> Client {
    let config = aws_config::defaults(BehaviorVersion::v2023_11_09())
        .region(Region::new(region.to_string()))
        .load()
        .await;

    Client::from_conf(
        aws_sdk_s3::config::Builder::from(&config)
            .retry_config(aws_config::retry::RetryConfig::standard()
            .with_max_attempts(AWS_MAX_RETRIES))
            .build()
    )
}

pub async fn get_object(client: &Client, bucket_name: &str, key: &str) -> Result<GetObjectOutput> {
    Ok(client
        .get_object()
        .bucket(bucket_name)
        .key(key)
        .send()
        .await?
    )
}

pub async fn list_keys_stream(client: Client, bucket: String, prefix: String) -> Result<HashMap<String, i64>> {
	let mut stream = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .into_paginator()
        .send();
    
	let mut files: HashMap<String, i64> = HashMap::new();
    while let Some(objects) = stream.next().await.transpose()? {
        for obj in objects.contents().iter().cloned() {
            if let Some(f_name) = &obj.key {
                let f_size = obj.size().unwrap_or(0);
                files.insert(f_name.clone(), f_size);
            }
        }
    }

	Ok(files)
}

pub async fn upload_object(client: Client, bucket_name: String, file_name: String, key: String, file_size: u64) -> Result<()> {
    let body = ByteStream::from_path(Path::new(&file_name)).await?;
    println!("Uploading file: {}", file_name);

    client
        .put_object()
        .bucket(&bucket_name)
        .key(&key)
        .body(body)
        .send()
        .await?;

    println!("Uploaded file: {}", file_name);

    let data: GetObjectOutput = get_object(&client, &bucket_name, &key).await?;
    let data_length = data.content_length().unwrap_or(0) as u64;
    if file_size == data_length {
        // println!("Data lengths match");
    } else {
        return Err(Error::Custom("Failed checking data size after upload".into()));
    }

    Ok(())
}

pub async fn upload_object_multipart(client: Client, bucket_name: String, file_name: String, key: String, file_size: u64, chunk_size: u64, max_chunks: u64) -> Result<()> {
    println!("Uploading file: {}", file_name);

    let multipart_upload_res: CreateMultipartUploadOutput = client
        .create_multipart_upload()
        .bucket(&bucket_name)
        .key(&key)
        .send()
        .await?;

    let upload_id = multipart_upload_res.upload_id().unwrap_or_default();
    let path = Path::new(&file_name);
    let mut chunk_count = (file_size / chunk_size) + 1;
    let mut size_of_last_chunk = file_size % chunk_size;

    if size_of_last_chunk == 0 {
        size_of_last_chunk = chunk_size;
        chunk_count -= 1;
    }
    if file_size == 0 {
        return Err(Error::Custom(format!("Bad file size for: {}", file_name)));
    }
    if chunk_count > max_chunks {
        return Err(Error::Custom(format!("Too many chunks file: {}. Try increasing your chunk size", file_name)));
    }

    let mut upload_parts: Vec<CompletedPart> = Vec::new();
    for chunk_index in 0..chunk_count {
        let this_chunk = if chunk_count - 1 == chunk_index {
            size_of_last_chunk
        } else {
            chunk_size
        };
        let stream = ByteStream::read_from()
            .path(path)
            .offset(chunk_index * chunk_size)
            .length(Length::Exact(this_chunk))
            .build()
            .await?;

        let part_number = (chunk_index as i32) + 1;
        let upload_part_res = client
            .upload_part()
            .key(&key)
            .bucket(&bucket_name)
            .upload_id(upload_id)
            .body(stream)
            .part_number(part_number)
            .send()
            .await?;

        upload_parts.push(
            CompletedPart::builder()
                .e_tag(upload_part_res.e_tag.unwrap_or_default())
                .part_number(part_number)
                .build(),
        );
    }

    let completed_multipart_upload: CompletedMultipartUpload = CompletedMultipartUpload::builder()
        .set_parts(Some(upload_parts))
        .build();

    let _complete_multipart_upload_res = client
        .complete_multipart_upload()
        .bucket(&bucket_name)
        .key(&key)
        .multipart_upload(completed_multipart_upload)
        .upload_id(upload_id)
        .send()
        .await?;

    println!("Uploaded file: {}", file_name);

    let data: GetObjectOutput = get_object(&client, &bucket_name, &key).await?;
    let data_length = data.content_length().unwrap_or(0) as u64;
    if file_size == data_length {
        // println!("Data lengths match.");
    } else {
        return Err(Error::Custom("Failed checking data size after upload".into()));
    }

    Ok(())
}