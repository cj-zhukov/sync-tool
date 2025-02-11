use crate::utils::constants::*;

use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::Client;

pub async fn get_aws_client(region: &str) -> Client {
    let config = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new(region.to_string()))
        .load()
        .await;

    Client::from_conf(
        aws_sdk_s3::config::Builder::from(&config)
            .retry_config(
                aws_config::retry::RetryConfig::standard().with_max_attempts(AWS_MAX_RETRIES),
            )
            .build(),
    )
}
