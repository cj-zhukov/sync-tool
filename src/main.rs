use std::time::Instant;

use clap::Parser;
use color_eyre::Result;
use log::info;

use sync_tool::cloud_storage::aws::AwsStorage;
use sync_tool::domain::CloudStorage;
use sync_tool::utils::aws::get_aws_client;
use sync_tool::utils::config::load_config;
use sync_tool::utils::logger::init_logger;
use sync_tool::{Cli, Mode};

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    init_logger();

    let now = Instant::now();
    let cli = Cli::parse();
    let config = load_config(&cli.config);
    let client = get_aws_client(&config.region).await;
    let aws_storage = AwsStorage::new(client);
    let source = config.source.to_string();
    let target = format!("s3://{}/{}", &config.bucket, &config.target);
    info!(
        "sync-tool started with mode: {} for source: {} target: {}",
        cli.mode.as_ref(),
        &source,
        &target
    );

    match cli.mode {
        Mode::Dif => aws_storage.dif(&config).await?,
        Mode::Upload => aws_storage.upload(&config).await?,
        Mode::Sync => aws_storage.sync(&config).await?,
    }

    info!(
        "sync-tool finished with mode: {} for source: {} target: {} elapsed: {:.2?}",
        cli.mode.as_ref(),
        &source,
        &target,
        now.elapsed()
    );
    Ok(())
}
