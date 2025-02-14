use std::{env, time::Instant};

use color_eyre::Result;

use sync_tool::cloud_storage::aws::AwsStorage;
use sync_tool::domain::CloudStorage;
use sync_tool::Mode;
use sync_tool::utils::aws::get_aws_client;
use sync_tool::utils::constants::*;

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;

    let now = Instant::now();
    let mut args = env::args();
    let _ = args.next();
    let mode = args.next().unwrap_or(DEFAULT_MODE.to_string());
    let config = CONFIG.to_owned();
    let client = get_aws_client(&config.region).await;
    let aws_storage = AwsStorage::new(client);

    if let Some(mode) = Mode::new(&mode) {
        let source = config.source.to_string();
        let target = format!("s3://{}/{}", &config.bucket, &config.target);
        println!(
            "sync-tool started with mode: {} for source: {} target: {}",
            mode.as_ref(),
            &source,
            &target
        );
        match mode {
            Mode::Dif => aws_storage.dif(&config).await?,
            Mode::Upload => aws_storage.upload(&config).await?,
            Mode::Sync => aws_storage.sync(&config).await?,
            Mode::Show => aws_storage.show(&config).await?,
        }
        println!(
            "sync-tool finished with mode: {} for source: {} target: {} elapsed: {:.2?}",
            mode.as_ref(),
            &source,
            &target,
            now.elapsed()
        );
    }

    Ok(())
}
