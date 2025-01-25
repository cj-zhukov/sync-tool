use std::{env, time::Instant};

use sync_tool::sync_tool::{dif, show, sync, upload, Mode};
use sync_tool::utils::aws::get_aws_client;
use sync_tool::utils::constants::*;

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let now = Instant::now();
    let mut args = env::args();
    let _ = args.next();
    let mode = args.next().unwrap_or(DEFAULT_MODE.to_string());
    let config = CONFIG.to_owned();
    if let Some(mode) = Mode::new(&mode) {
        let source = config.source.to_string();
        let target = format!("s3://{}/{}", &config.bucket, &config.target);
        println!(
            "sync-tool started with mode: {} for source: {} target: {}",
            mode.as_ref(),
            &source,
            &target
        );
        let client = get_aws_client(&config.region).await;
        match mode {
            Mode::Dif => dif(client, config).await?,
            Mode::Upload => upload(client, config).await?,
            Mode::Sync => sync(client, config).await?,
            Mode::Show => show(client, config).await?,
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
