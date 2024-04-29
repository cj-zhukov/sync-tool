use sync_tool::{Config, Mode, CONFIG_NAME, DEFAULT_MODE, get_aws_client, dif, upload, sync, show};
use std::{env, time::Instant};

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let now = Instant::now();
    let mut args = env::args().into_iter();
    let _ = args.next();
    let mode = args.next().unwrap_or(DEFAULT_MODE.to_string());
    let config_file = args.next().unwrap_or(CONFIG_NAME.to_string());
    let config = Config::new(&config_file).await?;
    if let Some(mode) = Mode::new(&mode) {
        let source = config.source.to_string();
        let target = format!("s3://{}/{}", &config.bucket, &config.target);
        println!("sync-tool started with mode: {} for source: {} target: {}", 
            mode.value(), &source, &target);
        let client = get_aws_client(&config.region).await;
        match mode {
            Mode::Dif => dif(client, config).await?,
            Mode::Upload => upload(client, config).await?,
            Mode::Sync => sync(client, config).await?,
            Mode::Show => show(client, config).await?,
        }
        println!("sync-tool finished with mode: {} for source: {} target: {} elapsed: {:.2?}",
            mode.value(), &source, &target, now.elapsed());
    }

	Ok(())
}