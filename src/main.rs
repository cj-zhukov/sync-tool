use sync_tool::{Config, run};
use std::env;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = env::args().into_iter();
    let _ = args.next().unwrap();
    let mode = args.next().unwrap_or("dif".to_string());
    let config_file = args.next().unwrap_or("sync-tool.json".to_string());
    let config = Config::new(&config_file).await?;
    run(config, mode).await?;

	Ok(())
}