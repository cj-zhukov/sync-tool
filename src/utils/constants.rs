use std::sync::LazyLock;

use config::{Config, File};

use super::config::AppConfig;

pub const AWS_MAX_RETRIES: u32 = 10;
pub const CONFIG_NAME: &str = "sync-tool.json"; // config file must exist
pub const DEFAULT_MODE: &str = "dif"; // dry-run is default mode

pub static CONFIG: LazyLock<AppConfig> = LazyLock::new(|| {
    let config_file = File::with_name(CONFIG_NAME);

    let config = Config::builder().add_source(config_file).build();

    match config {
        Ok(config) => {
            let res = config.try_deserialize();
            match res {
                Ok(res) => res,
                Err(e) => panic!("Failed parsing config cause: {e}"),
            }
        }
        Err(e) => panic!("Failed creating config cause: {e}"),
    }
});
