use async_trait::async_trait;

use crate::utils::config::AppConfig;
use crate::SyncToolError;

#[async_trait]
pub trait CloudStorage {
    async fn dif(&self, config: &AppConfig) -> Result<(), SyncToolError>;
    async fn show(&self, config: &AppConfig) -> Result<(), SyncToolError>;
    async fn upload(&self, config: &AppConfig) -> Result<(), SyncToolError>;
    async fn sync(&self, config: &AppConfig) -> Result<(), SyncToolError>;
}
