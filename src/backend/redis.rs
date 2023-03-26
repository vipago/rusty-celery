use super::{Backend, BackendBuilder, BackendError, ResultMetadata};
use async_trait::async_trait;
use redis::Client;
use redis::AsyncCommands;

pub struct RedisBackendBuilder {
    backend_url: String,
}

pub struct RedisBackend(Client);

#[async_trait]
impl BackendBuilder for RedisBackendBuilder {
    /// Create new `RedisBackendBuilder`.
    fn new(backend_url: &str) -> Self {
        Self {
            backend_url: backend_url.to_string(),
        }
    }

    /// Create new `RedisBackend`.
    async fn build(self: Box<Self>) -> Result<Box<dyn Backend>, BackendError> {
        let client = Client::open(self.backend_url.as_str())?;
        Ok(Box::new(RedisBackend(client)))
    }
}

#[async_trait]
impl Backend for RedisBackend {
    async fn store_result_inner(
        &self,
        task_id: &str,
        metadata: Option<ResultMetadata>,
    ) -> Result<(), BackendError> {
        let mut connection = self.0.get_async_connection().await?;
        match metadata {
            Some(metadata) => {
                connection.set(format!("task:{task_id}"), serde_json::to_string(&metadata).unwrap()).await?;
            }
            None => {
                connection.del(format!("task:{task_id}")).await?;
            }
        }
        Ok(())
    }

    async fn get_task_meta(
        &self,
        task_id: &str,
    ) -> Result<ResultMetadata, BackendError> {
        let mut connection = self.0.get_async_connection().await?;
        if !connection.exists(task_id).await? {
            return Err(BackendError::DocumentNotFound(task_id.to_string()));
        }
        let meta: String = connection.get(format!("task:{task_id}")).await?;
        let meta: ResultMetadata = serde_json::from_str(&meta)?;
        Ok(meta)
    }
}
