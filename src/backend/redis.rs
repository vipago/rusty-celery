use std::time::Duration;

use crate::task::TaskState;

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
        let key = format!("task:{task_id}");
        if !connection.exists(&key).await? {
            return Err(BackendError::DocumentNotFound(task_id.to_string()));
        }
        let meta: String = connection.get(&key).await?;
        let meta: ResultMetadata = serde_json::from_str(&meta)?;
        Ok(meta)
    }
    async fn wait_for_completion(&self, task_id: &str) -> Result<bool, BackendError> {
        let mut connection = self.0.get_async_connection().await?;
        let key = format!("task:{task_id}");
        loop {
            let result: String = connection.get(&key).await?;
            let result: ResultMetadata = serde_json::from_str(result.as_str())?;
            match result.status {
                TaskState::Pending => {
                    log::trace!("waiting for task: task {task_id} is still pending");
                },
                TaskState::Started => {
                    log::trace!("waiting for task: task {task_id} is running");
                },
                TaskState::Retry => {
                    log::trace!("waiting for task: task {task_id} is going to be retried");
                },
                TaskState::Failure => {
                    log::trace!("waiting for task: task {task_id} returned an error");
                    break Ok(false);
                },
                TaskState::Success => {
                    log::trace!("waiting for task: task {task_id} finished successfully");
                    break Ok(true);
                },
            }

            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}
