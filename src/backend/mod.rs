#[cfg(test)]
pub(crate) mod mock;

pub(crate) mod redis;

use crate::task::TaskState;
use crate::{error::BackendError, prelude::TaskError};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A results [`Backend`] is used to store and retrive the results and status of the tasks.
#[async_trait]
pub trait Backend: Send + Sync {
    /// Add task to collection
    async fn add_task(
        &self,
        task_id: &str,
    ) -> Result<(), BackendError> {
        let metadata = ResultMetadata {
            task_id: task_id.to_string(),
            status: TaskState::Pending,
            result: None,
            traceback: None,
            date_done: None,
        };
        self.store_result(task_id, metadata).await
    }

    /// Mark task as started to trace
    async fn mark_as_started(
        &self,
        task_id: &str,
    ) -> Result<(), BackendError> {
        let metadata = ResultMetadata {
            task_id: task_id.to_string(),
            status: TaskState::Started,
            result: None,
            traceback: None,
            date_done: None,
        };
        self.store_result(task_id, metadata).await
    }

    /// Mark task as finished and save result
    async fn mark_as_done(
        &self,
        task_id: &str,
        result: &str,
        date_done: DateTime<Utc>,
    ) -> Result<(), BackendError> {
        let metadata = ResultMetadata {
            task_id: task_id.to_string(),
            status: TaskState::Success,
            result: Some(result.to_string()),
            traceback: None,
            date_done: Some(date_done),
        };
        self.store_result(task_id, metadata).await
    }

    /// Mark task as failure and save error
    async fn mark_as_failure(
        &self,
        task_id: &str,
        traceback: TaskError,
        date_done: DateTime<Utc>,
    ) -> Result<(), BackendError> {
        let metadata = ResultMetadata {
            task_id: task_id.to_string(),
            status: TaskState::Failure,
            result: None,
            traceback: Some(traceback),
            date_done: Some(date_done),
        };
        self.store_result(task_id, metadata).await
    }

    /// Update task state and result.
    async fn store_result(
        &self,
        task_id: &str,
        metadata: ResultMetadata,
    ) -> Result<(), BackendError> {
        // TODO: Add retry
        self.store_result_inner(task_id, Some(metadata)).await
    }

    /// Forget task result
    async fn forget(
        &self,
        task_id: &str,
    ) -> Result<(), BackendError> {
        self.store_result_inner(task_id, None).await
    }

    /// Update task state and result.
    async fn store_result_inner(
        &self,
        task_id: &str,
        metadata: Option<ResultMetadata>,
    ) -> Result<(), BackendError>;

    /// Get task meta from backend.
    async fn get_task_meta(
        &self,
        task_id: &str,
    ) -> Result<ResultMetadata, BackendError>;

    /// Get current state of a given task.
    async fn get_state(
        &self,
        task_id: &str,
    ) -> Result<TaskState, BackendError> {
        Ok(self.get_task_meta(task_id).await?.status)
    }

    /// Get result of a given task.
    async fn get_result(
        &self,
        task_id: &str,
    ) -> Result<Option<String>, BackendError> {
        Ok(self.get_task_meta(task_id).await?.result)
    }

    /// Get result of a given task.
    async fn get_traceback(
        &self,
        task_id: &str,
    ) -> Result<Option<TaskError>, BackendError> {
        Ok(self.get_task_meta(task_id).await?.traceback)
    }
    /// Watches the backend and blocks until the state of the task changes to a status (commonly Success)
    async fn wait_for_task_state(&self, task_id: &str, state: TaskState) -> Result<(), BackendError>;
}

/// Metadata of the task stored in the storage used.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultMetadata {
    /// Task's ID.
    task_id: String,
    /// Current status of the task.
    status: TaskState,
    /// Result of the task.
    result: Option<String>,
    /// Error of the task.
    traceback: Option<TaskError>,
    /// Date of culmination of the task
    date_done: Option<DateTime<Utc>>,
}

/// A [`BackendBuilder`] is used to create a type of results [`Backend`] with a custom configuration.
#[async_trait]
pub trait BackendBuilder {
    /// Create a new `BackendBuilder`.
    fn new(broker_url: &str) -> Self where Self: Sized;
    /// Construct the `Backend` with the given configuration.
    async fn build(self: Box<Self>) -> Result<Box<dyn Backend>, BackendError>;
}
