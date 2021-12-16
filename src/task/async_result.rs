use serde::de::DeserializeOwned;

use crate::{backend::Backend, prelude::{BackendError, TaskError}};

use std::sync::Arc;

use super::TaskState;

/// An [`AsyncResult`] is a handle for the result of a task.
pub struct AsyncResult {
    task_id: String,
    backend: Option<Arc<dyn Backend>>,
}

impl AsyncResult {
    pub(crate) fn new(task_id: &str, backend: Option<Arc<dyn Backend>>) -> Self {
        Self {
            task_id: task_id.into(),
            backend
        }
    }

    /// Returns true if task is failed
    pub async fn failed(&self) -> Result<bool, BackendError> {
        self.throw_if_backend_not_set()?;
        let backend = self.backend.clone().unwrap();
        Ok(backend.get_state(&self.task_id).await? == TaskState::Failure)
    }

    /// Forget result of task
    pub async fn forget(&self) -> Result<(), BackendError> {
        self.throw_if_backend_not_set()?;
        let backend = self.backend.clone().unwrap();
        Ok(backend.forget(&self.task_id).await?)
    }

    /// Returns true if task is finished
    pub async fn ready(&self) -> Result<bool, BackendError> {
        self.throw_if_backend_not_set()?;
        let backend = self.backend.clone().unwrap();
        let state = backend.get_state(&self.task_id).await?;
        Ok(state == TaskState::Success || state == TaskState::Failure)
    }

    /// Get result of task
    pub async fn result<T: Send + Sync + Unpin + DeserializeOwned>(&self) -> Result<Option<T>, BackendError> {
        self.throw_if_backend_not_set()?;
        let backend = self.backend.clone().unwrap();
        let result = backend.get_result(&self.task_id).await?;
        if result.is_none() {
            return Ok(None);
        }
        Ok(serde_json::from_str(&result.unwrap())?)
    }

    /// Get traceback of task
    pub async fn traceback(&self) -> Result<Option<TaskError>, BackendError> {
        self.throw_if_backend_not_set()?;
        let backend = self.backend.clone().unwrap();
        Ok(backend.get_traceback(&self.task_id).await?)
    }

    /// Task's state
    pub async fn state(&self) -> Result<TaskState, BackendError> {
        self.throw_if_backend_not_set()?;
        let backend = self.backend.clone().unwrap();
        Ok(backend.get_state(&self.task_id).await?)
    }

    /// Returns true if task is succeeded
    pub async fn successful(&self) -> Result<bool, BackendError> {
        self.throw_if_backend_not_set()?;
        let backend = self.backend.clone().unwrap();
        let state = backend.get_state(&self.task_id).await?;
        Ok(state == TaskState::Success)
    }

    /// Task's ID
    pub fn task_id(&self) -> String {
        self.task_id.clone()
    }

    fn throw_if_backend_not_set(&self) -> Result<(), BackendError> {
        match &self.backend {
            Some(_) => Ok(()),
            None => Err(BackendError::NotSet)
        }
    }
}
