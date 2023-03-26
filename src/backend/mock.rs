use super::{Backend, BackendBuilder, BackendError, ResultMetadata};

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};

pub(crate) struct MockBackend;
pub(crate) struct MockBackendBuilder;

#[async_trait]
impl BackendBuilder for MockBackendBuilder {
    fn new(_: &str) -> Self {
        unimplemented!()
    }


    async fn build(self: Box<Self>) -> Result<Box<dyn Backend>, BackendError> {
        unimplemented!()
    }
}

#[async_trait]
impl Backend for MockBackend {
    async fn store_result_inner(
        &self,
        _: &str,
        _: Option<ResultMetadata>,
    ) -> Result<(), BackendError> {
        unimplemented!()
    }

    async fn get_task_meta(
        &self,
        _: &str,
    ) -> Result<super::ResultMetadata, crate::prelude::BackendError> {
        unimplemented!()
    }
}
