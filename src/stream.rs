//! Generate a stream of `Qdrant` points as arrow `RecordBatch`.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::*;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::RecordBatchStream;
use futures_util::Stream;

/// Simple stream that yields one `RecordBatch` from a Qdrant query
#[pin_project::pin_project]
pub struct QdrantQueryStream {
    schema: SchemaRef,
    #[pin]
    stream: Pin<Box<dyn Stream<Item = DataFusionResult<RecordBatch>> + Send>>,
}

impl QdrantQueryStream {
    pub fn new(
        schema: SchemaRef,
        stream: Pin<Box<dyn Stream<Item = DataFusionResult<RecordBatch>> + Send>>,
    ) -> Self {
        Self { schema, stream }
    }
}

impl Stream for QdrantQueryStream {
    type Item = datafusion::error::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.as_mut().project().stream.poll_next(cx)
    }
}

impl RecordBatchStream for QdrantQueryStream {
    fn schema(&self) -> SchemaRef { Arc::clone(&self.schema) }
}
