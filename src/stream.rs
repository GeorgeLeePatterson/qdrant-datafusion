//! Generate a stream of `Qdrant` points as Arrow `RecordBatch` values.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::*;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::RecordBatchStream;
use futures_util::Stream;

/// Stream that yields paginated `RecordBatch` values from `Qdrant` scan results.
#[pin_project::pin_project]
pub struct QdrantQueryStream {
    schema: SchemaRef,
    #[pin]
    stream: Pin<Box<dyn Stream<Item = DataFusionResult<RecordBatch>> + Send>>,
}

impl QdrantQueryStream {
    /// Create a new stream that yields record batches with the specified schema.
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
