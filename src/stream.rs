//! Generate a stream of `Qdrant` points as arrow `RecordBatch`.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::*;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::RecordBatchStream;
use futures_util::Stream;

/// Stream that yields `RecordBatch`es from Qdrant query results.
///
/// This stream implementation provides the bridge between `Qdrant`'s async query results
/// and `DataFusion`'s streaming execution model. It typically yields a single batch
/// containing all points from a `Qdrant` query, though it's designed to be extensible
/// for future pagination support.
///
/// # Implementation Notes
/// Currently optimized for `Qdrant`'s typical usage patterns where queries return
/// relatively small result sets in a single response. Future versions may add
/// support for streaming large result sets with pagination.
#[pin_project::pin_project]
pub struct QdrantQueryStream {
    schema: SchemaRef,
    #[pin]
    stream: Pin<Box<dyn Stream<Item = DataFusionResult<RecordBatch>> + Send>>,
}

impl QdrantQueryStream {
    /// Create a new stream that yields record batches with the specified schema.
    ///
    /// # Arguments
    /// * `schema` - The Arrow schema that defines the structure of record batches
    /// * `stream` - The underlying stream of record batch results
    ///
    /// # Returns
    /// A new `QdrantQueryStream` ready for `DataFusion` execution.
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
