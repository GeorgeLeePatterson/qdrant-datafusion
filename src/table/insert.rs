use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::sink::DataSink;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, SendableRecordBatchStream};
use futures_util::StreamExt;
use qdrant_client::Qdrant;
use qdrant_client::qdrant::UpsertPointsBuilder;

use crate::arrow::serialize::record_batch_to_points;

pub(super) struct QdrantInsertSink {
    client:     Arc<Qdrant>,
    collection: String,
    schema:     SchemaRef,
}

impl QdrantInsertSink {
    pub(super) fn new(client: Arc<Qdrant>, collection: String, schema: SchemaRef) -> Self {
        Self { client, collection, schema }
    }
}

impl fmt::Debug for QdrantInsertSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QdrantInsertSink")
            .field("client", &"Qdrant")
            .field("collection", &self.collection)
            .field("schema", &self.schema)
            .finish()
    }
}

impl DisplayAs for QdrantInsertSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "QdrantInsertSink: collection={}", self.collection)
    }
}

#[async_trait]
impl DataSink for QdrantInsertSink {
    fn as_any(&self) -> &dyn Any { self }

    fn schema(&self) -> &SchemaRef { &self.schema }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> DataFusionResult<u64> {
        let mut written = 0_u64;
        while let Some(batch) = data.next().await {
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }
            let points = record_batch_to_points(&batch, &self.schema)?;
            written += u64::try_from(points.len()).expect("point count fits u64");
            drop(
                self.client
                    .upsert_points(UpsertPointsBuilder::new(&self.collection, points))
                    .await
                    .map_err(|error| DataFusionError::External(Box::new(error)))?,
            );
        }
        Ok(written)
    }
}
