use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{Result as DataFusionResult, exec_err};
use datafusion::error::DataFusionError;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::Boundedness;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures_util::stream;
use qdrant_client::Qdrant;
use qdrant_client::qdrant::{CountPointsBuilder, DeletePointsBuilder, ScrollPointsBuilder};

use super::SCAN_PAGE_SIZE;
use super::mutation::match_mask_for_batch;
use crate::arrow::deserialize::QdrantRecordBatchBuilder;
use crate::arrow::serialize::record_batch_point_ids;
use crate::qdrant::filter::QdrantFilters;

#[derive(Clone)]
pub(super) struct QdrantDeleteExec {
    client:           Arc<Qdrant>,
    collection:       String,
    row_schema:       SchemaRef,
    exact_filters:    QdrantFilters,
    residual_filters: Vec<Arc<dyn PhysicalExpr>>,
    result_schema:    SchemaRef,
    properties:       Arc<PlanProperties>,
}

impl QdrantDeleteExec {
    pub(super) fn new(
        client: Arc<Qdrant>,
        collection: String,
        row_schema: SchemaRef,
        exact_filters: QdrantFilters,
        residual_filters: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        let result_schema =
            Arc::new(Schema::new(vec![Field::new("count", DataType::UInt64, false)]));
        let properties = Arc::new(PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(Arc::clone(&result_schema)),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            client,
            collection,
            row_schema,
            exact_filters,
            residual_filters,
            result_schema,
            properties,
        }
    }
}

impl fmt::Debug for QdrantDeleteExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QdrantDeleteExec")
            .field("client", &"Qdrant")
            .field("collection", &self.collection)
            .field("exact_filters", &self.exact_filters)
            .field("residual_filter_count", &self.residual_filters.len())
            .finish_non_exhaustive()
    }
}

impl DisplayAs for QdrantDeleteExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "QdrantDeleteExec: collection={}", self.collection)?;
        if !self.exact_filters.is_empty() {
            write!(f, ", filter_leaves={}", self.exact_filters.len())?;
        }
        if !self.residual_filters.is_empty() {
            write!(f, ", residual_filters={}", self.residual_filters.len())?;
        }
        Ok(())
    }
}

impl ExecutionPlan for QdrantDeleteExec {
    fn name(&self) -> &'static str { "QdrantDeleteExec" }

    fn as_any(&self) -> &dyn Any { self }

    fn properties(&self) -> &Arc<PlanProperties> { &self.properties }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![] }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            exec_err!("QdrantDeleteExec expects no children")
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> DataFusionResult<datafusion::execution::SendableRecordBatchStream> {
        if partition != 0 {
            return exec_err!("QdrantDeleteExec invalid partition {partition}");
        }

        let client = Arc::clone(&self.client);
        let collection = self.collection.clone();
        let row_schema = Arc::clone(&self.row_schema);
        let exact_filters = self.exact_filters.clone();
        let residual_filters = self.residual_filters.clone();
        let result_schema = Arc::clone(&self.result_schema);
        let fut = async move {
            let count = if residual_filters.is_empty() {
                execute_exact_delete(&client, &collection, exact_filters).await?
            } else {
                execute_residual_delete(
                    &client,
                    &collection,
                    Arc::clone(&row_schema),
                    exact_filters,
                    &residual_filters,
                )
                .await?
            };
            Ok(RecordBatch::try_new(Arc::clone(&result_schema), vec![Arc::new(UInt64Array::from(
                vec![count],
            )) as ArrayRef])?)
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.result_schema),
            stream::once(fut),
        )))
    }
}

async fn execute_exact_delete(
    client: &Arc<Qdrant>,
    collection: &str,
    exact_filters: QdrantFilters,
) -> DataFusionResult<u64> {
    let filter = exact_filters.to_filter();
    let mut count = CountPointsBuilder::new(collection).exact(true);
    if let Some(filter) = filter.clone() {
        count = count.filter(filter);
    }
    let count = client
        .count(count)
        .await
        .map_err(|error| DataFusionError::External(Box::new(error)))?
        .result
        .ok_or_else(|| {
            DataFusionError::Execution("Qdrant delete count response missing result".to_owned())
        })?
        .count;

    let delete = filter.unwrap_or_default();
    drop(
        client
            .delete_points(DeletePointsBuilder::new(collection).points(delete).wait(true))
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?,
    );
    Ok(count)
}

async fn execute_residual_delete(
    client: &Arc<Qdrant>,
    collection: &str,
    row_schema: SchemaRef,
    exact_filters: QdrantFilters,
    residual_filters: &[Arc<dyn PhysicalExpr>],
) -> DataFusionResult<u64> {
    let exact_filter = exact_filters.to_filter();
    let mut matched_ids = Vec::new();
    let mut next_offset = None;

    loop {
        let mut request = ScrollPointsBuilder::new(collection)
            .limit(u32::try_from(SCAN_PAGE_SIZE).expect("scan page size fits u32"))
            .with_payload(true)
            .with_vectors(true);
        if let Some(filter) = exact_filter.clone() {
            request = request.filter(filter);
        }
        if let Some(offset) = next_offset.clone() {
            request = request.offset(offset);
        }

        let response = client
            .scroll(request)
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let qdrant_client::qdrant::ScrollResponse { result, next_page_offset, .. } = response;

        if result.is_empty() {
            break;
        }

        let mut builder = QdrantRecordBatchBuilder::new(
            Arc::clone(&row_schema),
            result.len(),
            None,
            &BTreeMap::new(),
        )?;
        for point in result {
            builder.append_retrieved_point(point)?;
        }
        let batch = builder.finish()?;
        let (matched_rows, delete_mask) = match_mask_for_batch("DELETE", &batch, residual_filters)?;
        if matched_rows > 0 {
            matched_ids.extend(
                record_batch_point_ids(&batch)?
                    .into_iter()
                    .zip(delete_mask.iter())
                    .filter_map(|(id, matched)| (matched == Some(true)).then_some(id)),
            );
        }

        if next_page_offset.is_none() {
            break;
        }
        next_offset = next_page_offset;
    }

    let deleted = u64::try_from(matched_ids.len()).expect("point count fits u64");
    if deleted > 0 {
        drop(
            client
                .delete_points(DeletePointsBuilder::new(collection).points(matched_ids).wait(true))
                .await
                .map_err(|error| DataFusionError::External(Box::new(error)))?,
        );
    }
    Ok(deleted)
}
