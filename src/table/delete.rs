use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, RecordBatch, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::Result as DataFusionResult;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::Boundedness;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures_util::stream;
use qdrant_client::Qdrant;
use qdrant_client::qdrant::{CountPointsBuilder, DeletePointsBuilder};

use crate::qdrant::filter::QdrantFilters;

#[derive(Clone)]
pub(super) struct QdrantDeleteExec {
    client:     Arc<Qdrant>,
    collection: String,
    filters:    QdrantFilters,
    schema:     SchemaRef,
    properties: Arc<PlanProperties>,
}

impl QdrantDeleteExec {
    pub(super) fn new(client: Arc<Qdrant>, collection: String, filters: QdrantFilters) -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new("count", DataType::UInt64, false)]));
        let properties = Arc::new(PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(Arc::clone(&schema)),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self { client, collection, filters, schema, properties }
    }
}

impl fmt::Debug for QdrantDeleteExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QdrantDeleteExec")
            .field("client", &"Qdrant")
            .field("collection", &self.collection)
            .field("filters", &self.filters)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for QdrantDeleteExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "QdrantDeleteExec: collection={}", self.collection)?;
        if !self.filters.is_empty() {
            write!(f, ", filter_leaves={}", self.filters.len())?;
        }
        Ok(())
    }
}

impl ExecutionPlan for QdrantDeleteExec {
    fn name(&self) -> &'static str { "QdrantDeleteExec" }

    fn as_any(&self) -> &dyn Any { self }

    fn properties(&self) -> &Arc<PlanProperties> { &self.properties }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &dyn PhysicalExpr,
        )
            -> DataFusionResult<datafusion::common::tree_node::TreeNodeRecursion>,
    ) -> DataFusionResult<datafusion::common::tree_node::TreeNodeRecursion> {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![] }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            datafusion::common::exec_err!("QdrantDeleteExec expects no children")
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> DataFusionResult<datafusion::execution::SendableRecordBatchStream> {
        if partition != 0 {
            return datafusion::common::exec_err!("QdrantDeleteExec invalid partition {partition}");
        }

        let client = Arc::clone(&self.client);
        let collection = self.collection.clone();
        let filters = self.filters.clone();
        let schema = Arc::clone(&self.schema);
        let fut = async move {
            let filter = filters.to_filter();
            let mut count = CountPointsBuilder::new(collection.clone()).exact(true);
            if let Some(filter) = filter.clone() {
                count = count.filter(filter);
            }
            let count = client
                .count(count)
                .await
                .map_err(|error| datafusion::error::DataFusionError::External(Box::new(error)))?
                .result
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "Qdrant delete count response missing result".to_owned(),
                    )
                })?
                .count;

            let delete = filter.unwrap_or_default();
            drop(
                client
                    .delete_points(DeletePointsBuilder::new(&collection).points(delete).wait(true))
                    .await
                    .map_err(|error| {
                        datafusion::error::DataFusionError::External(Box::new(error))
                    })?,
            );

            Ok(RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(UInt64Array::from(vec![
                count,
            ])) as ArrayRef])?)
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(Arc::clone(&self.schema), stream::once(fut))))
    }
}
