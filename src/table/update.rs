use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BooleanArray, Datum, RecordBatch, UInt64Array};
use datafusion::arrow::compute::kernels::zip::zip;
use datafusion::arrow::compute::{and, filter_record_batch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result as DataFusionResult, exec_err};
use datafusion::error::DataFusionError;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::Boundedness;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures_util::stream;
use qdrant_client::Qdrant;
use qdrant_client::qdrant::{ScrollPointsBuilder, UpdateMode, UpsertPointsBuilder};

use super::SCAN_PAGE_SIZE;
use crate::arrow::deserialize::QdrantRecordBatchBuilder;
use crate::arrow::serialize::record_batch_to_points;
use crate::qdrant::filter::QdrantFilters;

#[derive(Clone)]
pub(super) struct QdrantUpdateExec {
    client:           Arc<Qdrant>,
    collection:       String,
    row_schema:       SchemaRef,
    result_schema:    SchemaRef,
    exact_filters:    QdrantFilters,
    assignments:      HashMap<String, Arc<dyn PhysicalExpr>>,
    residual_filters: Vec<Arc<dyn PhysicalExpr>>,
    properties:       Arc<PlanProperties>,
}

impl QdrantUpdateExec {
    pub(super) fn new(
        client: Arc<Qdrant>,
        collection: String,
        schema: SchemaRef,
        exact_filters: QdrantFilters,
        assignments: Vec<(String, Arc<dyn PhysicalExpr>)>,
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
            row_schema: schema,
            result_schema,
            exact_filters,
            assignments: assignments.into_iter().collect(),
            residual_filters,
            properties,
        }
    }
}

impl fmt::Debug for QdrantUpdateExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QdrantUpdateExec")
            .field("client", &"Qdrant")
            .field("collection", &self.collection)
            .field("exact_filters", &self.exact_filters)
            .field("assignment_count", &self.assignments.len())
            .field("residual_filter_count", &self.residual_filters.len())
            .finish_non_exhaustive()
    }
}

impl DisplayAs for QdrantUpdateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "QdrantUpdateExec: collection={}, assignments={}",
            self.collection,
            self.assignments.len()
        )?;
        if !self.exact_filters.is_empty() {
            write!(f, ", filter_leaves={}", self.exact_filters.len())?;
        }
        if !self.residual_filters.is_empty() {
            write!(f, ", residual_filters={}", self.residual_filters.len())?;
        }
        Ok(())
    }
}

impl ExecutionPlan for QdrantUpdateExec {
    fn name(&self) -> &'static str { "QdrantUpdateExec" }

    fn as_any(&self) -> &dyn Any { self }

    fn properties(&self) -> &Arc<PlanProperties> { &self.properties }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&dyn PhysicalExpr) -> DataFusionResult<TreeNodeRecursion>,
    ) -> DataFusionResult<TreeNodeRecursion> {
        let mut tnr = TreeNodeRecursion::Continue;
        for expr in self.assignments.values() {
            tnr = tnr.visit_sibling(|| f(expr.as_ref()))?;
        }
        for expr in &self.residual_filters {
            tnr = tnr.visit_sibling(|| f(expr.as_ref()))?;
        }
        Ok(tnr)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![] }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            exec_err!("QdrantUpdateExec expects no children")
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> DataFusionResult<datafusion::execution::SendableRecordBatchStream> {
        if partition != 0 {
            return exec_err!("QdrantUpdateExec invalid partition {partition}");
        }

        let client = Arc::clone(&self.client);
        let collection = self.collection.clone();
        let row_schema = Arc::clone(&self.row_schema);
        let exact_filters = self.exact_filters.clone();
        let assignments = self.assignments.clone();
        let residual_filters = self.residual_filters.clone();
        let result_schema = Arc::clone(&self.result_schema);

        let fut = async move {
            let mut total_updated = 0_u64;
            let exact_filter = exact_filters.to_filter();
            let mut next_offset = None;

            loop {
                let mut request = ScrollPointsBuilder::new(&collection)
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
                let qdrant_client::qdrant::ScrollResponse { result, next_page_offset, .. } =
                    response;

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
                let (matched_rows, update_mask) = update_mask_for_batch(&batch, &residual_filters)?;
                total_updated += u64::try_from(matched_rows).expect("row count fits u64");

                if matched_rows > 0 && !assignments.is_empty() {
                    let updated =
                        apply_assignments(&batch, &row_schema, &assignments, &update_mask)?;
                    let updated = if matched_rows == updated.num_rows() {
                        updated
                    } else {
                        filter_record_batch(&updated, &update_mask)?
                    };
                    if updated.num_rows() > 0 {
                        let points = record_batch_to_points(&updated, &row_schema)?;
                        drop(
                            client
                                .upsert_points(
                                    UpsertPointsBuilder::new(&collection, points)
                                        .wait(true)
                                        .update_mode(UpdateMode::Upsert),
                                )
                                .await
                                .map_err(|error| DataFusionError::External(Box::new(error)))?,
                        );
                    }
                }

                if next_page_offset.is_none() {
                    break;
                }
                next_offset = next_page_offset;
            }

            Ok(RecordBatch::try_new(Arc::clone(&result_schema), vec![Arc::new(UInt64Array::from(
                vec![total_updated],
            )) as ArrayRef])?)
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.result_schema),
            stream::once(fut),
        )))
    }
}

fn update_mask_for_batch(
    batch: &RecordBatch,
    residual_filters: &[Arc<dyn PhysicalExpr>],
) -> DataFusionResult<(usize, BooleanArray)> {
    if residual_filters.is_empty() {
        return Ok((batch.num_rows(), BooleanArray::from(vec![true; batch.num_rows()])));
    }

    let mut combined_mask: Option<BooleanArray> = None;
    for filter in residual_filters {
        let result = filter.evaluate(batch)?;
        let array = result.into_array(batch.num_rows())?;
        let bool_array = array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("UPDATE filter did not evaluate to boolean".to_owned())
            })?
            .clone();
        combined_mask = Some(match combined_mask {
            Some(existing) => and(&existing, &bool_array)?,
            None => bool_array,
        });
    }

    let combined_mask = combined_mask.expect("residual filters are non-empty");
    let normalized: BooleanArray =
        combined_mask.iter().map(|value| Some(value == Some(true))).collect();
    let matched_rows = normalized.iter().filter(|value| value == &Some(true)).count();
    Ok((matched_rows, normalized))
}

fn apply_assignments(
    batch: &RecordBatch,
    schema: &SchemaRef,
    assignments: &HashMap<String, Arc<dyn PhysicalExpr>>,
    update_mask: &BooleanArray,
) -> DataFusionResult<RecordBatch> {
    let mut new_columns = Vec::with_capacity(batch.num_columns());
    for field in schema.fields() {
        let column_name = field.name();
        let original_column = batch.column_by_name(column_name).ok_or_else(|| {
            DataFusionError::Internal(format!("column '{column_name}' not found in update batch"))
        })?;

        let new_column = if let Some(physical_expr) = assignments.get(column_name.as_str()) {
            let new_values = physical_expr.evaluate_selection(batch, update_mask)?;
            let new_array = new_values.into_array(batch.num_rows())?;
            zip(update_mask, &new_array as &dyn Datum, original_column as &dyn Datum)?
        } else {
            Arc::clone(original_column)
        };
        new_columns.push(new_column);
    }

    RecordBatch::try_new(Arc::clone(schema), new_columns)
        .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
}
