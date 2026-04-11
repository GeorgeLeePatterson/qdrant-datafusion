use std::any::Any;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Datum, RecordBatch, UInt64Array};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::compute::kernels::zip::zip;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{Result as DataFusionResult, exec_err};
use datafusion::error::DataFusionError;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::Boundedness;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures_util::stream;
use qdrant_client::Qdrant;
use qdrant_client::qdrant::{
    DeletePointsBuilder, GetPointsBuilder, ScrollPointsBuilder, UpdateMode, UpsertPointsBuilder,
};

use super::SCAN_PAGE_SIZE;
use super::mutation::match_mask_for_batch;
use crate::arrow::deserialize::QdrantRecordBatchBuilder;
use crate::arrow::schema::ID_FIELD_NAME;
use crate::arrow::serialize::{
    point_id_from_string, point_id_to_string, record_batch_id_strings, record_batch_to_points,
};
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
            let total_updated = if assignments.contains_key(ID_FIELD_NAME) {
                execute_key_rewrite_update(
                    &client,
                    &collection,
                    Arc::clone(&row_schema),
                    exact_filters,
                    &assignments,
                    &residual_filters,
                )
                .await?
            } else {
                execute_stable_id_update(
                    &client,
                    &collection,
                    Arc::clone(&row_schema),
                    exact_filters,
                    &assignments,
                    &residual_filters,
                )
                .await?
            };

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

async fn execute_stable_id_update(
    client: &Arc<Qdrant>,
    collection: &str,
    row_schema: SchemaRef,
    exact_filters: QdrantFilters,
    assignments: &HashMap<String, Arc<dyn PhysicalExpr>>,
    residual_filters: &[Arc<dyn PhysicalExpr>],
) -> DataFusionResult<u64> {
    let mut total_updated = 0_u64;
    let exact_filter = exact_filters.to_filter();
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

        let batch = record_batch_from_points(&row_schema, result)?;
        let (matched_rows, update_mask) = match_mask_for_batch("UPDATE", &batch, residual_filters)?;
        total_updated += u64::try_from(matched_rows).expect("row count fits u64");

        if matched_rows > 0 && !assignments.is_empty() {
            let updated =
                updated_rows(&batch, &row_schema, assignments, &update_mask, matched_rows)?;
            if updated.num_rows() > 0 {
                let points = record_batch_to_points(&updated, &row_schema)?;
                drop(
                    client
                        .upsert_points(
                            UpsertPointsBuilder::new(collection, points)
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

    Ok(total_updated)
}

async fn execute_key_rewrite_update(
    client: &Arc<Qdrant>,
    collection: &str,
    row_schema: SchemaRef,
    exact_filters: QdrantFilters,
    assignments: &HashMap<String, Arc<dyn PhysicalExpr>>,
    residual_filters: &[Arc<dyn PhysicalExpr>],
) -> DataFusionResult<u64> {
    let exact_filter = exact_filters.to_filter();
    let mut next_offset = None;
    let mut total_updated = 0_u64;
    let mut matched_ids = BTreeSet::new();
    let mut target_ids = BTreeSet::new();
    let mut changed_old_ids = Vec::new();
    let mut rewritten_points = Vec::new();

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

        let batch = record_batch_from_points(&row_schema, result)?;
        let (matched_rows, update_mask) = match_mask_for_batch("UPDATE", &batch, residual_filters)?;
        total_updated += u64::try_from(matched_rows).expect("row count fits u64");

        if matched_rows > 0 {
            let matched_batch = filtered_batch(&batch, &update_mask, matched_rows)?;
            let rewritten_batch =
                updated_rows(&batch, &row_schema, assignments, &update_mask, matched_rows)?;
            let old_ids = record_batch_id_strings(&matched_batch)?;
            let new_ids = record_batch_id_strings(&rewritten_batch)?;
            validate_rewritten_ids(&mut matched_ids, &mut target_ids, &old_ids, &new_ids)?;
            changed_old_ids.extend(old_ids.into_iter().zip(new_ids).filter_map(
                |(old_id, new_id)| (old_id != new_id).then(|| point_id_from_string(old_id)),
            ));
            rewritten_points.extend(record_batch_to_points(&rewritten_batch, &row_schema)?);
        }

        if next_page_offset.is_none() {
            break;
        }
        next_offset = next_page_offset;
    }

    assert_no_unmatched_id_collisions(client, collection, &matched_ids, &target_ids).await?;

    if !changed_old_ids.is_empty() {
        drop(
            client
                .delete_points(
                    DeletePointsBuilder::new(collection).points(changed_old_ids).wait(true),
                )
                .await
                .map_err(|error| DataFusionError::External(Box::new(error)))?,
        );
    }
    if !rewritten_points.is_empty() {
        drop(
            client
                .upsert_points(
                    UpsertPointsBuilder::new(collection, rewritten_points)
                        .wait(true)
                        .update_mode(UpdateMode::Upsert),
                )
                .await
                .map_err(|error| DataFusionError::External(Box::new(error)))?,
        );
    }

    Ok(total_updated)
}

fn record_batch_from_points(
    row_schema: &SchemaRef,
    points: Vec<qdrant_client::qdrant::RetrievedPoint>,
) -> DataFusionResult<RecordBatch> {
    let mut builder = QdrantRecordBatchBuilder::new(
        Arc::clone(row_schema),
        points.len(),
        None,
        &BTreeMap::new(),
    )?;
    for point in points {
        builder.append_retrieved_point(point)?;
    }
    builder.finish()
}

fn filtered_batch(
    batch: &RecordBatch,
    update_mask: &datafusion::arrow::array::BooleanArray,
    matched_rows: usize,
) -> DataFusionResult<RecordBatch> {
    if matched_rows == batch.num_rows() {
        Ok(batch.clone())
    } else {
        Ok(filter_record_batch(batch, update_mask)?)
    }
}

fn updated_rows(
    batch: &RecordBatch,
    schema: &SchemaRef,
    assignments: &HashMap<String, Arc<dyn PhysicalExpr>>,
    update_mask: &datafusion::arrow::array::BooleanArray,
    matched_rows: usize,
) -> DataFusionResult<RecordBatch> {
    let updated = apply_assignments(batch, schema, assignments, update_mask)?;
    if matched_rows == updated.num_rows() {
        Ok(updated)
    } else {
        Ok(filter_record_batch(&updated, update_mask)?)
    }
}

fn validate_rewritten_ids(
    matched_ids: &mut BTreeSet<String>,
    target_ids: &mut BTreeSet<String>,
    old_ids: &[String],
    new_ids: &[String],
) -> DataFusionResult<()> {
    for old_id in old_ids {
        let _ = matched_ids.insert(old_id.clone());
    }
    for new_id in new_ids {
        if !target_ids.insert(new_id.clone()) {
            return exec_err!(
                "UPDATE failed: rewritten '{}' values must remain unique within one update \
                 statement; duplicate target id '{}'",
                ID_FIELD_NAME,
                new_id
            );
        }
    }
    Ok(())
}

async fn assert_no_unmatched_id_collisions(
    client: &Arc<Qdrant>,
    collection: &str,
    matched_ids: &BTreeSet<String>,
    target_ids: &BTreeSet<String>,
) -> DataFusionResult<()> {
    let lookup_ids =
        target_ids.difference(matched_ids).cloned().map(point_id_from_string).collect::<Vec<_>>();
    if lookup_ids.is_empty() {
        return Ok(());
    }

    let response = client
        .get_points(
            GetPointsBuilder::new(collection, lookup_ids).with_payload(false).with_vectors(false),
        )
        .await
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    if response.result.is_empty() {
        return Ok(());
    }

    let conflicts = response
        .result
        .iter()
        .filter_map(|point| point.id.as_ref().map(point_id_to_string))
        .collect::<Vec<_>>();
    let conflicts =
        if conflicts.is_empty() { "<unknown>".to_owned() } else { conflicts.join(", ") };
    exec_err!(
        "UPDATE failed: rewritten '{}' values would collide with existing rows: {}",
        ID_FIELD_NAME,
        conflicts
    )
}

fn apply_assignments(
    batch: &RecordBatch,
    schema: &SchemaRef,
    assignments: &HashMap<String, Arc<dyn PhysicalExpr>>,
    update_mask: &datafusion::arrow::array::BooleanArray,
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
