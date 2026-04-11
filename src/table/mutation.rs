use std::sync::Arc;

use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::compute::and;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result as DataFusionResult;
use datafusion::error::DataFusionError;
use datafusion::physical_expr::PhysicalExpr;

pub(super) fn match_mask_for_batch(
    operation: &str,
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
                DataFusionError::Internal(format!("{operation} filter did not evaluate to boolean"))
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
