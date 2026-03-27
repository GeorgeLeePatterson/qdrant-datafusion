use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::common::exec_err;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use qdrant_client::Qdrant;
use qdrant_client::qdrant::{
    Condition, Direction, Filter, OrderByBuilder, RetrievedPoint, ScrollPointsBuilder,
    VectorsSelector, order_value, start_from,
};

use super::{
    QdrantContinuation, QdrantOrderValue, QdrantOrderedContinuation, QdrantPayloadSelector,
    QdrantScanSpec, QdrantVectorSelector, SCAN_PAGE_SIZE,
};
use crate::arrow::deserialize::QdrantRecordBatchBuilder;

#[derive(Clone)]
pub(super) struct QdrantScrollState {
    pub(super) client: Arc<Qdrant>,
    pub(super) collection: String,
    pub(super) pushdown: Arc<QdrantScanSpec>,
    pub(super) remaining: Option<usize>,
    pub(super) continuation: QdrantContinuation,
}

impl QdrantScrollState {
    pub(super) async fn execute_page(
        self,
    ) -> DataFusionResult<Option<(RecordBatch, Option<Self>)>> {
        let Self { client, collection, pushdown, remaining, continuation } = self;

        if remaining == Some(0) {
            return Ok(None);
        }

        let page_limit =
            remaining.map_or(SCAN_PAGE_SIZE, |remaining| remaining.min(SCAN_PAGE_SIZE));
        let page_limit = u32::try_from(page_limit).expect("scan page size fits in u32");
        let mut request = ScrollPointsBuilder::new(&collection)
            .limit(page_limit)
            .with_payload(matches!(pushdown.payload, QdrantPayloadSelector::Full));
        let mut filter = pushdown.filters.to_filter();
        let mut ordered = None;
        match &pushdown.vectors {
            QdrantVectorSelector::None => request = request.with_vectors(false),
            QdrantVectorSelector::All => request = request.with_vectors(true),
            QdrantVectorSelector::Named(names) => {
                request = request.with_vectors(VectorsSelector { names: names.clone() });
            }
        }
        match continuation {
            QdrantContinuation::Offset(Some(offset)) => request = request.offset(offset),
            QdrantContinuation::Offset(None) => {}
            QdrantContinuation::Ordered(next) => {
                let mut order_by = OrderByBuilder::new(&next.ordering.field).direction(
                    if next.ordering.descending {
                        Direction::Desc as i32
                    } else {
                        Direction::Asc as i32
                    },
                );
                if let Some(start_from) = next.start_from {
                    order_by = order_by.start_from(start_from.start_from());
                }
                request = request.order_by(order_by);
                if !next.boundary_ids.is_empty() {
                    filter
                        .get_or_insert_with(Filter::default)
                        .must_not
                        .push(Condition::has_id(next.boundary_ids.clone()));
                }
                ordered = Some(next);
            }
        }
        if let Some(filter) = filter {
            request = request.filter(filter);
        }

        let response = client
            .scroll(request)
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let qdrant_client::qdrant::ScrollResponse { result, next_page_offset, .. } = response;

        if result.is_empty() {
            return Ok(None);
        }

        if ordered.is_some() && next_page_offset.is_some() {
            return exec_err!("ordered scroll returned id offset");
        }
        let ordered = ordered.map(|ordered| ordered.next(&result)).transpose()?;
        let point_count = result.len();
        let mut builder =
            QdrantRecordBatchBuilder::new(Arc::clone(&pushdown.schema), point_count, None)?;
        for point in result {
            builder.append_retrieved_point(point)?;
        }
        let batch = builder.finish()?;
        let remaining = remaining.map(|remaining| remaining.saturating_sub(point_count));

        let next_state = match (remaining, ordered, next_page_offset) {
            (Some(0), _, _) | (_, None, None) => None,
            (remaining, Some(ordered), _) => Some(Self {
                client,
                collection,
                pushdown,
                remaining,
                continuation: QdrantContinuation::Ordered(ordered),
            }),
            (remaining, None, Some(offset)) => Some(Self {
                client,
                collection,
                pushdown,
                remaining,
                continuation: QdrantContinuation::Offset(Some(offset)),
            }),
        };

        Ok(Some((batch, next_state)))
    }
}

impl QdrantOrderValue {
    pub(super) fn start_from(self) -> start_from::Value {
        match self {
            Self::Integer(value) => start_from::Value::Integer(value),
            Self::Float(value) => start_from::Value::Float(value),
        }
    }
}

impl QdrantOrderedContinuation {
    pub(super) fn next(mut self, points: &[RetrievedPoint]) -> DataFusionResult<Self> {
        let Some(last_point) = points.last() else {
            return Ok(self);
        };
        let last_value =
            match last_point.order_value.as_ref().and_then(|value| value.variant.as_ref()) {
                Some(order_value::Variant::Int(value)) => QdrantOrderValue::Integer(*value),
                Some(order_value::Variant::Float(value)) => QdrantOrderValue::Float(*value),
                None => return exec_err!("ordered row missing order value"),
            };
        let mut page_boundary_ids = vec![];
        for point in points.iter().rev() {
            let point_value =
                match point.order_value.as_ref().and_then(|value| value.variant.as_ref()) {
                    Some(order_value::Variant::Int(value)) => QdrantOrderValue::Integer(*value),
                    Some(order_value::Variant::Float(value)) => QdrantOrderValue::Float(*value),
                    None => return exec_err!("ordered row missing order value"),
                };
            if point_value != last_value {
                break;
            }
            page_boundary_ids.push(
                point
                    .id
                    .clone()
                    .ok_or_else(|| DataFusionError::Execution("ordered row missing id".into()))?,
            );
        }
        page_boundary_ids.reverse();
        if self.start_from.as_ref() == Some(&last_value) {
            self.boundary_ids.extend(page_boundary_ids);
        } else {
            self.boundary_ids = page_boundary_ids;
        }
        self.start_from = Some(last_value);
        Ok(self)
    }
}
