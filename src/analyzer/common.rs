use std::sync::Arc;

use datafusion::common::ScalarValue;
use datafusion::datasource::{TableProvider, source_as_provider};
use datafusion::logical_expr::expr::{AggregateFunction, Alias};
use datafusion::logical_expr::utils::COUNT_STAR_EXPANSION;
use datafusion::logical_expr::{Expr, LogicalPlan};

use crate::pushdown::QdrantPayloadSchema;
use crate::table::QdrantTableProvider;

pub(crate) struct QdrantSource {
    pub(crate) client:         Arc<qdrant_client::Qdrant>,
    pub(crate) collection:     String,
    pub(crate) schema:         datafusion::arrow::datatypes::SchemaRef,
    pub(crate) payload_schema: Arc<QdrantPayloadSchema>,
    pub(crate) filters:        Vec<Expr>,
}

impl QdrantSource {
    pub(crate) fn from_plan(plan: &LogicalPlan) -> Option<Self> {
        let mut filters = vec![];
        let mut plan = plan;
        loop {
            match plan {
                LogicalPlan::Filter(filter) => {
                    filters.push(filter.predicate.clone());
                    plan = filter.input.as_ref();
                }
                LogicalPlan::TableScan(scan) => {
                    let provider = source_as_provider(&scan.source).ok()?;
                    let provider = provider.as_any().downcast_ref::<QdrantTableProvider>()?;
                    return Some(Self {
                        client: Arc::clone(provider.client()),
                        collection: provider.collection().to_owned(),
                        schema: provider.schema(),
                        payload_schema: Arc::clone(provider.payload_schema()),
                        filters,
                    });
                }
                _ => return None,
            }
        }
    }
}

pub(crate) fn count_star_like(expr: &Expr) -> bool {
    match expr {
        Expr::Alias(Alias { expr, .. }) => count_star_like(expr),
        Expr::AggregateFunction(AggregateFunction { func, params }) => {
            func.name() == "count"
                && !params.distinct
                && params.filter.is_none()
                && params.order_by.is_empty()
                && params.null_treatment.is_none()
                && matches!(params.args.as_slice(), [Expr::Literal(value, _)] if count_like_literal(value))
        }
        _ => false,
    }
}

fn count_like_literal(value: &ScalarValue) -> bool {
    !value.is_null()
        && (value == &COUNT_STAR_EXPANSION
            || matches!(
                value,
                ScalarValue::Int8(_)
                    | ScalarValue::Int16(_)
                    | ScalarValue::Int32(_)
                    | ScalarValue::Int64(_)
                    | ScalarValue::UInt8(_)
                    | ScalarValue::UInt16(_)
                    | ScalarValue::UInt32(_)
                    | ScalarValue::UInt64(_)
                    | ScalarValue::Utf8(_)
                    | ScalarValue::Utf8View(_)
                    | ScalarValue::LargeUtf8(_)
                    | ScalarValue::Boolean(_)
            ))
}
