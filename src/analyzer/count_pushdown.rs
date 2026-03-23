use std::sync::Arc;

use datafusion::common::Result;
use datafusion::common::tree_node::Transformed;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::optimizer::AnalyzerRule;

use super::common::{count_star_like, qdrant_source};
use crate::context::plan_node::QdrantCountNode;
use crate::pushdown::QdrantFilters;

#[derive(Debug, Clone, Copy)]
pub(crate) struct QdrantCountPushdown;

impl AnalyzerRule for QdrantCountPushdown {
    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &datafusion::common::config::ConfigOptions,
    ) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(|plan| {
            let LogicalPlan::Aggregate(aggregate) = plan else {
                return Ok(Transformed::no(plan));
            };
            if !aggregate.group_expr.is_empty() || aggregate.aggr_expr.len() != 1 {
                return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
            }
            if !count_star_like(&aggregate.aggr_expr[0]) {
                return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
            }

            let Some(source) = qdrant_source(aggregate.input.as_ref()) else {
                return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
            };
            if !source.filters.iter().all(|filter| {
                QdrantFilters::supports_exact(&source.schema, &source.payload_schema, filter)
            }) {
                return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
            }

            let filters =
                QdrantFilters::try_new(&source.schema, &source.payload_schema, &source.filters)?;
            Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                node: Arc::new(QdrantCountNode::new(
                    Arc::clone(&aggregate.schema),
                    source.client,
                    source.collection,
                    filters,
                )),
            })))
        })
        .map(|transformed| transformed.data)
    }

    fn name(&self) -> &'static str { "qdrant_count_pushdown" }
}
