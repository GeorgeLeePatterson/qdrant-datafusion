use std::sync::Arc;

use datafusion::common::Result;
use datafusion::logical_expr::LogicalPlan;

use super::common::{count_star_like, qdrant_source};
use crate::context::plan_node::QdrantCountNode;
use crate::pushdown::filter::QdrantFilters;

pub(super) fn count_node(plan: &LogicalPlan) -> Result<Option<QdrantCountNode>> {
    let LogicalPlan::Aggregate(aggregate) = plan else {
        return Ok(None);
    };
    if !aggregate.group_expr.is_empty() || aggregate.aggr_expr.len() != 1 {
        return Ok(None);
    }
    if !count_star_like(&aggregate.aggr_expr[0]) {
        return Ok(None);
    }

    let Some(source) = qdrant_source(aggregate.input.as_ref()) else {
        return Ok(None);
    };
    if !source
        .filters
        .iter()
        .all(|filter| QdrantFilters::supports_exact(&source.schema, &source.payload_schema, filter))
    {
        return Ok(None);
    }

    let filters = QdrantFilters::try_new(&source.schema, &source.payload_schema, &source.filters)?;
    Ok(Some(QdrantCountNode::new(
        Arc::clone(&aggregate.schema),
        source.client,
        source.collection,
        filters,
    )))
}
