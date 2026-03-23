use std::sync::Arc;

use datafusion::common::tree_node::Transformed;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{Expr, Extension, LogicalPlan};
use datafusion::optimizer::AnalyzerRule;

use super::common::{count_star_like, qdrant_source};
use crate::context::plan_node::{QdrantFacetNode, QdrantFacetOutput};
use crate::pushdown::{QdrantFilters, QdrantPayloadField, logical_payload_path};

#[derive(Debug, Clone, Copy)]
pub(crate) struct QdrantFacetPushdown;

impl AnalyzerRule for QdrantFacetPushdown {
    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &datafusion::common::config::ConfigOptions,
    ) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(|plan| {
            let Some(node) = facet_node(&plan)? else {
                return Ok(Transformed::no(plan));
            };
            Ok(Transformed::yes(LogicalPlan::Extension(Extension { node: Arc::new(node) })))
        })
        .map(|transformed| transformed.data)
    }

    fn name(&self) -> &'static str { "qdrant_facet_pushdown" }
}

fn facet_node(plan: &LogicalPlan) -> Result<Option<QdrantFacetNode>> {
    let (schema, mut output_exprs, plan) = match plan {
        LogicalPlan::Projection(projection) => (
            Arc::clone(&projection.schema),
            Some(projection.expr.as_slice()),
            projection.input.as_ref(),
        ),
        _ => (Arc::clone(plan.schema()), None, plan),
    };
    let (limit_rows, sort) = match plan {
        LogicalPlan::Limit(limit) => {
            let Some(limit_rows) = facet_limit(limit) else {
                return Ok(None);
            };
            let LogicalPlan::Sort(sort) = limit.input.as_ref() else {
                return Ok(None);
            };
            (limit_rows, sort)
        }
        LogicalPlan::Sort(sort) => {
            let Some(limit_rows) = sort.fetch.and_then(|value| u64::try_from(value).ok()) else {
                return Ok(None);
            };
            (limit_rows, sort)
        }
        _ => return Ok(None),
    };
    let aggregate_input = if let LogicalPlan::Projection(projection) = sort.input.as_ref() {
        if output_exprs.is_some() {
            return Ok(None);
        }
        output_exprs = Some(projection.expr.as_slice());
        projection.input.as_ref()
    } else {
        sort.input.as_ref()
    };
    let LogicalPlan::Aggregate(aggregate) = aggregate_input else {
        return Ok(None);
    };
    if aggregate.group_expr.len() != 1
        || aggregate.aggr_expr.len() != 1
        || !count_star_like(&aggregate.aggr_expr[0])
    {
        return Ok(None);
    }
    let key_name = aggregate.schema.field(0).name().clone();
    let count_name = aggregate.schema.field(1).name().clone();
    let Some(field) = logical_payload_path(&aggregate.group_expr[0]) else {
        return Ok(None);
    };
    let Some(source) = qdrant_source(aggregate.input.as_ref()) else {
        return Ok(None);
    };
    if source.payload_schema.field(field.key()) != Some(QdrantPayloadField::Keyword) {
        return Ok(None);
    }
    if !source
        .filters
        .iter()
        .all(|filter| QdrantFilters::supports_exact(&source.schema, &source.payload_schema, filter))
    {
        return Ok(None);
    }
    let outputs = match output_exprs {
        Some(output_exprs) => {
            facet_outputs(output_exprs, &aggregate.group_expr[0], &key_name, &count_name)
        }
        None => Some(vec![QdrantFacetOutput::Key, QdrantFacetOutput::Count]),
    };
    let Some(outputs) = outputs else {
        return Ok(None);
    };
    if !count_sort_supported(sort, &schema, &outputs, &count_name) {
        return Ok(None);
    }
    let filters = QdrantFilters::try_new(&source.schema, &source.payload_schema, &source.filters)?;
    Ok(Some(QdrantFacetNode::new(
        schema,
        source.client,
        source.collection,
        filters,
        field,
        limit_rows,
        outputs,
    )))
}

fn facet_limit(limit: &datafusion::logical_expr::logical_plan::Limit) -> Option<u64> {
    match limit.skip.as_deref() {
        None | Some(Expr::Literal(ScalarValue::Int64(None | Some(0)), _)) => {}
        _ => return None,
    }
    match limit.fetch.as_deref() {
        Some(Expr::Literal(ScalarValue::Int64(Some(value)), _)) if *value > 0 => {
            u64::try_from(*value).ok()
        }
        _ => None,
    }
}

fn facet_outputs(
    exprs: &[Expr],
    group_expr: &Expr,
    key_name: &str,
    count_name: &str,
) -> Option<Vec<QdrantFacetOutput>> {
    let group_expr = group_expr.clone().unalias_nested().data;
    let outputs = exprs
        .iter()
        .map(|expr| {
            let expr = expr.clone().unalias_nested().data;
            let is_key = expr == group_expr
                || matches!(&expr, Expr::Column(column) if column.name == key_name);
            let is_count = count_star_like(&expr)
                || matches!(&expr, Expr::Column(column) if column.name == count_name);
            if is_key {
                Some(QdrantFacetOutput::Key)
            } else if is_count {
                Some(QdrantFacetOutput::Count)
            } else {
                None
            }
        })
        .collect::<Option<Vec<_>>>()?;
    let key_count = outputs.iter().filter(|output| **output == QdrantFacetOutput::Key).count();
    let count_count = outputs.iter().filter(|output| **output == QdrantFacetOutput::Count).count();
    (outputs.len() == 2 && key_count == 1 && count_count == 1).then_some(outputs)
}

fn count_sort_supported(
    sort: &datafusion::logical_expr::logical_plan::Sort,
    schema: &datafusion::common::DFSchemaRef,
    outputs: &[QdrantFacetOutput],
    aggregate_count_name: &str,
) -> bool {
    if sort.expr.len() != 1 || sort.expr[0].asc {
        return false;
    }
    if count_star_like(&sort.expr[0].expr) {
        return true;
    }
    let Expr::Column(column) = sort.expr[0].expr.clone().unalias_nested().data else {
        return false;
    };
    if column.name == aggregate_count_name {
        return true;
    }
    outputs
        .iter()
        .zip(schema.fields())
        .any(|(output, field)| *output == QdrantFacetOutput::Count && field.name() == &column.name)
}
