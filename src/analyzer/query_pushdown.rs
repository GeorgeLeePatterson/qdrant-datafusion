use std::sync::Arc;

use datafusion::common::{DFSchemaRef, Result, plan_err};
use datafusion::logical_expr::expr::Alias;
use datafusion::logical_expr::utils::split_conjunction_owned;
use datafusion::logical_expr::{BinaryExpr, Expr, LogicalPlan, Operator};

use super::common::QdrantSource;
use crate::arrow::schema::UNNAMED_VECTOR_FIELD_NAME;
use crate::context::plan_node::{
    QdrantKernelNode, QdrantKernelSpec, QdrantOp, QdrantOpNode, QdrantQueryKernel,
};
use crate::pushdown::filter::QdrantFilters;

pub(super) fn query_node(plan: &LogicalPlan) -> Result<Option<QdrantKernelNode>> {
    let (limit, sort_input): (_, _) = match plan {
        LogicalPlan::Limit(limit) => {
            let Some(limit_rows) = query_limit(limit) else {
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

    let Some(sorted_score_field_name) = query_sort_score_field(sort_input) else {
        return Ok(None);
    };

    let (query_schema, output_score_field_name, sort_input, projected) =
        match sort_input.input.as_ref() {
            LogicalPlan::Projection(projection) => {
                if !projected_query_output(projection, &sorted_score_field_name) {
                    return Ok(None);
                }
                (
                    Arc::clone(&projection.schema),
                    sorted_score_field_name,
                    projection.input.as_ref(),
                    true,
                )
            }
            plan => (
                Arc::clone(
                    QdrantOpNode::from_plan(plan)
                        .map_or_else(|| plan.schema(), QdrantOpNode::output_schema),
                ),
                sorted_score_field_name,
                plan,
                false,
            ),
        };

    let (residual_filters, op) = match sort_input {
        LogicalPlan::Filter(filter) => {
            let Some(op) = QdrantOpNode::from_plan(filter.input.as_ref()) else {
                return Ok(None);
            };
            let (score_threshold, residual_filters) =
                split_query_filters(filter.predicate.clone(), op.score_field_name())?;
            if !projected && output_score_field_name != op.score_field_name() {
                return Ok(None);
            }
            return build_query_node(
                op,
                residual_filters,
                limit,
                score_threshold,
                query_schema,
                output_score_field_name,
            )
            .map(Some);
        }
        plan => {
            let Some(op) = QdrantOpNode::from_plan(plan) else {
                return Ok(None);
            };
            (vec![], op)
        }
    };

    if !projected && output_score_field_name != op.score_field_name() {
        return Ok(None);
    }
    build_query_node(op, residual_filters, limit, None, query_schema, output_score_field_name)
        .map(Some)
}

fn build_query_node(
    op: &QdrantOpNode,
    residual_filters: Vec<Expr>,
    limit: u64,
    score_threshold: Option<f32>,
    schema: DFSchemaRef,
    score_field_name: String,
) -> Result<QdrantKernelNode> {
    let Some(source) = QdrantSource::from_plan(op.input()) else {
        return plan_err!("nearest operator requires a qdrant source");
    };
    let mut filters = source.filters.clone();
    filters.extend(residual_filters);
    if !filters
        .iter()
        .all(|filter| QdrantFilters::supports_exact(&source.schema, &source.payload_schema, filter))
    {
        return plan_err!("unsupported nearest filter outside score predicate");
    }
    let filters = QdrantFilters::try_new(&source.schema, &source.payload_schema, &filters)?;
    let QdrantOp::Query(query) = op.op();
    Ok(QdrantKernelNode::with_spec(
        schema,
        source.client,
        QdrantKernelSpec::Query(QdrantQueryKernel {
            collection: source.collection,
            filters,
            query: query.query.clone(),
            using: (query.vector_field != UNNAMED_VECTOR_FIELD_NAME)
                .then(|| query.vector_field.clone()),
            limit,
            score_threshold,
            score_field_name,
        }),
    ))
}

fn query_limit(limit: &datafusion::logical_expr::logical_plan::Limit) -> Option<u64> {
    match limit.skip.as_deref() {
        None | Some(Expr::Literal(datafusion::common::ScalarValue::Int64(None | Some(0)), _)) => {}
        _ => return None,
    }
    match limit.fetch.as_deref() {
        Some(Expr::Literal(datafusion::common::ScalarValue::Int64(Some(value)), _))
            if *value > 0 =>
        {
            u64::try_from(*value).ok()
        }
        _ => None,
    }
}

fn query_sort_score_field(sort: &datafusion::logical_expr::logical_plan::Sort) -> Option<String> {
    if sort.expr.len() != 1 || sort.expr[0].asc {
        return None;
    }
    match sort.expr[0].expr.clone().unalias_nested().data {
        Expr::Column(column) => Some(column.name),
        _ => None,
    }
}

fn projected_query_output(
    projection: &datafusion::logical_expr::logical_plan::Projection,
    sorted_score_field_name: &str,
) -> bool {
    let mut projected_score = false;
    for expr in &projection.expr {
        match expr {
            Expr::Column(column) => {
                if column.name == sorted_score_field_name {
                    projected_score = true;
                }
            }
            Expr::Alias(Alias { expr, name, .. }) => {
                let Expr::Column(column) = expr.clone().unalias_nested().data else {
                    return false;
                };
                if name == sorted_score_field_name {
                    projected_score = true;
                } else if name != &column.name {
                    return false;
                }
            }
            _ => return false,
        }
    }
    projected_score
}

fn split_query_filters(
    predicate: Expr,
    score_field_name: &str,
) -> Result<(Option<f32>, Vec<Expr>)> {
    let mut score_threshold = None;
    let mut residual = vec![];
    for expr in split_conjunction_owned(predicate) {
        if let Some(threshold) = score_threshold_expr(&expr, score_field_name)? {
            score_threshold =
                Some(score_threshold.map_or(threshold, |existing: f32| existing.max(threshold)));
        } else {
            residual.push(expr);
        }
    }
    Ok((score_threshold, residual))
}

fn score_threshold_expr(expr: &Expr, score_field_name: &str) -> Result<Option<f32>> {
    let expr = expr.clone().unalias_nested().data;
    let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr else {
        return Ok(None);
    };
    let Expr::Column(column) = left.as_ref().clone().unalias_nested().data else {
        return Ok(None);
    };
    if column.name != score_field_name {
        return Ok(None);
    }
    match op {
        Operator::Gt | Operator::GtEq => query_threshold(right.as_ref()).map(Some),
        _ => Ok(None),
    }
}

#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
fn query_threshold(expr: &Expr) -> Result<f32> {
    match expr.clone().unalias_nested().data {
        Expr::Negative(expr) => Ok(-query_threshold(&expr)?),
        Expr::Cast(cast) => query_threshold(&cast.expr),
        Expr::TryCast(cast) => query_threshold(&cast.expr),
        Expr::Literal(value, _) => match value {
            datafusion::common::ScalarValue::Float32(Some(value)) => Ok(value),
            datafusion::common::ScalarValue::Float64(Some(value)) => Ok(value as f32),
            datafusion::common::ScalarValue::Int8(Some(value)) => Ok(f32::from(value)),
            datafusion::common::ScalarValue::Int16(Some(value)) => Ok(f32::from(value)),
            datafusion::common::ScalarValue::Int32(Some(value)) => Ok(value as f32),
            datafusion::common::ScalarValue::Int64(Some(value)) => Ok(value as f32),
            datafusion::common::ScalarValue::UInt8(Some(value)) => Ok(f32::from(value)),
            datafusion::common::ScalarValue::UInt16(Some(value)) => Ok(f32::from(value)),
            datafusion::common::ScalarValue::UInt32(Some(value)) => Ok(value as f32),
            datafusion::common::ScalarValue::UInt64(Some(value)) => Ok(value as f32),
            _ => plan_err!("nearest score threshold must be numeric"),
        },
        _ => plan_err!("nearest score threshold must be numeric"),
    }
}
