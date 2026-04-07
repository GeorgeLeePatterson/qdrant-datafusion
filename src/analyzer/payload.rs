use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::{Expr, LogicalPlan};

use super::source::Source;
use crate::expr_fn::payload_access_expr;
use crate::qdrant::QdrantPayloadAccess;

pub(crate) fn rewrite_typed_payload_plan(
    plan: &LogicalPlan,
    source: &Source,
) -> Result<Option<LogicalPlan>> {
    let rewritten =
        plan.clone().transform_up(|plan| rewrite_typed_payload_plan_node(&plan, source))?;
    Ok(rewritten.transformed.then_some(rewritten.data))
}

fn rewrite_typed_payload_plan_node(
    plan: &LogicalPlan,
    source: &Source,
) -> Result<Transformed<LogicalPlan>> {
    let exprs = plan.expressions();
    if exprs.is_empty() {
        return Ok(Transformed::no(plan.clone()));
    }
    let mut transformed = false;
    let mut rewritten_exprs = Vec::with_capacity(exprs.len());
    for (index, expr) in exprs.iter().enumerate() {
        let rewritten = expr
            .clone()
            .transform_up(|nested| Ok(rewrite_typed_payload_expr_node(&nested, source)))?;
        transformed |= rewritten.transformed;
        let mut rewritten_expr = rewritten.data;
        if rewritten.transformed
            && matches!(plan, LogicalPlan::Projection(_) | LogicalPlan::Aggregate(_))
        {
            rewritten_expr = rewritten_expr.alias(plan.schema().field(index).name().clone());
        }
        rewritten_exprs.push(rewritten_expr);
    }

    if !transformed {
        return Ok(Transformed::no(plan.clone()));
    }

    let rewritten = plan
        .with_new_exprs(rewritten_exprs, plan.inputs().into_iter().cloned().collect())?
        .recompute_schema()?;
    Ok(Transformed::yes(rewritten))
}

fn rewrite_typed_payload_expr_node(expr: &Expr, source: &Source) -> Transformed<Expr> {
    let Some(access) = QdrantPayloadAccess::from_raw_logical_expr(expr) else {
        return Transformed::no(expr.clone());
    };
    let path = access.path().key().to_owned();
    let payload = access.payload_expr();
    let data_type = source
        .payload_field(&path)
        .and_then(crate::qdrant::QdrantPayloadField::projection_data_type)
        .unwrap_or(datafusion::arrow::datatypes::DataType::Utf8);
    let Some(rewritten) = payload_access_expr(payload, path, &data_type) else {
        return Transformed::no(expr.clone());
    };
    Transformed::yes(rewritten)
}
