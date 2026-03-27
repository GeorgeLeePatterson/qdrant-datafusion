use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, Result, plan_err};
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::optimizer::AnalyzerRule;

use super::common::QdrantSource;
use crate::context::plan_node::{QdrantOpNode, QdrantQueryOp, QdrantQueryVariant};
use crate::expr_fn::{QDRANT_NEAREST_SCORE_FUNCTION_NAME, QdrantNearestCall};

#[derive(Debug, Clone, Copy)]
pub(crate) struct QdrantOpPushdown;

impl AnalyzerRule for QdrantOpPushdown {
    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &datafusion::common::config::ConfigOptions,
    ) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(rewrite_qdrant_ops).map(|transformed| transformed.data)
    }

    fn name(&self) -> &'static str {
        "qdrant_op_pushdown"
    }
}

fn rewrite_qdrant_ops(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let calls = collect_nearest_calls(&plan)?;
    if calls.is_empty() {
        return Ok(Transformed::no(plan));
    }

    let Some(first) = calls.first() else {
        return plan_err!("internal qdrant op rewrite expected at least one nearest call");
    };

    if calls.iter().skip(1).any(|call| !same_call(first, call)) {
        return plan_err!(
            "multiple {QDRANT_NEAREST_SCORE_FUNCTION_NAME} markers in one qdrant region"
        );
    }

    let score_field_name = first.score_field_name.clone();

    let new_inputs = if let Some(matches) = plan
        .inputs()
        .into_iter()
        .next()
        .and_then(unary_qdrant_op)
        .map(|op| op.matches_nearest_call(first))
    {
        if !matches {
            return plan_err!(
                "multiple {QDRANT_NEAREST_SCORE_FUNCTION_NAME} markers in one qdrant region"
            );
        }
        let plan_inputs = plan.inputs();
        if plan_inputs.len() != 1 {
            return plan_err!("internal qdrant op rewrite expected exactly one input");
        }
        plan_inputs.into_iter().cloned().collect::<Vec<_>>()
    } else {
        let Some(input) = plan.inputs().into_iter().next().cloned() else {
            return plan_err!("{QDRANT_NEAREST_SCORE_FUNCTION_NAME} requires a qdrant table");
        };
        if QdrantSource::from_plan(&input).is_none() {
            return plan_err!("{QDRANT_NEAREST_SCORE_FUNCTION_NAME} requires a qdrant table");
        }
        vec![LogicalPlan::Extension(Extension {
            node: Arc::new(QdrantOpNode::query(
                input,
                QdrantQueryOp {
                    query: QdrantQueryVariant::Nearest { vector: first.vector.clone() },
                    vector_field: first.vector_field.clone(),
                    score_field_name: score_field_name.clone(),
                },
            )?),
        })]
    };

    let exprs = plan
        .expressions()
        .into_iter()
        .map(|expr| {
            expr.transform_up(|expr| {
                if QdrantNearestCall::from_expr(&expr)?.is_some() {
                    return Ok(Transformed::yes(datafusion::prelude::Expr::Column(
                        Column::new_unqualified(score_field_name.clone()),
                    )));
                }
                Ok(Transformed::no(expr))
            })
            .map(|transformed| transformed.data)
        })
        .collect::<DataFusionResult<Vec<_>>>()?;

    Ok(Transformed::yes(plan.with_new_exprs(exprs, new_inputs)?))
}

fn collect_nearest_calls(plan: &LogicalPlan) -> Result<Vec<QdrantNearestCall>> {
    let mut calls = vec![];
    for expr in plan.expressions() {
        let _ = expr.apply(|expr| {
            if let Some(call) = QdrantNearestCall::from_expr(expr)? {
                calls.push(call);
                Ok(TreeNodeRecursion::Jump)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        })?;
    }
    Ok(calls)
}

fn same_call(lhs: &QdrantNearestCall, rhs: &QdrantNearestCall) -> bool {
    lhs.vector_field == rhs.vector_field
        && lhs.score_field_name == rhs.score_field_name
        && lhs
            .vector
            .iter()
            .map(|value| value.to_bits())
            .eq(rhs.vector.iter().map(|value| value.to_bits()))
}

fn unary_qdrant_op(plan: &LogicalPlan) -> Option<&QdrantOpNode> {
    let mut plan = plan;
    loop {
        if let Some(op) = QdrantOpNode::from_plan(plan) {
            return Some(op);
        }
        if plan.inputs().len() != 1 {
            return None;
        }
        plan = plan.inputs()[0];
    }
}
