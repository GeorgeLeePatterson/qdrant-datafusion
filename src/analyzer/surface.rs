use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::Expr;

use super::query::*;
use crate::expr_fn::{
    ContextCall, DiscoverCall, FormulaCall, FusionCall, NearestCall, NearestWithMmrCall,
    OrderByCall, RecommendCall, RelevanceFeedbackCall, SampleCall,
};

#[derive(Debug, Clone)]
pub(crate) enum SurfaceCall {
    Query(QuerySurfaceCall),
}

impl SurfaceCall {
    pub(super) fn from_expr(expr: &Expr) -> Result<Option<Self>> {
        QuerySurfaceCall::from_expr(expr).map(|call| call.map(Self::Query))
    }

    pub(super) fn collect(exprs: &[Expr]) -> Result<Option<Self>> {
        let mut surface: Option<Self> = None;
        for expr in exprs {
            let _ = expr.apply(|node| {
                let Some(call) = Self::from_expr(node)? else {
                    return Ok(TreeNodeRecursion::Continue);
                };
                if let Some(existing) = &surface {
                    if !existing.same_semantics(&call) {
                        return plan_err!("multiple qdrant surface calls in one qdrant region");
                    }
                } else {
                    surface = Some(call);
                }
                Ok(TreeNodeRecursion::Jump)
            })?;
        }
        Ok(surface)
    }

    fn same_semantics(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Query(lhs), Self::Query(rhs)) => lhs.same_semantics(rhs),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum QuerySurfaceCall {
    Nearest(NearestQuery),
    Recommend(RecommendQuery),
    Discover(DiscoverQuery),
    Context(ContextQuery),
    OrderBy(OrderByQuery),
    Fusion(FusionQuery),
    Sample(SampleQuery),
    Formula(FormulaQuery),
    NearestWithMmr(NearestWithMmrQuery),
    RelevanceFeedback(RelevanceFeedbackQuery),
}

impl QuerySurfaceCall {
    fn from_expr(expr: &Expr) -> Result<Option<Self>> {
        if let Some(call) = NearestCall::from_expr(expr)? {
            return Ok(Some(Self::Nearest(call.into())));
        }
        if let Some(call) = RecommendCall::from_expr(expr)? {
            return Ok(Some(Self::Recommend(call.try_into()?)));
        }
        if let Some(call) = DiscoverCall::from_expr(expr)? {
            return Ok(Some(Self::Discover(call.try_into()?)));
        }
        if let Some(call) = ContextCall::from_expr(expr)? {
            return Ok(Some(Self::Context(call.try_into()?)));
        }
        if let Some(call) = OrderByCall::from_expr(expr)? {
            return Ok(Some(Self::OrderBy(call.try_into()?)));
        }
        if let Some(call) = FusionCall::from_expr(expr)? {
            return Ok(Some(Self::Fusion(call.try_into()?)));
        }
        if let Some(call) = SampleCall::from_expr(expr)? {
            return Ok(Some(Self::Sample(call.try_into()?)));
        }
        if let Some(call) = FormulaCall::from_expr(expr)? {
            return Ok(Some(Self::Formula(call.try_into()?)));
        }
        if let Some(call) = NearestWithMmrCall::from_expr(expr)? {
            return Ok(Some(Self::NearestWithMmr(call.try_into()?)));
        }
        if let Some(call) = RelevanceFeedbackCall::from_expr(expr)? {
            return Ok(Some(Self::RelevanceFeedback(call.try_into()?)));
        }
        Ok(None)
    }

    fn same_semantics(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Nearest(lhs), Self::Nearest(rhs)) => lhs.same_semantics(rhs),
            (Self::Recommend(lhs), Self::Recommend(rhs)) => lhs.same_semantics(rhs),
            (Self::Discover(lhs), Self::Discover(rhs)) => lhs.same_semantics(rhs),
            (Self::Context(lhs), Self::Context(rhs)) => lhs.same_semantics(rhs),
            (Self::OrderBy(lhs), Self::OrderBy(rhs)) => lhs.same_semantics(rhs),
            (Self::Fusion(lhs), Self::Fusion(rhs)) => lhs.same_semantics(rhs),
            (Self::Sample(lhs), Self::Sample(rhs)) => lhs.same_semantics(rhs),
            (Self::Formula(lhs), Self::Formula(rhs)) => lhs.same_semantics(rhs),
            (Self::NearestWithMmr(lhs), Self::NearestWithMmr(rhs)) => lhs.same_semantics(rhs),
            (Self::RelevanceFeedback(lhs), Self::RelevanceFeedback(rhs)) => lhs.same_semantics(rhs),
            _ => false,
        }
    }
}
