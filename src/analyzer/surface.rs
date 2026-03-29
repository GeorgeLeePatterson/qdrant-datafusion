use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::Expr;

use super::query::*;

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
        if let Some(query) = NearestQuery::from_expr(expr)? {
            return Ok(Some(Self::Nearest(query)));
        }
        if let Some(query) = RecommendQuery::from_expr(expr)? {
            return Ok(Some(Self::Recommend(query)));
        }
        if let Some(query) = DiscoverQuery::from_expr(expr)? {
            return Ok(Some(Self::Discover(query)));
        }
        if let Some(query) = ContextQuery::from_expr(expr)? {
            return Ok(Some(Self::Context(query)));
        }
        if let Some(query) = OrderByQuery::from_expr(expr)? {
            return Ok(Some(Self::OrderBy(query)));
        }
        if let Some(query) = FusionQuery::from_expr(expr)? {
            return Ok(Some(Self::Fusion(query)));
        }
        if let Some(query) = SampleQuery::from_expr(expr)? {
            return Ok(Some(Self::Sample(query)));
        }
        if let Some(query) = FormulaQuery::from_expr(expr)? {
            return Ok(Some(Self::Formula(query)));
        }
        if let Some(query) = NearestWithMmrQuery::from_expr(expr)? {
            return Ok(Some(Self::NearestWithMmr(query)));
        }
        if let Some(query) = RelevanceFeedbackQuery::from_expr(expr)? {
            return Ok(Some(Self::RelevanceFeedback(query)));
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
