use std::collections::BTreeSet;
use std::hash::Hash;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::expr::BinaryExpr;
use datafusion::logical_expr::utils::split_conjunction_owned;
use datafusion::logical_expr::{Expr, LogicalPlan, Operator};

use super::common::count_star_like;
use super::query::QueryKind;
use super::kernel::{FacetKernel, KernelSpec, QueryKernel, limit_rows, numeric_literal_f32};
use super::source::Source;
use super::state::{FiltersState, KernelState};
use super::surface::QuerySurfaceCall;
use crate::analyzer::surface::SurfaceCall;
use crate::pushdown::QdrantPayloadPath;
use crate::pushdown::filter::QdrantFilters;

#[derive(Debug, Clone)]
pub(crate) struct QueryOp {
    query:               QueryKind,
    query_score_outputs: OutputNames,
    score_threshold:     Option<f32>,
    sorted:              bool,
}

impl QueryOp {
    fn from_surface(surface: QuerySurfaceCall) -> Self {
        Self {
            query:               QueryKind::from_surface(surface),
            query_score_outputs: OutputNames::default(),
            score_threshold:     None,
            sorted:              false,
        }
    }

    fn validate_on_source(&self, source: &Source) -> Result<()> {
        self.query.validate_on_source(source)
    }

    pub(super) fn project(mut self, plan: &LogicalPlan) -> Result<Option<Self>> {
        let LogicalPlan::Projection(projection) = plan else {
            return Ok(None);
        };
        for expr in &projection.expr {
            if !self.projection_expr_supported(expr)? {
                return Ok(None);
            }
        }
        self.query_score_outputs =
            OutputNames::from_projection(plan, |expr| self.is_query_score_expr(expr))?;
        Ok(Some(self))
    }

    fn filter(
        mut self,
        source: &Source,
        filters: &mut FiltersState,
        predicate: &Expr,
    ) -> Result<Option<Self>> {
        let mut threshold = self.score_threshold;
        let mut exact_filters = Vec::new();
        for expr in split_conjunction_owned(predicate.clone()) {
            if let Some(value) = self.query_score_threshold_expr(&expr)? {
                threshold = Some(threshold.map_or(value, |current| current.max(value)));
                continue;
            }
            if self.contains_query_score_expr(&expr)? {
                return plan_err!("query output filters only admit score-threshold predicates");
            }
            if !QdrantFilters::supports_exact(&source.schema, &source.payload_schema, &expr) {
                return Ok(None);
            }
            exact_filters.push(expr);
        }
        filters.exprs.extend(exact_filters);
        self.score_threshold = threshold;
        Ok(Some(self))
    }

    fn sort(mut self, plan: &LogicalPlan) -> Result<Option<Self>> {
        let LogicalPlan::Sort(sort) = plan else {
            return Ok(None);
        };
        if sort.expr.len() != 1 || sort.expr[0].asc {
            return Ok(None);
        }
        if !self.is_query_score_expr(&sort.expr[0].expr)? {
            return Ok(None);
        }
        self.sorted = true;
        Ok(Some(self))
    }

    fn kernel(
        self,
        collection: String,
        filters: FiltersState,
        plan: &LogicalPlan,
    ) -> Result<Option<KernelState>> {
        if !self.sorted {
            return Ok(None);
        }
        Ok(Some(KernelState {
            spec: KernelSpec::Query(QueryKernel {
                collection,
                filters,
                query: self,
                limit: limit_rows(plan)?,
            }),
        }))
    }

    fn projection_expr_supported(&self, expr: &Expr) -> Result<bool> {
        let expr = expr.clone().unalias_nested().data;
        Ok(matches!(expr, Expr::Column(_)) || self.is_query_score_expr(&expr)?)
    }

    fn is_query_score_expr(&self, expr: &Expr) -> Result<bool> {
        if self.query_score_outputs.matches_column(expr) {
            return Ok(true);
        }
        Ok(matches!(
            SurfaceCall::from_expr(expr)?,
            Some(SurfaceCall::Query(surface)) if self.query.matches_surface(&surface)
        ))
    }

    fn contains_query_score_expr(&self, expr: &Expr) -> Result<bool> {
        let mut found = false;
        let _ = expr.apply(|node| {
            if self.is_query_score_expr(node)? {
                found = true;
                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        })?;
        Ok(found)
    }

    fn query_score_threshold_expr(&self, expr: &Expr) -> Result<Option<f32>> {
        let expr = expr.clone().unalias_nested().data;
        let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr else {
            return Ok(None);
        };
        if self.is_query_score_expr(left.as_ref())? {
            return match op {
                Operator::Gt | Operator::GtEq => numeric_literal_f32(right.as_ref()).map(Some),
                _ => Ok(None),
            };
        }
        if self.is_query_score_expr(right.as_ref())? {
            return match op {
                Operator::Lt | Operator::LtEq => numeric_literal_f32(left.as_ref()).map(Some),
                _ => Ok(None),
            };
        }
        Ok(None)
    }
}

#[derive(Debug, Clone)]
pub(crate) enum Op {
    Query(QueryOp),
    Facet(FacetOp),
}

impl Op {
    pub(super) fn from_surface(surface: SurfaceCall, source: &Source) -> Result<Self> {
        match surface {
            SurfaceCall::Query(surface) => {
                let op = QueryOp::from_surface(surface);
                op.validate_on_source(source)?;
                Ok(Self::Query(op))
            }
        }
    }

    pub(super) fn project(self, plan: &LogicalPlan) -> Result<Option<Self>> {
        match self {
            Self::Query(op) => op.project(plan).map(|op| op.map(Self::Query)),
            Self::Facet(op) => op.project(plan).map(|op| op.map(Self::Facet)),
        }
    }

    pub(super) fn filter(
        self,
        source: &Source,
        filters: &mut FiltersState,
        predicate: &Expr,
    ) -> Result<Option<Self>> {
        match self {
            Self::Query(op) => op.filter(source, filters, predicate).map(|op| op.map(Self::Query)),
            Self::Facet(_) => Ok(None),
        }
    }

    pub(super) fn sort(self, plan: &LogicalPlan) -> Result<Option<Self>> {
        match self {
            Self::Query(op) => op.sort(plan).map(|op| op.map(Self::Query)),
            Self::Facet(op) => op.sort(plan).map(|op| op.map(Self::Facet)),
        }
    }

    pub(super) fn kernel(
        self,
        collection: String,
        filters: FiltersState,
        plan: &LogicalPlan,
    ) -> Result<Option<KernelState>> {
        match self {
            Self::Query(op) => op.kernel(collection, filters, plan),
            Self::Facet(op) => op.kernel(collection, filters, plan),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FacetOp {
    pub(super) field:         QdrantPayloadPath,
    pub(super) key_outputs:   OutputNames,
    pub(super) count_outputs: OutputNames,
    pub(super) sorted:        bool,
}

impl FacetOp {
    pub(super) fn project(mut self, plan: &LogicalPlan) -> Result<Option<Self>> {
        let LogicalPlan::Projection(projection) = plan else {
            return Ok(None);
        };
        if !projection.expr.iter().all(|expr| self.projection_expr_supported(expr)) {
            return Ok(None);
        }
        self.key_outputs = OutputNames::from_projection(plan, |expr| Ok(self.is_key_expr(expr)))?;
        self.count_outputs =
            OutputNames::from_projection(plan, |expr| Ok(self.is_count_expr(expr)))?;
        Ok(Some(self))
    }

    pub(super) fn sort(mut self, plan: &LogicalPlan) -> Result<Option<Self>> {
        let LogicalPlan::Sort(sort) = plan else {
            return Ok(None);
        };
        if sort.expr.len() != 1 || sort.expr[0].asc || !self.is_count_expr(&sort.expr[0].expr) {
            return Ok(None);
        }
        self.sorted = true;
        Ok(Some(self))
    }

    pub(super) fn kernel(
        self,
        collection: String,
        filters: FiltersState,
        plan: &LogicalPlan,
    ) -> Result<Option<KernelState>> {
        if !self.sorted {
            return Ok(None);
        }
        Ok(Some(KernelState {
            spec: KernelSpec::Facet(FacetKernel {
                collection,
                filters,
                op: self,
                limit: limit_rows(plan)?,
            }),
        }))
    }

    fn projection_expr_supported(&self, expr: &Expr) -> bool {
        self.is_key_expr(expr) || self.is_count_expr(expr)
    }

    fn is_key_expr(&self, expr: &Expr) -> bool { self.key_outputs.matches_column(expr) }

    fn is_count_expr(&self, expr: &Expr) -> bool {
        self.count_outputs.matches_column(expr)
            || count_star_like(&expr.clone().unalias_nested().data)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct OutputNames(BTreeSet<String>);

impl OutputNames {
    pub(super) fn single(name: String) -> Self { Self(BTreeSet::from([name])) }

    fn contains_name(&self, name: &str) -> bool { self.0.contains(name) }

    fn matches_column(&self, expr: &Expr) -> bool {
        matches!(expr.clone().unalias_nested().data, Expr::Column(column) if self.contains_name(&column.name))
    }

    fn from_projection(
        plan: &LogicalPlan,
        mut matches: impl FnMut(&Expr) -> Result<bool>,
    ) -> Result<Self> {
        let LogicalPlan::Projection(projection) = plan else {
            return plan_err!("prototype projection state mismatch");
        };
        let mut names = BTreeSet::new();
        for (index, expr) in projection.expr.iter().enumerate() {
            if matches(expr)? {
                let _ = names.insert(projection.schema.field(index).name().clone());
            }
        }
        Ok(Self(names))
    }
}
