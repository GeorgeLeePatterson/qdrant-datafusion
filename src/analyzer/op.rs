use std::collections::BTreeSet;
use std::hash::Hash;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchemaRef, Result, plan_err};
use datafusion::logical_expr::expr::BinaryExpr;
use datafusion::logical_expr::utils::split_conjunction_owned;
use datafusion::logical_expr::{Distinct, Expr, LogicalPlan, Operator, SortExpr};

use super::common::count_star_like;
use super::kernel::{
    FacetKernel, KernelSpec, QueryGroupsKernel, QueryKernel, limit_rows, numeric_literal_f32,
};
use super::query::{QueryBranchPlan, QueryDescriptor, QueryKind};
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
    prefetch:            Vec<QueryBranchPlan>,
}

impl QueryOp {
    fn from_surface(surface: QuerySurfaceCall) -> Self {
        Self {
            query:               QueryKind::from_surface(surface),
            query_score_outputs: OutputNames::default(),
            score_threshold:     None,
            sorted:              false,
            prefetch:            vec![],
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
        source: Source,
        filters: &FiltersState,
        plan: &LogicalPlan,
    ) -> Result<Option<KernelState>> {
        if !self.sorted {
            return Ok(None);
        }
        let exact_filters = filters.exact(&source)?;
        Ok(Some(KernelState::new(KernelSpec::Query(QueryKernel::new(
            source,
            exact_filters,
            self,
            limit_rows(plan)?,
        )))))
    }

    fn distinct_on_kernel(
        mut self,
        source: Source,
        filters: &FiltersState,
        plan: &LogicalPlan,
    ) -> Result<Option<KernelState>> {
        let LogicalPlan::Distinct(Distinct::On(distinct_on)) = plan else {
            return Ok(None);
        };
        if distinct_on.on_expr.len() != 1 {
            return Ok(None);
        }
        let Some(group_field) = QdrantPayloadPath::from_logical_expr(&distinct_on.on_expr[0])
        else {
            return Ok(None);
        };
        if !source
            .payload_schema
            .field(group_field.key())
            .is_some_and(crate::pushdown::QdrantPayloadField::supports_facet)
        {
            return Ok(None);
        }
        if !distinct_on
            .select_expr
            .iter()
            .all(|expr| self.projection_expr_supported(expr).unwrap_or(false))
        {
            return Ok(None);
        }
        let Some(group_descending) = self.query_groups_sort_supported(
            distinct_on.sort_expr.as_deref().unwrap_or(&[]),
            &group_field,
        )?
        else {
            return Ok(None);
        };
        self.query_score_outputs = OutputNames::from_exprs_and_schema(
            &distinct_on.select_expr,
            &distinct_on.schema,
            |expr| self.is_query_score_expr(expr),
        )?;
        let exact_filters = filters.exact(&source)?;
        Ok(Some(KernelState::new(KernelSpec::QueryGroups(QueryGroupsKernel::new(
            source,
            exact_filters,
            self,
            None,
            group_field.key().to_owned(),
            1,
            group_descending,
        )))))
    }

    fn projection_expr_supported(&self, expr: &Expr) -> Result<bool> {
        let expr = expr.clone().unalias_nested().data;
        Ok(matches!(expr, Expr::Column(_)) || self.is_query_score_expr(&expr)?)
    }

    fn is_query_score_expr(&self, expr: &Expr) -> Result<bool> {
        if self.query_score_outputs.matches_column(expr) {
            return Ok(true);
        }
        let expr = expr.clone().unalias_nested().data;
        Ok(matches!(
            SurfaceCall::from_expr(&expr)?,
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

    pub(crate) fn descriptor(&self) -> Result<QueryDescriptor> {
        self.query.descriptor(self.prefetch.len())
    }

    pub(crate) fn branch_plan(
        &self,
        filter: Option<QdrantFilters>,
        limit: Option<u64>,
    ) -> Result<QueryBranchPlan> {
        let mut branch = QueryBranchPlan::descriptor(
            self.descriptor()?,
            filter.and_then(|filters| filters.to_filter()),
            self.score_threshold,
            limit,
        );
        branch.prefetch.clone_from(&self.prefetch);
        Ok(branch)
    }

    pub(crate) fn with_prefetch(mut self, prefetch: Vec<QueryBranchPlan>) -> Self {
        self.prefetch = prefetch;
        self
    }

    pub(crate) fn prefetch_count(&self) -> usize { self.prefetch.len() }

    pub(crate) fn score_output_names(&self) -> BTreeSet<String> { self.query_score_outputs.names() }

    pub(crate) fn score_threshold(&self) -> Option<f32> { self.score_threshold }

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

    fn query_groups_sort_supported(
        &self,
        sort_exprs: &[SortExpr],
        group_field: &QdrantPayloadPath,
    ) -> Result<Option<bool>> {
        if sort_exprs.len() != 2 {
            return Ok(None);
        }
        let Some(group_sort_field) = QdrantPayloadPath::from_logical_expr(&sort_exprs[0].expr)
        else {
            return Ok(None);
        };
        if &group_sort_field != group_field {
            return Ok(None);
        }
        if sort_exprs[1].asc || !self.is_query_score_expr(&sort_exprs[1].expr)? {
            return Ok(None);
        }
        Ok(Some(!sort_exprs[0].asc))
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
            Self::Facet(op) => Ok(op.sort(plan).map(Self::Facet)),
        }
    }

    pub(super) fn kernel(
        self,
        source: Source,
        filters: &FiltersState,
        plan: &LogicalPlan,
    ) -> Result<Option<KernelState>> {
        match self {
            Self::Query(op) => op.kernel(source, filters, plan),
            Self::Facet(op) => op.kernel(source, filters, plan),
        }
    }

    pub(super) fn distinct_on_kernel(
        self,
        source: Source,
        filters: &FiltersState,
        plan: &LogicalPlan,
    ) -> Result<Option<KernelState>> {
        match self {
            Self::Query(op) => op.distinct_on_kernel(source, filters, plan),
            Self::Facet(_) => Ok(None),
        }
    }

    pub(super) fn with_prefetch(self, prefetch: Vec<QueryBranchPlan>) -> Result<Self> {
        match self {
            Self::Query(op) => Ok(Self::Query(op.with_prefetch(prefetch))),
            Self::Facet(_) => plan_err!("facet operations do not admit query prefetch branches"),
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

    pub(super) fn sort(mut self, plan: &LogicalPlan) -> Option<Self> {
        let LogicalPlan::Sort(sort) = plan else {
            return None;
        };
        if sort.expr.len() != 1 || sort.expr[0].asc || !self.is_count_expr(&sort.expr[0].expr) {
            return None;
        }
        self.sorted = true;
        Some(self)
    }

    pub(super) fn kernel(
        self,
        source: Source,
        filters: &FiltersState,
        plan: &LogicalPlan,
    ) -> Result<Option<KernelState>> {
        if !self.sorted {
            return Ok(None);
        }
        let exact_filters = filters.exact(&source)?;
        Ok(Some(KernelState::new(KernelSpec::Facet(FacetKernel::new(
            source,
            exact_filters,
            self,
            limit_rows(plan)?,
        )))))
    }

    pub(crate) fn field(&self) -> &QdrantPayloadPath { &self.field }

    pub(crate) fn is_key_output_name(&self, name: &str) -> bool {
        self.key_outputs.contains_name(name)
    }

    pub(crate) fn is_count_output_name(&self, name: &str) -> bool {
        self.count_outputs.contains_name(name)
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

    pub(crate) fn names(&self) -> BTreeSet<String> { self.0.clone() }

    fn matches_column(&self, expr: &Expr) -> bool {
        matches!(expr.clone().unalias_nested().data, Expr::Column(column) if self.contains_name(&column.name))
    }

    fn from_projection(
        plan: &LogicalPlan,
        matches: impl FnMut(&Expr) -> Result<bool>,
    ) -> Result<Self> {
        let LogicalPlan::Projection(projection) = plan else {
            return plan_err!("prototype projection state mismatch");
        };
        Self::from_exprs_and_schema(&projection.expr, &projection.schema, matches)
    }

    fn from_exprs_and_schema(
        exprs: &[Expr],
        schema: &DFSchemaRef,
        mut matches: impl FnMut(&Expr) -> Result<bool>,
    ) -> Result<Self> {
        let mut names = BTreeSet::new();
        for (index, expr) in exprs.iter().enumerate() {
            if matches(expr)? {
                let _ = names.insert(schema.field(index).name().clone());
            }
        }
        Ok(Self(names))
    }
}
