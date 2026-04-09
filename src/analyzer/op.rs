use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::hash::Hash;
use std::sync::Arc;

use datafusion::common::tree_node::TreeNode;
use datafusion::common::{Column, DFSchemaRef, Result, plan_err};
use datafusion::logical_expr::expr::BinaryExpr;
use datafusion::logical_expr::utils::{conjunction, expr_to_columns, split_conjunction_owned};
use datafusion::logical_expr::{
    Distinct, DistinctOn, Expr, Extension, LogicalPlan, LogicalPlanBuilder, Operator, SortExpr,
};

use super::common::count_star_like;
use super::kernel::{
    FacetKernel, KernelSpec, QueryGroupsKernel, QueryKernel, limit_rows, numeric_literal_f32,
};
use super::node::KernelNode;
use super::payload::rewrite_typed_payload_plan;
use super::query::{QueryBranchPlan, QueryDescriptor, QueryKind, QueryPrefetchBranch};
use super::source::Source;
use super::state::{FiltersState, KernelState};
use super::surface::QuerySurfaceCall;
use crate::analyzer::surface::SurfaceCall;
use crate::qdrant::QdrantPayloadPath;
use crate::qdrant::filter::QdrantFilters;

#[derive(Debug, Clone)]
pub(crate) struct QueryOp {
    query:               QueryKind,
    query_score_outputs: OutputNames,
    payload_outputs:     PayloadOutputs,
    score_threshold:     Option<f32>,
    prefetch:            Vec<QueryPrefetchBranch>,
}

impl QueryOp {
    fn from_surface(surface: QuerySurfaceCall) -> Self {
        Self {
            query:               QueryKind::from_surface(surface),
            query_score_outputs: OutputNames::default(),
            payload_outputs:     PayloadOutputs::default(),
            score_threshold:     None,
            prefetch:            vec![],
        }
    }

    fn validate_on_source(&self, source: &Source) -> Result<()> {
        self.query.validate_on_source(source)
    }

    pub(super) fn project(mut self, source: &Source, plan: &LogicalPlan) -> Result<Option<Self>> {
        let LogicalPlan::Projection(projection) = plan else {
            return Ok(None);
        };
        for expr in &projection.expr {
            if !self.projection_expr_supported(source, expr)? {
                return Ok(None);
            }
        }
        self.query_score_outputs =
            OutputNames::from_projection(plan, |expr| self.is_query_score_expr(expr))?;
        self.payload_outputs =
            PayloadOutputs::from_projection(plan, |expr| self.payload_output_path(source, expr));
        Ok(Some(self))
    }

    fn filter(
        mut self,
        source: &Source,
        filters: &mut FiltersState,
        predicate: &Expr,
    ) -> Result<Option<Self>> {
        let split = self.split_filter(source, predicate)?;
        if split.residual.is_some() {
            return Ok(None);
        }
        filters.exprs.extend(split.exact_filters);
        self = split.op;
        Ok(Some(self))
    }

    fn sort(self, plan: &LogicalPlan) -> Result<Option<Self>> {
        let LogicalPlan::Sort(sort) = plan else {
            return Ok(None);
        };
        if sort.expr.len() != 1 || sort.expr[0].asc {
            return Ok(None);
        }
        if !self.is_query_score_expr(&sort.expr[0].expr)? {
            return Ok(None);
        }
        Ok(Some(self))
    }

    fn kernel(
        self,
        source: Source,
        filters: &FiltersState,
        plan: &LogicalPlan,
    ) -> Result<Option<KernelState>> {
        let limit = match plan {
            LogicalPlan::Limit(_) => Some(limit_rows(plan)?),
            LogicalPlan::Projection(_) | LogicalPlan::Sort(_) => None,
            _ => return Ok(None),
        };
        let exact_filters = filters.exact(&source)?;
        drop(self.descriptor(&source)?);
        Ok(Some(KernelState::new(KernelSpec::Query(QueryKernel::new(
            source,
            exact_filters,
            self,
            limit,
        )))))
    }

    fn local_projection_shell(
        self,
        source: Source,
        filters: &FiltersState,
        plan: &LogicalPlan,
    ) -> Result<Option<LogicalPlan>> {
        let LogicalPlan::Projection(projection) = plan else {
            return Ok(None);
        };
        let mut shell =
            LocalQueryShellBuilder::new(&self, &source, projection.input.schema(), false);
        let rewritten_exprs = projection
            .expr
            .iter()
            .map(|expr| shell.rewrite_expr(expr))
            .collect::<Result<Vec<_>>>()?;
        let support_plan = LogicalPlanBuilder::from(projection.input.as_ref().clone())
            .project(shell.support_exprs(&rewritten_exprs)?)?
            .build()?;
        let Some(query) = self.project(&source, &support_plan)? else {
            return Ok(None);
        };
        let exact_filters = filters.exact(&source)?;
        let kernel_plan = query_kernel_plan(
            QueryKernel::new(source, exact_filters, query, None),
            Arc::clone(support_plan.schema()),
        );
        plan.with_new_exprs(rewritten_exprs, vec![kernel_plan])?.recompute_schema().map(Some)
    }

    fn local_filter_shell(
        self,
        source: Source,
        filters: &FiltersState,
        plan: &LogicalPlan,
    ) -> Result<Option<LogicalPlan>> {
        let LogicalPlan::Filter(filter) = plan else {
            return Ok(None);
        };
        let split = self.split_filter(&source, &filter.predicate)?;
        let Some(residual) = split.residual else {
            return Ok(None);
        };
        let mut shell =
            LocalQueryShellBuilder::new(&split.op, &source, filter.input.schema(), true);
        let rewritten_predicate = shell.rewrite_expr(&residual)?;
        let support_plan = LogicalPlanBuilder::from(filter.input.as_ref().clone())
            .project(shell.support_exprs(std::slice::from_ref(&rewritten_predicate))?)?
            .build()?;
        let Some(query) = split.op.project(&source, &support_plan)? else {
            return Ok(None);
        };
        let mut remote_filters = filters.clone();
        remote_filters.exprs.extend(split.exact_filters);
        let exact_filters = remote_filters.exact(&source)?;
        let kernel_plan = query_kernel_plan(
            QueryKernel::new(source, exact_filters, query, None),
            Arc::clone(support_plan.schema()),
        );
        let filter_plan = plan.with_new_exprs(vec![rewritten_predicate], vec![kernel_plan])?;
        LogicalPlanBuilder::from(filter_plan)
            .project(
                filter.input.schema().columns().into_iter().map(Expr::Column).collect::<Vec<_>>(),
            )?
            .build()?
            .recompute_schema()
            .map(Some)
    }

    fn local_aggregate_shell(
        self,
        source: Source,
        filters: &FiltersState,
        plan: &LogicalPlan,
    ) -> Result<Option<LogicalPlan>> {
        let LogicalPlan::Aggregate(aggregate) = plan else {
            return Ok(None);
        };
        let mut shell =
            LocalQueryShellBuilder::new(&self, &source, aggregate.input.schema(), false);
        let rewritten_group_expr = aggregate
            .group_expr
            .iter()
            .map(|expr| shell.rewrite_expr(expr))
            .collect::<Result<Vec<_>>>()?;
        let rewritten_aggr_expr = aggregate
            .aggr_expr
            .iter()
            .map(|expr| shell.rewrite_expr(expr))
            .collect::<Result<Vec<_>>>()?;
        let support_exprs = shell
            .support_exprs(&[rewritten_group_expr.clone(), rewritten_aggr_expr.clone()].concat())?;
        let support_plan = LogicalPlanBuilder::from(aggregate.input.as_ref().clone())
            .project(support_exprs)?
            .build()?;
        let Some(query) = self.project(&source, &support_plan)? else {
            return Ok(None);
        };
        let exact_filters = filters.exact(&source)?;
        let kernel_plan = query_kernel_plan(
            QueryKernel::new(source, exact_filters, query, None),
            Arc::clone(support_plan.schema()),
        );
        let aggregate_plan = LogicalPlanBuilder::from(kernel_plan)
            .aggregate(rewritten_group_expr, rewritten_aggr_expr)?
            .build()?;
        let renamed_output_exprs = aggregate_plan
            .schema()
            .columns()
            .into_iter()
            .enumerate()
            .map(|(index, column)| {
                Expr::Column(column).alias(aggregate.schema.field(index).name().clone())
            })
            .collect::<Vec<_>>();
        LogicalPlanBuilder::from(aggregate_plan)
            .project(renamed_output_exprs)?
            .build()?
            .recompute_schema()
            .map(Some)
    }

    fn local_window_shell(
        self,
        source: Source,
        filters: &FiltersState,
        plan: &LogicalPlan,
    ) -> Result<Option<LogicalPlan>> {
        let LogicalPlan::Window(window) = plan else {
            return Ok(None);
        };
        let mut shell = LocalQueryShellBuilder::new(&self, &source, window.input.schema(), true);
        let rewritten_window_expr = window
            .window_expr
            .iter()
            .map(|expr| shell.rewrite_expr(expr))
            .collect::<Result<Vec<_>>>()?;
        let support_plan = LogicalPlanBuilder::from(window.input.as_ref().clone())
            .project(shell.support_exprs(&rewritten_window_expr)?)?
            .build()?;
        let Some(query) = self.project(&source, &support_plan)? else {
            return Ok(None);
        };
        let exact_filters = filters.exact(&source)?;
        let kernel_plan = query_kernel_plan(
            QueryKernel::new(source, exact_filters, query, None),
            Arc::clone(support_plan.schema()),
        );
        let window_plan =
            LogicalPlanBuilder::from(kernel_plan).window(rewritten_window_expr)?.build()?;
        let window_plan_columns = window_plan.schema().columns();
        let original_input_len = window.input.schema().fields().len();
        let support_input_len = support_plan.schema().fields().len();
        let window_output_len = window.window_expr.len();
        let renamed_output_exprs = window_plan
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(index, _)| {
                if index < original_input_len {
                    let (qualifier, field) = window.schema.qualified_field(index);
                    return Some(
                        Expr::Column(window_plan_columns[index].clone())
                            .alias_qualified(qualifier.cloned(), field.name().clone()),
                    );
                }
                if index < support_input_len {
                    return None;
                }
                let output_index = index - support_input_len;
                (output_index < window_output_len).then(|| {
                    let schema_index = original_input_len + output_index;
                    let (qualifier, field) = window.schema.qualified_field(schema_index);
                    Expr::Column(window_plan_columns[index].clone())
                        .alias_qualified(qualifier.cloned(), field.name().clone())
                })
            })
            .collect::<Vec<_>>();
        LogicalPlanBuilder::from(window_plan)
            .project(renamed_output_exprs)?
            .build()?
            .recompute_schema()
            .map(Some)
    }

    fn local_distinct_on_shell(
        self,
        source: Source,
        filters: &FiltersState,
        plan: &LogicalPlan,
    ) -> Result<Option<LogicalPlan>> {
        let LogicalPlan::Distinct(Distinct::On(distinct_on)) = plan else {
            return Ok(None);
        };
        let mut shell =
            LocalQueryShellBuilder::new(&self, &source, distinct_on.input.schema(), false);
        let rewritten_on_expr = distinct_on
            .on_expr
            .iter()
            .map(|expr| shell.rewrite_expr(expr))
            .collect::<Result<Vec<_>>>()?;
        let rewritten_select_expr = distinct_on
            .select_expr
            .iter()
            .map(|expr| shell.rewrite_expr(expr))
            .collect::<Result<Vec<_>>>()?;
        let rewritten_sort_expr = distinct_on
            .sort_expr
            .as_ref()
            .map(|sort_exprs| {
                sort_exprs
                    .iter()
                    .map(|sort_expr| {
                        Ok(SortExpr {
                            expr:        shell.rewrite_expr(&sort_expr.expr)?,
                            asc:         sort_expr.asc,
                            nulls_first: sort_expr.nulls_first,
                        })
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .transpose()?;
        let mut support_expr_inputs =
            [rewritten_on_expr.clone(), rewritten_select_expr.clone()].concat();
        if let Some(sort_exprs) = &rewritten_sort_expr {
            support_expr_inputs.extend(sort_exprs.iter().map(|sort_expr| sort_expr.expr.clone()));
        }
        let support_plan = LogicalPlanBuilder::from(distinct_on.input.as_ref().clone())
            .project(shell.support_exprs(&support_expr_inputs)?)?
            .build()?;
        let Some(query) = self.project(&source, &support_plan)? else {
            return Ok(None);
        };
        let exact_filters = filters.exact(&source)?;
        let kernel_plan = query_kernel_plan(
            QueryKernel::new(source, exact_filters, query, None),
            Arc::clone(support_plan.schema()),
        );
        Ok(Some(
            LogicalPlan::Distinct(Distinct::On(DistinctOn::try_new(
                rewritten_on_expr,
                rewritten_select_expr,
                rewritten_sort_expr,
                Arc::new(kernel_plan),
            )?))
            .recompute_schema()?,
        ))
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
        let Some(group_field) =
            source.payload_schema.path_for_logical_expr(&distinct_on.on_expr[0])
        else {
            return Ok(None);
        };
        if !source
            .payload_schema
            .field_for_path(group_field.key())
            .is_some_and(crate::qdrant::QdrantPayloadField::supports_grouping)
        {
            return Ok(None);
        }
        if !distinct_on
            .select_expr
            .iter()
            .all(|expr| self.projection_expr_supported(&source, expr).unwrap_or(false))
        {
            return Ok(None);
        }
        let Some(group_descending) = self.query_groups_sort_supported(
            &source,
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
        self.payload_outputs = PayloadOutputs::from_exprs_and_schema(
            &distinct_on.select_expr,
            &distinct_on.schema,
            |expr| self.payload_output_path(&source, expr),
        );
        let exact_filters = filters.exact(&source)?;
        Ok(Some(KernelState::new(KernelSpec::QueryGroups(QueryGroupsKernel::new(
            source,
            exact_filters,
            self,
            group_field.key().to_owned(),
            1,
            group_descending,
        )))))
    }

    fn projection_expr_supported(&self, source: &Source, expr: &Expr) -> Result<bool> {
        let expr = expr.clone().unalias_nested().data;
        Ok(matches!(expr, Expr::Column(_))
            || self.is_query_score_expr(&expr)?
            || self.payload_output_path(source, &expr).is_some())
    }

    fn payload_output_path(&self, source: &Source, expr: &Expr) -> Option<String> {
        if let Some(path) = self.payload_outputs.path_for_expr(expr) {
            return Some(path);
        }
        source.payload_schema.path_for_logical_expr(expr).map(|path| path.key().to_owned())
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

    pub(crate) fn descriptor(&self, source: &Source) -> Result<QueryDescriptor> {
        self.query.descriptor(source, &self.prefetch)
    }

    pub(crate) fn branch_plan(
        &self,
        source: &Source,
        filter: Option<QdrantFilters>,
        limit: Option<u64>,
    ) -> Result<QueryBranchPlan> {
        let mut branch = QueryBranchPlan::descriptor(
            self.descriptor(source)?,
            filter.and_then(|filters| filters.to_filter()),
            self.score_threshold,
            limit,
        );
        branch.prefetch = self.prefetch.iter().map(|branch| branch.branch.clone()).collect();
        Ok(branch)
    }

    pub(crate) fn with_prefetch(mut self, prefetch: Vec<QueryPrefetchBranch>) -> Self {
        self.prefetch = prefetch;
        self
    }

    pub(crate) fn prefetch_count(&self) -> usize { self.prefetch.len() }

    pub(crate) fn score_output_names(&self) -> BTreeSet<String> { self.query_score_outputs.names() }

    pub(crate) fn rewrite_score_surface_to_output_column(
        &self,
        expr: &Expr,
    ) -> Result<Option<Expr>> {
        if !self.is_query_score_expr(expr)? {
            return Ok(None);
        }
        let mut names = self.query_score_outputs.names().into_iter();
        let Some(name) = names.next() else {
            return Ok(None);
        };
        if names.next().is_some() {
            return Ok(None);
        }
        Ok(Some(Expr::Column(Column::from_name(name))))
    }

    pub(crate) fn payload_output_paths(&self) -> BTreeMap<String, String> {
        self.payload_outputs.paths()
    }

    pub(crate) fn score_threshold(&self) -> Option<f32> { self.score_threshold }

    fn split_filter(&self, source: &Source, predicate: &Expr) -> Result<QueryFilterSplit> {
        let mut op = self.clone();
        let mut exact_filters = Vec::new();
        let mut residuals = Vec::new();
        for expr in split_conjunction_owned(predicate.clone()) {
            if let Some(value) = op.query_score_threshold_expr(&expr)? {
                op.score_threshold =
                    Some(op.score_threshold.map_or(value, |current| current.max(value)));
                continue;
            }
            if QdrantFilters::supports_exact(&source.schema, &source.payload_schema, &expr) {
                exact_filters.push(expr);
            } else {
                residuals.push(expr);
            }
        }
        Ok(QueryFilterSplit { op, exact_filters, residual: conjunction(residuals) })
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

    fn query_groups_sort_supported(
        &self,
        source: &Source,
        sort_exprs: &[SortExpr],
        group_field: &QdrantPayloadPath,
    ) -> Result<Option<bool>> {
        if !(1..=2).contains(&sort_exprs.len()) {
            return Ok(None);
        }
        let Some(group_sort_field) =
            source.payload_schema.path_for_logical_ordering_expr(&sort_exprs[0].expr)
        else {
            return Ok(None);
        };
        if &group_sort_field != group_field {
            return Ok(None);
        }
        if sort_exprs.len() == 2
            && (sort_exprs[1].asc || !self.is_query_score_expr(&sort_exprs[1].expr)?)
        {
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

    pub(super) fn project(self, source: &Source, plan: &LogicalPlan) -> Result<Option<Self>> {
        match self {
            Self::Query(op) => op.project(source, plan).map(|op| op.map(Self::Query)),
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

    pub(super) fn local_projection_shell(
        self,
        source: Source,
        filters: &FiltersState,
        plan: &LogicalPlan,
    ) -> Result<Option<LogicalPlan>> {
        match self {
            Self::Query(op) => op.local_projection_shell(source, filters, plan),
            Self::Facet(_) => Ok(None),
        }
    }

    pub(super) fn local_filter_shell(
        self,
        source: Source,
        filters: &FiltersState,
        plan: &LogicalPlan,
    ) -> Result<Option<LogicalPlan>> {
        match self {
            Self::Query(op) => op.local_filter_shell(source, filters, plan),
            Self::Facet(_) => Ok(None),
        }
    }

    pub(super) fn local_aggregate_shell(
        self,
        source: Source,
        filters: &FiltersState,
        plan: &LogicalPlan,
    ) -> Result<Option<LogicalPlan>> {
        match self {
            Self::Query(op) => op.local_aggregate_shell(source, filters, plan),
            Self::Facet(_) => Ok(None),
        }
    }

    pub(super) fn local_window_shell(
        self,
        source: Source,
        filters: &FiltersState,
        plan: &LogicalPlan,
    ) -> Result<Option<LogicalPlan>> {
        match self {
            Self::Query(op) => op.local_window_shell(source, filters, plan),
            Self::Facet(_) => Ok(None),
        }
    }

    pub(super) fn local_distinct_on_shell(
        self,
        source: Source,
        filters: &FiltersState,
        plan: &LogicalPlan,
    ) -> Result<Option<LogicalPlan>> {
        match self {
            Self::Query(op) => op.local_distinct_on_shell(source, filters, plan),
            Self::Facet(_) => Ok(None),
        }
    }

    pub(super) fn local_fallback(
        self,
        source: &Source,
        plan: &LogicalPlan,
    ) -> Result<Option<LogicalPlan>> {
        match self {
            Self::Query(_) => Ok(None),
            Self::Facet(_) => rewrite_typed_payload_plan(plan, source)
                .map(|rewritten| Some(rewritten.unwrap_or_else(|| plan.clone()))),
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

    pub(super) fn with_prefetch(self, prefetch: Vec<QueryPrefetchBranch>) -> Result<Self> {
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

    pub(super) fn sort(self, plan: &LogicalPlan) -> Option<Self> {
        let LogicalPlan::Sort(sort) = plan else {
            return None;
        };
        if sort.expr.len() != 1 || sort.expr[0].asc || !self.is_count_expr(&sort.expr[0].expr) {
            return None;
        }
        Some(self)
    }

    pub(super) fn kernel(
        self,
        source: Source,
        filters: &FiltersState,
        plan: &LogicalPlan,
    ) -> Result<Option<KernelState>> {
        let exact_filters = filters.exact(&source)?;
        Ok(Some(KernelState::new(KernelSpec::Facet(FacetKernel::new(
            source,
            exact_filters,
            self,
            Some(limit_rows(plan)?),
        )))))
    }

    pub(crate) fn field(&self) -> &QdrantPayloadPath { &self.field }

    pub(crate) fn is_key_output_name(&self, name: &str) -> bool {
        self.key_outputs.contains_name(name)
    }

    pub(crate) fn is_count_output_name(&self, name: &str) -> bool {
        self.count_outputs.contains_name(name)
    }

    pub(crate) fn preserves_count_desc_sort(&self, expr: &Expr) -> bool { self.is_count_expr(expr) }

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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct PayloadOutputs(BTreeMap<String, String>);

impl PayloadOutputs {
    fn path_for_expr(&self, expr: &Expr) -> Option<String> {
        match expr.clone().unalias_nested().data {
            Expr::Column(column) => self.0.get(&column.name).cloned(),
            _ => None,
        }
    }

    pub(crate) fn paths(&self) -> BTreeMap<String, String> { self.0.clone() }

    fn from_projection(plan: &LogicalPlan, matches: impl FnMut(&Expr) -> Option<String>) -> Self {
        let LogicalPlan::Projection(projection) = plan else {
            unreachable!("prototype projection state mismatch");
        };
        Self::from_exprs_and_schema(&projection.expr, &projection.schema, matches)
    }

    fn from_exprs_and_schema(
        exprs: &[Expr],
        schema: &DFSchemaRef,
        mut matches: impl FnMut(&Expr) -> Option<String>,
    ) -> Self {
        let mut paths = BTreeMap::new();
        for (index, expr) in exprs.iter().enumerate() {
            if let Some(path) = matches(expr) {
                drop(paths.insert(schema.field(index).name().clone(), path));
            }
        }
        Self(paths)
    }
}

#[derive(Debug, Clone)]
struct QueryFilterSplit {
    op:            QueryOp,
    exact_filters: Vec<Expr>,
    residual:      Option<Expr>,
}

#[derive(Debug)]
struct LocalQueryShellBuilder<'a> {
    query:                 &'a QueryOp,
    source:                &'a Source,
    input_schema:          &'a DFSchemaRef,
    preserve_input_schema: bool,
    hidden_exprs:          Vec<(Expr, String)>,
    used_names:            BTreeSet<String>,
}

impl<'a> LocalQueryShellBuilder<'a> {
    fn new(
        query: &'a QueryOp,
        source: &'a Source,
        input_schema: &'a DFSchemaRef,
        preserve_input_schema: bool,
    ) -> Self {
        Self {
            query,
            source,
            input_schema,
            preserve_input_schema,
            hidden_exprs: vec![],
            used_names: input_schema.fields().iter().map(|field| field.name().clone()).collect(),
        }
    }

    fn rewrite_expr(&mut self, expr: &Expr) -> Result<Expr> {
        expr.clone()
            .transform_up(|nested| self.rewrite_expr_node(&nested))
            .map(|rewritten| rewritten.data)
    }

    fn support_exprs(&self, rewritten_exprs: &[Expr]) -> Result<Vec<Expr>> {
        let mut support_exprs = if self.preserve_input_schema {
            self.input_schema.columns().into_iter().map(Expr::Column).collect::<Vec<_>>()
        } else {
            let mut required_columns = HashSet::new();
            for expr in rewritten_exprs {
                expr_to_columns(expr, &mut required_columns)?;
            }
            self.input_schema
                .columns()
                .into_iter()
                .filter(|column| required_columns.contains(column))
                .map(Expr::Column)
                .collect::<Vec<_>>()
        };
        support_exprs.extend(
            self.hidden_exprs.iter().map(|(expr, alias)| expr.clone().alias(alias.clone())),
        );
        Ok(support_exprs)
    }

    fn rewrite_expr_node(
        &mut self,
        expr: &Expr,
    ) -> Result<datafusion::common::tree_node::Transformed<Expr>> {
        let normalized = expr.clone().unalias_nested().data;
        if self.query.is_query_score_expr(&normalized)?
            || self.query.payload_output_path(self.source, &normalized).is_some()
        {
            let alias = self.hidden_alias_for(normalized);
            return Ok(datafusion::common::tree_node::Transformed::yes(Expr::Column(
                Column::from_name(alias),
            )));
        }
        Ok(datafusion::common::tree_node::Transformed::no(expr.clone()))
    }

    fn hidden_alias_for(&mut self, expr: Expr) -> String {
        if let Some((_, alias)) = self.hidden_exprs.iter().find(|(existing, _)| *existing == expr) {
            return alias.clone();
        }
        let alias = self.next_hidden_name();
        self.hidden_exprs.push((expr, alias.clone()));
        alias
    }

    fn next_hidden_name(&mut self) -> String {
        let mut index = self.hidden_exprs.len();
        loop {
            let candidate = format!("__qdrant_local_{index}");
            if self.used_names.insert(candidate.clone()) {
                return candidate;
            }
            index += 1;
        }
    }
}

fn query_kernel_plan(kernel: QueryKernel, output_schema: DFSchemaRef) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(KernelNode::new(output_schema, KernelSpec::Query(kernel))),
    })
}
