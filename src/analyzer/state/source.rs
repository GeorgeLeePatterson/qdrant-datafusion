use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, plan_err};
use datafusion::datasource::source_as_provider;
use datafusion::logical_expr::{Distinct, Expr, LogicalPlan};

use super::{FiltersState, KernelState, ProcessingState, State};
use crate::analyzer::kernel::{CountKernel, KernelSpec};
use crate::analyzer::op::{FacetOp, Op, OutputNames};
use crate::analyzer::payload::rewrite_typed_payload_plan;
use crate::analyzer::source::Source;
use crate::analyzer::surface::SurfaceCall;
use crate::qdrant::QdrantPayloadPath;
use crate::qdrant::filter::QdrantFilters;
use crate::table::QdrantTableProvider;

#[derive(Debug, Clone)]
pub(crate) struct SourceState {
    pub(crate) source:          Source,
    pub(crate) filters:         FiltersState,
    projected_payload_paths:    BTreeMap<String, QdrantPayloadPath>,
    projected_non_null_columns: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AggregateSurface {
    Local,
    Count,
    Facet(FacetOp),
}

impl SourceState {
    pub(crate) fn from_scan(
        scan: &datafusion::logical_expr::logical_plan::TableScan,
    ) -> Option<Self> {
        let provider = source_as_provider(&scan.source).ok()?;
        let schema = provider.schema();
        let projected_non_null_columns = schema
            .fields()
            .iter()
            .filter(|field| !field.is_nullable())
            .map(|field| field.name().clone())
            .collect();
        let provider = provider.as_any().downcast_ref::<QdrantTableProvider>()?;
        Some(Self {
            source: Source {
                client: Arc::clone(provider.client()),
                collection: provider.collection().to_owned(),
                schema,
                payload_schema: Arc::clone(provider.payload_schema()),
                ordered_scroll_contract: provider.ordered_scroll_contract(),
            },
            filters: FiltersState::default(),
            projected_payload_paths: BTreeMap::new(),
            projected_non_null_columns,
        })
    }

    pub(super) fn projection(
        self,
        mut plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        let surface = SurfaceCall::collect(&plan.expressions())?;
        if let Some(surface) = surface {
            return self.open(surface)?.projection(plan, transformed);
        }
        let mut transformed = transformed;
        if let Some(rewritten) = rewrite_typed_payload_plan(&plan, &self.source)? {
            plan = rewritten;
            transformed = true;
        }
        if let LogicalPlan::Projection(projection) = &plan
            && let Some((projected_payload_paths, projected_non_null_columns)) =
                self.projected_projection_state(&projection.expr, &projection.schema)
        {
            return Ok(super::super::Analysis::new(
                plan,
                State::Source(Self {
                    source: self.source,
                    filters: self.filters,
                    projected_payload_paths,
                    projected_non_null_columns,
                }),
                transformed,
            ));
        }
        self.localize(plan, transformed)
    }

    pub(super) fn filter(
        self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        let LogicalPlan::Filter(filter) = &plan else {
            return plan_err!("prototype filter state mismatch");
        };
        let surface = SurfaceCall::collect(std::slice::from_ref(&filter.predicate))?;
        if let Some(surface) = surface {
            return self.open(surface)?.filter(plan, transformed);
        }
        if QdrantFilters::supports_exact(
            &self.source.schema,
            &self.source.payload_schema,
            &filter.predicate,
        ) {
            let filters = self.filters.push(filter.predicate.clone());
            return Ok(super::super::Analysis::new(
                plan,
                State::Source(Self {
                    source: self.source,
                    filters,
                    projected_payload_paths: self.projected_payload_paths,
                    projected_non_null_columns: self.projected_non_null_columns,
                }),
                transformed,
            ));
        }
        self.localize(plan, transformed)
    }

    pub(super) fn sort(
        self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        let surface = SurfaceCall::collect(&plan.expressions())?;
        let Some(surface) = surface else {
            return self.localize(plan, transformed);
        };
        self.open(surface)?.sort(plan, transformed)
    }

    pub(super) fn limit(
        self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        self.localize(plan, transformed)
    }

    pub(super) fn aggregate(
        self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        if let Some(surface) = SurfaceCall::collect(&plan.expressions())? {
            return self.open(surface)?.aggregate(plan, transformed);
        }
        match AggregateSurface::of(&plan, &self)? {
            AggregateSurface::Local => self.localize(plan, transformed),
            AggregateSurface::Count => {
                let exact_filters = self.filters.exact(&self.source)?;
                KernelState::new(KernelSpec::Count(CountKernel::new(self.source, exact_filters)))
                    .absorb(plan, transformed)
            }
            AggregateSurface::Facet(op) => ProcessingState {
                source:  self.source,
                filters: self.filters,
                op:      Op::Facet(op),
            }
            .absorb(plan, transformed),
        }
    }

    pub(super) fn unary(
        self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        if matches!(plan, LogicalPlan::SubqueryAlias(_)) {
            return Ok(super::super::Analysis::new(plan, State::Source(self), transformed));
        }
        if let LogicalPlan::Distinct(Distinct::All(input)) = &plan {
            return Ok(super::super::Analysis::new(
                input.as_ref().clone(),
                State::Source(self),
                true,
            ));
        }
        if let Some(surface) = SurfaceCall::collect(&plan.expressions())? {
            return self.open(surface)?.unary(plan, transformed);
        }
        self.localize(plan, transformed)
    }

    fn open(&self, surface: SurfaceCall) -> Result<ProcessingState> {
        Ok(ProcessingState {
            source:  self.source.clone(),
            filters: self.filters.clone(),
            op:      Op::from_surface(surface, &self.source)?,
        })
    }

    fn localize(self, mut plan: LogicalPlan, transformed: bool) -> Result<super::super::Analysis> {
        let mut transformed = transformed;
        if let Some(rewritten) = rewrite_typed_payload_plan(&plan, &self.source)? {
            plan = rewritten;
            transformed = true;
        }
        Ok(super::super::Analysis::new(plan, State::local(), transformed))
    }

    fn projected_projection_state(
        &self,
        exprs: &[Expr],
        schema: &datafusion::common::DFSchemaRef,
    ) -> Option<(BTreeMap<String, QdrantPayloadPath>, BTreeSet<String>)> {
        let mut projected_payload_paths = BTreeMap::new();
        let mut projected_non_null_columns = BTreeSet::new();
        for (index, expr) in exprs.iter().enumerate() {
            let name = schema.field(index).name().clone();
            if let Some(path) = self.payload_path_for_expr(expr) {
                drop(projected_payload_paths.insert(name, path));
                continue;
            }
            if self.non_null_column_for_expr(expr) {
                let _ = projected_non_null_columns.insert(name);
                continue;
            }
            if !projection_passthrough_column(expr) {
                return None;
            }
        }
        Some((projected_payload_paths, projected_non_null_columns))
    }

    fn payload_path_for_expr(&self, expr: &Expr) -> Option<QdrantPayloadPath> {
        self.source
            .payload_schema
            .path_for_logical_expr(expr)
            .or_else(|| self.projected_payload_path_for_expr(expr))
    }

    fn projected_payload_path_for_expr(&self, expr: &Expr) -> Option<QdrantPayloadPath> {
        match expr {
            Expr::Alias(alias) => self.projected_payload_path_for_expr(&alias.expr),
            Expr::Column(column) => self.projected_payload_paths.get(&column.name).cloned(),
            Expr::Cast(cast) => {
                self.projected_payload_path_for_cast(&cast.expr, cast.field.data_type())
            }
            Expr::TryCast(cast) => {
                self.projected_payload_path_for_cast(&cast.expr, cast.field.data_type())
            }
            _ => None,
        }
    }

    fn projected_payload_path_for_cast(
        &self,
        expr: &Expr,
        data_type: &DataType,
    ) -> Option<QdrantPayloadPath> {
        let path = self.projected_payload_path_for_expr(expr)?;
        self.source
            .payload_field(path.key())
            .is_some_and(|field| field.supports_exact_payload_cast(data_type))
            .then_some(path)
    }

    pub(crate) fn has_projected_payload_paths(&self) -> bool {
        !self.projected_payload_paths.is_empty()
    }

    fn non_null_column_for_expr(&self, expr: &Expr) -> bool {
        match expr.clone().unalias_nested().data {
            Expr::Column(column) => self.projected_non_null_columns.contains(&column.name),
            _ => false,
        }
    }

    fn exact_row_count_expr(&self, expr: &Expr) -> bool {
        let Some(arg) = crate::analyzer::common::count_like_arg(expr) else {
            return false;
        };
        match arg.clone().unalias_nested().data {
            Expr::Literal(value, _) => crate::analyzer::common::count_like_literal(&value),
            Expr::Column(column) => self.projected_non_null_columns.contains(&column.name),
            _ => false,
        }
    }
}

impl AggregateSurface {
    fn of(plan: &LogicalPlan, state: &SourceState) -> Result<Self> {
        let LogicalPlan::Aggregate(aggregate) = plan else {
            return plan_err!("prototype aggregate state mismatch");
        };
        if aggregate.group_expr.is_empty()
            && aggregate.aggr_expr.len() == 1
            && state.exact_row_count_expr(&aggregate.aggr_expr[0])
        {
            return Ok(Self::Count);
        }
        if aggregate.group_expr.len() != 1
            || aggregate.aggr_expr.len() != 1
            || !state.exact_row_count_expr(&aggregate.aggr_expr[0])
        {
            return Ok(Self::Local);
        }
        let Some(field) = state.payload_path_for_expr(&aggregate.group_expr[0]) else {
            return Ok(Self::Local);
        };
        let Some(field_type) = state.source.payload_schema.field_for_path(field.key()) else {
            return Ok(Self::Local);
        };
        if !field_type.supports_facet() {
            return Ok(Self::Local);
        }
        Ok(Self::Facet(FacetOp {
            field,
            key_outputs: OutputNames::single(aggregate.schema.field(0).name().clone()),
            count_outputs: OutputNames::single(aggregate.schema.field(1).name().clone()),
        }))
    }
}

fn projection_passthrough_column(expr: &Expr) -> bool {
    matches!(expr.clone().unalias_nested().data, Expr::Column(_))
}
