use std::sync::Arc;

use datafusion::common::{Result, plan_err};
use datafusion::datasource::source_as_provider;
use datafusion::logical_expr::expr::Alias;
use datafusion::logical_expr::{Distinct, Expr, LogicalPlan};

use super::{FiltersState, KernelState, ProcessingState, State};
use crate::analyzer::kernel::{CountKernel, KernelSpec};
use crate::analyzer::op::{FacetOp, Op, OutputNames};
use crate::analyzer::source::Source;
use crate::analyzer::surface::SurfaceCall;
use crate::pushdown::QdrantPayloadPath;
use crate::pushdown::filter::QdrantFilters;
use crate::table::QdrantTableProvider;

#[derive(Debug, Clone)]
pub(crate) struct SourceState {
    pub(crate) source: Source,
    pub(crate) filters: FiltersState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AggregateSurface {
    Local,
    Count,
    Facet(FacetOp),
}

impl AggregateSurface {
    fn of(plan: &LogicalPlan, source: &Source) -> Result<Self> {
        let LogicalPlan::Aggregate(aggregate) = plan else {
            return plan_err!("prototype aggregate state mismatch");
        };
        if aggregate.group_expr.is_empty()
            && aggregate.aggr_expr.len() == 1
            && crate::analyzer::common::count_star_like(&aggregate.aggr_expr[0])
        {
            return Ok(Self::Count);
        }
        if aggregate.group_expr.len() != 1
            || aggregate.aggr_expr.len() != 1
            || !crate::analyzer::common::count_star_like(&aggregate.aggr_expr[0])
        {
            return Ok(Self::Local);
        }
        let Some(field) = QdrantPayloadPath::from_logical_expr(&aggregate.group_expr[0]) else {
            return Ok(Self::Local);
        };
        let Some(field_type) = source.payload_schema.field(field.key()) else {
            return Ok(Self::Local);
        };
        if !field_type.supports_facet() {
            return Ok(Self::Local);
        }
        Ok(Self::Facet(FacetOp {
            field,
            key_outputs: OutputNames::single(aggregate.schema.field(0).name().clone()),
            count_outputs: OutputNames::single(aggregate.schema.field(1).name().clone()),
            sorted: false,
        }))
    }
}

impl SourceState {
    pub(crate) fn from_scan(
        scan: &datafusion::logical_expr::logical_plan::TableScan,
    ) -> Option<Self> {
        let provider = source_as_provider(&scan.source).ok()?;
        let schema = provider.schema();
        let provider = provider.as_any().downcast_ref::<QdrantTableProvider>()?;
        Some(Self {
            source: Source {
                client: Arc::clone(provider.client()),
                collection: provider.collection().to_owned(),
                schema,
                payload_schema: Arc::clone(provider.payload_schema()),
            },
            filters: FiltersState::default(),
        })
    }

    pub(super) fn projection(
        self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        let surface = SurfaceCall::collect(&plan.expressions())?;
        if let Some(surface) = surface {
            return self.open(surface)?.projection(plan, transformed);
        }
        if let LogicalPlan::Projection(projection) = &plan
            && projection.expr.iter().all(|expr| {
                matches!(expr.clone().unalias_nested().data, Expr::Column(_))
                    || matches!(expr, Expr::Alias(Alias { expr, .. }) if matches!(expr.clone().unalias_nested().data, Expr::Column(_)))
            })
        {
            return Ok(super::super::Analysis::new(plan, State::Source(self), transformed));
        }
        Ok(super::super::Analysis::new(plan, State::local(), transformed))
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
                State::Source(Self { source: self.source, filters }),
                transformed,
            ));
        }
        Ok(super::super::Analysis::new(plan, State::local(), transformed))
    }

    pub(super) fn sort(
        self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        let surface = SurfaceCall::collect(&plan.expressions())?;
        let Some(surface) = surface else {
            return Ok(super::super::Analysis::new(plan, State::local(), transformed));
        };
        self.open(surface)?.sort(plan, transformed)
    }

    pub(super) fn limit(
        self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        Ok(super::super::Analysis::new(plan, State::local(), transformed))
    }

    pub(super) fn aggregate(
        self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        if SurfaceCall::collect(&plan.expressions())?.is_some() {
            return Ok(super::super::fatal(
                plan,
                transformed,
                "qdrant surface call is not admitted inside aggregate semantics",
            ));
        }
        match AggregateSurface::of(&plan, &self.source)? {
            AggregateSurface::Local => {
                Ok(super::super::Analysis::new(plan, State::local(), transformed))
            }
            AggregateSurface::Count => {
                let exact_filters = self.filters.exact(&self.source)?;
                KernelState::new(KernelSpec::Count(CountKernel::new(self.source, exact_filters)))
                    .absorb(plan, transformed)
            }
            AggregateSurface::Facet(op) => {
                ProcessingState { source: self.source, filters: self.filters, op: Op::Facet(op) }
                    .absorb(plan, transformed)
            }
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
        if SurfaceCall::collect(&plan.expressions())?.is_some() {
            return Ok(super::super::fatal(
                plan,
                transformed,
                "qdrant surface call requires projection, filter, or sort",
            ));
        }
        Ok(super::super::Analysis::new(plan, State::local(), transformed))
    }

    fn open(&self, surface: SurfaceCall) -> Result<ProcessingState> {
        Ok(ProcessingState {
            source: self.source.clone(),
            filters: self.filters.clone(),
            op: Op::from_surface(surface, &self.source)?,
        })
    }
}
