use std::sync::Arc;

use datafusion::common::{DFSchemaRef, Result, plan_err};
use datafusion::datasource::source_as_provider;
use datafusion::logical_expr::expr::Alias;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Distinct, Expr, Extension, LogicalPlan};

use super::node::{STATE_NODE_NAME, StateNode};
use super::kernel::{CountKernel, KernelSpec};
use super::op::Op;
use super::source::{MergeableSetJoin, MergeableUnion, Source};
use super::surface::SurfaceCall;
use super::{Analysis, analyze_plan};
use crate::analyzer::fatal;
use crate::pushdown::filter::QdrantFilters;
use crate::table::QdrantTableProvider;

#[derive(Debug, Clone)]
pub(super) struct SemanticError {
    pub(super) message: String,
}

impl SemanticError {
    fn new(message: impl Into<String>) -> Self { Self { message: message.into() } }
}

// ============================================================================
// State
// ============================================================================

#[derive(Debug, Clone)]
pub(crate) enum State {
    Local(LocalState),
    Source(SourceState),
    Processing(ProcessingState),
    Composite(CompositeState),
    Kernel(KernelState),
    Fatal(FatalState),
}

impl State {
    pub(super) fn local() -> Self { Self::Local(LocalState) }

    pub(super) fn fatal(message: impl Into<String>) -> Self {
        Self::Fatal(FatalState { error: SemanticError::new(message) })
    }

    pub(super) fn materialized(self, schema: DFSchemaRef) -> Result<StateNode> {
        match self {
            Self::Processing(_) | Self::Kernel(_) => Ok(StateNode { schema, state: self }),
            _ => plan_err!("{STATE_NODE_NAME} only materializes processing and kernel states"),
        }
    }

    pub(super) fn projection(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        match self {
            Self::Local(state) => state.projection(plan, transformed),
            Self::Source(state) => state.projection(plan, transformed),
            Self::Processing(state) => state.projection(plan, transformed),
            Self::Composite(state) => state.projection(plan, transformed),
            Self::Kernel(state) => state.projection(plan, transformed),
            Self::Fatal(state) => state.projection(plan, transformed),
        }
    }

    pub(super) fn filter(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        match self {
            Self::Local(state) => state.filter(plan, transformed),
            Self::Source(state) => state.filter(plan, transformed),
            Self::Processing(state) => state.filter(plan, transformed),
            Self::Composite(state) => state.filter(plan, transformed),
            Self::Kernel(state) => state.filter(plan, transformed),
            Self::Fatal(state) => state.filter(plan, transformed),
        }
    }

    pub(super) fn sort(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        match self {
            Self::Local(state) => state.sort(plan, transformed),
            Self::Source(state) => state.sort(plan, transformed),
            Self::Processing(state) => state.sort(plan, transformed),
            Self::Composite(state) => state.sort(plan, transformed),
            Self::Kernel(state) => state.sort(plan, transformed),
            Self::Fatal(state) => state.sort(plan, transformed),
        }
    }

    pub(super) fn limit(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        match self {
            Self::Local(state) => state.limit(plan, transformed),
            Self::Source(state) => state.limit(plan, transformed),
            Self::Processing(state) => state.limit(plan, transformed),
            Self::Composite(state) => state.limit(plan, transformed),
            Self::Kernel(state) => state.limit(plan, transformed),
            Self::Fatal(state) => state.limit(plan, transformed),
        }
    }

    pub(super) fn aggregate(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        match self {
            Self::Local(state) => state.aggregate(plan, transformed),
            Self::Source(state) => state.aggregate(plan, transformed),
            Self::Processing(state) => state.aggregate(plan, transformed),
            Self::Composite(state) => state.aggregate(plan, transformed),
            Self::Kernel(state) => state.aggregate(plan, transformed),
            Self::Fatal(state) => state.aggregate(plan, transformed),
        }
    }

    pub(super) fn unary(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        match self {
            Self::Local(state) => state.unary(plan, transformed),
            Self::Source(state) => state.unary(plan, transformed),
            Self::Processing(state) => state.unary(plan, transformed),
            Self::Composite(state) => state.unary(plan, transformed),
            Self::Kernel(state) => state.unary(plan, transformed),
            Self::Fatal(state) => state.unary(plan, transformed),
        }
    }
}

impl State {
    fn is_qdrant_present(&self) -> bool {
        matches!(self, Self::Source(_) | Self::Processing(_) | Self::Composite(_) | Self::Kernel(_))
    }

    pub(super) fn requires_composite_coordination(&self) -> bool {
        matches!(self, Self::Processing(_) | Self::Composite(_))
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct LocalState;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AggregateSurface {
    Local,
    Count,
    Facet(super::op::FacetOp),
}

impl AggregateSurface {
    fn of(plan: &LogicalPlan, source: &Source) -> Result<Self> {
        let LogicalPlan::Aggregate(aggregate) = plan else {
            return plan_err!("prototype aggregate state mismatch");
        };
        if aggregate.group_expr.is_empty()
            && aggregate.aggr_expr.len() == 1
            && super::common::count_star_like(&aggregate.aggr_expr[0])
        {
            return Ok(Self::Count);
        }
        if aggregate.group_expr.len() != 1
            || aggregate.aggr_expr.len() != 1
            || !super::common::count_star_like(&aggregate.aggr_expr[0])
        {
            return Ok(Self::Local);
        }
        let Some(field) = crate::pushdown::QdrantPayloadPath::from_logical_expr(&aggregate.group_expr[0]) else {
            return Ok(Self::Local);
        };
        let Some(field_type) = source.payload_schema.field(field.key()) else {
            return Ok(Self::Local);
        };
        if !field_type.supports_facet() {
            return Ok(Self::Local);
        }
        Ok(Self::Facet(super::op::FacetOp {
            field,
            key_outputs: super::op::OutputNames::single(aggregate.schema.field(0).name().clone()),
            count_outputs: super::op::OutputNames::single(aggregate.schema.field(1).name().clone()),
            sorted: false,
        }))
    }
}

impl LocalState {
    fn projection(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        if SurfaceCall::collect(&plan.expressions())?.is_some() {
            return Ok(fatal(plan, transformed, "qdrant surface call requires a qdrant source"));
        }
        Ok(Analysis::new(plan, State::Local(self), transformed))
    }

    fn filter(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        if SurfaceCall::collect(&plan.expressions())?.is_some() {
            return Ok(fatal(plan, transformed, "qdrant surface call requires a qdrant source"));
        }
        Ok(Analysis::new(plan, State::Local(self), transformed))
    }

    fn sort(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        if SurfaceCall::collect(&plan.expressions())?.is_some() {
            return Ok(fatal(plan, transformed, "qdrant surface call requires a qdrant source"));
        }
        Ok(Analysis::new(plan, State::Local(self), transformed))
    }

    fn limit(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        Ok(Analysis::new(plan, State::Local(self), transformed))
    }

    fn aggregate(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        if SurfaceCall::collect(&plan.expressions())?.is_some() {
            return Ok(fatal(plan, transformed, "qdrant surface call requires a qdrant source"));
        }
        Ok(Analysis::new(plan, State::Local(self), transformed))
    }

    fn unary(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        if SurfaceCall::collect(&plan.expressions())?.is_some() {
            return Ok(fatal(plan, transformed, "qdrant surface call requires a qdrant source"));
        }
        Ok(Analysis::new(plan, State::Local(self), transformed))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SourceState {
    pub(super) source:  Source,
    pub(super) filters: FiltersState,
}

impl SourceState {
    pub(super) fn from_scan(
        scan: &datafusion::logical_expr::logical_plan::TableScan,
    ) -> Option<Self> {
        let provider = source_as_provider(&scan.source).ok()?;
        let schema = provider.schema();
        let provider = provider.as_any().downcast_ref::<QdrantTableProvider>()?;
        Some(Self {
            source:  Source {
                client: Arc::clone(provider.client()),
                collection: provider.collection().to_owned(),
                schema,
                payload_schema: Arc::clone(provider.payload_schema()),
            },
            filters: FiltersState::default(),
        })
    }

    fn projection(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        let surface = SurfaceCall::collect(&plan.expressions())?;
        if let Some(surface) = surface {
            return self.open(surface)?.projection(plan, transformed);
        }
        if let LogicalPlan::Projection(projection) = &plan &&
            projection.expr.iter().all(|expr| {
                matches!(expr.clone().unalias_nested().data, Expr::Column(_))
                    || matches!(expr, Expr::Alias(Alias { expr, .. }) if matches!(expr.clone().unalias_nested().data, Expr::Column(_)))
            }) {
                return Ok(Analysis::new(plan, State::Source(self), transformed));
        };
        Ok(Analysis::new(plan, State::local(), transformed))
    }

    fn filter(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
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
            return Ok(Analysis::new(
                plan,
                State::Source(Self { source: self.source, filters }),
                transformed,
            ));
        }
        Ok(Analysis::new(plan, State::local(), transformed))
    }

    fn sort(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        let surface = SurfaceCall::collect(&plan.expressions())?;
        let Some(surface) = surface else {
            return Ok(Analysis::new(plan, State::local(), transformed));
        };
        self.open(surface)?.sort(plan, transformed)
    }

    fn limit(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        Ok(Analysis::new(plan, State::local(), transformed))
    }

    fn aggregate(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        if SurfaceCall::collect(&plan.expressions())?.is_some() {
            return Ok(fatal(
                plan,
                transformed,
                "qdrant surface call is not admitted inside aggregate semantics",
            ));
        }
        match AggregateSurface::of(&plan, &self.source)? {
            AggregateSurface::Local => Ok(Analysis::new(plan, State::local(), transformed)),
            AggregateSurface::Count => KernelState {
                spec: KernelSpec::Count(CountKernel {
                    collection: self.source.collection,
                    filters:    self.filters,
                }),
            }
            .absorb(plan, transformed),
            AggregateSurface::Facet(op) => ProcessingState {
                source:  self.source,
                filters: self.filters,
                op:      Op::Facet(op),
            }
            .absorb(plan, transformed),
        }
    }

    fn unary(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        if matches!(plan, LogicalPlan::SubqueryAlias(_)) {
            return Ok(Analysis::new(plan, State::Source(self), transformed));
        }
        if let LogicalPlan::Distinct(Distinct::All(input)) = &plan {
            return Ok(Analysis::new(input.as_ref().clone(), State::Source(self), true));
        }
        if SurfaceCall::collect(&plan.expressions())?.is_some() {
            return Ok(fatal(
                plan,
                transformed,
                "qdrant surface call requires projection, filter, or sort",
            ));
        }
        Ok(Analysis::new(plan, State::local(), transformed))
    }

    fn open(&self, surface: SurfaceCall) -> Result<ProcessingState> {
        Ok(ProcessingState {
            source:  self.source.clone(),
            filters: self.filters.clone(),
            op:      Op::from_surface(surface, &self.source)?,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ProcessingState {
    pub(super) source:  Source,
    pub(super) filters: FiltersState,
    pub(super) op:      Op,
}

impl ProcessingState {
    fn projection(mut self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        let Some(op) = self.op.project(&plan)? else {
            return Ok(fatal(
                plan,
                transformed,
                "processing projection is not admitted by the current qdrant op",
            ));
        };
        self.op = op;
        self.absorb(plan, transformed)
    }

    fn filter(mut self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        let LogicalPlan::Filter(filter) = &plan else {
            return plan_err!("prototype filter state mismatch");
        };
        let Some(op) = self.op.filter(&self.source, &mut self.filters, &filter.predicate)? else {
            return Ok(fatal(
                plan,
                transformed,
                "processing filter is not admitted by the current qdrant op",
            ));
        };
        self.op = op;
        self.absorb(plan, transformed)
    }

    fn sort(mut self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        let Some(op) = self.op.sort(&plan)? else {
            return Ok(fatal(
                plan,
                transformed,
                "processing sort is not admitted by the current qdrant op",
            ));
        };
        self.op = op;
        self.absorb(plan, transformed)
    }

    fn limit(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        let Some(kernel) = self.op.kernel(self.source.collection.clone(), self.filters, &plan)?
        else {
            return Ok(fatal(
                plan,
                transformed,
                "processing limit is not admitted by the current qdrant op",
            ));
        };
        kernel.absorb(plan, transformed)
    }

    fn aggregate(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        Ok(fatal(plan, transformed, "nested aggregate above qdrant processing is invalid"))
    }

    fn unary(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        if matches!(plan, LogicalPlan::SubqueryAlias(_)) {
            return self.absorb(plan, transformed);
        }
        Ok(fatal(
            plan,
            transformed,
            "qdrant processing must close to a kernel or stay region-owned",
        ))
    }

    fn absorb(self, plan: LogicalPlan, _transformed: bool) -> Result<Analysis> {
        let schema = Arc::clone(plan.schema());
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(State::Processing(self.clone()).materialized(schema)?),
        });
        Ok(Analysis::new(plan, State::Processing(self), true))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct KernelState {
    pub(super) spec: KernelSpec,
}

impl KernelState {
    fn projection(mut self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        let maybe_spec = self.spec.project(&plan)?;
        if let Some(spec) = maybe_spec {
            self.spec = spec;
            return self.absorb(plan, transformed);
        }
        if SurfaceCall::collect(&plan.expressions())?.is_some() {
            return Ok(fatal(
                plan,
                transformed,
                "surface call above kernel does not match the extracted qdrant kernel",
            ));
        }
        Ok(Analysis::new(plan, State::local(), transformed))
    }

    fn filter(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        if SurfaceCall::collect(&plan.expressions())?.is_some() {
            return Ok(fatal(
                plan,
                transformed,
                "surface call above kernel does not match the extracted qdrant kernel",
            ));
        }
        Ok(Analysis::new(plan, State::local(), transformed))
    }

    fn sort(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        if SurfaceCall::collect(&plan.expressions())?.is_some() {
            return Ok(fatal(
                plan,
                transformed,
                "surface call above kernel does not match the extracted qdrant kernel",
            ));
        }
        Ok(Analysis::new(plan, State::local(), transformed))
    }

    fn limit(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        Ok(Analysis::new(plan, State::local(), transformed))
    }

    fn aggregate(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        Ok(Analysis::new(plan, State::local(), transformed))
    }

    fn unary(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        if matches!(plan, LogicalPlan::SubqueryAlias(_)) {
            return self.absorb(plan, transformed);
        }
        Ok(Analysis::new(plan, State::local(), transformed))
    }

    fn absorb(self, plan: LogicalPlan, _transformed: bool) -> Result<Analysis> {
        let schema = Arc::clone(plan.schema());
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(State::Kernel(self.clone()).materialized(schema)?),
        });
        Ok(Analysis::new(plan, State::Kernel(self), true))
    }
}

#[derive(Debug, Clone)]
pub(crate) enum CompositeState {
    Mergeable(MergeableState),
    Batchable(BatchableState),
    Coordinated(CoordinatedState),
}

impl CompositeState {
    pub(super) fn from_plan(plan: &LogicalPlan, children: &[State]) -> Result<Option<Self>> {
        if let Some(state) = MergeableState::from_plan(plan, children)? {
            return Ok(Some(Self::Mergeable(state)));
        }
        if let Some(state) = BatchableState::from_plan(plan, children) {
            return Ok(Some(Self::Batchable(state)));
        }
        if let Some(state) = CoordinatedState::from_plan(plan, children) {
            return Ok(Some(Self::Coordinated(state)));
        }
        Ok(None)
    }

    pub(super) fn finish_root(self, plan: LogicalPlan) -> Result<LogicalPlan> {
        match self {
            Self::Mergeable(state) => state.rewrite_current(&plan)?.ok_or_else(|| {
                datafusion::error::DataFusionError::Plan(
                    "unfinished mergeable qdrant composite at query root".to_owned(),
                )
            }),
            Self::Batchable(_) => {
                plan_err!("unfinished batchable qdrant composite at query root")
            }
            Self::Coordinated(_) => {
                plan_err!("unfinished coordinated qdrant composite at query root")
            }
        }
    }

    fn projection(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        match self {
            Self::Mergeable(state) => state.projection(plan, transformed),
            Self::Batchable(state) => state.projection(plan, transformed),
            Self::Coordinated(state) => state.projection(plan, transformed),
        }
    }

    fn filter(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        match self {
            Self::Mergeable(state) => state.filter(plan, transformed),
            Self::Batchable(state) => state.filter(plan, transformed),
            Self::Coordinated(state) => state.filter(plan, transformed),
        }
    }

    fn sort(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        match self {
            Self::Mergeable(state) => state.sort(plan, transformed),
            Self::Batchable(state) => state.sort(plan, transformed),
            Self::Coordinated(state) => state.sort(plan, transformed),
        }
    }

    fn limit(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        match self {
            Self::Mergeable(state) => state.limit(plan, transformed),
            Self::Batchable(state) => state.limit(plan, transformed),
            Self::Coordinated(state) => state.limit(plan, transformed),
        }
    }

    fn aggregate(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        match self {
            Self::Mergeable(state) => state.aggregate(plan, transformed),
            Self::Batchable(state) => state.aggregate(plan, transformed),
            Self::Coordinated(state) => state.aggregate(plan, transformed),
        }
    }

    fn unary(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        match self {
            Self::Mergeable(state) => state.unary(plan, transformed),
            Self::Batchable(state) => state.unary(plan, transformed),
            Self::Coordinated(state) => state.unary(plan, transformed),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct MergeableState {
    kind: MergeableKind,
}

impl MergeableState {
    fn from_plan(plan: &LogicalPlan, children: &[State]) -> Result<Option<Self>> {
        if let Some(union) = MergeableUnion::from_plan(plan, children)? {
            return Ok(Some(Self { kind: MergeableKind::Union(union) }));
        }
        if let Some(set_join) = MergeableSetJoin::from_plan(plan, children)? {
            return Ok(Some(Self { kind: MergeableKind::SetJoin(set_join) }));
        }
        Ok(None)
    }

    fn projection(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        self.consume_or_preserve(plan, transformed)
    }

    fn filter(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        self.consume_or_preserve(plan, transformed)
    }

    fn sort(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        self.consume_or_preserve(plan, transformed)
    }

    fn limit(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        self.consume_or_preserve(plan, transformed)
    }

    fn aggregate(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        self.consume_or_preserve(plan, transformed)
    }

    fn unary(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        self.consume_or_preserve(plan, transformed)
    }

    fn consume_or_preserve(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        if let Some(rewritten) = self.rewrite_current(&plan)? {
            return analyze_plan(rewritten);
        }
        if self.kind.preserves(&plan) {
            return Ok(Analysis::new(
                plan,
                State::Composite(CompositeState::Mergeable(self)),
                transformed,
            ));
        }
        Ok(fatal(
            plan,
            transformed,
            "mergeable qdrant composite may not cross this boundary before collapsing",
        ))
    }

    fn rewrite_current(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        self.kind.rewrite_current(plan)
    }
}

#[derive(Debug, Clone)]
pub(crate) enum MergeableKind {
    Union(MergeableUnion),
    SetJoin(MergeableSetJoin),
}

impl MergeableKind {
    fn preserves(&self, plan: &LogicalPlan) -> bool {
        matches!(plan, LogicalPlan::SubqueryAlias(_))
            && matches!(self, Self::Union(union) if !union.can_union_all_merge())
    }

    fn rewrite_current(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        match self {
            Self::Union(union) => union.rewrite_current(plan),
            Self::SetJoin(join) => join.rewrite_current(plan),
        }
    }
}


#[derive(Debug, Clone)]
pub(crate) struct BatchableState {
    branches: usize,
}

impl BatchableState {
    fn from_plan(plan: &LogicalPlan, children: &[State]) -> Option<Self> {
        matches!(plan, LogicalPlan::Union(_))
            .then_some(children)
            .filter(|states| {
                !states.is_empty() && states.iter().all(|state| matches!(state, State::Kernel(_)))
            })
            .map(|states| Self { branches: states.len() })
    }

    fn projection(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        self.pass_or_fail(plan, transformed)
    }

    fn filter(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        self.pass_or_fail(plan, transformed)
    }

    fn sort(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        self.pass_or_fail(plan, transformed)
    }

    fn limit(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        self.pass_or_fail(plan, transformed)
    }

    fn aggregate(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        self.pass_or_fail(plan, transformed)
    }

    fn unary(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        self.pass_or_fail(plan, transformed)
    }

    fn pass_or_fail(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        if matches!(plan, LogicalPlan::SubqueryAlias(_)) {
            return Ok(Analysis::new(
                plan,
                State::Composite(CompositeState::Batchable(self)),
                transformed,
            ));
        }
        Ok(fatal(
            plan,
            transformed,
            format!("batchable qdrant composite with {} branches is not yet closed", self.branches),
        ))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CoordinatedState {
    pub(super) branches: usize,
}

impl CoordinatedState {
    fn from_plan(_plan: &LogicalPlan, children: &[State]) -> Option<Self> {
        let outstanding =
            children.iter().filter(|state| state.requires_composite_coordination()).count();
        let qdrant = children.iter().filter(|state| state.is_qdrant_present()).count();
        (outstanding > 0 || qdrant > 1).then_some(Self { branches: children.len() })
    }

    fn projection(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        self.pass_or_fail(plan, transformed)
    }

    fn filter(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        self.pass_or_fail(plan, transformed)
    }

    fn sort(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        self.pass_or_fail(plan, transformed)
    }

    fn limit(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        self.pass_or_fail(plan, transformed)
    }

    fn aggregate(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        self.pass_or_fail(plan, transformed)
    }

    fn unary(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        self.pass_or_fail(plan, transformed)
    }

    fn pass_or_fail(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        if matches!(plan, LogicalPlan::SubqueryAlias(_)) {
            return Ok(Analysis::new(
                plan,
                State::Composite(CompositeState::Coordinated(self)),
                transformed,
            ));
        }
        Ok(fatal(
            plan,
            transformed,
            format!(
                "coordinated qdrant composite with {} branches is not yet closed",
                self.branches
            ),
        ))
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct FiltersState {
    pub(super) exprs: Vec<Expr>,
}

impl FiltersState {
    fn push(mut self, expr: Expr) -> Self {
        self.exprs.push(expr);
        self
    }

    pub(super) fn exact(&self, source: &Source) -> Result<QdrantFilters> {
        QdrantFilters::try_new(&source.schema, &source.payload_schema, &self.exprs)
    }

    pub(super) fn combined_expr(&self) -> Option<Expr> { conjunction(self.exprs.clone()) }
}

#[derive(Debug, Clone)]
pub(crate) struct FatalState {
    pub(super) error: SemanticError,
}

impl FatalState {
    fn projection(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        Ok(Analysis::new(plan, State::Fatal(self), transformed))
    }

    fn filter(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        Ok(Analysis::new(plan, State::Fatal(self), transformed))
    }

    fn sort(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        Ok(Analysis::new(plan, State::Fatal(self), transformed))
    }

    fn limit(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        Ok(Analysis::new(plan, State::Fatal(self), transformed))
    }

    fn aggregate(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        Ok(Analysis::new(plan, State::Fatal(self), transformed))
    }

    fn unary(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        Ok(Analysis::new(plan, State::Fatal(self), transformed))
    }
}
