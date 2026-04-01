use std::sync::Arc;

use datafusion::common::tree_node::TreeNode;
use datafusion::common::{DFSchemaRef, Result};
use datafusion::logical_expr::{Extension, LogicalPlan};

use super::{FiltersState, ProcessingState, State};
use crate::analyzer::kernel::{KernelSpec, QueryBatchKernel, QueryKernel};
use crate::analyzer::node::KernelNode;
use crate::analyzer::source::{MergeableSetJoin, MergeableUnion};
use crate::analyzer::surface::SurfaceCall;

#[derive(Debug, Clone)]
pub(crate) enum CompositeState {
    Mergeable(Box<MergeableState>),
    Batchable(BatchableState),
    Coordinated(CoordinatedState),
}

impl CompositeState {
    pub(crate) fn from_plan(plan: &LogicalPlan, children: &[State]) -> Result<Option<Self>> {
        if let Some(state) = MergeableState::from_plan(plan, children)? {
            return Ok(Some(Self::Mergeable(Box::new(state))));
        }
        if let Some(state) = BatchableState::from_plan(plan, children) {
            return Ok(Some(Self::Batchable(state)));
        }
        if let Some(state) = CoordinatedState::from_plan(plan, children) {
            return Ok(Some(Self::Coordinated(state)));
        }
        Ok(None)
    }

    pub(crate) fn finish_root(self, plan: LogicalPlan) -> Result<LogicalPlan> {
        match self {
            Self::Mergeable(state) => state.rewrite_current(&plan)?.ok_or_else(|| {
                datafusion::error::DataFusionError::Plan(
                    "unfinished mergeable qdrant composite at query root".to_owned(),
                )
            }),
            Self::Batchable(state) => state.finish_root(&plan),
            Self::Coordinated(_) => CoordinatedState::finish_root(plan),
        }
    }

    pub(super) fn projection(
        self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        match self {
            Self::Mergeable(state) => state.projection(plan, transformed),
            Self::Batchable(state) => state.projection(plan, transformed),
            Self::Coordinated(state) => state.projection(plan, transformed),
        }
    }

    pub(super) fn filter(
        self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        match self {
            Self::Mergeable(state) => state.filter(plan, transformed),
            Self::Batchable(state) => state.filter(plan, transformed),
            Self::Coordinated(state) => state.filter(plan, transformed),
        }
    }

    pub(super) fn sort(
        self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        match self {
            Self::Mergeable(state) => state.sort(plan, transformed),
            Self::Batchable(state) => state.sort(plan, transformed),
            Self::Coordinated(state) => state.sort(plan, transformed),
        }
    }

    pub(super) fn limit(
        self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        match self {
            Self::Mergeable(state) => state.limit(plan, transformed),
            Self::Batchable(state) => Ok(state.limit(plan, transformed)),
            Self::Coordinated(state) => state.limit(plan, transformed),
        }
    }

    pub(super) fn aggregate(
        self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        match self {
            Self::Mergeable(state) => state.aggregate(plan, transformed),
            Self::Batchable(state) => Ok(state.aggregate(plan, transformed)),
            Self::Coordinated(state) => state.aggregate(plan, transformed),
        }
    }

    pub(super) fn unary(
        self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
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

    fn projection(self, plan: LogicalPlan, transformed: bool) -> Result<super::super::Analysis> {
        self.consume_or_preserve(plan, transformed)
    }

    fn filter(self, plan: LogicalPlan, transformed: bool) -> Result<super::super::Analysis> {
        self.consume_or_preserve(plan, transformed)
    }

    fn sort(self, plan: LogicalPlan, transformed: bool) -> Result<super::super::Analysis> {
        self.consume_or_preserve(plan, transformed)
    }

    fn limit(self, plan: LogicalPlan, transformed: bool) -> Result<super::super::Analysis> {
        self.consume_or_preserve(plan, transformed)
    }

    fn aggregate(self, plan: LogicalPlan, transformed: bool) -> Result<super::super::Analysis> {
        self.consume_or_preserve(plan, transformed)
    }

    fn unary(self, plan: LogicalPlan, transformed: bool) -> Result<super::super::Analysis> {
        self.consume_or_preserve(plan, transformed)
    }

    fn consume_or_preserve(
        self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        if let Some(rewritten) = self.rewrite_current(&plan)? {
            return super::super::analyze_plan(rewritten);
        }
        if self.kind.preserves(&plan) {
            return Ok(super::super::Analysis::new(
                plan,
                State::Composite(CompositeState::Mergeable(Box::new(self))),
                transformed,
            ));
        }
        Ok(super::super::fatal(
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
    queries: Vec<QueryKernel>,
}

impl BatchableState {
    fn from_plan(plan: &LogicalPlan, children: &[State]) -> Option<Self> {
        if !matches!(plan, LogicalPlan::Union(_)) {
            return None;
        }
        let queries = children
            .iter()
            .map(|state| match state {
                State::Kernel(kernel) => match kernel.spec() {
                    KernelSpec::Query(query) => Some(query.clone()),
                    _ => None,
                },
                _ => None,
            })
            .collect::<Option<Vec<_>>>()?;
        (!queries.is_empty()).then_some(Self { queries })
    }

    fn projection(self, plan: LogicalPlan, transformed: bool) -> Result<super::super::Analysis> {
        if let Some(surface) = SurfaceCall::collect(&plan.expressions())? {
            return self.open(surface, plan.schema())?.projection(plan, transformed);
        }
        Ok(self.pass_or_fail(plan, transformed))
    }

    fn filter(self, plan: LogicalPlan, transformed: bool) -> Result<super::super::Analysis> {
        if let Some(surface) = SurfaceCall::collect(&plan.expressions())? {
            return self.open(surface, plan.schema())?.filter(plan, transformed);
        }
        Ok(self.pass_or_fail(plan, transformed))
    }

    fn sort(self, plan: LogicalPlan, transformed: bool) -> Result<super::super::Analysis> {
        if let Some(surface) = SurfaceCall::collect(&plan.expressions())? {
            return self.open(surface, plan.schema())?.sort(plan, transformed);
        }
        Ok(self.pass_or_fail(plan, transformed))
    }

    fn limit(self, plan: LogicalPlan, transformed: bool) -> super::super::Analysis {
        self.pass_or_fail(plan, transformed)
    }

    fn aggregate(self, plan: LogicalPlan, transformed: bool) -> super::super::Analysis {
        self.pass_or_fail(plan, transformed)
    }

    fn unary(self, plan: LogicalPlan, transformed: bool) -> Result<super::super::Analysis> {
        if let Some(surface) = SurfaceCall::collect(&plan.expressions())? {
            return self.open(surface, plan.schema())?.unary(plan, transformed);
        }
        Ok(self.pass_or_fail(plan, transformed))
    }

    fn pass_or_fail(self, plan: LogicalPlan, transformed: bool) -> super::super::Analysis {
        if matches!(plan, LogicalPlan::SubqueryAlias(_)) {
            return super::super::Analysis::new(
                plan,
                State::Composite(CompositeState::Batchable(self)),
                transformed,
            );
        }
        super::super::fatal(
            plan,
            transformed,
            format!(
                "batchable qdrant composite with {} branches is not yet closed",
                self.queries.len()
            ),
        )
    }

    fn open(self, surface: SurfaceCall, _output_schema: &DFSchemaRef) -> Result<ProcessingState> {
        let source = self.queries.first().expect("validated batchable state").source().clone();
        let prefetch = self
            .queries
            .iter()
            .map(QueryKernel::prefetch_branch_unqualified)
            .collect::<Result<Vec<_>>>()?;
        let op =
            crate::analyzer::op::Op::from_surface(surface, &source)?.with_prefetch(prefetch)?;
        Ok(ProcessingState { source, filters: FiltersState::default(), op })
    }

    fn finish_root(self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        let schema = Arc::clone(plan.schema());
        let kernel = QueryBatchKernel::try_new(self.queries)?;
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(KernelNode::new(schema, KernelSpec::QueryBatch(kernel))),
        }))
    }
}

#[expect(
    dead_code,
    reason = "coordinated execution bookkeeping is scaffolded for future closure work"
)]
#[derive(Debug, Clone)]
pub(crate) struct CoordinatedState {
    branches:    usize,
    outstanding: usize,
    qdrant:      usize,
}

impl CoordinatedState {
    fn from_plan(_plan: &LogicalPlan, children: &[State]) -> Option<Self> {
        let outstanding =
            children.iter().filter(|state| state.requires_composite_coordination()).count();
        let qdrant = children.iter().filter(|state| state.is_qdrant_present()).count();
        // Multiple closed qdrant child kernels can compose locally. Coordination is only needed
        // while some child branch still carries unfinished qdrant work.
        (outstanding > 0).then_some(Self { branches: children.len(), outstanding, qdrant })
    }

    fn finish_root(plan: LogicalPlan) -> Result<LogicalPlan> {
        let with_subqueries = plan.map_subqueries(|subquery| {
            super::super::analyze_root(subquery).map(|analysis| analysis.transformed)
        })?;
        Ok(with_subqueries
            .data
            .map_children(|child| {
                super::super::analyze_root(child).map(|analysis| analysis.transformed)
            })?
            .data)
    }

    fn projection(self, plan: LogicalPlan, transformed: bool) -> Result<super::super::Analysis> {
        self.pass_or_fail(plan, transformed)
    }

    fn filter(self, plan: LogicalPlan, transformed: bool) -> Result<super::super::Analysis> {
        self.pass_or_fail(plan, transformed)
    }

    fn sort(self, plan: LogicalPlan, transformed: bool) -> Result<super::super::Analysis> {
        self.pass_or_fail(plan, transformed)
    }

    fn limit(self, plan: LogicalPlan, transformed: bool) -> Result<super::super::Analysis> {
        self.pass_or_fail(plan, transformed)
    }

    fn aggregate(self, plan: LogicalPlan, transformed: bool) -> Result<super::super::Analysis> {
        self.pass_or_fail(plan, transformed)
    }

    fn unary(self, plan: LogicalPlan, transformed: bool) -> Result<super::super::Analysis> {
        self.pass_or_fail(plan, transformed)
    }

    fn pass_or_fail(self, plan: LogicalPlan, transformed: bool) -> Result<super::super::Analysis> {
        if SurfaceCall::collect(&plan.expressions())?.is_some() {
            return Ok(super::super::fatal(
                plan,
                transformed,
                "qdrant surface calls may not cross coordinated multi-branch boundaries",
            ));
        }
        Ok(super::super::Analysis::new(
            plan,
            State::Composite(CompositeState::Coordinated(self)),
            transformed,
        ))
    }
}
