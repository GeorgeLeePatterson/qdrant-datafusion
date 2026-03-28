use std::sync::Arc;

use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::{Extension, LogicalPlan};

use super::State;
use crate::analyzer::kernel::{KernelSpec, QueryBatchKernel, QueryKernel};
use crate::analyzer::node::KernelNode;
use crate::analyzer::source::{MergeableSetJoin, MergeableUnion};

#[derive(Debug, Clone)]
pub(crate) enum CompositeState {
    Mergeable(MergeableState),
    Batchable(BatchableState),
    Coordinated(CoordinatedState),
}

impl CompositeState {
    pub(crate) fn from_plan(plan: &LogicalPlan, children: &[State]) -> Result<Option<Self>> {
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

    pub(crate) fn coordinated(branches: usize) -> Self {
        Self::Coordinated(CoordinatedState { branches })
    }

    pub(crate) fn finish_root(self, plan: LogicalPlan) -> Result<LogicalPlan> {
        match self {
            Self::Mergeable(state) => state.rewrite_current(&plan)?.ok_or_else(|| {
                datafusion::error::DataFusionError::Plan(
                    "unfinished mergeable qdrant composite at query root".to_owned(),
                )
            }),
            Self::Batchable(state) => state.finish_root(plan),
            Self::Coordinated(_) => {
                plan_err!("unfinished coordinated qdrant composite at query root")
            }
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
            Self::Batchable(state) => state.limit(plan, transformed),
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
            Self::Batchable(state) => state.aggregate(plan, transformed),
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
                State::Composite(CompositeState::Mergeable(self)),
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
    branches: usize,
}

impl BatchableState {
    fn from_plan(plan: &LogicalPlan, children: &[State]) -> Option<Self> {
        matches!(plan, LogicalPlan::Union(_))
            .then_some(children)
            .filter(|states| {
                !states.is_empty()
                    && states.iter().all(|state| {
                        matches!(state, State::Kernel(kernel) if matches!(kernel.spec(), KernelSpec::Query(_)))
                    })
            })
            .map(|states| Self { branches: states.len() })
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
        if matches!(plan, LogicalPlan::SubqueryAlias(_)) {
            return Ok(super::super::Analysis::new(
                plan,
                State::Composite(CompositeState::Batchable(self)),
                transformed,
            ));
        }
        Ok(super::super::fatal(
            plan,
            transformed,
            format!("batchable qdrant composite with {} branches is not yet closed", self.branches),
        ))
    }

    fn finish_root(self, plan: LogicalPlan) -> Result<LogicalPlan> {
        let schema = Arc::clone(plan.schema());
        let kernel = QueryBatchKernel::try_new(self.extract_query_kernels(&plan)?)?;
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(KernelNode::new(schema, KernelSpec::QueryBatch(kernel))),
        }))
    }

    fn extract_query_kernels(&self, plan: &LogicalPlan) -> Result<Vec<QueryKernel>> {
        match plan {
            LogicalPlan::Union(union) => union
                .inputs
                .iter()
                .map(|plan| Self::extract_query_kernel_branch(plan.as_ref()))
                .collect(),
            LogicalPlan::SubqueryAlias(alias) => self.extract_query_kernels(alias.input.as_ref()),
            _ => plan_err!(
                "batchable qdrant composite with {} branches could not close to a query batch kernel",
                self.branches
            ),
        }
    }

    fn extract_query_kernel_branch(plan: &LogicalPlan) -> Result<QueryKernel> {
        match plan {
            LogicalPlan::Extension(extension) => {
                let Some(node) = extension.node.as_any().downcast_ref::<KernelNode>() else {
                    return plan_err!("batchable qdrant composite branch is not a qdrant kernel node");
                };
                let KernelSpec::Query(kernel) = node.spec() else {
                    return plan_err!("batchable qdrant composite branch is not a query kernel");
                };
                Ok(kernel.clone())
            }
            LogicalPlan::SubqueryAlias(alias) => Self::extract_query_kernel_branch(alias.input.as_ref()),
            _ => plan_err!("batchable qdrant composite branch is not reducible to a query kernel"),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CoordinatedState {
    branches: usize,
}

impl CoordinatedState {
    fn from_plan(_plan: &LogicalPlan, children: &[State]) -> Option<Self> {
        let outstanding =
            children.iter().filter(|state| state.requires_composite_coordination()).count();
        let qdrant = children.iter().filter(|state| state.is_qdrant_present()).count();
        (outstanding > 0 || qdrant > 1).then_some(Self { branches: children.len() })
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
        if matches!(plan, LogicalPlan::SubqueryAlias(_)) {
            return Ok(super::super::Analysis::new(
                plan,
                State::Composite(CompositeState::Coordinated(self)),
                transformed,
            ));
        }
        Ok(super::super::fatal(
            plan,
            transformed,
            format!(
                "coordinated qdrant composite with {} branches is not yet closed",
                self.branches
            ),
        ))
    }
}
