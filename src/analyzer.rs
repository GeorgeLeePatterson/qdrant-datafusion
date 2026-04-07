mod common;
mod kernel;
mod node;
mod op;
mod optimize;
mod payload;
mod query;
mod source;
mod state;
mod surface;

use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::AnalyzerRule;

pub(crate) use self::kernel::{
    CountKernel, FacetKernel, KernelSpec, QueryBatchKernel, QueryGroupsKernel, QueryKernel,
};
pub(crate) use self::node::{KERNEL_NODE_NAME, KernelNode};
pub(crate) use self::optimize::CoordinatedCombiners;
pub(crate) use self::query::{QueryRequest, QueryRequestPlan};
pub(crate) use self::state::State;
use self::state::{CompositeState, SourceState};
use self::surface::SurfaceCall;

#[derive(Debug, Clone, Copy)]
pub(crate) struct Pushdown;

impl AnalyzerRule for Pushdown {
    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &datafusion::common::config::ConfigOptions,
    ) -> Result<LogicalPlan> {
        analyze_root(plan).map(|analysis| analysis.transformed.data)
    }

    fn name(&self) -> &'static str { "prototype_qdrant_pushdown" }
}

struct Analysis {
    state:       State,
    transformed: Transformed<LogicalPlan>,
}

impl Analysis {
    pub(super) fn new(plan: LogicalPlan, state: State, transformed: bool) -> Self {
        Self { state, transformed: Transformed::new_transformed(plan, transformed) }
    }

    fn finish_root(self) -> Result<Self> {
        match self.state {
            State::Fatal(state) => plan_err!("{}", state.error.message),
            State::Processing(_) => plan_err!("unfinished qdrant region at query root"),
            State::Composite(state) => analyze_root(state.finish_root(self.transformed.data)?),
            _ => Ok(self),
        }
    }
}

fn analyze_root(plan: LogicalPlan) -> Result<Analysis> { analyze_plan(plan)?.finish_root() }

fn analyze_plan(plan: LogicalPlan) -> Result<Analysis> {
    let with_subqueries = plan
        .map_subqueries(|subquery| analyze_root(subquery).map(|analysis| analysis.transformed))?;
    if matches!(with_subqueries.data, LogicalPlan::RecursiveQuery(_)) {
        return analyze_recursive(with_subqueries);
    }
    let mut child_states = vec![];
    let rewritten = with_subqueries.transform_sibling(|plan| {
        plan.map_children(|child| {
            analyze_plan(child).map(|analysis| {
                child_states.push(analysis.state.clone());
                analysis.transformed
            })
        })
    })?;

    let transformed = rewritten.transformed;
    let plan = rewritten.data;

    match child_states.as_slice() {
        [] => Ok(analyze_leaf(plan, transformed)),
        [child] => analyze_unary(plan, child.clone(), transformed),
        children => analyze_multi(plan, children, transformed),
    }
}

fn analyze_leaf(plan: LogicalPlan, transformed: bool) -> Analysis {
    if let LogicalPlan::TableScan(scan) = &plan {
        if let Some(state) = SourceState::from_scan(scan) {
            return Analysis::new(plan, State::Source(state), transformed);
        }
        return Analysis::new(plan, State::local(), transformed);
    }
    if let LogicalPlan::Extension(extension) = &plan
        && let Some(node) = extension.node.as_any().downcast_ref::<KernelNode>()
    {
        let state = State::Kernel(state::KernelState::with_output_schema(
            node.spec().clone(),
            Arc::clone(node.output_schema()),
        ));
        return Analysis::new(plan, state, transformed);
    }
    Analysis::new(plan, State::local(), transformed)
}

fn analyze_recursive(plan: Transformed<LogicalPlan>) -> Result<Analysis> {
    let rewritten = plan.transform_sibling(|plan| {
        plan.map_children(|child| analyze_root(child).map(|analysis| analysis.transformed))
    })?;
    if SurfaceCall::collect(&rewritten.data.expressions())?.is_some() {
        return Ok(fatal(
            rewritten.data,
            rewritten.transformed,
            "qdrant surface calls may not cross recursive query boundaries",
        ));
    }
    Ok(Analysis::new(rewritten.data, State::local(), rewritten.transformed))
}

fn analyze_unary(plan: LogicalPlan, child: State, transformed: bool) -> Result<Analysis> {
    match plan {
        LogicalPlan::Projection(_) => child.projection(plan, transformed),
        LogicalPlan::Filter(_) => child.filter(plan, transformed),
        LogicalPlan::Sort(_) => child.sort(plan, transformed),
        LogicalPlan::Limit(_) => child.limit(plan, transformed),
        LogicalPlan::Aggregate(_) => child.aggregate(plan, transformed),
        _ => child.unary(plan, transformed),
    }
}

fn analyze_multi(plan: LogicalPlan, children: &[State], transformed: bool) -> Result<Analysis> {
    if let Some(fatal) = children.iter().find_map(|state| match state {
        State::Fatal(fatal) => Some(fatal.clone()),
        _ => None,
    }) {
        return Ok(Analysis::new(plan, State::Fatal(fatal), transformed));
    }
    if let Some(surface) = SurfaceCall::collect(&plan.expressions())?
        && !surface.allows_multi_branch_coordination()
    {
        return Ok(fatal(
            plan,
            transformed,
            "qdrant surface calls may not cross multi-branch boundaries",
        ));
    }
    if let Some(state) = CompositeState::from_plan(&plan, children)? {
        return Ok(Analysis::new(plan, State::Composite(state), transformed));
    }
    Ok(Analysis::new(plan, State::local(), transformed))
}

fn fatal(plan: LogicalPlan, transformed: bool, message: impl Into<String>) -> Analysis {
    Analysis::new(plan, State::fatal(message), transformed)
}
