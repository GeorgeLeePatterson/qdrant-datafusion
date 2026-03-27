mod common;
mod node;
mod op;
mod query;
mod source;
mod state;
mod surface;
mod kernel;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::AnalyzerRule;

use self::state::{CompositeState, CoordinatedState, SourceState};
use self::surface::SurfaceCall;

pub(crate) use self::kernel::{CountKernel, FacetKernel, KernelSpec, QueryKernel};
pub(crate) use self::node::{STATE_NODE_NAME, StateNode};
pub(crate) use self::query::QueryExecution;
pub(crate) use self::state::State;


// ============================================================================
// Analyzer
// ============================================================================

#[derive(Debug, Clone, Copy)]
pub(crate) struct PrototypePushdown;

impl AnalyzerRule for PrototypePushdown {
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
        [] => analyze_leaf(plan, transformed),
        [child] => analyze_unary(plan, child.clone(), transformed),
        children => analyze_multi(plan, children.to_vec(), transformed),
    }
}

fn analyze_leaf(plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
    if let LogicalPlan::TableScan(scan) = &plan {
        if let Some(state) = SourceState::from_scan(scan) {
            return Ok(Analysis::new(plan, State::Source(state), transformed));
        }
        return Ok(Analysis::new(plan, State::local(), transformed));
    }
    if let LogicalPlan::Extension(extension) = &plan
        && let Some(node) = extension.node.as_any().downcast_ref::<StateNode>()
    {
        let state = node.state.clone();
        return Ok(Analysis::new(plan, state, transformed));
    }
    Ok(Analysis::new(plan, State::local(), transformed))
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

fn analyze_multi(plan: LogicalPlan, children: Vec<State>, transformed: bool) -> Result<Analysis> {
    if let Some(fatal) = children.iter().find_map(|state| match state {
        State::Fatal(fatal) => Some(fatal.clone()),
        _ => None,
    }) {
        return Ok(Analysis::new(plan, State::Fatal(fatal), transformed));
    }
    if SurfaceCall::collect(&plan.expressions())?.is_some() {
        return Ok(fatal(
            plan,
            transformed,
            "qdrant surface calls may not cross multi-branch boundaries",
        ));
    }
    if let Some(state) = CompositeState::from_plan(&plan, &children)? {
        return Ok(Analysis::new(plan, State::Composite(state), transformed));
    }
    if children.iter().any(State::requires_composite_coordination) {
        return Ok(Analysis::new(
            plan,
            State::Composite(CompositeState::Coordinated(CoordinatedState {
                branches: children.len(),
            })),
            transformed,
        ));
    }
    Ok(Analysis::new(plan, State::local(), transformed))
}

fn fatal(plan: LogicalPlan, transformed: bool, message: impl Into<String>) -> Analysis {
    Analysis::new(plan, State::fatal(message), transformed)
}