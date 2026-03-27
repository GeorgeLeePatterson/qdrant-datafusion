#![allow(dead_code)]
#![allow(clippy::pedantic)]

use std::collections::{BTreeSet, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchemaRef, NullEquality, Result, ScalarValue, plan_err};
use datafusion::datasource::{provider_as_source, source_as_provider};
use datafusion::logical_expr::expr::{AggregateFunction, Alias, BinaryExpr};
use datafusion::logical_expr::utils::{
    COUNT_STAR_EXPANSION, conjunction, disjunction, split_conjunction_owned,
};
use datafusion::logical_expr::{
    Distinct, Expr, Extension, JoinType, LogicalPlan, LogicalPlanBuilder, Operator,
    UserDefinedLogicalNodeCore,
};
use datafusion::optimizer::AnalyzerRule;
use qdrant_client::Qdrant;
use qdrant_client::qdrant::PointId;
use qdrant_client::qdrant::point_id::PointIdOptions;

use crate::expr_fn::QdrantNearestCall;
use crate::pushdown::filter::QdrantFilters;
use crate::pushdown::{QdrantPayloadPath, QdrantPayloadSchema};
use crate::table::QdrantTableProvider;

const STATE_NODE_NAME: &str = "PrototypeStateNode";

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

    fn name(&self) -> &'static str {
        "prototype_qdrant_pushdown"
    }
}

struct Analysis {
    state: State,
    transformed: Transformed<LogicalPlan>,
}

impl Analysis {
    fn new(plan: LogicalPlan, state: State, transformed: bool) -> Self {
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

#[derive(Debug, Clone)]
enum State {
    Local(LocalState),
    Source(SourceState),
    Processing(ProcessingState),
    Composite(CompositeState),
    Kernel(KernelState),
    Fatal(FatalState),
}

impl State {
    fn local() -> Self {
        Self::Local(LocalState)
    }

    fn fatal(message: impl Into<String>) -> Self {
        Self::Fatal(FatalState { error: SemanticError::new(message) })
    }

    fn materialized(self, schema: DFSchemaRef) -> Result<StateNode> {
        match self {
            Self::Processing(_) | Self::Kernel(_) => Ok(StateNode { schema, state: self }),
            _ => plan_err!("{STATE_NODE_NAME} only materializes processing and kernel states"),
        }
    }

    fn projection(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        match self {
            Self::Local(state) => state.projection(plan, transformed),
            Self::Source(state) => state.projection(plan, transformed),
            Self::Processing(state) => state.projection(plan, transformed),
            Self::Composite(state) => state.projection(plan, transformed),
            Self::Kernel(state) => state.projection(plan, transformed),
            Self::Fatal(state) => state.projection(plan, transformed),
        }
    }

    fn filter(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        match self {
            Self::Local(state) => state.filter(plan, transformed),
            Self::Source(state) => state.filter(plan, transformed),
            Self::Processing(state) => state.filter(plan, transformed),
            Self::Composite(state) => state.filter(plan, transformed),
            Self::Kernel(state) => state.filter(plan, transformed),
            Self::Fatal(state) => state.filter(plan, transformed),
        }
    }

    fn sort(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        match self {
            Self::Local(state) => state.sort(plan, transformed),
            Self::Source(state) => state.sort(plan, transformed),
            Self::Processing(state) => state.sort(plan, transformed),
            Self::Composite(state) => state.sort(plan, transformed),
            Self::Kernel(state) => state.sort(plan, transformed),
            Self::Fatal(state) => state.sort(plan, transformed),
        }
    }

    fn limit(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        match self {
            Self::Local(state) => state.limit(plan, transformed),
            Self::Source(state) => state.limit(plan, transformed),
            Self::Processing(state) => state.limit(plan, transformed),
            Self::Composite(state) => state.limit(plan, transformed),
            Self::Kernel(state) => state.limit(plan, transformed),
            Self::Fatal(state) => state.limit(plan, transformed),
        }
    }

    fn aggregate(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        match self {
            Self::Local(state) => state.aggregate(plan, transformed),
            Self::Source(state) => state.aggregate(plan, transformed),
            Self::Processing(state) => state.aggregate(plan, transformed),
            Self::Composite(state) => state.aggregate(plan, transformed),
            Self::Kernel(state) => state.aggregate(plan, transformed),
            Self::Fatal(state) => state.aggregate(plan, transformed),
        }
    }

    fn unary(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
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

    fn requires_composite_coordination(&self) -> bool {
        matches!(self, Self::Processing(_) | Self::Composite(_))
    }
}

#[derive(Debug, Clone, Default)]
struct LocalState;

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
struct SourceState {
    source: Source,
    filters: FiltersState,
}

impl SourceState {
    fn from_scan(scan: &datafusion::logical_expr::logical_plan::TableScan) -> Option<Self> {
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

    fn projection(self, plan: LogicalPlan, transformed: bool) -> Result<Analysis> {
        let surface = SurfaceCall::collect(&plan.expressions())?;
        if let Some(surface) = surface {
            return self.open(surface)?.projection(plan, transformed);
        }
        if projection_is_column_only(&plan) {
            return Ok(Analysis::new(plan, State::Source(self), transformed));
        }
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
                    filters: self.filters,
                }),
            }
            .absorb(plan, transformed),
            AggregateSurface::Facet(op) => {
                ProcessingState { source: self.source, filters: self.filters, op: Op::Facet(op) }
                    .absorb(plan, transformed)
            }
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
            source: self.source.clone(),
            filters: self.filters.clone(),
            op: Op::from_surface(surface, &self.source)?,
        })
    }
}

#[derive(Debug, Clone)]
struct ProcessingState {
    source: Source,
    filters: FiltersState,
    op: Op,
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
struct KernelState {
    spec: KernelSpec,
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
enum CompositeState {
    Mergeable(MergeableState),
    Batchable(BatchableState),
    Coordinated(CoordinatedState),
}

impl CompositeState {
    fn from_plan(plan: &LogicalPlan, children: &[State]) -> Result<Option<Self>> {
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

    fn finish_root(self, plan: LogicalPlan) -> Result<LogicalPlan> {
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
struct MergeableState {
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
enum MergeableKind {
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
struct MergeableUnion {
    source: Source,
    branches: Vec<Option<Expr>>,
    branch_ids: Vec<Option<Vec<PointId>>>,
}

impl MergeableUnion {
    fn from_plan(plan: &LogicalPlan, children: &[State]) -> Result<Option<Self>> {
        let LogicalPlan::Union(_) = plan else {
            return Ok(None);
        };
        let mut branches = children
            .iter()
            .map(MergeableBranch::from_state)
            .collect::<Result<Vec<_>>>()?
            .into_iter();
        let Some(first) = branches.next() else {
            return Ok(None);
        };
        let Some(first) = first else {
            return Ok(None);
        };
        let source = first.source.clone();
        let mut filters = vec![first.filter];
        let mut ids = vec![first.ids];
        for branch in branches {
            let Some(branch) = branch else {
                return Ok(None);
            };
            if !source.merge_compatible_with(&branch.source) {
                return Ok(None);
            }
            filters.push(branch.filter);
            ids.push(branch.ids);
        }
        Ok(Some(Self { source, branches: filters, branch_ids: ids }))
    }

    fn rewrite_current(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        let merged = if matches!(plan, LogicalPlan::Distinct(Distinct::All(_))) {
            self.merged_plan(false)?
        } else if self.can_union_all_merge() {
            self.merged_plan(true)?
        } else {
            None
        };
        let Some(merged) = merged else {
            return Ok(None);
        };
        match plan {
            LogicalPlan::Union(_) | LogicalPlan::Distinct(Distinct::All(_)) => Ok(Some(merged)),
            _ if plan.inputs().len() == 1 => {
                plan.with_new_exprs(plan.expressions(), vec![merged])?.recompute_schema().map(Some)
            }
            _ => Ok(None),
        }
    }

    fn can_union_all_merge(&self) -> bool {
        self.branches.iter().all(Option::is_some) && self.point_id_sets_are_disjoint()
    }

    fn merged_plan(&self, require_disjoint: bool) -> Result<Option<LogicalPlan>> {
        if require_disjoint && !self.can_union_all_merge() {
            return Ok(None);
        }
        self.source.planner_scan(self.merged_filter()).map(Some)
    }

    fn merged_filter(&self) -> Option<Expr> {
        (!self.branches.iter().any(Option::is_none))
            .then(|| disjunction(self.branches.iter().flatten().cloned()))
            .flatten()
    }

    fn point_id_sets_are_disjoint(&self) -> bool {
        let Some(id_sets) = self.branch_ids.iter().cloned().collect::<Option<Vec<Vec<PointId>>>>()
        else {
            return false;
        };
        let mut seen = HashSet::new();
        id_sets.into_iter().all(|ids| {
            ids.into_iter()
                .map(|id| match id.point_id_options.as_ref() {
                    Some(PointIdOptions::Num(value)) => format!("num:{value}"),
                    Some(PointIdOptions::Uuid(value)) => format!("uuid:{value}"),
                    None => "missing".to_owned(),
                })
                .collect::<HashSet<_>>()
                .into_iter()
                .all(|key| seen.insert(key))
        })
    }
}

#[derive(Debug, Clone)]
struct MergeableSetJoin {
    source: Source,
    left_filter: Option<Expr>,
    right_filter: Option<Expr>,
    join_type: JoinType,
}

impl MergeableSetJoin {
    fn from_plan(plan: &LogicalPlan, children: &[State]) -> Result<Option<Self>> {
        let LogicalPlan::Join(join) = plan else {
            return Ok(None);
        };
        if join.filter.is_some()
            || join.null_aware
            || join.null_equality != NullEquality::NullEqualsNull
            || !matches!(join.join_type, JoinType::LeftSemi | JoinType::LeftAnti)
            || !full_row_join_keys(join)
            || children.len() != 2
        {
            return Ok(None);
        }
        let Some(left) = MergeableBranch::from_state(&children[0])? else {
            return Ok(None);
        };
        let Some(right) = MergeableBranch::from_state(&children[1])? else {
            return Ok(None);
        };
        if !left.source.merge_compatible_with(&right.source) {
            return Ok(None);
        }
        Ok(Some(Self {
            source: left.source,
            left_filter: left.filter,
            right_filter: right.filter,
            join_type: join.join_type,
        }))
    }

    fn rewrite_current(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        let merged = self.source.planner_scan(self.merged_filter())?;
        match plan {
            LogicalPlan::Join(_) | LogicalPlan::Distinct(Distinct::All(_)) => Ok(Some(merged)),
            _ if plan.inputs().len() == 1 => {
                plan.with_new_exprs(plan.expressions(), vec![merged])?.recompute_schema().map(Some)
            }
            _ => Ok(None),
        }
    }

    fn merged_filter(&self) -> Option<Expr> {
        match self.join_type {
            JoinType::LeftSemi => conjunction(
                [self.left_filter.clone(), self.right_filter.clone()].into_iter().flatten(),
            ),
            JoinType::LeftAnti => match (&self.left_filter, &self.right_filter) {
                (_, None) => Some(Expr::Literal(ScalarValue::Boolean(Some(false)), None)),
                (None, Some(right)) => Some(Expr::Not(Box::new(right.clone()))),
                (Some(left), Some(right)) => {
                    conjunction([left.clone(), Expr::Not(Box::new(right.clone()))])
                }
            },
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
struct MergeableBranch {
    source: Source,
    filter: Option<Expr>,
    ids: Option<Vec<PointId>>,
}

impl MergeableBranch {
    fn from_state(state: &State) -> Result<Option<Self>> {
        let State::Source(source_state) = state else {
            return Ok(None);
        };
        let ids = source_state.filters.exact(&source_state.source)?.possible_point_ids();
        Ok(Some(Self {
            source: source_state.source.clone(),
            filter: source_state.filters.combined_expr(),
            ids,
        }))
    }
}

#[derive(Debug, Clone)]
struct BatchableState {
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
struct CoordinatedState {
    branches: usize,
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

#[derive(Debug, Clone)]
struct FatalState {
    error: SemanticError,
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

#[derive(Debug, Clone)]
struct SemanticError {
    message: String,
}

impl SemanticError {
    fn new(message: impl Into<String>) -> Self {
        Self { message: message.into() }
    }
}

#[derive(Clone)]
struct Source {
    client: Arc<Qdrant>,
    collection: String,
    schema: SchemaRef,
    payload_schema: Arc<QdrantPayloadSchema>,
}

impl std::fmt::Debug for Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Source")
            .field("client", &"Qdrant")
            .field("collection", &self.collection)
            .field("schema", &self.schema)
            .field("payload_schema", &self.payload_schema)
            .finish()
    }
}

impl Source {
    fn merge_compatible_with(&self, other: &Self) -> bool {
        self.collection == other.collection
            && Arc::ptr_eq(&self.client, &other.client)
            && format!("{:?}", self.schema) == format!("{:?}", other.schema)
            && format!("{:?}", self.payload_schema) == format!("{:?}", other.payload_schema)
    }

    fn planner_scan(&self, filter: Option<Expr>) -> Result<LogicalPlan> {
        let provider = Arc::new(QdrantTableProvider::new_for_planner(
            self.collection.clone(),
            Arc::clone(&self.client),
            Arc::clone(&self.schema),
            Arc::clone(&self.payload_schema),
        ));
        let builder =
            LogicalPlanBuilder::scan(self.collection.clone(), provider_as_source(provider), None)?;
        match filter {
            Some(filter) => builder.filter(filter)?.build(),
            None => builder.build(),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct FiltersState {
    exprs: Vec<Expr>,
}

impl FiltersState {
    fn push(mut self, expr: Expr) -> Self {
        self.exprs.push(expr);
        self
    }

    fn exact(&self, source: &Source) -> Result<QdrantFilters> {
        QdrantFilters::try_new(&source.schema, &source.payload_schema, &self.exprs)
    }

    fn combined_expr(&self) -> Option<Expr> {
        conjunction(self.exprs.clone())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct OutputNames(BTreeSet<String>);

impl OutputNames {
    fn single(name: String) -> Self {
        Self(BTreeSet::from([name]))
    }

    fn contains_name(&self, name: &str) -> bool {
        self.0.contains(name)
    }

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

#[derive(Debug, Clone)]
enum SurfaceCall {
    Query(QuerySurfaceCall),
}

impl SurfaceCall {
    fn from_expr(expr: &Expr) -> Result<Option<Self>> {
        QuerySurfaceCall::from_expr(expr).map(|call| call.map(Self::Query))
    }

    fn collect(exprs: &[Expr]) -> Result<Option<Self>> {
        let mut surface: Option<Self> = None;
        for expr in exprs {
            let _ = expr.apply(|node| {
                let Some(call) = Self::from_expr(node)? else {
                    return Ok(TreeNodeRecursion::Continue);
                };
                if let Some(existing) = &surface {
                    if !existing.same_semantics(&call) {
                        return plan_err!("multiple qdrant surface calls in one qdrant region");
                    }
                } else {
                    surface = Some(call);
                }
                Ok(TreeNodeRecursion::Jump)
            })?;
        }
        Ok(surface)
    }

    fn same_semantics(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Query(lhs), Self::Query(rhs)) => lhs.same_semantics(rhs),
        }
    }
}

#[derive(Debug, Clone)]
enum QuerySurfaceCall {
    Nearest(NearestQuery),
    Recommend(RecommendQuery),
    Discover(DiscoverQuery),
    Context(ContextQuery),
    OrderBy(OrderByQuery),
    Fusion(FusionQuery),
    Sample(SampleQuery),
    Formula(FormulaQuery),
    NearestWithMmr(NearestWithMmrQuery),
    RelevanceFeedback(RelevanceFeedbackQuery),
}

impl QuerySurfaceCall {
    fn from_expr(expr: &Expr) -> Result<Option<Self>> {
        if let Some(query) = NearestQuery::from_expr(expr)? {
            return Ok(Some(Self::Nearest(query)));
        }
        Ok(None)
    }

    fn same_semantics(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Nearest(lhs), Self::Nearest(rhs)) => lhs.same_semantics(rhs),
            (Self::Recommend(lhs), Self::Recommend(rhs)) => lhs.same_semantics(rhs),
            (Self::Discover(lhs), Self::Discover(rhs)) => lhs.same_semantics(rhs),
            (Self::Context(lhs), Self::Context(rhs)) => lhs.same_semantics(rhs),
            (Self::OrderBy(lhs), Self::OrderBy(rhs)) => lhs.same_semantics(rhs),
            (Self::Fusion(lhs), Self::Fusion(rhs)) => lhs.same_semantics(rhs),
            (Self::Sample(lhs), Self::Sample(rhs)) => lhs.same_semantics(rhs),
            (Self::Formula(lhs), Self::Formula(rhs)) => lhs.same_semantics(rhs),
            (Self::NearestWithMmr(lhs), Self::NearestWithMmr(rhs)) => lhs.same_semantics(rhs),
            (Self::RelevanceFeedback(lhs), Self::RelevanceFeedback(rhs)) => lhs.same_semantics(rhs),
            _ => false,
        }
    }
}

#[derive(Debug, Clone)]
struct NearestQuery {
    using: Option<String>,
    input: NearestInput,
}

impl NearestQuery {
    fn from_expr(expr: &Expr) -> Result<Option<Self>> {
        QdrantNearestCall::from_expr(expr).map(|call| call.map(Into::into))
    }

    fn same_semantics(&self, other: &Self) -> bool {
        self.using == other.using && self.input.same_semantics(&other.input)
    }

    fn validate_on_source(&self, source: &Source) -> Result<()> {
        let Some(using) = self.using.as_deref() else {
            return Ok(());
        };
        self.input.validate_on_source(source, using)
    }
}

impl From<QdrantNearestCall> for NearestQuery {
    fn from(call: QdrantNearestCall) -> Self {
        Self {
            using: Some(call.vector_field),
            input: NearestInput::Dense(DenseNearestInput { vector: call.vector }),
        }
    }
}

#[derive(Debug, Clone)]
enum NearestInput {
    Dense(DenseNearestInput),
    Sparse(SparseNearestInput),
    MultiDense(MultiDenseNearestInput),
    Id(IdNearestInput),
    Document(DocumentNearestInput),
    Image(ImageNearestInput),
    Object(ObjectNearestInput),
}

impl NearestInput {
    fn same_semantics(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Dense(lhs), Self::Dense(rhs)) => lhs.same_semantics(rhs),
            (Self::Sparse(lhs), Self::Sparse(rhs)) => lhs.same_semantics(rhs),
            (Self::MultiDense(lhs), Self::MultiDense(rhs)) => lhs.same_semantics(rhs),
            (Self::Id(lhs), Self::Id(rhs)) => lhs.same_semantics(rhs),
            (Self::Document(lhs), Self::Document(rhs)) => lhs.same_semantics(rhs),
            (Self::Image(lhs), Self::Image(rhs)) => lhs.same_semantics(rhs),
            (Self::Object(lhs), Self::Object(rhs)) => lhs.same_semantics(rhs),
            _ => false,
        }
    }

    fn validate_on_source(&self, source: &Source, using: &str) -> Result<()> {
        match self {
            Self::Dense(input) => input.validate_on_source(source, using),
            Self::Sparse(input) => input.validate_on_source(source, using),
            Self::MultiDense(input) => input.validate_on_source(source, using),
            Self::Id(input) => input.validate_on_source(source, using),
            Self::Document(input) => input.validate_on_source(source, using),
            Self::Image(input) => input.validate_on_source(source, using),
            Self::Object(input) => input.validate_on_source(source, using),
        }
    }
}

#[derive(Debug, Clone)]
struct DenseNearestInput {
    vector: Vec<f32>,
}

impl DenseNearestInput {
    fn same_semantics(&self, other: &Self) -> bool {
        self.vector
            .iter()
            .map(|value| value.to_bits())
            .eq(other.vector.iter().map(|value| value.to_bits()))
    }

    fn validate_on_source(&self, source: &Source, using: &str) -> Result<()> {
        match QueryVectorBinding::from_source(source, using)? {
            QueryVectorBinding::DenseFixed { width } => {
                if width != self.vector.len() {
                    return plan_err!("query vector width does not match source vector width");
                }
                Ok(())
            }
            QueryVectorBinding::DenseVariable => Ok(()),
            QueryVectorBinding::Sparse => {
                plan_err!("dense query input requires a dense vector binding")
            }
            QueryVectorBinding::MultiDense => {
                plan_err!("dense query input requires a single dense vector binding")
            }
            QueryVectorBinding::Document => {
                plan_err!("dense query input does not bind to a document inference field")
            }
            QueryVectorBinding::Image => {
                plan_err!("dense query input does not bind to an image inference field")
            }
            QueryVectorBinding::Object => {
                plan_err!("dense query input does not bind to an object inference field")
            }
            QueryVectorBinding::Unsupported(data_type) => plan_err!(
                "dense query input does not bind to source field '{}' of type {:?}",
                using,
                data_type
            ),
        }
    }
}

#[derive(Debug, Clone)]
struct SparseNearestInput {
    indices: Vec<u32>,
    values: Vec<f32>,
}

impl SparseNearestInput {
    fn same_semantics(&self, other: &Self) -> bool {
        self.indices == other.indices
            && self
                .values
                .iter()
                .map(|value| value.to_bits())
                .eq(other.values.iter().map(|value| value.to_bits()))
    }

    fn validate_on_source(&self, source: &Source, using: &str) -> Result<()> {
        if self.indices.len() != self.values.len() {
            return plan_err!("sparse query input requires matching index and value lengths");
        }
        match QueryVectorBinding::from_source(source, using)? {
            QueryVectorBinding::Sparse => Ok(()),
            QueryVectorBinding::DenseFixed { .. } | QueryVectorBinding::DenseVariable => {
                plan_err!("sparse query input requires a sparse vector binding")
            }
            QueryVectorBinding::MultiDense => {
                plan_err!("sparse query input does not bind to a multivector field")
            }
            QueryVectorBinding::Document => {
                plan_err!("sparse query input does not bind to a document inference field")
            }
            QueryVectorBinding::Image => {
                plan_err!("sparse query input does not bind to an image inference field")
            }
            QueryVectorBinding::Object => {
                plan_err!("sparse query input does not bind to an object inference field")
            }
            QueryVectorBinding::Unsupported(data_type) => plan_err!(
                "sparse query input does not bind to source field '{}' of type {:?}",
                using,
                data_type
            ),
        }
    }
}

#[derive(Debug, Clone)]
struct MultiDenseNearestInput {
    vectors: Vec<Vec<f32>>,
}

impl MultiDenseNearestInput {
    fn same_semantics(&self, other: &Self) -> bool {
        self.vectors.len() == other.vectors.len()
            && self.vectors.iter().zip(&other.vectors).all(|(lhs, rhs)| {
                lhs.iter().map(|value| value.to_bits()).eq(rhs.iter().map(|value| value.to_bits()))
            })
    }

    fn validate_on_source(&self, source: &Source, using: &str) -> Result<()> {
        match QueryVectorBinding::from_source(source, using)? {
            QueryVectorBinding::MultiDense => Ok(()),
            QueryVectorBinding::DenseFixed { .. }
            | QueryVectorBinding::DenseVariable
            | QueryVectorBinding::Sparse => {
                plan_err!("multivector query input requires a multivector binding")
            }
            QueryVectorBinding::Document => {
                plan_err!("multivector query input does not bind to a document inference field")
            }
            QueryVectorBinding::Image => {
                plan_err!("multivector query input does not bind to an image inference field")
            }
            QueryVectorBinding::Object => {
                plan_err!("multivector query input does not bind to an object inference field")
            }
            QueryVectorBinding::Unsupported(data_type) => plan_err!(
                "multivector query input does not bind to source field '{}' of type {:?}",
                using,
                data_type
            ),
        }
    }
}

#[derive(Debug, Clone)]
struct IdNearestInput {
    point_id: PointId,
}

impl IdNearestInput {
    fn same_semantics(&self, other: &Self) -> bool {
        match (&self.point_id.point_id_options, &other.point_id.point_id_options) {
            (Some(PointIdOptions::Num(lhs)), Some(PointIdOptions::Num(rhs))) => lhs == rhs,
            (Some(PointIdOptions::Uuid(lhs)), Some(PointIdOptions::Uuid(rhs))) => lhs == rhs,
            (None, None) => true,
            _ => false,
        }
    }

    fn validate_on_source(&self, _source: &Source, _using: &str) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct DocumentNearestInput {
    text: String,
    model: Option<String>,
}

impl DocumentNearestInput {
    fn same_semantics(&self, other: &Self) -> bool {
        self.text == other.text && self.model == other.model
    }

    fn validate_on_source(&self, _source: &Source, _using: &str) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct ImageNearestInput {
    image: Vec<u8>,
    model: Option<String>,
}

impl ImageNearestInput {
    fn same_semantics(&self, other: &Self) -> bool {
        self.image == other.image && self.model == other.model
    }

    fn validate_on_source(&self, _source: &Source, _using: &str) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct ObjectNearestInput {
    object: Vec<(String, ScalarValue)>,
    model: Option<String>,
}

impl ObjectNearestInput {
    fn same_semantics(&self, other: &Self) -> bool {
        self.object == other.object && self.model == other.model
    }

    fn validate_on_source(&self, _source: &Source, _using: &str) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
enum QueryVectorBinding {
    DenseFixed { width: usize },
    DenseVariable,
    Sparse,
    MultiDense,
    Document,
    Image,
    Object,
    Unsupported(DataType),
}

impl QueryVectorBinding {
    fn from_source(source: &Source, using: &str) -> Result<Self> {
        let field = match source.schema.field_with_name(using) {
            Ok(field) => field,
            Err(_) => return plan_err!("query vector field '{}' not found", using),
        };
        Ok(Self::from_data_type(field.data_type()))
    }

    fn from_data_type(data_type: &DataType) -> Self {
        match data_type {
            DataType::FixedSizeList(field, width)
                if matches!(
                    field.data_type(),
                    DataType::Float16 | DataType::Float32 | DataType::Float64
                ) =>
            {
                Self::DenseFixed { width: usize::try_from(*width).unwrap_or_default() }
            }
            DataType::List(field) | DataType::LargeList(field)
                if matches!(
                    field.data_type(),
                    DataType::Float16 | DataType::Float32 | DataType::Float64
                ) =>
            {
                Self::DenseVariable
            }
            DataType::List(field) | DataType::LargeList(field)
                if matches!(
                    field.data_type(),
                    DataType::FixedSizeList(_, _) | DataType::List(_) | DataType::LargeList(_)
                ) =>
            {
                Self::MultiDense
            }
            DataType::List(field) | DataType::LargeList(field)
                if matches!(field.data_type(), DataType::Struct(_)) =>
            {
                Self::Sparse
            }
            DataType::Struct(fields)
                if fields.iter().any(|field| field.name() == "indices")
                    && fields.iter().any(|field| field.name() == "values") =>
            {
                Self::Sparse
            }
            DataType::Utf8 | DataType::LargeUtf8 => Self::Document,
            DataType::Binary | DataType::LargeBinary => Self::Image,
            DataType::Struct(_) => Self::Object,
            other => Self::Unsupported(other.clone()),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct RecommendQuery;

impl RecommendQuery {
    fn same_semantics(&self, _other: &Self) -> bool {
        true
    }

    fn validate_on_source(&self, _source: &Source) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
struct DiscoverQuery;

impl DiscoverQuery {
    fn same_semantics(&self, _other: &Self) -> bool {
        true
    }

    fn validate_on_source(&self, _source: &Source) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
struct ContextQuery;

impl ContextQuery {
    fn same_semantics(&self, _other: &Self) -> bool {
        true
    }

    fn validate_on_source(&self, _source: &Source) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
struct OrderByQuery;

impl OrderByQuery {
    fn same_semantics(&self, _other: &Self) -> bool {
        true
    }

    fn validate_on_source(&self, _source: &Source) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
struct FusionQuery;

impl FusionQuery {
    fn same_semantics(&self, _other: &Self) -> bool {
        true
    }

    fn validate_on_source(&self, _source: &Source) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
struct SampleQuery;

impl SampleQuery {
    fn same_semantics(&self, _other: &Self) -> bool {
        true
    }

    fn validate_on_source(&self, _source: &Source) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
struct FormulaQuery;

impl FormulaQuery {
    fn same_semantics(&self, _other: &Self) -> bool {
        true
    }

    fn validate_on_source(&self, _source: &Source) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
struct NearestWithMmrQuery;

impl NearestWithMmrQuery {
    fn same_semantics(&self, _other: &Self) -> bool {
        true
    }

    fn validate_on_source(&self, _source: &Source) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
struct RelevanceFeedbackQuery;

impl RelevanceFeedbackQuery {
    fn same_semantics(&self, _other: &Self) -> bool {
        true
    }

    fn validate_on_source(&self, _source: &Source) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct QueryOp {
    query: QueryKind,
    query_score_outputs: OutputNames,
    score_threshold: Option<f32>,
    sorted: bool,
}

impl QueryOp {
    fn from_surface(surface: QuerySurfaceCall) -> Self {
        Self {
            query: QueryKind::from_surface(surface),
            query_score_outputs: OutputNames::default(),
            score_threshold: None,
            sorted: false,
        }
    }

    fn validate_on_source(&self, source: &Source) -> Result<()> {
        self.query.validate_on_source(source)
    }

    fn project(mut self, plan: &LogicalPlan) -> Result<Option<Self>> {
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
enum QueryKind {
    Nearest(NearestQuery),
    Recommend(RecommendQuery),
    Discover(DiscoverQuery),
    Context(ContextQuery),
    OrderBy(OrderByQuery),
    Fusion(FusionQuery),
    Sample(SampleQuery),
    Formula(FormulaQuery),
    NearestWithMmr(NearestWithMmrQuery),
    RelevanceFeedback(RelevanceFeedbackQuery),
}

impl QueryKind {
    fn from_surface(surface: QuerySurfaceCall) -> Self {
        match surface {
            QuerySurfaceCall::Nearest(query) => Self::Nearest(query),
            QuerySurfaceCall::Recommend(query) => Self::Recommend(query),
            QuerySurfaceCall::Discover(query) => Self::Discover(query),
            QuerySurfaceCall::Context(query) => Self::Context(query),
            QuerySurfaceCall::OrderBy(query) => Self::OrderBy(query),
            QuerySurfaceCall::Fusion(query) => Self::Fusion(query),
            QuerySurfaceCall::Sample(query) => Self::Sample(query),
            QuerySurfaceCall::Formula(query) => Self::Formula(query),
            QuerySurfaceCall::NearestWithMmr(query) => Self::NearestWithMmr(query),
            QuerySurfaceCall::RelevanceFeedback(query) => Self::RelevanceFeedback(query),
        }
    }

    fn validate_on_source(&self, source: &Source) -> Result<()> {
        match self {
            Self::Nearest(query) => query.validate_on_source(source),
            Self::Recommend(query) => query.validate_on_source(source),
            Self::Discover(query) => query.validate_on_source(source),
            Self::Context(query) => query.validate_on_source(source),
            Self::OrderBy(query) => query.validate_on_source(source),
            Self::Fusion(query) => query.validate_on_source(source),
            Self::Sample(query) => query.validate_on_source(source),
            Self::Formula(query) => query.validate_on_source(source),
            Self::NearestWithMmr(query) => query.validate_on_source(source),
            Self::RelevanceFeedback(query) => query.validate_on_source(source),
        }
    }

    fn matches_surface(&self, surface: &QuerySurfaceCall) -> bool {
        match (self, surface) {
            (Self::Nearest(lhs), QuerySurfaceCall::Nearest(rhs)) => lhs.same_semantics(rhs),
            (Self::Recommend(lhs), QuerySurfaceCall::Recommend(rhs)) => lhs.same_semantics(rhs),
            (Self::Discover(lhs), QuerySurfaceCall::Discover(rhs)) => lhs.same_semantics(rhs),
            (Self::Context(lhs), QuerySurfaceCall::Context(rhs)) => lhs.same_semantics(rhs),
            (Self::OrderBy(lhs), QuerySurfaceCall::OrderBy(rhs)) => lhs.same_semantics(rhs),
            (Self::Fusion(lhs), QuerySurfaceCall::Fusion(rhs)) => lhs.same_semantics(rhs),
            (Self::Sample(lhs), QuerySurfaceCall::Sample(rhs)) => lhs.same_semantics(rhs),
            (Self::Formula(lhs), QuerySurfaceCall::Formula(rhs)) => lhs.same_semantics(rhs),
            (Self::NearestWithMmr(lhs), QuerySurfaceCall::NearestWithMmr(rhs)) => {
                lhs.same_semantics(rhs)
            }
            (Self::RelevanceFeedback(lhs), QuerySurfaceCall::RelevanceFeedback(rhs)) => {
                lhs.same_semantics(rhs)
            }
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FacetOp {
    field: QdrantPayloadPath,
    key_outputs: OutputNames,
    count_outputs: OutputNames,
    sorted: bool,
}

impl FacetOp {
    fn project(mut self, plan: &LogicalPlan) -> Result<Option<Self>> {
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

    fn sort(mut self, plan: &LogicalPlan) -> Result<Option<Self>> {
        let LogicalPlan::Sort(sort) = plan else {
            return Ok(None);
        };
        if sort.expr.len() != 1 || sort.expr[0].asc || !self.is_count_expr(&sort.expr[0].expr) {
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

    fn is_key_expr(&self, expr: &Expr) -> bool {
        self.key_outputs.matches_column(expr)
    }

    fn is_count_expr(&self, expr: &Expr) -> bool {
        self.count_outputs.matches_column(expr)
            || count_star_like(&expr.clone().unalias_nested().data)
    }
}

#[derive(Debug, Clone)]
enum KernelSpec {
    Count(CountKernel),
    Query(QueryKernel),
    Facet(FacetKernel),
}

impl KernelSpec {
    fn project(self, plan: &LogicalPlan) -> Result<Option<Self>> {
        match self {
            Self::Count(_) => Ok(None),
            Self::Query(mut kernel) => {
                let Some(query) = kernel.query.project(plan)? else {
                    return Ok(None);
                };
                kernel.query = query;
                Ok(Some(Self::Query(kernel)))
            }
            Self::Facet(mut kernel) => {
                let Some(op) = kernel.op.project(plan)? else {
                    return Ok(None);
                };
                kernel.op = op;
                Ok(Some(Self::Facet(kernel)))
            }
        }
    }
}

#[derive(Debug, Clone)]
struct CountKernel {
    collection: String,
    filters: FiltersState,
}

#[derive(Debug, Clone)]
struct QueryKernel {
    collection: String,
    filters: FiltersState,
    query: QueryOp,
    limit: u64,
}

#[derive(Debug, Clone)]
struct FacetKernel {
    collection: String,
    filters: FiltersState,
    op: FacetOp,
    limit: u64,
}

#[derive(Debug, Clone)]
struct StateNode {
    schema: DFSchemaRef,
    state: State,
}

impl UserDefinedLogicalNodeCore for StateNode {
    fn name(&self) -> &str {
        STATE_NODE_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.state {
            State::Processing(state) => write!(f, "{STATE_NODE_NAME}: processing {:?}", state.op),
            State::Kernel(state) => write!(f, "{STATE_NODE_NAME}: kernel {:?}", state.spec),
            State::Local(_) | State::Source(_) | State::Composite(_) | State::Fatal(_) => {
                write!(f, "{STATE_NODE_NAME}: invalid materialized state")
            }
        }
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if !exprs.is_empty() {
            return plan_err!("{STATE_NODE_NAME} expects no expressions");
        }
        if !inputs.is_empty() {
            return plan_err!("{STATE_NODE_NAME} expects no inputs");
        }
        Ok(self.clone())
    }
}

impl PartialEq for StateNode {
    fn eq(&self, other: &Self) -> bool {
        format!("{:?}", self.state) == format!("{:?}", other.state)
            && format!("{:?}", self.schema) == format!("{:?}", other.schema)
    }
}

impl Eq for StateNode {}

impl PartialOrd for StateNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        (format!("{:?}", self.state), format!("{:?}", self.schema))
            .partial_cmp(&(format!("{:?}", other.state), format!("{:?}", other.schema)))
    }
}

impl Hash for StateNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        STATE_NODE_NAME.hash(state);
        format!("{:?}", self.state).hash(state);
        format!("{:?}", self.schema).hash(state);
    }
}

fn analyze_root(plan: LogicalPlan) -> Result<Analysis> {
    analyze_plan(plan)?.finish_root()
}

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

#[derive(Debug, Clone, PartialEq, Eq)]
enum AggregateSurface {
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
            && count_star_like(&aggregate.aggr_expr[0])
        {
            return Ok(Self::Count);
        }
        if aggregate.group_expr.len() != 1
            || aggregate.aggr_expr.len() != 1
            || !count_star_like(&aggregate.aggr_expr[0])
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

fn projection_is_column_only(plan: &LogicalPlan) -> bool {
    let LogicalPlan::Projection(projection) = plan else {
        return false;
    };
    projection.expr.iter().all(|expr| {
        matches!(expr.clone().unalias_nested().data, Expr::Column(_))
            || matches!(expr, Expr::Alias(Alias { expr, .. }) if matches!(expr.clone().unalias_nested().data, Expr::Column(_)))
    })
}

fn full_row_join_keys(join: &datafusion::logical_expr::logical_plan::Join) -> bool {
    let left_fields = join.left.schema().fields();
    let right_fields = join.right.schema().fields();
    join.on.len() == left_fields.len()
        && left_fields.len() == right_fields.len()
        && join.on.iter().zip(left_fields.iter().zip(right_fields.iter())).all(
            |((left, right), (left_field, right_field))| match (left, right) {
                (Expr::Column(left), Expr::Column(right)) => {
                    left.name == *left_field.name() && right.name == *right_field.name()
                }
                _ => false,
            },
        )
}

fn limit_rows(plan: &LogicalPlan) -> Result<u64> {
    fn integer_literal_u64(expr: &Expr) -> Result<u64> {
        match expr.clone().unalias_nested().data {
            Expr::Cast(cast) => integer_literal_u64(&cast.expr),
            Expr::TryCast(cast) => integer_literal_u64(&cast.expr),
            Expr::Literal(value, _) => match value {
                ScalarValue::Int8(Some(value)) if value >= 0 => Ok(value as u64),
                ScalarValue::Int16(Some(value)) if value >= 0 => Ok(value as u64),
                ScalarValue::Int32(Some(value)) if value >= 0 => Ok(value as u64),
                ScalarValue::Int64(Some(value)) if value >= 0 => Ok(value as u64),
                ScalarValue::UInt8(Some(value)) => Ok(u64::from(value)),
                ScalarValue::UInt16(Some(value)) => Ok(u64::from(value)),
                ScalarValue::UInt32(Some(value)) => Ok(u64::from(value)),
                ScalarValue::UInt64(Some(value)) => Ok(value),
                _ => plan_err!("qdrant limit requires a non-negative integer literal"),
            },
            _ => plan_err!("qdrant limit requires a non-negative integer literal"),
        }
    }

    let LogicalPlan::Limit(limit) = plan else {
        return plan_err!("prototype limit state mismatch");
    };
    match limit.skip.as_deref() {
        None => {}
        Some(expr) if integer_literal_u64(expr)? == 0 => {}
        _ => return plan_err!("qdrant limit does not support skip"),
    }
    match limit.fetch.as_deref() {
        Some(expr) => {
            let value = integer_literal_u64(expr)?;
            if value > 0 {
                Ok(value)
            } else {
                plan_err!("qdrant limit requires a positive literal fetch")
            }
        }
        None => plan_err!("qdrant limit requires a positive literal fetch"),
    }
}

#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
fn numeric_literal_f32(expr: &Expr) -> Result<f32> {
    match expr.clone().unalias_nested().data {
        Expr::Negative(expr) => Ok(-numeric_literal_f32(&expr)?),
        Expr::Cast(cast) => numeric_literal_f32(&cast.expr),
        Expr::TryCast(cast) => numeric_literal_f32(&cast.expr),
        Expr::Literal(value, _) => match value {
            ScalarValue::Float32(Some(value)) => Ok(value),
            ScalarValue::Float64(Some(value)) => Ok(value as f32),
            ScalarValue::Int8(Some(value)) => Ok(f32::from(value)),
            ScalarValue::Int16(Some(value)) => Ok(f32::from(value)),
            ScalarValue::Int32(Some(value)) => Ok(value as f32),
            ScalarValue::Int64(Some(value)) => Ok(value as f32),
            ScalarValue::UInt8(Some(value)) => Ok(f32::from(value)),
            ScalarValue::UInt16(Some(value)) => Ok(f32::from(value)),
            ScalarValue::UInt32(Some(value)) => Ok(value as f32),
            ScalarValue::UInt64(Some(value)) => Ok(value as f32),
            _ => plan_err!("score thresholds must be numeric"),
        },
        _ => plan_err!("score thresholds must be numeric"),
    }
}

fn count_star_like(expr: &Expr) -> bool {
    match expr {
        Expr::Alias(Alias { expr, .. }) => count_star_like(expr),
        Expr::AggregateFunction(AggregateFunction { func, params }) => {
            func.name() == "count"
                && !params.distinct
                && params.filter.is_none()
                && params.order_by.is_empty()
                && params.null_treatment.is_none()
                && matches!(params.args.as_slice(), [Expr::Literal(value, _)] if count_like_literal(value))
        }
        _ => false,
    }
}

fn count_like_literal(value: &ScalarValue) -> bool {
    !value.is_null()
        && (value == &COUNT_STAR_EXPANSION
            || matches!(
                value,
                ScalarValue::Int8(_)
                    | ScalarValue::Int16(_)
                    | ScalarValue::Int32(_)
                    | ScalarValue::Int64(_)
                    | ScalarValue::UInt8(_)
                    | ScalarValue::UInt16(_)
                    | ScalarValue::UInt32(_)
                    | ScalarValue::UInt64(_)
                    | ScalarValue::Utf8(_)
                    | ScalarValue::Utf8View(_)
                    | ScalarValue::LargeUtf8(_)
                    | ScalarValue::Boolean(_)
            ))
}
