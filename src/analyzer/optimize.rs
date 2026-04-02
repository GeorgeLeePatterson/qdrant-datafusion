use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchemaRef, Result, plan_err};
use datafusion::logical_expr::{Expr, Extension, JoinType, LogicalPlan};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};

use super::kernel::KernelSpec;
use super::node::KernelNode;
use super::op::Op;
use super::query::{FusionQuery, QueryPrefetchBranch};
use super::source::Source;
use super::state::FiltersState;
use super::surface::{QuerySurfaceCall, SurfaceCall};
use crate::arrow::schema::ID_FIELD_NAME;
use crate::expr_fn::{
    ConditionCall, DatetimeValueCall, DecayCall, FORMULA_SCORE_FUNCTION_NAME,
    FUSION_SCORE_FUNCTION_NAME, FormulaCall, GeoDistanceCall, PayloadDatetimeCall, PayloadNumCall,
};
use crate::pushdown::QdrantPayloadPath;

#[derive(Debug, Clone, Copy)]
pub(crate) struct CoordinatedCombiners;

#[derive(Debug, Clone)]
struct CoordinatedCandidate {
    surface: SurfaceCall,
    source: Source,
    prefetch: Vec<QueryPrefetchBranch>,
    projection_chain: Vec<LogicalPlan>,
    sort_plan: LogicalPlan,
}

impl OptimizerRule for CoordinatedCombiners {
    fn name(&self) -> &'static str {
        "qdrant_coordinated_combiners"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> std::result::Result<Transformed<LogicalPlan>, datafusion::common::DataFusionError> {
        let transformed = plan.transform_up(|plan| {
            if let Some(rewritten) = try_rewrite_combiner(&plan)? {
                Ok(Transformed::yes(rewritten))
            } else {
                Ok(Transformed::no(plan))
            }
        })?;
        let transformed = transformed.data.transform_up(|plan| {
            if let Some(rewritten) = try_rewrite_local_formula(&plan)? {
                Ok(Transformed::yes(rewritten))
            } else {
                Ok(Transformed::no(plan))
            }
        })?;
        reject_unlowered_coordinated_combiners(&transformed.data)?;
        Ok(transformed)
    }
}

fn try_rewrite_combiner(plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
    let Some(candidate) = coordinated_candidate(plan)? else {
        return Ok(None);
    };
    let CoordinatedCandidate { surface, source, prefetch, projection_chain, sort_plan } = candidate;
    let mut op = Op::from_surface(surface, &source)?.with_prefetch(prefetch)?;
    for projection_plan in projection_chain {
        let Some(projected) = op.project(&projection_plan)? else {
            return Ok(None);
        };
        op = projected;
    }
    let Some(op) = op.sort(&sort_plan)? else {
        return Ok(None);
    };
    let Some(kernel) = op.kernel(source, &FiltersState::default(), plan)? else {
        return Ok(None);
    };
    Ok(Some(LogicalPlan::Extension(Extension {
        node: Arc::new(KernelNode::new(Arc::clone(plan.schema()), kernel.spec().clone())),
    })))
}

fn coordinated_candidate(plan: &LogicalPlan) -> Result<Option<CoordinatedCandidate>> {
    let LogicalPlan::Limit(limit) = plan else {
        return Ok(None);
    };
    let Some(search) = descend_to_combiner(limit.input.as_ref(), CoordinationSearch::default())?
    else {
        return Ok(None);
    };
    let CoordinationSearch {
        effective_sort,
        preserved_projections,
        combiner_projection,
        surface,
        branch_input,
    } = search;
    let (Some(surface), Some(combiner_projection), Some(branch_input)) =
        (surface, combiner_projection, branch_input)
    else {
        return Ok(None);
    };
    if !surface.allows_multi_branch_coordination() {
        return Ok(None);
    }
    let Some(sort_plan) = effective_sort else {
        return Ok(None);
    };
    let Some((source, prefetch)) = collect_prefetch_branches(&surface, branch_input)? else {
        return Ok(None);
    };
    if prefetch.len() < 2 {
        return Ok(None);
    }
    let mut projection_chain = vec![combiner_projection];
    projection_chain.extend(preserved_projections.into_iter().rev());
    Ok(Some(CoordinatedCandidate { surface, source, prefetch, projection_chain, sort_plan }))
}

#[derive(Default)]
struct CoordinationSearch<'a> {
    effective_sort: Option<LogicalPlan>,
    preserved_projections: Vec<LogicalPlan>,
    combiner_projection: Option<LogicalPlan>,
    surface: Option<SurfaceCall>,
    branch_input: Option<&'a LogicalPlan>,
}

fn descend_to_combiner<'a>(
    plan: &'a LogicalPlan,
    mut search: CoordinationSearch<'a>,
) -> Result<Option<CoordinationSearch<'a>>> {
    match plan {
        LogicalPlan::SubqueryAlias(alias) => descend_to_combiner(alias.input.as_ref(), search),
        LogicalPlan::Sort(sort) => {
            if search.effective_sort.is_none() {
                search.effective_sort = Some(LogicalPlan::Sort(sort.clone()));
            }
            descend_to_combiner(sort.input.as_ref(), search)
        }
        LogicalPlan::Projection(projection) => {
            if let Some(surface) = SurfaceCall::collect(&projection.expr)? {
                search.surface = Some(surface);
                search.combiner_projection = Some(LogicalPlan::Projection(projection.clone()));
                search.branch_input = Some(projection.input.as_ref());
                Ok(Some(search))
            } else if projection_preserves_coordination(projection) {
                search.preserved_projections.push(LogicalPlan::Projection(projection.clone()));
                descend_to_combiner(projection.input.as_ref(), search)
            } else {
                Ok(None)
            }
        }
        _ => Ok(None),
    }
}

fn projection_preserves_coordination(
    projection: &datafusion::logical_expr::logical_plan::Projection,
) -> bool {
    projection.expr.iter().all(projection_expr_preserves_coordination)
}

fn projection_expr_preserves_coordination(expr: &Expr) -> bool {
    matches!(expr.clone().unalias_nested().data, Expr::Column(_))
}

fn try_rewrite_local_formula(plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
    let LogicalPlan::Projection(projection) = plan else {
        return Ok(None);
    };
    let mut transformed = false;
    let mut exprs = Vec::with_capacity(projection.expr.len());
    for expr in &projection.expr {
        let rewritten = rewrite_local_formula_expr(expr, projection.input.schema())?;
        transformed |= rewritten.transformed;
        exprs.push(rewritten.data);
    }
    if !transformed {
        return Ok(None);
    }
    plan.with_new_exprs(exprs, vec![projection.input.as_ref().clone()])?
        .recompute_schema()
        .map(Some)
}

fn rewrite_local_formula_expr(
    expr: &Expr,
    input_schema: &DFSchemaRef,
) -> Result<Transformed<Expr>> {
    expr.clone().transform_up(|nested| {
        let Some(call) = FormulaCall::from_expr(&nested)? else {
            return Ok(Transformed::no(nested));
        };
        let Some(rewritten) = rewrite_formula_for_local_fallback(&call.formula, input_schema)?
        else {
            return Ok(Transformed::no(nested));
        };
        Ok(Transformed::yes(rewritten))
    })
}

fn rewrite_formula_for_local_fallback(
    expr: &Expr,
    _input_schema: &DFSchemaRef,
) -> Result<Option<Expr>> {
    let mut supported = true;
    let rewritten = expr.clone().transform_up(|nested| {
        if SurfaceCall::from_expr(&nested)?.is_some()
            || ConditionCall::from_expr(&nested)?.is_some()
            || GeoDistanceCall::from_expr(&nested)?.is_some()
            || DecayCall::from_expr(&nested)?.is_some()
        {
            supported = false;
            return Ok(Transformed::no(nested));
        }
        if PayloadNumCall::from_expr(&nested)?.is_some() {
            supported = false;
            return Ok(Transformed::no(nested));
        }
        if PayloadDatetimeCall::from_expr(&nested)?.is_some() {
            supported = false;
            return Ok(Transformed::no(nested));
        }
        if let Some(call) = DatetimeValueCall::from_expr(&nested)? {
            return Ok(Transformed::yes(call.value));
        }
        if QdrantPayloadPath::from_logical_expr(&nested).is_some() {
            supported = false;
            return Ok(Transformed::no(nested));
        }
        Ok(Transformed::no(nested))
    })?;
    Ok(supported.then_some(rewritten.data))
}

fn reject_unlowered_coordinated_combiners(plan: &LogicalPlan) -> Result<()> {
    let mut unsupported = None;
    let _ = plan.apply(|node| {
        if let Some(surface) = SurfaceCall::collect(&node.expressions())?
            && surface.allows_multi_branch_coordination()
        {
            unsupported = Some(surface);
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    match unsupported {
        Some(SurfaceCall::Query(QuerySurfaceCall::Formula(_))) => {
            plan_err!(
                "unsupported coordinated {FORMULA_SCORE_FUNCTION_NAME} shape; admitted cases are \
                 either locally executable SQL arithmetic over resolved columns or a coordinated \
                 remote rewrite with an effective score-desc sort, a finite LIMIT, and an \
                 id-preserving FULL OUTER JOIN USING (id) over closed qdrant query branches"
            )
        }
        Some(SurfaceCall::Query(QuerySurfaceCall::Fusion(_))) => {
            plan_err!(
                "unsupported coordinated {FUSION_SCORE_FUNCTION_NAME} shape; coordinated rewrite \
                 requires explicit score-column inputs, an effective score-desc sort, a finite \
                 LIMIT, and an id-preserving FULL OUTER JOIN USING (id) over closed qdrant query \
                 branches"
            )
        }
        _ => Ok(()),
    }
}

fn collect_prefetch_branches(
    surface: &SurfaceCall,
    plan: &LogicalPlan,
) -> Result<Option<(Source, Vec<QueryPrefetchBranch>)>> {
    let mut leaves = vec![];
    collect_prefetch_branch_leaves(plan, &mut leaves)?;
    let Some((first_source, _)) = leaves.first() else {
        return Ok(None);
    };
    if leaves.iter().skip(1).any(|(source, _)| !first_source.merge_compatible_with(source)) {
        return Ok(None);
    }
    let source = first_source.clone();
    let prefetch = match surface {
        SurfaceCall::Query(QuerySurfaceCall::Fusion(query)) if query.has_explicit_inputs() => {
            order_fusion_prefetch_branches(query, &leaves)?
        }
        SurfaceCall::Query(_) => leaves.into_iter().map(|(_, branch)| branch).collect(),
    };
    Ok(Some((source, prefetch)))
}

fn order_fusion_prefetch_branches(
    query: &FusionQuery,
    leaves: &[(Source, QueryPrefetchBranch)],
) -> Result<Vec<QueryPrefetchBranch>> {
    let mut ordered = vec![];
    let mut used = BTreeSet::new();
    for expr in query.score_inputs() {
        let expr = expr.clone().unalias_nested().data;
        let Expr::Column(column) = expr else {
            return plan_err!(
                "{FUSION_SCORE_FUNCTION_NAME} coordinated rewrite only admits explicit score \
                 column inputs"
            );
        };
        let matches = leaves
            .iter()
            .enumerate()
            .filter_map(|(index, (_, branch))| {
                score_output_matches(&branch.score_output_columns, &column).then_some(index)
            })
            .collect::<Vec<_>>();
        let index = match matches.as_slice() {
            [index] => *index,
            [] => {
                return plan_err!(
                    "{FUSION_SCORE_FUNCTION_NAME} explicit input '{}' does not resolve to a \
                     qdrant score branch",
                    column.flat_name()
                );
            }
            _ => {
                return plan_err!(
                    "{FUSION_SCORE_FUNCTION_NAME} explicit input '{}' is ambiguous across qdrant \
                     score branches",
                    column.flat_name()
                );
            }
        };
        if !used.insert(index) {
            return plan_err!(
                "{FUSION_SCORE_FUNCTION_NAME} coordinated rewrite does not admit repeated qdrant \
                 score branch inputs"
            );
        }
        ordered.push(leaves[index].1.clone());
    }
    if used.len() != leaves.len() {
        return plan_err!(
            "{FUSION_SCORE_FUNCTION_NAME} coordinated rewrite requires an explicit score input \
             for every qdrant branch"
        );
    }
    Ok(ordered)
}

fn score_output_matches(score_output_columns: &BTreeSet<Column>, column: &Column) -> bool {
    if score_output_columns.contains(column) {
        return true;
    }
    if column.relation.is_some() {
        return false;
    }
    let mut matches = score_output_columns.iter().filter(|candidate| candidate.name == column.name);
    matches.next().is_some() && matches.next().is_none()
}

fn collect_prefetch_branch_leaves(
    plan: &LogicalPlan,
    leaves: &mut Vec<(Source, QueryPrefetchBranch)>,
) -> Result<()> {
    match plan {
        LogicalPlan::Join(join) if supported_coordination_join(join) => {
            collect_prefetch_branch_leaves(join.left.as_ref(), leaves)?;
            collect_prefetch_branch_leaves(join.right.as_ref(), leaves)?;
            Ok(())
        }
        _ => {
            let Some((source, branch)) = query_branch_from_plan(plan)? else {
                return Ok(());
            };
            leaves.push((source, branch));
            Ok(())
        }
    }
}

fn supported_coordination_join(join: &datafusion::logical_expr::logical_plan::Join) -> bool {
    join.join_type == JoinType::Full
        && join.filter.is_none()
        && join.on.len() == 1
        && matches!(
            (&join.on[0].0, &join.on[0].1),
            (Expr::Column(left), Expr::Column(right))
                if left.name == ID_FIELD_NAME && right.name == ID_FIELD_NAME
        )
}

fn query_branch_from_plan(plan: &LogicalPlan) -> Result<Option<(Source, QueryPrefetchBranch)>> {
    match plan {
        LogicalPlan::SubqueryAlias(alias) => {
            query_branch_from_plan_with_schema(plan.schema(), alias.input.as_ref())
        }
        _ => query_branch_from_plan_with_schema(plan.schema(), plan),
    }
}

fn query_branch_from_plan_with_schema(
    output_schema: &DFSchemaRef,
    plan: &LogicalPlan,
) -> Result<Option<(Source, QueryPrefetchBranch)>> {
    let LogicalPlan::Extension(extension) = plan else {
        return Ok(None);
    };
    let Some(node) = extension.node.as_any().downcast_ref::<KernelNode>() else {
        return Ok(None);
    };
    let KernelSpec::Query(query) = node.spec() else {
        return Ok(None);
    };
    Ok(Some((query.source().clone(), query.prefetch_branch(output_schema)?)))
}
