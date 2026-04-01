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
use crate::expr_fn::{FORMULA_SCORE_FUNCTION_NAME, FUSION_SCORE_FUNCTION_NAME};

#[derive(Debug, Clone, Copy)]
pub(crate) struct CoordinatedCombiners;

impl OptimizerRule for CoordinatedCombiners {
    fn name(&self) -> &'static str { "qdrant_coordinated_combiners" }

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
        reject_unlowered_coordinated_combiners(&transformed.data)?;
        Ok(transformed)
    }
}

fn try_rewrite_combiner(plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
    let LogicalPlan::Limit(limit) = plan else {
        return Ok(None);
    };
    let LogicalPlan::Sort(sort) = limit.input.as_ref() else {
        return Ok(None);
    };
    let LogicalPlan::Projection(projection) = sort.input.as_ref() else {
        return Ok(None);
    };
    let Some(surface) = SurfaceCall::collect(&projection.expr)? else {
        return Ok(None);
    };
    if !surface.allows_multi_branch_coordination() {
        return Ok(None);
    }
    let Some((source, prefetch)) = collect_prefetch_branches(&surface, projection.input.as_ref())?
    else {
        return Ok(None);
    };
    if prefetch.len() < 2 {
        return Ok(None);
    }

    let projection_plan = LogicalPlan::Projection(projection.clone());
    let sort_plan = LogicalPlan::Sort(sort.clone());

    let op = Op::from_surface(surface, &source)?.with_prefetch(prefetch)?;
    let Some(op) = op.project(&projection_plan)? else {
        return Ok(None);
    };
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
                "unsupported coordinated {FORMULA_SCORE_FUNCTION_NAME} shape; admitted rewrite \
                 requires LIMIT -> SORT(score DESC) -> PROJECTION over FULL OUTER JOIN USING (id) \
                 of closed qdrant query branches"
            )
        }
        Some(SurfaceCall::Query(QuerySurfaceCall::Fusion(_))) => {
            plan_err!(
                "unsupported coordinated {FUSION_SCORE_FUNCTION_NAME} shape; admitted rewrite \
                 requires LIMIT -> SORT(score DESC) -> PROJECTION over FULL OUTER JOIN USING (id) \
                 of closed qdrant query branches with explicit score-column inputs"
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
