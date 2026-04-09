use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchemaRef, Result, plan_err};
use datafusion::functions_window::expr_fn::row_number;
use datafusion::logical_expr::expr_fn::{cast, when};
use datafusion::logical_expr::utils::expr_to_columns;
use datafusion::logical_expr::{Expr, Extension, Join, JoinType, LogicalPlan};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::prelude::{col, lit};

use super::kernel::KernelSpec;
use super::node::KernelNode;
use super::op::Op;
use super::query::{FormulaQuery, FusionQuery, QueryPrefetchBranch};
use super::source::Source;
use super::state::FiltersState;
use super::surface::{QuerySurfaceCall, SurfaceCall};
use crate::arrow::schema::ID_FIELD_NAME;
use crate::expr_fn::{
    ConditionCall, DatetimeValueCall, DecayCall, FORMULA_SCORE_FUNCTION_NAME,
    FUSION_SCORE_FUNCTION_NAME, FormulaCall, FusionCall, GeoDistanceCall, PayloadDatetimeCall,
    PayloadNumCall, payload_access_expr,
};
use crate::qdrant::{QdrantPayloadAccess, QdrantPayloadPath};

#[derive(Debug, Clone, Copy)]
pub(crate) struct CoordinatedCombiners;

#[derive(Debug, Clone)]
struct CoordinatedCandidate {
    surface:          SurfaceCall,
    source:           Source,
    prefetch:         Vec<QueryPrefetchBranch>,
    projection_chain: Vec<LogicalPlan>,
    sort_plan:        LogicalPlan,
}

impl OptimizerRule for CoordinatedCombiners {
    fn name(&self) -> &'static str { "qdrant_coordinated_combiners" }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> std::result::Result<Transformed<LogicalPlan>, datafusion::common::DataFusionError> {
        let transformed = plan.transform_up(|plan| {
            if let Some(rewritten) = try_rewrite_sort_through_projection(&plan)? {
                Ok(Transformed::yes(rewritten))
            } else {
                Ok(Transformed::no(plan))
            }
        })?;
        let transformed = transformed.data.transform_up(|plan| {
            if let Some(rewritten) = try_rewrite_single_branch_formula(&plan)? {
                Ok(Transformed::yes(rewritten))
            } else {
                Ok(Transformed::no(plan))
            }
        })?;
        let transformed = transformed.data.transform_up(|plan| {
            if let Some(rewritten) = try_rewrite_combiner(&plan)? {
                Ok(Transformed::yes(rewritten))
            } else {
                Ok(Transformed::no(plan))
            }
        })?;
        let transformed = transformed.data.transform_up(|plan| {
            if let Some(rewritten) = try_rewrite_local_payload_projection(&plan)? {
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
        let transformed = transformed.data.transform_up(|plan| {
            if let Some(rewritten) = try_rewrite_local_fusion(&plan)? {
                Ok(Transformed::yes(rewritten))
            } else {
                Ok(Transformed::no(plan))
            }
        })?;
        reject_unlowered_coordinated_combiners(&transformed.data)?;
        Ok(transformed)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum JoinSide {
    Left,
    Right,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalFusionInput {
    side:             JoinSide,
    hidden_rank_name: String,
}

fn try_rewrite_single_branch_formula(plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
    match plan {
        LogicalPlan::Projection(projection) => match projection.input.as_ref() {
            LogicalPlan::Sort(sort) => {
                let LogicalPlan::Join(join) = sort.input.as_ref() else {
                    return Ok(None);
                };
                rewrite_single_branch_formula_plan(
                    join,
                    Some(projection.expr.as_slice()),
                    Some(sort.expr.as_slice()),
                )
            }
            LogicalPlan::Join(join) => {
                rewrite_single_branch_formula_plan(join, Some(projection.expr.as_slice()), None)
            }
            _ => Ok(None),
        },
        LogicalPlan::Sort(sort) => {
            let LogicalPlan::Join(join) = sort.input.as_ref() else {
                return Ok(None);
            };
            rewrite_single_branch_formula_plan(join, None, Some(sort.expr.as_slice()))
        }
        _ => Ok(None),
    }
}

fn rewrite_single_branch_formula_plan(
    join: &Join,
    projection_exprs: Option<&[Expr]>,
    sort_exprs: Option<&[datafusion::logical_expr::SortExpr]>,
) -> Result<Option<LogicalPlan>> {
    let Some((formula_expr, formula_query, side)) =
        branch_local_formula_rewrite(join, projection_exprs, sort_exprs)?
    else {
        return Ok(None);
    };
    let hidden_name = unique_formula_output_name(&join.schema);
    let rewritten_join = rewrite_join_with_formula_branch(join, &formula_expr, &hidden_name, side)?;
    let rewritten_projection_exprs = projection_exprs
        .map(|exprs| {
            exprs
                .iter()
                .map(|expr| rewrite_formula_surface_to_column(expr, &formula_query, &hidden_name))
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?;
    let rewritten_sort_exprs = sort_exprs
        .map(|exprs| {
            exprs
                .iter()
                .map(|sort_expr| {
                    Ok(datafusion::logical_expr::SortExpr {
                        expr:        rewrite_formula_surface_to_column(
                            &sort_expr.expr,
                            &formula_query,
                            &hidden_name,
                        )?,
                        asc:         sort_expr.asc,
                        nulls_first: sort_expr.nulls_first,
                    })
                })
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?;

    let mut plan = LogicalPlan::Join(rewritten_join);
    if let Some(sort_exprs) = rewritten_sort_exprs {
        plan = LogicalPlan::Sort(datafusion::logical_expr::logical_plan::Sort {
            expr:  sort_exprs,
            input: Arc::new(plan),
            fetch: None,
        });
    }
    if let Some(projection_exprs) = rewritten_projection_exprs {
        plan = datafusion::logical_expr::LogicalPlanBuilder::from(plan)
            .project(projection_exprs)?
            .build()?;
    }
    plan.recompute_schema().map(Some)
}

fn branch_local_formula_rewrite(
    join: &Join,
    projection_exprs: Option<&[Expr]>,
    sort_exprs: Option<&[datafusion::logical_expr::SortExpr]>,
) -> Result<Option<(Expr, FormulaQuery, JoinSide)>> {
    let mut formula_expr = None;
    let mut formula_query = None;
    if let Some(exprs) = projection_exprs {
        for expr in exprs {
            collect_formula_surface(expr, &mut formula_expr, &mut formula_query)?;
        }
    }
    if let Some(exprs) = sort_exprs {
        for expr in exprs {
            collect_formula_surface(&expr.expr, &mut formula_expr, &mut formula_query)?;
        }
    }
    let (Some(formula_expr), Some(formula_query)) = (formula_expr, formula_query) else {
        return Ok(None);
    };
    let Some(side) = formula_join_side(join, &formula_expr)? else {
        return Ok(None);
    };
    let target_plan = match side {
        JoinSide::Left => join.left.as_ref(),
        JoinSide::Right => join.right.as_ref(),
    };
    let Some((source, branch)) = query_branch_from_plan(target_plan)? else {
        return Ok(None);
    };
    if formula_query.validate_on_source(&source).is_err() {
        return Ok(None);
    }
    if formula_query.descriptor(&source, &[branch]).is_err() {
        return Ok(None);
    }
    Ok(Some((formula_expr, formula_query, side)))
}

fn collect_formula_surface(
    expr: &Expr,
    formula_expr: &mut Option<Expr>,
    formula_query: &mut Option<FormulaQuery>,
) -> Result<()> {
    let mut found = None;
    let _ = expr.apply(|node| {
        let Some(call) = FormulaCall::from_expr(node)? else {
            return Ok(TreeNodeRecursion::Continue);
        };
        found = Some((node.clone(), FormulaQuery::try_from(call)?));
        Ok(TreeNodeRecursion::Jump)
    })?;
    let Some((candidate_expr, candidate_query)) = found else {
        return Ok(());
    };
    if let Some(existing) = formula_query {
        if !existing.same_semantics(&candidate_query) {
            return Ok(());
        }
    } else {
        *formula_expr = Some(candidate_expr);
        *formula_query = Some(candidate_query);
    }
    Ok(())
}

fn formula_join_side(join: &Join, formula_expr: &Expr) -> Result<Option<JoinSide>> {
    let Some(call) = FormulaCall::from_expr(formula_expr)? else {
        return Ok(None);
    };
    let mut columns = HashSet::new();
    expr_to_columns(&call.formula, &mut columns)?;
    let mut side = None;
    for column in columns {
        let in_left = join.left.schema().index_of_column(&column).is_ok();
        let in_right = join.right.schema().index_of_column(&column).is_ok();
        let column_side = match (in_left, in_right) {
            (true, false) => JoinSide::Left,
            (false, true) => JoinSide::Right,
            _ => return Ok(None),
        };
        if let Some(existing) = side {
            if existing != column_side {
                return Ok(None);
            }
        } else {
            side = Some(column_side);
        }
    }
    Ok(side)
}

fn rewrite_join_with_formula_branch(
    join: &Join,
    formula_expr: &Expr,
    hidden_name: &str,
    side: JoinSide,
) -> Result<Join> {
    let (left, right) = match side {
        JoinSide::Left => (
            Arc::new(formula_projected_branch(join.left.as_ref(), formula_expr, hidden_name)?),
            Arc::clone(&join.right),
        ),
        JoinSide::Right => (
            Arc::clone(&join.left),
            Arc::new(formula_projected_branch(join.right.as_ref(), formula_expr, hidden_name)?),
        ),
    };
    Join::try_new(
        left,
        right,
        join.on.clone(),
        join.filter.clone(),
        join.join_type,
        join.join_constraint,
        join.null_equality,
        join.null_aware,
    )
}

fn formula_projected_branch(
    plan: &LogicalPlan,
    formula_expr: &Expr,
    hidden_name: &str,
) -> Result<LogicalPlan> {
    let mut projection_exprs =
        plan.schema().columns().into_iter().map(Expr::Column).collect::<Vec<_>>();
    projection_exprs.push(formula_expr.clone().alias(hidden_name.to_owned()));
    let projected = datafusion::logical_expr::LogicalPlanBuilder::from(plan.clone())
        .project(projection_exprs)?
        .build()?;
    datafusion::logical_expr::LogicalPlanBuilder::from(projected)
        .sort(vec![col(hidden_name).sort(false, false)])?
        .build()
}

fn rewrite_formula_surface_to_column(
    expr: &Expr,
    target: &FormulaQuery,
    hidden_name: &str,
) -> Result<Expr> {
    expr.clone()
        .transform_up(|nested| {
            let Some(call) = FormulaCall::from_expr(&nested)? else {
                return Ok(Transformed::no(nested));
            };
            let query = FormulaQuery::try_from(call)?;
            if !query.same_semantics(target) {
                return Ok(Transformed::no(nested));
            }
            Ok(Transformed::yes(Expr::Column(Column::from_name(hidden_name))))
        })
        .map(|rewritten| rewritten.data)
}

fn unique_formula_output_name(schema: &DFSchemaRef) -> String {
    let base = "__qdrant_formula_score";
    if schema.fields().iter().all(|field| field.name() != base) {
        return base.to_owned();
    }
    let mut index = 0_usize;
    loop {
        let candidate = format!("{base}_{index}");
        if schema.fields().iter().all(|field| field.name().as_str() != candidate) {
            return candidate;
        }
        index += 1;
    }
}

fn try_rewrite_local_fusion(plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
    let LogicalPlan::Projection(projection) = plan else {
        return Ok(None);
    };
    match projection.input.as_ref() {
        LogicalPlan::Sort(sort) => {
            let LogicalPlan::Join(join) = sort.input.as_ref() else {
                return Ok(None);
            };
            rewrite_local_fusion_plan(join, projection.expr.as_slice(), Some(sort.expr.as_slice()))
        }
        LogicalPlan::Join(join) => {
            rewrite_local_fusion_plan(join, projection.expr.as_slice(), None)
        }
        _ => Ok(None),
    }
}

fn rewrite_local_fusion_plan(
    join: &Join,
    projection_exprs: &[Expr],
    sort_exprs: Option<&[datafusion::logical_expr::SortExpr]>,
) -> Result<Option<LogicalPlan>> {
    let Some((fusion_query, rrf_k, inputs)) =
        branch_local_fusion_rewrite(join, projection_exprs, sort_exprs)?
    else {
        return Ok(None);
    };
    let left_hidden = inputs
        .iter()
        .find(|input| input.side == JoinSide::Left)
        .map(|input| input.hidden_rank_name.as_str());
    let right_hidden = inputs
        .iter()
        .find(|input| input.side == JoinSide::Right)
        .map(|input| input.hidden_rank_name.as_str());
    let left = match left_hidden {
        Some(hidden_name) => Arc::new(fusion_projected_branch(join.left.as_ref(), hidden_name)?),
        None => Arc::clone(&join.left),
    };
    let right = match right_hidden {
        Some(hidden_name) => Arc::new(fusion_projected_branch(join.right.as_ref(), hidden_name)?),
        None => Arc::clone(&join.right),
    };
    let rewritten_join = Join::try_new(
        left,
        right,
        join.on.clone(),
        join.filter.clone(),
        join.join_type,
        join.join_constraint,
        join.null_equality,
        join.null_aware,
    )?;
    let rewritten_projection_exprs = projection_exprs
        .iter()
        .map(|expr| rewrite_fusion_surface_to_local_expr(expr, &fusion_query, &inputs, rrf_k))
        .collect::<Result<Vec<_>>>()?;
    let rewritten_sort_exprs = sort_exprs
        .map(|exprs| {
            exprs
                .iter()
                .map(|sort_expr| {
                    Ok(datafusion::logical_expr::SortExpr {
                        expr:        rewrite_fusion_surface_to_local_expr(
                            &sort_expr.expr,
                            &fusion_query,
                            &inputs,
                            rrf_k,
                        )?,
                        asc:         sort_expr.asc,
                        nulls_first: sort_expr.nulls_first,
                    })
                })
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?;

    let mut plan = LogicalPlan::Join(rewritten_join);
    if let Some(sort_exprs) = rewritten_sort_exprs {
        plan = LogicalPlan::Sort(datafusion::logical_expr::logical_plan::Sort {
            expr:  sort_exprs,
            input: Arc::new(plan),
            fetch: None,
        });
    }
    plan = datafusion::logical_expr::LogicalPlanBuilder::from(plan)
        .project(rewritten_projection_exprs)?
        .build()?;
    plan.recompute_schema().map(Some)
}

fn branch_local_fusion_rewrite(
    join: &Join,
    projection_exprs: &[Expr],
    sort_exprs: Option<&[datafusion::logical_expr::SortExpr]>,
) -> Result<Option<(FusionQuery, u32, Vec<LocalFusionInput>)>> {
    if !supported_local_fusion_join(join) {
        return Ok(None);
    }

    let mut fusion_query = None;
    for expr in projection_exprs {
        collect_fusion_surface(expr, &mut fusion_query)?;
    }
    if let Some(exprs) = sort_exprs {
        for expr in exprs {
            collect_fusion_surface(&expr.expr, &mut fusion_query)?;
        }
    }
    let Some(fusion_query) = fusion_query else {
        return Ok(None);
    };
    let Some(rrf_k) = fusion_query.local_rrf_k() else {
        return Ok(None);
    };

    let Some((_, left_branch)) = query_branch_from_plan(join.left.as_ref())? else {
        return Ok(None);
    };
    let Some((_, right_branch)) = query_branch_from_plan(join.right.as_ref())? else {
        return Ok(None);
    };

    let mut seen_sides = BTreeSet::new();
    let mut inputs = Vec::with_capacity(fusion_query.score_inputs().len());
    for score_input in fusion_query.score_inputs() {
        let expr = score_input.clone().unalias_nested().data;
        let Expr::Column(column) = expr else {
            return Ok(None);
        };
        let side = match (
            score_output_matches(&left_branch.score_output_columns, &column),
            score_output_matches(&right_branch.score_output_columns, &column),
        ) {
            (true, false) => JoinSide::Left,
            (false, true) => JoinSide::Right,
            _ => return Ok(None),
        };
        if !seen_sides.insert(side) {
            return Ok(None);
        }
        inputs.push(LocalFusionInput {
            side,
            hidden_rank_name: unique_fusion_rank_output_name(&join.schema, side),
        });
    }
    Ok(Some((fusion_query, rrf_k, inputs)))
}

fn collect_fusion_surface(expr: &Expr, fusion_query: &mut Option<FusionQuery>) -> Result<()> {
    let mut found = None;
    let _ = expr.apply(|node| {
        let Some(call) = FusionCall::from_expr(node)? else {
            return Ok(TreeNodeRecursion::Continue);
        };
        found = Some(FusionQuery::try_from(call)?);
        Ok(TreeNodeRecursion::Jump)
    })?;
    let Some(found_query) = found else {
        return Ok(());
    };
    if let Some(existing) = fusion_query {
        if !existing.same_semantics(&found_query) {
            return Ok(());
        }
    } else {
        *fusion_query = Some(found_query);
    }
    Ok(())
}

fn supported_local_fusion_join(join: &Join) -> bool {
    matches!(join.join_type, JoinType::Inner | JoinType::Left | JoinType::Right)
        && join.filter.is_none()
        && join.on.len() == 1
        && matches!(
            (&join.on[0].0, &join.on[0].1),
            (Expr::Column(left), Expr::Column(right))
                if left.name == ID_FIELD_NAME && right.name == ID_FIELD_NAME
        )
}

fn unique_fusion_rank_output_name(schema: &DFSchemaRef, side: JoinSide) -> String {
    let base = match side {
        JoinSide::Left => "__qdrant_fusion_left_rank",
        JoinSide::Right => "__qdrant_fusion_right_rank",
    };
    if schema.fields().iter().all(|field| field.name() != base) {
        return base.to_owned();
    }
    let mut index = 0_usize;
    loop {
        let candidate = format!("{base}_{index}");
        if schema.fields().iter().all(|field| field.name().as_str() != candidate) {
            return candidate;
        }
        index += 1;
    }
}

fn fusion_projected_branch(plan: &LogicalPlan, hidden_name: &str) -> Result<LogicalPlan> {
    let windowed = datafusion::logical_expr::LogicalPlanBuilder::from(plan.clone())
        .window(vec![row_number().alias(hidden_name.to_owned())])?
        .build()?;
    let mut projection_exprs =
        plan.schema().columns().into_iter().map(Expr::Column).collect::<Vec<_>>();
    projection_exprs.push(Expr::Column(Column::from_name(hidden_name)));
    datafusion::logical_expr::LogicalPlanBuilder::from(windowed)
        .project(projection_exprs)?
        .build()
}

fn rewrite_fusion_surface_to_local_expr(
    expr: &Expr,
    target: &FusionQuery,
    inputs: &[LocalFusionInput],
    rrf_k: u32,
) -> Result<Expr> {
    expr.clone()
        .transform_up(|nested| {
            let Some(call) = FusionCall::from_expr(&nested)? else {
                return Ok(Transformed::no(nested));
            };
            let query = FusionQuery::try_from(call)?;
            if !query.same_semantics(target) {
                return Ok(Transformed::no(nested));
            }
            Ok(Transformed::yes(local_rrf_expr(rrf_k, inputs)?))
        })
        .map(|rewritten| rewritten.data)
}

fn local_rrf_expr(rrf_k: u32, inputs: &[LocalFusionInput]) -> Result<Expr> {
    let mut combined = None;
    for input in inputs {
        let rank_expr = Expr::Column(Column::from_name(input.hidden_rank_name.clone()));
        let contribution = when(rank_expr.clone().is_null(), lit(0.0_f32)).otherwise(cast(
            lit(1.0_f64)
                / (cast(rank_expr, datafusion::arrow::datatypes::DataType::Float64)
                    + lit(f64::from(rrf_k))),
            datafusion::arrow::datatypes::DataType::Float32,
        ))?;
        combined = Some(match combined {
            Some(existing) => existing + contribution,
            None => contribution,
        });
    }
    combined.ok_or_else(|| {
        datafusion::error::DataFusionError::Plan(format!(
            "{FUSION_SCORE_FUNCTION_NAME} local fallback requires one or more explicit score \
             inputs"
        ))
    })
}

fn try_rewrite_sort_through_projection(plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
    let LogicalPlan::Sort(sort) = plan else {
        return Ok(None);
    };
    let LogicalPlan::Projection(projection) = sort.input.as_ref() else {
        return Ok(None);
    };
    if SurfaceCall::collect(&projection.expr)?.is_some() {
        return Ok(None);
    }
    let mut rewritten = false;
    let mut rewritten_sort_exprs = Vec::with_capacity(sort.expr.len());
    for sort_expr in &sort.expr {
        let rewritten_expr = rewrite_sort_expr_through_projection(&sort_expr.expr, projection)?;
        rewritten |= rewritten_expr.transformed;
        rewritten_sort_exprs.push(datafusion::logical_expr::SortExpr {
            expr:        rewritten_expr.data,
            asc:         sort_expr.asc,
            nulls_first: sort_expr.nulls_first,
        });
    }
    if !rewritten {
        return Ok(None);
    }
    let rewritten_sort = LogicalPlan::Sort(datafusion::logical_expr::logical_plan::Sort {
        expr:  rewritten_sort_exprs,
        input: Arc::new(projection.input.as_ref().clone()),
        fetch: sort.fetch,
    });
    LogicalPlan::Projection(projection.clone())
        .with_new_exprs(projection.expr.clone(), vec![rewritten_sort])?
        .recompute_schema()
        .map(Some)
}

fn rewrite_sort_expr_through_projection(
    expr: &Expr,
    projection: &datafusion::logical_expr::logical_plan::Projection,
) -> Result<Transformed<Expr>> {
    expr.clone().transform_up(|nested| {
        let Expr::Column(column) = &nested else {
            return Ok(Transformed::no(nested));
        };
        let Ok(index) = projection.schema.index_of_column(column) else {
            return Ok(Transformed::no(nested));
        };
        Ok(Transformed::yes(projection.expr[index].clone().unalias_nested().data))
    })
}

fn try_rewrite_combiner(plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
    let Some(candidate) = coordinated_candidate(plan)? else {
        return Ok(None);
    };
    let CoordinatedCandidate { surface, source, prefetch, projection_chain, sort_plan } = candidate;
    let mut op = Op::from_surface(surface, &source)?.with_prefetch(prefetch)?;
    for projection_plan in projection_chain {
        let Some(projected) = op.project(&source, &projection_plan)? else {
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
    let Some(search) = descend_to_combiner(plan, CoordinationSearch::default())? else {
        return Ok(None);
    };
    let CoordinationSearch {
        effective_sort,
        preserved_projections,
        combiner_projection,
        surface,
        branch_input,
    } = search;
    let (Some(surface), Some(branch_input)) = (surface, branch_input) else {
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
    if !coordination_prefetch_supported(&surface, prefetch.len()) {
        return Ok(None);
    }
    let mut projection_chain = combiner_projection.into_iter().collect::<Vec<_>>();
    projection_chain.extend(preserved_projections.into_iter().rev());
    Ok(Some(CoordinatedCandidate { surface, source, prefetch, projection_chain, sort_plan }))
}

fn coordination_prefetch_supported(surface: &SurfaceCall, prefetch_len: usize) -> bool {
    match surface {
        SurfaceCall::Query(QuerySurfaceCall::Formula(_)) => prefetch_len >= 1,
        SurfaceCall::Query(_) => prefetch_len >= 2,
    }
}

#[derive(Default)]
struct CoordinationSearch<'a> {
    effective_sort:        Option<LogicalPlan>,
    preserved_projections: Vec<LogicalPlan>,
    combiner_projection:   Option<LogicalPlan>,
    surface:               Option<SurfaceCall>,
    branch_input:          Option<&'a LogicalPlan>,
}

fn descend_to_combiner<'a>(
    plan: &'a LogicalPlan,
    mut search: CoordinationSearch<'a>,
) -> Result<Option<CoordinationSearch<'a>>> {
    match plan {
        LogicalPlan::SubqueryAlias(alias) => {
            if search.surface.is_some() {
                search.branch_input = Some(alias.input.as_ref());
            }
            descend_to_combiner(alias.input.as_ref(), search)
        }
        LogicalPlan::Sort(sort) => {
            if search.effective_sort.is_none() {
                search.effective_sort = Some(LogicalPlan::Sort(sort.clone()));
            }
            if search.surface.is_none()
                && let Some(surface) = SurfaceCall::collect(
                    &sort.expr.iter().map(|sort_expr| sort_expr.expr.clone()).collect::<Vec<_>>(),
                )?
            {
                search.surface = Some(surface);
                search.branch_input = Some(sort.input.as_ref());
            } else if search.surface.is_some() {
                search.branch_input = Some(sort.input.as_ref());
            }
            descend_to_combiner(sort.input.as_ref(), search)
        }
        LogicalPlan::Projection(projection) => {
            if let Some(surface) = SurfaceCall::collect(&projection.expr)? {
                if let Some(existing) = &search.surface
                    && !existing.same_semantics(&surface)
                {
                    return Ok(None);
                }
                search.surface = Some(surface);
                search.combiner_projection = Some(LogicalPlan::Projection(projection.clone()));
                search.branch_input = Some(projection.input.as_ref());
                Ok(Some(search))
            } else if projection_preserves_coordination(projection) {
                search.preserved_projections.push(LogicalPlan::Projection(projection.clone()));
                if search.surface.is_some() {
                    search.branch_input = Some(projection.input.as_ref());
                }
                descend_to_combiner(projection.input.as_ref(), search)
            } else {
                Ok(None)
            }
        }
        _ => Ok(search.surface.is_some().then_some(search)),
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

fn try_rewrite_local_payload_projection(plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
    let LogicalPlan::Projection(projection) = plan else {
        return Ok(None);
    };
    let mut transformed = false;
    let mut rewritten_exprs = Vec::with_capacity(projection.expr.len());
    for (index, expr) in projection.expr.iter().enumerate() {
        if SurfaceCall::collect(std::slice::from_ref(expr))?.is_some() {
            rewritten_exprs.push(expr.clone());
            continue;
        }
        let rewritten = rewrite_local_payload_projection_expr(
            expr,
            projection.schema.field(index).data_type(),
        )?;
        transformed |= rewritten.transformed;
        rewritten_exprs.push(rewritten.data);
    }
    if !transformed {
        return Ok(None);
    }
    plan.with_new_exprs(rewritten_exprs, vec![projection.input.as_ref().clone()])?
        .recompute_schema()
        .map(Some)
}

fn rewrite_local_payload_projection_expr(
    expr: &Expr,
    data_type: &datafusion::arrow::datatypes::DataType,
) -> Result<Transformed<Expr>> {
    expr.clone()
        .transform_up(|nested| Ok(rewrite_direct_payload_projection_expr(&nested, data_type)))
}

fn rewrite_direct_payload_projection_expr(
    expr: &Expr,
    data_type: &datafusion::arrow::datatypes::DataType,
) -> Transformed<Expr> {
    let Some(access) = QdrantPayloadAccess::from_raw_logical_expr(expr) else {
        return Transformed::no(expr.clone());
    };
    let (payload, path) = access.into_parts();
    let Some(rewritten) = payload_access_expr(payload, path, data_type) else {
        return Transformed::no(expr.clone());
    };
    Transformed::yes(rewritten)
}

fn try_rewrite_local_formula(plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
    let exprs = plan.expressions();
    if exprs.is_empty() {
        return Ok(None);
    }
    let mut transformed = false;
    let mut rewritten_exprs = Vec::with_capacity(exprs.len());
    for expr in &exprs {
        let rewritten = rewrite_local_formula_expr(expr)?;
        transformed |= rewritten.transformed;
        rewritten_exprs.push(rewritten.data);
    }
    if !transformed {
        return Ok(None);
    }
    plan.with_new_exprs(rewritten_exprs, plan.inputs().into_iter().cloned().collect())?
        .recompute_schema()
        .map(Some)
}

fn rewrite_local_formula_expr(expr: &Expr) -> Result<Transformed<Expr>> {
    expr.clone().transform_up(|nested| {
        let Some(call) = FormulaCall::from_expr(&nested)? else {
            return Ok(Transformed::no(nested));
        };
        let Some(rewritten) = rewrite_formula_for_local_fallback(&call.formula)? else {
            return Ok(Transformed::no(nested));
        };
        Ok(Transformed::yes(rewritten))
    })
}

fn rewrite_formula_for_local_fallback(expr: &Expr) -> Result<Option<Expr>> {
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
                 remote rewrite with an effective score-desc sort and an id-preserving FULL OUTER \
                 JOIN USING (id) over closed qdrant query branches"
            )
        }
        Some(SurfaceCall::Query(QuerySurfaceCall::Fusion(_))) => {
            plan_err!(
                "unsupported coordinated {FUSION_SCORE_FUNCTION_NAME} shape; coordinated rewrite \
                 requires explicit score-column inputs, an effective score-desc sort, and an \
                 id-preserving FULL OUTER JOIN USING (id) over closed qdrant query branches"
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

fn supported_coordination_join(join: &Join) -> bool {
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
