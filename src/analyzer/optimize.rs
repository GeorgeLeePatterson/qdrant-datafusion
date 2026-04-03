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
    qdrant_payload_bool_access, qdrant_payload_datetime_access, qdrant_payload_float_access,
    qdrant_payload_int_access, qdrant_payload_text_access,
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
            if let Some(rewritten) = try_rewrite_sort_through_projection(&plan)? {
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
        reject_unlowered_coordinated_combiners(&transformed.data)?;
        Ok(transformed)
    }
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
            expr: rewritten_expr.data,
            asc: sort_expr.asc,
            nulls_first: sort_expr.nulls_first,
        });
    }
    if !rewritten {
        return Ok(None);
    }
    let rewritten_sort = LogicalPlan::Sort(datafusion::logical_expr::logical_plan::Sort {
        expr: rewritten_sort_exprs,
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
    if prefetch.len() < 2 {
        return Ok(None);
    }
    let mut projection_chain = combiner_projection.into_iter().collect::<Vec<_>>();
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
    let Expr::BinaryExpr(binary) = expr.clone().unalias_nested().data else {
        return Transformed::no(expr.clone());
    };
    if binary.op != datafusion::logical_expr::Operator::Colon {
        return Transformed::no(expr.clone());
    }
    let Expr::Column(column) = binary.left.as_ref() else {
        return Transformed::no(expr.clone());
    };
    if column.name != crate::arrow::schema::PAYLOAD_FIELD_NAME {
        return Transformed::no(expr.clone());
    }
    let Some(path) = QdrantPayloadPath::from_logical_expr(expr).map(|path| path.key().to_owned())
    else {
        return Transformed::no(expr.clone());
    };
    let payload = Expr::Column(column.clone());
    let rewritten = match data_type {
        datafusion::arrow::datatypes::DataType::Utf8 => qdrant_payload_text_access(payload, path),
        datafusion::arrow::datatypes::DataType::LargeUtf8 => {
            Expr::Cast(datafusion::logical_expr::expr::Cast::new(
                Box::new(qdrant_payload_text_access(payload, path)),
                datafusion::arrow::datatypes::DataType::LargeUtf8,
            ))
        }
        datafusion::arrow::datatypes::DataType::Boolean => {
            qdrant_payload_bool_access(payload, path)
        }
        datafusion::arrow::datatypes::DataType::Int64 => qdrant_payload_int_access(payload, path),
        datafusion::arrow::datatypes::DataType::Int8
        | datafusion::arrow::datatypes::DataType::Int16
        | datafusion::arrow::datatypes::DataType::Int32
        | datafusion::arrow::datatypes::DataType::UInt8
        | datafusion::arrow::datatypes::DataType::UInt16
        | datafusion::arrow::datatypes::DataType::UInt32
        | datafusion::arrow::datatypes::DataType::UInt64 => {
            Expr::Cast(datafusion::logical_expr::expr::Cast::new(
                Box::new(qdrant_payload_int_access(payload, path)),
                data_type.clone(),
            ))
        }
        datafusion::arrow::datatypes::DataType::Float64 => {
            qdrant_payload_float_access(payload, path)
        }
        datafusion::arrow::datatypes::DataType::Float32 => {
            Expr::Cast(datafusion::logical_expr::expr::Cast::new(
                Box::new(qdrant_payload_float_access(payload, path)),
                datafusion::arrow::datatypes::DataType::Float32,
            ))
        }
        datafusion::arrow::datatypes::DataType::Timestamp(
            datafusion::arrow::datatypes::TimeUnit::Millisecond,
            None,
        ) => qdrant_payload_datetime_access(payload, path),
        _ => return Transformed::no(expr.clone()),
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
