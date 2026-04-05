use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::{Expr, LogicalPlan};

use super::State;
use crate::analyzer::KernelSpec;
use crate::analyzer::node::KernelNode;
use crate::analyzer::surface::SurfaceCall;

#[derive(Debug, Clone, Default)]
pub(crate) struct LocalState;

#[expect(
    clippy::unnecessary_wraps,
    reason = "state transition methods share a uniform Result-based interface across variants"
)]
impl LocalState {
    pub(super) fn projection(
        self,
        mut plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        let mut transformed = transformed;
        if let Some(rewritten) = rewrite_projection_over_local_query_filter_shell(&plan)? {
            plan = rewritten;
            transformed = true;
        }
        if unsupported_local_surface(&plan)? {
            return Ok(super::super::fatal(
                plan,
                transformed,
                "qdrant surface call requires a qdrant source",
            ));
        }
        Ok(super::super::Analysis::new(plan, State::Local(self), transformed))
    }

    pub(super) fn filter(
        self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        if unsupported_local_surface(&plan)? {
            return Ok(super::super::fatal(
                plan,
                transformed,
                "qdrant surface call requires a qdrant source",
            ));
        }
        Ok(super::super::Analysis::new(plan, State::Local(self), transformed))
    }

    pub(super) fn sort(
        self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        if unsupported_local_surface(&plan)? {
            return Ok(super::super::fatal(
                plan,
                transformed,
                "qdrant surface call requires a qdrant source",
            ));
        }
        Ok(super::super::Analysis::new(plan, State::Local(self), transformed))
    }

    pub(super) fn limit(
        self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        Ok(super::super::Analysis::new(plan, State::Local(self), transformed))
    }

    pub(super) fn aggregate(
        self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        if unsupported_local_surface(&plan)? {
            return Ok(super::super::fatal(
                plan,
                transformed,
                "qdrant surface call requires a qdrant source",
            ));
        }
        Ok(super::super::Analysis::new(plan, State::Local(self), transformed))
    }

    pub(super) fn unary(
        self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        if unsupported_local_surface(&plan)? {
            return Ok(super::super::fatal(
                plan,
                transformed,
                "qdrant surface call requires a qdrant source",
            ));
        }
        Ok(super::super::Analysis::new(plan, State::Local(self), transformed))
    }
}

fn unsupported_local_surface(plan: &LogicalPlan) -> Result<bool> {
    Ok(SurfaceCall::collect(&plan.expressions())?
        .is_some_and(|surface| !surface.allows_multi_branch_coordination()))
}

fn rewrite_projection_over_local_query_filter_shell(
    plan: &LogicalPlan,
) -> Result<Option<LogicalPlan>> {
    let LogicalPlan::Projection(projection) = plan else {
        return Ok(None);
    };
    let LogicalPlan::Projection(shell_projection) = projection.input.as_ref() else {
        return Ok(None);
    };
    if !shell_projection
        .expr
        .iter()
        .all(|expr| matches!(expr.clone().unalias_nested().data, Expr::Column(_)))
    {
        return Ok(None);
    }
    let filter_plan = shell_projection.input.as_ref().clone();
    let LogicalPlan::Filter(filter) = &filter_plan else {
        return Ok(None);
    };
    let LogicalPlan::Extension(extension) = filter.input.as_ref() else {
        return Ok(None);
    };
    let Some(node) = extension.node.as_any().downcast_ref::<KernelNode>() else {
        return Ok(None);
    };
    let KernelSpec::Query(query) = node.spec() else {
        return Ok(None);
    };

    let mut transformed = false;
    let rewritten_exprs = projection
        .expr
        .iter()
        .enumerate()
        .map(|(index, expr)| {
            expr.clone()
                .transform_up(|nested| {
                    if let Some(rewritten) =
                        query.query().rewrite_score_surface_to_output_column(&nested)?
                    {
                        transformed = true;
                        return Ok(Transformed::yes(rewritten));
                    }
                    Ok(Transformed::no(nested.clone()))
                })
                .map(|rewritten| {
                    rewritten.data.alias(projection.schema.field(index).name().clone())
                })
        })
        .collect::<Result<Vec<_>>>()?;
    if !transformed {
        return Ok(None);
    }
    plan.with_new_exprs(rewritten_exprs, vec![filter_plan])?.recompute_schema().map(Some)
}
