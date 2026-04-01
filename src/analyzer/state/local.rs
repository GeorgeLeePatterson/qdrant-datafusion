use datafusion::common::Result;
use datafusion::logical_expr::LogicalPlan;

use super::State;
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
