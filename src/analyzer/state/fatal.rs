use datafusion::common::Result;
use datafusion::logical_expr::LogicalPlan;

use super::{SemanticError, State};

#[derive(Debug, Clone)]
pub(crate) struct FatalState {
    pub(crate) error: SemanticError,
}

impl FatalState {
    pub(super) fn projection(
        self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        Ok(super::super::Analysis::new(plan, State::Fatal(self), transformed))
    }

    pub(super) fn filter(
        self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        Ok(super::super::Analysis::new(plan, State::Fatal(self), transformed))
    }

    pub(super) fn sort(
        self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        Ok(super::super::Analysis::new(plan, State::Fatal(self), transformed))
    }

    pub(super) fn limit(
        self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        Ok(super::super::Analysis::new(plan, State::Fatal(self), transformed))
    }

    pub(super) fn aggregate(
        self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        Ok(super::super::Analysis::new(plan, State::Fatal(self), transformed))
    }

    pub(super) fn unary(
        self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        Ok(super::super::Analysis::new(plan, State::Fatal(self), transformed))
    }
}
