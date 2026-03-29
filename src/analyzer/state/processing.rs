use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::LogicalPlan;

use super::{FiltersState, State};
use crate::analyzer::op::Op;
use crate::analyzer::source::Source;

#[derive(Debug, Clone)]
pub(crate) struct ProcessingState {
    pub(super) source: Source,
    pub(super) filters: FiltersState,
    pub(super) op: Op,
}

impl ProcessingState {
    pub(super) fn projection(
        mut self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        let Some(op) = self.op.project(&plan)? else {
            return Ok(super::super::fatal(
                plan,
                transformed,
                "processing projection is not admitted by the current qdrant op",
            ));
        };
        self.op = op;
        self.absorb(plan, transformed)
    }

    pub(super) fn filter(
        mut self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        let LogicalPlan::Filter(filter) = &plan else {
            return plan_err!("prototype filter state mismatch");
        };
        let Some(op) = self.op.filter(&self.source, &mut self.filters, &filter.predicate)? else {
            return Ok(super::super::fatal(
                plan,
                transformed,
                "processing filter is not admitted by the current qdrant op",
            ));
        };
        self.op = op;
        self.absorb(plan, transformed)
    }

    pub(super) fn sort(
        mut self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        let Some(op) = self.op.sort(&plan)? else {
            return Ok(super::super::fatal(
                plan,
                transformed,
                "processing sort is not admitted by the current qdrant op",
            ));
        };
        self.op = op;
        self.absorb(plan, transformed)
    }

    pub(super) fn limit(
        self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        let Some(kernel_state) = self.op.kernel(self.source, self.filters, &plan)? else {
            return Ok(super::super::fatal(
                plan,
                transformed,
                "processing limit is not admitted by the current qdrant op",
            ));
        };
        kernel_state.absorb(plan, transformed)
    }

    pub(super) fn aggregate(
        self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        Ok(super::super::fatal(
            plan,
            transformed,
            "nested aggregate above qdrant processing is invalid",
        ))
    }

    pub(super) fn unary(
        self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        if let Some(kernel_state) =
            self.op.clone().distinct_on_kernel(self.source.clone(), self.filters.clone(), &plan)?
        {
            return kernel_state.absorb(plan, transformed);
        }
        if matches!(plan, LogicalPlan::SubqueryAlias(_)) {
            return self.absorb(plan, transformed);
        }
        Ok(super::super::fatal(
            plan,
            transformed,
            "qdrant processing must close to a kernel or stay region-owned",
        ))
    }

    pub(super) fn absorb(
        self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        Ok(super::super::Analysis::new(plan, State::Processing(self), transformed))
    }
}
