use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::LogicalPlan;

use super::{FiltersState, State};
use crate::analyzer::op::Op;
use crate::analyzer::source::Source;

#[derive(Debug, Clone)]
pub(crate) struct ProcessingState {
    pub(super) source:  Source,
    pub(super) filters: FiltersState,
    pub(super) op:      Op,
}

#[expect(
    clippy::unnecessary_wraps,
    reason = "state transition methods share a uniform Result-based interface across variants"
)]
#[expect(
    clippy::unused_self,
    reason = "processing transition methods stay instance-based to mirror the state machine \
              surface"
)]
impl ProcessingState {
    pub(super) fn projection(
        mut self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        let Some(op) = self.op.clone().project(&self.source, &plan)? else {
            if let Some(local_shell) =
                self.op.clone().local_projection_shell(self.source.clone(), &self.filters, &plan)?
            {
                return Ok(super::super::Analysis::new(local_shell, State::local(), true));
            }
            if let Some(local_plan) = self.op.clone().local_fallback(&self.source, &plan)? {
                return Ok(super::super::Analysis::new(local_plan, State::local(), true));
            }
            return Ok(super::super::fatal(
                plan,
                transformed,
                "processing projection is not admitted by the current qdrant op",
            ));
        };
        self.op = op;
        if matches!(self.op, Op::Query(_))
            && let Some(kernel_state) =
                self.op.clone().kernel(self.source.clone(), &self.filters, &plan)?
        {
            return kernel_state.absorb(plan, transformed);
        }
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
        let Some(op) =
            self.op.clone().filter(&self.source, &mut self.filters, &filter.predicate)?
        else {
            if let Some(local_shell) =
                self.op.clone().local_filter_shell(self.source.clone(), &self.filters, &plan)?
            {
                return Ok(super::super::Analysis::new(local_shell, State::local(), true));
            }
            if let Some(local_plan) = self.op.clone().local_fallback(&self.source, &plan)? {
                return Ok(super::super::Analysis::new(local_plan, State::local(), true));
            }
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
        let prior_op = self.op.clone();
        let Some(op) = prior_op.sort(&plan)? else {
            if let Some(local_plan) = self.op.clone().local_fallback(&self.source, &plan)? {
                return Ok(super::super::Analysis::new(local_plan, State::local(), true));
            }
            return Ok(super::super::fatal(
                plan,
                transformed,
                "processing sort is not admitted by the current qdrant op",
            ));
        };
        self.op = op;
        if matches!(self.op, Op::Query(_))
            && let Some(kernel_state) =
                self.op.clone().kernel(self.source.clone(), &self.filters, &plan)?
        {
            return kernel_state.absorb(plan, transformed);
        }
        self.absorb(plan, transformed)
    }

    pub(super) fn limit(
        self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        let source = self.source.clone();
        let op = self.op.clone();
        let Some(kernel_state) = self.op.kernel(source, &self.filters, &plan)? else {
            if let Some(local_plan) = op.local_fallback(&self.source, &plan)? {
                return Ok(super::super::Analysis::new(local_plan, State::local(), true));
            }
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
            self.op.clone().distinct_on_kernel(self.source.clone(), &self.filters, &plan)?
        {
            return kernel_state.absorb(plan, transformed);
        }
        if matches!(plan, LogicalPlan::SubqueryAlias(_)) {
            if let Some(local_plan) = self.op.clone().local_fallback(&self.source, &plan)? {
                return Ok(super::super::Analysis::new(local_plan, State::local(), true));
            }
            return self.absorb(plan, transformed);
        }
        if let Some(local_plan) = self.op.local_fallback(&self.source, &plan)? {
            return Ok(super::super::Analysis::new(local_plan, State::local(), true));
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
