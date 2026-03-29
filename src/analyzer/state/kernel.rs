use std::sync::Arc;

use datafusion::common::Result;
use datafusion::logical_expr::{Extension, LogicalPlan};

use super::State;
use crate::analyzer::kernel::KernelSpec;
use crate::analyzer::node::KernelNode;
use crate::analyzer::surface::SurfaceCall;

#[derive(Debug, Clone)]
pub(crate) struct KernelState {
    spec: KernelSpec,
}

impl KernelState {
    pub(crate) fn new(spec: KernelSpec) -> Self { Self { spec } }

    pub(crate) fn spec(&self) -> &KernelSpec { &self.spec }

    pub(super) fn projection(
        mut self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        let maybe_spec = self.spec.project(&plan)?;
        if let Some(spec) = maybe_spec {
            self.spec = spec;
            return self.absorb(plan, transformed);
        }
        if SurfaceCall::collect(&plan.expressions())?.is_some() {
            return Ok(super::super::fatal(
                plan,
                transformed,
                "surface call above kernel does not match the extracted qdrant kernel",
            ));
        }
        Ok(super::super::Analysis::new(plan, State::local(), transformed))
    }

    pub(super) fn filter(
        self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        if SurfaceCall::collect(&plan.expressions())?.is_some() {
            return Ok(super::super::fatal(
                plan,
                transformed,
                "surface call above kernel does not match the extracted qdrant kernel",
            ));
        }
        Ok(super::super::Analysis::new(plan, State::local(), transformed))
    }

    pub(super) fn sort(
        self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        if SurfaceCall::collect(&plan.expressions())?.is_some() {
            return Ok(super::super::fatal(
                plan,
                transformed,
                "surface call above kernel does not match the extracted qdrant kernel",
            ));
        }
        Ok(super::super::Analysis::new(plan, State::local(), transformed))
    }

    pub(super) fn limit(
        mut self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        if let Some(spec) = self.spec.clone().limit(&plan)? {
            self.spec = spec;
            return self.absorb(plan, transformed);
        }
        Ok(super::super::Analysis::new(plan, State::local(), transformed))
    }

    pub(super) fn aggregate(
        self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        Ok(super::super::Analysis::new(plan, State::local(), transformed))
    }

    pub(super) fn unary(
        self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        if matches!(plan, LogicalPlan::SubqueryAlias(_)) {
            return self.absorb(plan, transformed);
        }
        Ok(super::super::Analysis::new(plan, State::local(), transformed))
    }

    pub(super) fn absorb(
        self,
        plan: LogicalPlan,
        _transformed: bool,
    ) -> Result<super::super::Analysis> {
        let schema = Arc::clone(plan.schema());
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(KernelNode::new(schema, self.spec.clone())),
        });
        Ok(super::super::Analysis::new(plan, State::Kernel(self), true))
    }
}
