use std::sync::Arc;

use datafusion::common::{DFSchemaRef, Result};
use datafusion::logical_expr::{Extension, LogicalPlan};

use super::{FiltersState, ProcessingState, State};
use crate::analyzer::kernel::KernelSpec;
use crate::analyzer::node::KernelNode;
use crate::analyzer::op::Op;
use crate::analyzer::surface::SurfaceCall;

#[derive(Debug, Clone)]
pub(crate) struct KernelState {
    spec:          KernelSpec,
    output_schema: Option<DFSchemaRef>,
}

#[expect(
    clippy::unnecessary_wraps,
    reason = "kernel transitions keep the same Result-based signature as the rest of the state               machine"
)]
#[expect(
    clippy::unused_self,
    reason = "kernel transition methods remain instance-based for symmetry with the state machine"
)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "kernel transitions accept owned plans to match the shared state transition surface"
)]
impl KernelState {
    pub(crate) fn new(spec: KernelSpec) -> Self { Self { spec, output_schema: None } }

    pub(crate) fn with_output_schema(spec: KernelSpec, output_schema: DFSchemaRef) -> Self {
        Self { spec, output_schema: Some(output_schema) }
    }

    pub(crate) fn spec(&self) -> &KernelSpec { &self.spec }

    pub(super) fn projection(
        mut self,
        plan: LogicalPlan,
        transformed: bool,
    ) -> Result<super::super::Analysis> {
        let maybe_spec = self.spec.clone().project(&plan)?;
        if let Some(spec) = maybe_spec {
            self.spec = spec;
            return self.absorb(plan, transformed);
        }
        if let Some(surface) = SurfaceCall::collect(&plan.expressions())? {
            if let Some(processing) = self.clone().open(surface)? {
                return processing.projection(plan, transformed);
            }
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
        if let Some(surface) = SurfaceCall::collect(&plan.expressions())? {
            if let Some(processing) = self.clone().open(surface)? {
                return processing.filter(plan, transformed);
            }
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
        if let Some(surface) = SurfaceCall::collect(&plan.expressions())? {
            if let Some(processing) = self.clone().open(surface)? {
                return processing.sort(plan, transformed);
            }
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
        let output_schema = Arc::clone(plan.schema());
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(KernelNode::new(Arc::clone(&output_schema), self.spec.clone())),
        });
        Ok(super::super::Analysis::new(
            plan,
            State::Kernel(Self::with_output_schema(self.spec, output_schema)),
            true,
        ))
    }

    fn open(self, surface: SurfaceCall) -> Result<Option<ProcessingState>> {
        let Some(output_schema) = self.output_schema.as_ref() else {
            return Ok(None);
        };
        match self.spec {
            KernelSpec::Query(query) => {
                let prefetch = vec![query.prefetch_branch(output_schema)?];
                open_query_surface(surface, query.source(), prefetch).map(Some)
            }
            KernelSpec::QueryBatch(batch) => {
                open_query_surface(surface, batch.source(), batch.prefetch_branches()?).map(Some)
            }
            KernelSpec::Count(_) | KernelSpec::QueryGroups(_) | KernelSpec::Facet(_) => Ok(None),
        }
    }
}

fn open_query_surface(
    surface: SurfaceCall,
    source: &crate::analyzer::source::Source,
    prefetch: Vec<crate::analyzer::query::QueryPrefetchBranch>,
) -> Result<ProcessingState> {
    let op = Op::from_surface(surface, source)?.with_prefetch(prefetch)?;
    Ok(ProcessingState { source: source.clone(), filters: FiltersState::default(), op })
}
