use std::sync::Arc;

use datafusion::execution::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};

use super::exec::{
    QdrantCountExec, QdrantFacetExec, QdrantQueryBatchExec, QdrantQueryExec, QdrantQueryGroupsExec,
};
use crate::analyzer::{KERNEL_NODE_NAME, KernelNode, KernelSpec};

#[derive(Clone, Copy, Debug)]
pub(crate) struct QdrantExtensionPlanner;

#[async_trait::async_trait]
impl ExtensionPlanner for QdrantExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> datafusion::error::Result<Option<Arc<dyn ExecutionPlan>>> {
        match node.name() {
            KERNEL_NODE_NAME => {
                let node = node.as_any().downcast_ref::<KernelNode>().ok_or_else(|| {
                    datafusion::error::DataFusionError::Plan(
                        "Failed to downcast KernelNode".to_owned(),
                    )
                })?;
                let schema = Arc::clone(node.output_schema().inner());
                let plan: Arc<dyn ExecutionPlan> = match node.spec() {
                    KernelSpec::Count(spec) => {
                        Arc::new(QdrantCountExec::new(spec.clone(), &schema))
                    }
                    KernelSpec::Facet(spec) => {
                        Arc::new(QdrantFacetExec::new(spec.clone(), &schema))
                    }
                    KernelSpec::Query(spec) => {
                        Arc::new(QdrantQueryExec::new(spec.clone(), &schema))
                    }
                    KernelSpec::QueryBatch(spec) => {
                        Arc::new(QdrantQueryBatchExec::new(spec.clone(), &schema))
                    }
                    KernelSpec::QueryGroups(spec) => {
                        Arc::new(QdrantQueryGroupsExec::new(spec.clone(), &schema))
                    }
                };
                Ok(Some(plan))
            }
            _ => Ok(None),
        }
    }
}
