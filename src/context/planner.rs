use std::sync::Arc;

use datafusion::common::plan_datafusion_err;
use datafusion::execution::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};

use super::plan_node::{QDRANT_KERNEL_NODE_NAME, QdrantKernelNode};

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
            QDRANT_KERNEL_NODE_NAME => {
                let node = node
                    .as_any()
                    .downcast_ref::<QdrantKernelNode>()
                    .ok_or(plan_datafusion_err!("Failed to downcast QdrantKernelNode"))?;
                Ok(Some(node.execute()))
            }
            _ => Ok(None),
        }
    }
}
