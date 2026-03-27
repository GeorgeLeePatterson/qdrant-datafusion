use std::sync::Arc;

use datafusion::execution::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};

use super::exec::execution_plan_for_state_node;
use crate::analyzer::{STATE_NODE_NAME, StateNode};

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
            STATE_NODE_NAME => {
                let node = node
                    .as_any()
                    .downcast_ref::<StateNode>()
                    .ok_or_else(|| datafusion::error::DataFusionError::Plan("Failed to downcast StateNode".to_owned()))?;
                Ok(Some(execution_plan_for_state_node(node)?))
            }
            _ => Ok(None),
        }
    }
}
