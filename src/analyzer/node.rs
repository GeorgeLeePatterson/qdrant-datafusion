use std::hash::{Hash, Hasher};

use datafusion::common::{DFSchemaRef, Result, plan_err};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};

use super::state::State;

pub(crate) const STATE_NODE_NAME: &str = "PrototypeStateNode";

#[derive(Debug, Clone)]
pub(crate) struct StateNode {
    pub(super) schema: DFSchemaRef,
    pub(super) state:  State,
}

impl StateNode {
    pub(crate) fn state(&self) -> &State { &self.state }

    pub(crate) fn output_schema(&self) -> &DFSchemaRef { &self.schema }
}

impl UserDefinedLogicalNodeCore for StateNode {
    fn name(&self) -> &str { STATE_NODE_NAME }

    fn inputs(&self) -> Vec<&LogicalPlan> { vec![] }

    fn schema(&self) -> &DFSchemaRef { &self.schema }

    fn expressions(&self) -> Vec<Expr> { vec![] }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.state {
            State::Processing(state) => write!(f, "{STATE_NODE_NAME}: processing {:?}", state.op),
            State::Kernel(state) => write!(f, "{STATE_NODE_NAME}: kernel {:?}", state.spec),
            State::Local(_) | State::Source(_) | State::Composite(_) | State::Fatal(_) => {
                write!(f, "{STATE_NODE_NAME}: invalid materialized state")
            }
        }
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if !exprs.is_empty() {
            return plan_err!("{STATE_NODE_NAME} expects no expressions");
        }
        if !inputs.is_empty() {
            return plan_err!("{STATE_NODE_NAME} expects no inputs");
        }
        Ok(self.clone())
    }
}

impl PartialEq for StateNode {
    fn eq(&self, other: &Self) -> bool {
        format!("{:?}", self.state) == format!("{:?}", other.state)
            && format!("{:?}", self.schema) == format!("{:?}", other.schema)
    }
}

impl Eq for StateNode {}

impl PartialOrd for StateNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        (format!("{:?}", self.state), format!("{:?}", self.schema))
            .partial_cmp(&(format!("{:?}", other.state), format!("{:?}", other.schema)))
    }
}

impl Hash for StateNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        STATE_NODE_NAME.hash(state);
        format!("{:?}", self.state).hash(state);
        format!("{:?}", self.schema).hash(state);
    }
}
