use std::hash::{Hash, Hasher};

use datafusion::common::{DFSchemaRef, Result, plan_err};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};

use super::kernel::KernelSpec;

pub(crate) const KERNEL_NODE_NAME: &str = "QdrantKernelNode";

#[derive(Debug, Clone)]
pub(crate) struct KernelNode {
    schema: DFSchemaRef,
    spec:   KernelSpec,
}

impl KernelNode {
    pub(crate) fn new(schema: DFSchemaRef, spec: KernelSpec) -> Self { Self { schema, spec } }

    pub(crate) fn spec(&self) -> &KernelSpec { &self.spec }

    pub(crate) fn output_schema(&self) -> &DFSchemaRef { &self.schema }
}

impl UserDefinedLogicalNodeCore for KernelNode {
    fn name(&self) -> &str { KERNEL_NODE_NAME }

    fn inputs(&self) -> Vec<&LogicalPlan> { vec![] }

    fn schema(&self) -> &DFSchemaRef { &self.schema }

    fn expressions(&self) -> Vec<Expr> { vec![] }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{KERNEL_NODE_NAME}: {:?}", self.spec)
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if !exprs.is_empty() {
            return plan_err!("{KERNEL_NODE_NAME} expects no expressions");
        }
        if !inputs.is_empty() {
            return plan_err!("{KERNEL_NODE_NAME} expects no inputs");
        }
        Ok(self.clone())
    }
}

impl PartialEq for KernelNode {
    fn eq(&self, other: &Self) -> bool {
        format!("{:?}", self.spec) == format!("{:?}", other.spec)
            && format!("{:?}", self.schema) == format!("{:?}", other.schema)
    }
}

impl Eq for KernelNode {}

impl PartialOrd for KernelNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        (format!("{:?}", self.spec), format!("{:?}", self.schema))
            .partial_cmp(&(format!("{:?}", other.spec), format!("{:?}", other.schema)))
    }
}

impl Hash for KernelNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        KERNEL_NODE_NAME.hash(state);
        format!("{:?}", self.spec).hash(state);
        format!("{:?}", self.schema).hash(state);
    }
}
