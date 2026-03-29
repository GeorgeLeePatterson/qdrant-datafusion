use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::Expr;

use super::super::source::Source;
use super::QueryDescriptor;

#[derive(Debug, Clone, Default)]
pub(crate) struct RelevanceFeedbackQuery;

impl RelevanceFeedbackQuery {
    pub(crate) fn from_expr(_expr: &Expr) -> Result<Option<Self>> {
        Ok(None)
    }

    pub(crate) fn same_semantics(&self, _other: &Self) -> bool {
        true
    }

    pub(super) fn validate_on_source(&self, _source: &Source) -> Result<()> {
        Ok(())
    }

    pub(super) fn descriptor(&self) -> Result<QueryDescriptor> {
        exec_err!("RelevanceFeedbackQuery execution is not yet implemented")
    }
}
