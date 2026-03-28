use datafusion::common::Result;

use super::super::source::Source;

#[derive(Debug, Clone, Default)]
pub(crate) struct FormulaQuery;

impl FormulaQuery {
    pub(crate) fn same_semantics(&self, _other: &Self) -> bool {
        true
    }

    pub(super) fn validate_on_source(&self, _source: &Source) -> Result<()> {
        Ok(())
    }
}
