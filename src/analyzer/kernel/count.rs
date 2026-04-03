use std::sync::Arc;

use qdrant_client::Qdrant;

use super::super::source::Source;
use crate::qdrant::filter::QdrantFilters;

#[derive(Debug, Clone)]
pub(crate) struct CountKernel {
    source: Source,
    filters: QdrantFilters,
}

impl CountKernel {
    pub(crate) fn new(source: Source, filters: QdrantFilters) -> Self {
        Self { source, filters }
    }

    pub(crate) fn client(&self) -> Arc<Qdrant> {
        Arc::clone(self.source.client())
    }

    pub(crate) fn collection(&self) -> &str {
        self.source.collection()
    }

    pub(crate) fn filters(&self) -> &QdrantFilters {
        &self.filters
    }
}
