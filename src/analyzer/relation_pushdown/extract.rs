use datafusion::common::Result;
use datafusion::logical_expr::LogicalPlan;

use super::super::count_pushdown::count_node;
use super::super::facet_pushdown::facet_node;
use super::{
    QdrantCompositionClass, QdrantRelationCandidate, QdrantSourceClass, QdrantTopologyClass,
};

impl QdrantRelationCandidate {
    pub(super) fn from_plan(plan: &LogicalPlan) -> Result<Option<Self>> {
        if let Some(node) = facet_node(plan)? {
            return Ok(Some(Self {
                source: QdrantSourceClass::SingleQdrant,
                topology: QdrantTopologyClass::of(plan),
                composition: QdrantCompositionClass::Atomic,
                node,
            }));
        }
        if let Some(node) = count_node(plan)? {
            return Ok(Some(Self {
                source: QdrantSourceClass::SingleQdrant,
                topology: QdrantTopologyClass::of(plan),
                composition: QdrantCompositionClass::Atomic,
                node,
            }));
        }
        Ok(None)
    }
}
