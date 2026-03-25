use datafusion::common::Result;
use datafusion::logical_expr::LogicalPlan;

use super::super::count_pushdown::count_node;
use super::super::facet_pushdown::facet_node;
use super::{
    QdrantCompositionClass, QdrantRelationCandidate, QdrantRelationNode, QdrantSourceClass,
    topology_class,
};

pub(super) fn exact_self_candidate(plan: &LogicalPlan) -> Result<Option<QdrantRelationCandidate>> {
    if let Some(node) = facet_node(plan)? {
        return Ok(Some(QdrantRelationCandidate {
            source:      QdrantSourceClass::SingleQdrant,
            topology:    topology_class(plan),
            composition: QdrantCompositionClass::Atomic,
            node:        QdrantRelationNode::Facet(node),
        }));
    }
    if let Some(node) = count_node(plan)? {
        return Ok(Some(QdrantRelationCandidate {
            source:      QdrantSourceClass::SingleQdrant,
            topology:    topology_class(plan),
            composition: QdrantCompositionClass::Atomic,
            node:        QdrantRelationNode::Count(node),
        }));
    }
    Ok(None)
}
