use std::sync::Arc;

use datafusion::common::Result;
use datafusion::common::tree_node::Transformed;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::optimizer::AnalyzerRule;

use super::count_pushdown::count_node;
use super::facet_pushdown::facet_node;
use crate::context::plan_node::{QdrantCountNode, QdrantFacetNode};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QdrantSourceClass {
    SingleQdrant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QdrantTopologyClass {
    UnaryRelationChange,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QdrantCompositionClass {
    Atomic,
}

#[derive(Debug, Clone)]
enum QdrantRelationNode {
    Count(QdrantCountNode),
    Facet(QdrantFacetNode),
}

#[derive(Debug, Clone)]
struct QdrantRelationCandidate {
    source:      QdrantSourceClass,
    topology:    QdrantTopologyClass,
    composition: QdrantCompositionClass,
    node:        QdrantRelationNode,
}

impl QdrantRelationCandidate {
    fn into_plan(self) -> LogicalPlan {
        let _ = (self.source, self.topology, self.composition);
        let node: Arc<dyn datafusion::logical_expr::UserDefinedLogicalNode> = match self.node {
            QdrantRelationNode::Count(node) => Arc::new(node),
            QdrantRelationNode::Facet(node) => Arc::new(node),
        };
        LogicalPlan::Extension(Extension { node })
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct QdrantRelationPushdown;

impl AnalyzerRule for QdrantRelationPushdown {
    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &datafusion::common::config::ConfigOptions,
    ) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(|plan| {
            let Some(candidate) = relation_candidate(&plan)? else {
                return Ok(Transformed::no(plan));
            };
            Ok(Transformed::yes(candidate.into_plan()))
        })
        .map(|transformed| transformed.data)
    }

    fn name(&self) -> &'static str { "qdrant_relation_pushdown" }
}

fn relation_candidate(plan: &LogicalPlan) -> Result<Option<QdrantRelationCandidate>> {
    if let Some(node) = facet_node(plan)? {
        return Ok(Some(QdrantRelationCandidate {
            source:      QdrantSourceClass::SingleQdrant,
            topology:    QdrantTopologyClass::UnaryRelationChange,
            composition: QdrantCompositionClass::Atomic,
            node:        QdrantRelationNode::Facet(node),
        }));
    }
    if let Some(node) = count_node(plan)? {
        return Ok(Some(QdrantRelationCandidate {
            source:      QdrantSourceClass::SingleQdrant,
            topology:    QdrantTopologyClass::UnaryRelationChange,
            composition: QdrantCompositionClass::Atomic,
            node:        QdrantRelationNode::Count(node),
        }));
    }
    Ok(None)
}
