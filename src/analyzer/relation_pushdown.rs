use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, plan_err};
use datafusion::datasource::source_as_provider;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::optimizer::AnalyzerRule;

use super::count_pushdown::count_node;
use super::facet_pushdown::facet_node;
use crate::context::plan_node::{
    QDRANT_COUNT_NODE_NAME, QDRANT_FACET_NODE_NAME, QdrantCountNode, QdrantFacetNode,
};
use crate::pushdown::logical_payload_path;
use crate::table::QdrantTableProvider;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QdrantSourceClass {
    None,
    SingleQdrant,
    MultiQdrant,
    Mixed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QdrantTopologyClass {
    Leaf,
    UnaryChain,
    UnaryRelationChange,
    MultiBranch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QdrantCompositionClass {
    Atomic,
    Mergeable,
    Batchable,
    Coordinated,
    LocalCompose,
    Invalid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QdrantKernelClass {
    None,
    ExactSelf,
    ExactChild,
    ExactChildren,
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

#[derive(Debug, Clone)]
struct QdrantSubtreeStatus {
    class:     QdrantSubtreeClass,
    candidate: Option<QdrantRelationCandidate>,
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
            let status = subtree_status(&plan)?;
            if let Some(candidate) = status.candidate {
                return Ok(Transformed::yes(candidate.into_plan()));
            }
            if status.class.composition == QdrantCompositionClass::Invalid {
                return plan_err!("unsupported qdrant payload access outside admitted kernel");
            }
            Ok(Transformed::no(plan))
        })
        .map(|transformed| transformed.data)
    }

    fn name(&self) -> &'static str { "qdrant_relation_pushdown" }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct QdrantSubtreeClass {
    source:      QdrantSourceClass,
    topology:    QdrantTopologyClass,
    composition: QdrantCompositionClass,
    kernel:      QdrantKernelClass,
}

fn subtree_status(plan: &LogicalPlan) -> Result<QdrantSubtreeStatus> {
    let candidate = exact_self_candidate(plan)?;
    let source = source_class(plan);
    let topology = topology_class(plan);
    let kernel = kernel_class(plan, candidate.is_some());
    let composition = composition_class(plan, source, topology, kernel)?;
    let class = QdrantSubtreeClass { source, topology, composition, kernel };
    let candidate = if class.source == QdrantSourceClass::SingleQdrant
        && class.composition == QdrantCompositionClass::Atomic
    {
        candidate
    } else {
        None
    };
    Ok(QdrantSubtreeStatus { class, candidate })
}

fn exact_self_candidate(plan: &LogicalPlan) -> Result<Option<QdrantRelationCandidate>> {
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

#[cfg(test)]
fn subtree_class(plan: &LogicalPlan) -> Result<QdrantSubtreeClass> {
    subtree_status(plan).map(|status| status.class)
}

fn source_class(plan: &LogicalPlan) -> QdrantSourceClass {
    match plan {
        LogicalPlan::TableScan(scan) => {
            let Ok(provider) = source_as_provider(&scan.source) else {
                return QdrantSourceClass::None;
            };
            if provider.as_any().is::<QdrantTableProvider>() {
                QdrantSourceClass::SingleQdrant
            } else {
                QdrantSourceClass::None
            }
        }
        LogicalPlan::Extension(extension)
            if matches!(extension.node.name(), QDRANT_COUNT_NODE_NAME | QDRANT_FACET_NODE_NAME) =>
        {
            QdrantSourceClass::SingleQdrant
        }
        _ => combine_sources(plan.inputs().into_iter().map(source_class)),
    }
}

fn combine_sources(sources: impl IntoIterator<Item = QdrantSourceClass>) -> QdrantSourceClass {
    let mut qdrant_count = 0_u8;
    let mut has_other = false;
    for source in sources {
        match source {
            QdrantSourceClass::None => {}
            QdrantSourceClass::SingleQdrant => qdrant_count = qdrant_count.saturating_add(1),
            QdrantSourceClass::MultiQdrant => qdrant_count = qdrant_count.saturating_add(2),
            QdrantSourceClass::Mixed => {
                qdrant_count = qdrant_count.saturating_add(1);
                has_other = true;
            }
        }
    }
    match (qdrant_count, has_other) {
        (0, false) => QdrantSourceClass::None,
        (1, false) => QdrantSourceClass::SingleQdrant,
        (_, false) => QdrantSourceClass::MultiQdrant,
        _ => QdrantSourceClass::Mixed,
    }
}

fn topology_class(plan: &LogicalPlan) -> QdrantTopologyClass {
    match plan {
        LogicalPlan::TableScan(_)
        | LogicalPlan::EmptyRelation(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::DescribeTable(_)
        | LogicalPlan::Extension(_) => QdrantTopologyClass::Leaf,
        LogicalPlan::Aggregate(_) | LogicalPlan::Distinct(_) => {
            QdrantTopologyClass::UnaryRelationChange
        }
        _ if plan.inputs().len() > 1 => QdrantTopologyClass::MultiBranch,
        _ => QdrantTopologyClass::UnaryChain,
    }
}

fn composition_class(
    plan: &LogicalPlan,
    source: QdrantSourceClass,
    topology: QdrantTopologyClass,
    kernel: QdrantKernelClass,
) -> Result<QdrantCompositionClass> {
    if invalid_payload_access_surface(plan, source, kernel)? {
        return Ok(QdrantCompositionClass::Invalid);
    }
    if kernel == QdrantKernelClass::ExactSelf {
        return Ok(QdrantCompositionClass::Atomic);
    }
    if matches!(plan, LogicalPlan::Union(_)) {
        return Ok(match kernel {
            QdrantKernelClass::ExactChild | QdrantKernelClass::ExactChildren => {
                QdrantCompositionClass::Batchable
            }
            QdrantKernelClass::None if source == QdrantSourceClass::MultiQdrant => {
                QdrantCompositionClass::Mergeable
            }
            _ => QdrantCompositionClass::LocalCompose,
        });
    }
    Ok(match (source, topology, kernel) {
        (
            QdrantSourceClass::SingleQdrant,
            QdrantTopologyClass::Leaf
            | QdrantTopologyClass::UnaryChain
            | QdrantTopologyClass::UnaryRelationChange,
            QdrantKernelClass::None,
        ) => QdrantCompositionClass::Atomic,
        (
            QdrantSourceClass::MultiQdrant,
            QdrantTopologyClass::Leaf
            | QdrantTopologyClass::UnaryChain
            | QdrantTopologyClass::UnaryRelationChange,
            QdrantKernelClass::None,
        ) => QdrantCompositionClass::Coordinated,
        (QdrantSourceClass::SingleQdrant | QdrantSourceClass::MultiQdrant, _, _) => {
            QdrantCompositionClass::LocalCompose
        }
        _ => QdrantCompositionClass::LocalCompose,
    })
}

fn kernel_class(plan: &LogicalPlan, exact_self: bool) -> QdrantKernelClass {
    if exact_self || is_qdrant_relation_extension(plan) {
        return QdrantKernelClass::ExactSelf;
    }
    combine_kernels(plan.inputs().into_iter().map(|child| kernel_class(child, false)))
}

fn combine_kernels(kernels: impl IntoIterator<Item = QdrantKernelClass>) -> QdrantKernelClass {
    let kernel_count = kernels
        .into_iter()
        .map(|kernel| match kernel {
            QdrantKernelClass::None => 0_u8,
            QdrantKernelClass::ExactSelf | QdrantKernelClass::ExactChild => 1_u8,
            QdrantKernelClass::ExactChildren => 2_u8,
        })
        .fold(0_u8, u8::saturating_add);
    match kernel_count {
        0 => QdrantKernelClass::None,
        1 => QdrantKernelClass::ExactChild,
        _ => QdrantKernelClass::ExactChildren,
    }
}

fn is_qdrant_relation_extension(plan: &LogicalPlan) -> bool {
    matches!(
        plan,
        LogicalPlan::Extension(extension)
            if matches!(extension.node.name(), QDRANT_COUNT_NODE_NAME | QDRANT_FACET_NODE_NAME)
    )
}

fn invalid_payload_access_surface(
    plan: &LogicalPlan,
    source: QdrantSourceClass,
    kernel: QdrantKernelClass,
) -> Result<bool> {
    if !matches!(source, QdrantSourceClass::SingleQdrant | QdrantSourceClass::MultiQdrant)
        || !matches!(kernel, QdrantKernelClass::None | QdrantKernelClass::ExactChild)
    {
        return Ok(false);
    }
    if !matches!(plan, LogicalPlan::Projection(_) | LogicalPlan::Window(_)) {
        return Ok(false);
    }
    plan_uses_payload_access(plan)
}

fn plan_uses_payload_access(plan: &LogicalPlan) -> Result<bool> {
    let mut found = false;
    let _ = plan.apply_expressions(|expr| {
        if expr.exists(|expr| Ok(logical_payload_path(expr).is_some()))? {
            found = true;
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    })?;
    Ok(found)
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;
    use std::collections::HashSet;
    use std::fmt;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{Result, ToDFSchema};
    use datafusion::config::ConfigOptions;
    use datafusion::logical_expr::{
        BinaryExpr, Expr, Extension, LogicalPlan, LogicalPlanBuilder, Operator,
        UserDefinedLogicalNodeCore,
    };
    use datafusion::optimizer::AnalyzerRule;
    use datafusion::prelude::{col, lit};
    use qdrant_client::Qdrant;

    use super::{
        QdrantCompositionClass, QdrantKernelClass, QdrantRelationPushdown, QdrantSourceClass,
        QdrantSubtreeClass, QdrantTopologyClass, subtree_class,
    };
    use crate::arrow::schema::PAYLOAD_FIELD_NAME;
    use crate::context::plan_node::{QDRANT_COUNT_NODE_NAME, QdrantCountNode};
    use crate::pushdown::QdrantFilters;

    #[derive(Debug, Clone, Hash, PartialEq, Eq)]
    struct DummyQdrantNode {
        name:   &'static str,
        schema: datafusion::common::DFSchemaRef,
    }

    impl PartialOrd for DummyQdrantNode {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.name.cmp(other.name)) }
    }

    impl UserDefinedLogicalNodeCore for DummyQdrantNode {
        fn name(&self) -> &str { self.name }

        fn inputs(&self) -> Vec<&LogicalPlan> { vec![] }

        fn schema(&self) -> &datafusion::common::DFSchemaRef { &self.schema }

        fn expressions(&self) -> Vec<Expr> { vec![] }

        fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.name)
        }

        fn with_exprs_and_inputs(
            &self,
            exprs: Vec<Expr>,
            inputs: Vec<LogicalPlan>,
        ) -> Result<Self> {
            assert!(exprs.is_empty(), "{} expects no expressions", self.name);
            assert!(inputs.is_empty(), "{} expects no inputs", self.name);
            Ok(self.clone())
        }

        fn prevent_predicate_push_down_columns(&self) -> HashSet<String> { HashSet::new() }
    }

    fn count_extension_plan() -> LogicalPlan {
        let schema = Schema::new(vec![Field::new("count", DataType::Int64, false)])
            .to_dfschema_ref()
            .expect("df schema");
        LogicalPlan::Extension(Extension {
            node: Arc::new(QdrantCountNode::new(
                schema,
                Arc::new(Qdrant::from_url("http://localhost:6334").build().expect("qdrant client")),
                "docs".to_owned(),
                QdrantFilters::default(),
            )),
        })
    }

    fn payload_extension_plan() -> LogicalPlan {
        let schema = Schema::new(vec![Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true)])
            .to_dfschema_ref()
            .expect("df schema");
        LogicalPlan::Extension(Extension {
            node: Arc::new(DummyQdrantNode { name: QDRANT_COUNT_NODE_NAME, schema }),
        })
    }

    fn payload_path(path: &str) -> Expr {
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col(PAYLOAD_FIELD_NAME)),
            Operator::Colon,
            Box::new(lit(path)),
        ))
    }

    #[test]
    fn subtree_classifies_qdrant_extension_as_atomic_leaf() {
        let class = subtree_class(&count_extension_plan()).expect("subtree class");
        assert_eq!(class, QdrantSubtreeClass {
            source:      QdrantSourceClass::SingleQdrant,
            topology:    QdrantTopologyClass::Leaf,
            composition: QdrantCompositionClass::Atomic,
            kernel:      QdrantKernelClass::ExactSelf,
        });
    }

    #[test]
    fn subtree_classifies_union_of_qdrant_extensions_as_batchable_multibranch() {
        let left = count_extension_plan();
        let right = count_extension_plan();
        let union = LogicalPlanBuilder::from(left)
            .union(right)
            .expect("union")
            .build()
            .expect("union plan");
        let class = subtree_class(&union).expect("subtree class");
        assert_eq!(class, QdrantSubtreeClass {
            source:      QdrantSourceClass::MultiQdrant,
            topology:    QdrantTopologyClass::MultiBranch,
            composition: QdrantCompositionClass::Batchable,
            kernel:      QdrantKernelClass::ExactChildren,
        });
    }

    #[test]
    fn subtree_classifies_non_qdrant_projection_as_local_compose_unary_chain() {
        let projection = LogicalPlanBuilder::values(vec![vec![lit(1_i64)]])
            .expect("values")
            .project(vec![col("column1")])
            .expect("projection")
            .build()
            .expect("projection plan");
        let class = subtree_class(&projection).expect("subtree class");
        assert_eq!(class, QdrantSubtreeClass {
            source:      QdrantSourceClass::None,
            topology:    QdrantTopologyClass::UnaryChain,
            composition: QdrantCompositionClass::LocalCompose,
            kernel:      QdrantKernelClass::None,
        });
    }

    #[test]
    fn subtree_classifies_projection_over_qdrant_kernel_as_local_shell() {
        let projection = LogicalPlanBuilder::from(count_extension_plan())
            .project(vec![col("count")])
            .expect("projection")
            .build()
            .expect("projection plan");
        let class = subtree_class(&projection).expect("subtree class");
        assert_eq!(class, QdrantSubtreeClass {
            source:      QdrantSourceClass::SingleQdrant,
            topology:    QdrantTopologyClass::UnaryChain,
            composition: QdrantCompositionClass::LocalCompose,
            kernel:      QdrantKernelClass::ExactChild,
        });
    }

    #[test]
    fn subtree_classifies_payload_projection_over_qdrant_source_as_invalid() {
        let projection = LogicalPlanBuilder::from(payload_extension_plan())
            .project(vec![payload_path("tag")])
            .expect("projection")
            .build()
            .expect("projection plan");
        let class = subtree_class(&projection).expect("subtree class");
        assert_eq!(class, QdrantSubtreeClass {
            source:      QdrantSourceClass::SingleQdrant,
            topology:    QdrantTopologyClass::UnaryChain,
            composition: QdrantCompositionClass::Invalid,
            kernel:      QdrantKernelClass::ExactChild,
        });
    }

    #[test]
    fn analyzer_rejects_invalid_qdrant_payload_projection() {
        let projection = LogicalPlanBuilder::from(payload_extension_plan())
            .project(vec![payload_path("tag")])
            .expect("projection")
            .build()
            .expect("projection plan");
        let error = QdrantRelationPushdown
            .analyze(projection, &ConfigOptions::default())
            .expect_err("invalid payload access should error");
        assert!(
            error.to_string().contains("unsupported qdrant payload access outside admitted kernel"),
            "{error}"
        );
    }
}
