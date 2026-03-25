mod classify;
mod extract;
mod mergeable;

use std::sync::Arc;

use classify::{subtree_status, topology_class};
use datafusion::common::tree_node::Transformed;
use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::{Extension, JoinType, LogicalPlan};
use datafusion::optimizer::AnalyzerRule;
use qdrant_client::qdrant::PointId;

use self::mergeable::{redundant_raw_qdrant_distinct_plan, set_join_plan, union_plan};
use crate::context::plan_node::{
    QDRANT_COUNT_NODE_NAME, QDRANT_FACET_NODE_NAME, QdrantCountNode, QdrantFacetNode,
};
use crate::pushdown::filter::QdrantFilters;
use crate::table::QdrantTableProvider;

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
            if let Some(distinct_input) = redundant_raw_qdrant_distinct_plan(&plan) {
                return Ok(Transformed::yes(distinct_input));
            }
            if status.class.composition == QdrantCompositionClass::Mergeable
                && let Some(merged) = set_join_plan(&plan)?
            {
                return Ok(Transformed::yes(merged));
            }
            if status.class.composition == QdrantCompositionClass::Mergeable
                && let Some(merged) = union_plan(&plan)?
            {
                return Ok(Transformed::yes(merged));
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct QdrantSubtreeClass {
    source:      QdrantSourceClass,
    topology:    QdrantTopologyClass,
    composition: QdrantCompositionClass,
    kernel:      QdrantKernelClass,
}

struct RawQdrantUnion {
    collection:     String,
    client:         Arc<qdrant_client::Qdrant>,
    schema:         datafusion::arrow::datatypes::SchemaRef,
    payload_schema: Arc<crate::pushdown::QdrantPayloadSchema>,
    branches:       Vec<Option<datafusion::logical_expr::Expr>>,
    branch_ids:     Vec<Option<Vec<PointId>>>,
}

struct RawQdrantSetJoin {
    collection:     String,
    client:         Arc<qdrant_client::Qdrant>,
    schema:         datafusion::arrow::datatypes::SchemaRef,
    payload_schema: Arc<crate::pushdown::QdrantPayloadSchema>,
    left_filter:    Option<datafusion::logical_expr::Expr>,
    right_filter:   Option<datafusion::logical_expr::Expr>,
    join_type:      JoinType,
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;
    use std::collections::{HashMap, HashSet};
    use std::fmt;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{Result, ToDFSchema};
    use datafusion::config::ConfigOptions;
    use datafusion::datasource::provider_as_source;
    use datafusion::functions_aggregate::expr_fn::count;
    use datafusion::logical_expr::{
        BinaryExpr, Expr, Extension, LogicalPlan, LogicalPlanBuilder, Operator,
        UserDefinedLogicalNodeCore,
    };
    use datafusion::optimizer::AnalyzerRule;
    use datafusion::prelude::{col, lit};
    use qdrant_client::Qdrant;
    use qdrant_client::qdrant::{
        KeywordIndexParams, PayloadIndexParams, PayloadSchemaInfo, PayloadSchemaType,
        payload_index_params,
    };

    use super::*;
    use crate::arrow::schema::{ID_FIELD_NAME, PAYLOAD_FIELD_NAME};
    use crate::context::plan_node::{
        QDRANT_COUNT_NODE_NAME, QDRANT_FACET_NODE_NAME, QdrantCountNode,
    };
    use crate::pushdown::QdrantPayloadSchema;
    use crate::pushdown::filter::QdrantFilters;
    use crate::table::QdrantTableProvider;

    fn subtree_class(plan: &LogicalPlan) -> Result<QdrantSubtreeClass> {
        subtree_status(plan).map(|status| status.class)
    }

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

    fn raw_scan_plan(collection: &str) -> LogicalPlan {
        let provider = QdrantTableProvider::new_test(
            collection,
            Schema::new(vec![Field::new(ID_FIELD_NAME, DataType::Utf8, false)]),
            QdrantPayloadSchema::default(),
        );
        LogicalPlanBuilder::scan(collection, provider_as_source(Arc::new(provider)), None)
            .expect("scan")
            .build()
            .expect("scan plan")
    }

    fn raw_filtered_scan_plan(collection: &str, id: &str) -> LogicalPlan {
        LogicalPlanBuilder::from(raw_scan_plan(collection))
            .filter(col(ID_FIELD_NAME).eq(lit(id)))
            .expect("filter")
            .build()
            .expect("filtered scan plan")
    }

    fn payload_scan_plan(collection: &str) -> LogicalPlan {
        let provider = QdrantTableProvider::new_test(
            collection,
            Schema::new(vec![
                Field::new(ID_FIELD_NAME, DataType::Utf8, false),
                Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
            ]),
            QdrantPayloadSchema::from(HashMap::from([("tag".to_owned(), PayloadSchemaInfo {
                data_type: PayloadSchemaType::Keyword as i32,
                params:    Some(PayloadIndexParams {
                    index_params: Some(payload_index_params::IndexParams::KeywordIndexParams(
                        KeywordIndexParams::default(),
                    )),
                }),
                points:    None,
            })])),
        );
        LogicalPlanBuilder::scan(collection, provider_as_source(Arc::new(provider)), None)
            .expect("scan")
            .build()
            .expect("scan plan")
    }

    fn payload_filtered_scan_plan(collection: &str, id: &str) -> LogicalPlan {
        LogicalPlanBuilder::from(payload_scan_plan(collection))
            .filter(col(ID_FIELD_NAME).eq(lit(id)))
            .expect("filter")
            .build()
            .expect("filtered scan plan")
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
    fn subtree_classifies_same_collection_raw_union_as_mergeable() {
        let left = raw_filtered_scan_plan("vectors", "row-1");
        let right = raw_filtered_scan_plan("vectors", "row-2");
        let union = LogicalPlanBuilder::from(left)
            .union(right)
            .expect("union")
            .build()
            .expect("union plan");
        let class = subtree_class(&union).expect("subtree class");
        assert_eq!(class, QdrantSubtreeClass {
            source:      QdrantSourceClass::MultiQdrant,
            topology:    QdrantTopologyClass::MultiBranch,
            composition: QdrantCompositionClass::Mergeable,
            kernel:      QdrantKernelClass::None,
        });
    }

    #[test]
    fn subtree_classifies_overlapping_same_collection_raw_union_as_local_compose() {
        let left = raw_filtered_scan_plan("vectors", "row-1");
        let right = raw_filtered_scan_plan("vectors", "row-1");
        let union = LogicalPlanBuilder::from(left)
            .union(right)
            .expect("union")
            .build()
            .expect("union plan");
        let class = subtree_class(&union).expect("subtree class");
        assert_eq!(class, QdrantSubtreeClass {
            source:      QdrantSourceClass::MultiQdrant,
            topology:    QdrantTopologyClass::MultiBranch,
            composition: QdrantCompositionClass::LocalCompose,
            kernel:      QdrantKernelClass::None,
        });
    }

    #[test]
    fn subtree_classifies_cross_collection_raw_union_as_local_compose() {
        let left = raw_filtered_scan_plan("vectors_a", "row-1");
        let right = raw_filtered_scan_plan("vectors_b", "row-2");
        let union = LogicalPlanBuilder::from(left)
            .union(right)
            .expect("union")
            .build()
            .expect("union plan");
        let class = subtree_class(&union).expect("subtree class");
        assert_eq!(class, QdrantSubtreeClass {
            source:      QdrantSourceClass::MultiQdrant,
            topology:    QdrantTopologyClass::MultiBranch,
            composition: QdrantCompositionClass::LocalCompose,
            kernel:      QdrantKernelClass::None,
        });
    }

    #[test]
    fn subtree_classifies_same_collection_raw_union_distinct_as_mergeable() {
        let union = LogicalPlanBuilder::from(raw_filtered_scan_plan("vectors", "row-1"))
            .union_distinct(raw_filtered_scan_plan("vectors", "row-1"))
            .expect("union distinct")
            .build()
            .expect("union distinct plan");
        let class = subtree_class(&union).expect("subtree class");
        assert_eq!(class, QdrantSubtreeClass {
            source:      QdrantSourceClass::MultiQdrant,
            topology:    QdrantTopologyClass::UnaryRelationChange,
            composition: QdrantCompositionClass::Mergeable,
            kernel:      QdrantKernelClass::None,
        });
    }

    #[test]
    fn subtree_classifies_cross_collection_raw_union_distinct_as_coordinated() {
        let union = LogicalPlanBuilder::from(raw_filtered_scan_plan("vectors_a", "row-1"))
            .union_distinct(raw_filtered_scan_plan("vectors_b", "row-1"))
            .expect("union distinct")
            .build()
            .expect("union distinct plan");
        let class = subtree_class(&union).expect("subtree class");
        assert_eq!(class, QdrantSubtreeClass {
            source:      QdrantSourceClass::MultiQdrant,
            topology:    QdrantTopologyClass::UnaryRelationChange,
            composition: QdrantCompositionClass::Coordinated,
            kernel:      QdrantKernelClass::None,
        });
    }

    #[test]
    fn subtree_classifies_same_collection_raw_intersect_as_mergeable() {
        let intersect = LogicalPlanBuilder::intersect(
            raw_filtered_scan_plan("vectors", "row-1"),
            raw_filtered_scan_plan("vectors", "row-1"),
            false,
        )
        .expect("intersect");
        let class = subtree_class(&intersect).expect("subtree class");
        assert_eq!(class, QdrantSubtreeClass {
            source:      QdrantSourceClass::MultiQdrant,
            topology:    QdrantTopologyClass::MultiBranch,
            composition: QdrantCompositionClass::Mergeable,
            kernel:      QdrantKernelClass::None,
        });
    }

    #[test]
    fn subtree_classifies_same_collection_raw_except_as_mergeable() {
        let except = LogicalPlanBuilder::except(
            raw_filtered_scan_plan("vectors", "row-1"),
            raw_filtered_scan_plan("vectors", "row-2"),
            false,
        )
        .expect("except");
        let class = subtree_class(&except).expect("subtree class");
        assert_eq!(class, QdrantSubtreeClass {
            source:      QdrantSourceClass::MultiQdrant,
            topology:    QdrantTopologyClass::MultiBranch,
            composition: QdrantCompositionClass::Mergeable,
            kernel:      QdrantKernelClass::None,
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

    #[test]
    fn analyzer_rewrites_mergeable_raw_union_to_single_filtered_scan() {
        let union = LogicalPlanBuilder::from(raw_filtered_scan_plan("vectors", "row-1"))
            .union(raw_filtered_scan_plan("vectors", "row-2"))
            .expect("union")
            .build()
            .expect("union plan");
        let analyzed = QdrantRelationPushdown
            .analyze(union, &ConfigOptions::default())
            .expect("analyzed plan");

        assert!(matches!(analyzed, LogicalPlan::Filter(_)));
        assert_eq!(subtree_class(&analyzed).expect("subtree class"), QdrantSubtreeClass {
            source:      QdrantSourceClass::SingleQdrant,
            topology:    QdrantTopologyClass::UnaryChain,
            composition: QdrantCompositionClass::Atomic,
            kernel:      QdrantKernelClass::None,
        });
    }

    #[test]
    fn analyzer_rewrites_mergeable_raw_union_distinct_to_single_filtered_scan() {
        let union = LogicalPlanBuilder::from(raw_filtered_scan_plan("vectors", "row-1"))
            .union_distinct(raw_filtered_scan_plan("vectors", "row-1"))
            .expect("union distinct")
            .build()
            .expect("union distinct plan");
        let analyzed = QdrantRelationPushdown
            .analyze(union, &ConfigOptions::default())
            .expect("analyzed plan");

        assert!(matches!(analyzed, LogicalPlan::Filter(_)));
        assert_eq!(subtree_class(&analyzed).expect("subtree class"), QdrantSubtreeClass {
            source:      QdrantSourceClass::SingleQdrant,
            topology:    QdrantTopologyClass::UnaryChain,
            composition: QdrantCompositionClass::Atomic,
            kernel:      QdrantKernelClass::None,
        });
    }

    #[test]
    fn analyzer_rewrites_raw_intersect_to_single_filtered_scan() {
        let intersect = LogicalPlanBuilder::intersect(
            raw_filtered_scan_plan("vectors", "row-1"),
            raw_filtered_scan_plan("vectors", "row-1"),
            true,
        )
        .expect("intersect");
        let analyzed = QdrantRelationPushdown
            .analyze(intersect, &ConfigOptions::default())
            .expect("analyzed plan");

        assert!(matches!(analyzed, LogicalPlan::Filter(_)));
        assert_eq!(subtree_class(&analyzed).expect("subtree class"), QdrantSubtreeClass {
            source:      QdrantSourceClass::SingleQdrant,
            topology:    QdrantTopologyClass::UnaryChain,
            composition: QdrantCompositionClass::Atomic,
            kernel:      QdrantKernelClass::None,
        });
    }

    #[test]
    fn analyzer_rewrites_raw_except_to_single_filtered_scan() {
        let except = LogicalPlanBuilder::except(
            raw_filtered_scan_plan("vectors", "row-1"),
            raw_filtered_scan_plan("vectors", "row-2"),
            true,
        )
        .expect("except");
        let analyzed = QdrantRelationPushdown
            .analyze(except, &ConfigOptions::default())
            .expect("analyzed plan");

        assert!(matches!(analyzed, LogicalPlan::Filter(_)));
        assert_eq!(subtree_class(&analyzed).expect("subtree class"), QdrantSubtreeClass {
            source:      QdrantSourceClass::SingleQdrant,
            topology:    QdrantTopologyClass::UnaryChain,
            composition: QdrantCompositionClass::Atomic,
            kernel:      QdrantKernelClass::None,
        });
    }

    #[test]
    fn analyzer_drops_redundant_raw_qdrant_distinct() {
        let distinct = LogicalPlanBuilder::from(raw_filtered_scan_plan("vectors", "row-1"))
            .distinct()
            .expect("distinct")
            .build()
            .expect("distinct plan");
        let analyzed = QdrantRelationPushdown
            .analyze(distinct, &ConfigOptions::default())
            .expect("analyzed plan");

        assert!(matches!(analyzed, LogicalPlan::Filter(_)));
        assert_eq!(subtree_class(&analyzed).expect("subtree class"), QdrantSubtreeClass {
            source:      QdrantSourceClass::SingleQdrant,
            topology:    QdrantTopologyClass::UnaryChain,
            composition: QdrantCompositionClass::Atomic,
            kernel:      QdrantKernelClass::None,
        });
    }

    #[test]
    fn analyzer_rewrites_nested_mergeable_set_region_to_single_filtered_scan() {
        let union = LogicalPlanBuilder::from(raw_filtered_scan_plan("vectors", "row-1"))
            .union_distinct(raw_filtered_scan_plan("vectors", "row-2"))
            .expect("union distinct")
            .build()
            .expect("union distinct plan");
        let except =
            LogicalPlanBuilder::except(union, raw_filtered_scan_plan("vectors", "row-2"), false)
                .expect("except");
        let analyzed = QdrantRelationPushdown
            .analyze(except, &ConfigOptions::default())
            .expect("analyzed plan");

        assert!(matches!(analyzed, LogicalPlan::Filter(_)));
        assert_eq!(subtree_class(&analyzed).expect("subtree class"), QdrantSubtreeClass {
            source:      QdrantSourceClass::SingleQdrant,
            topology:    QdrantTopologyClass::UnaryChain,
            composition: QdrantCompositionClass::Atomic,
            kernel:      QdrantKernelClass::None,
        });
    }

    #[test]
    fn analyzer_rewrites_count_over_mergeable_child_kernel_to_qdrant_count() {
        let union = LogicalPlanBuilder::from(raw_filtered_scan_plan("vectors", "row-1"))
            .union_distinct(raw_filtered_scan_plan("vectors", "row-2"))
            .expect("union distinct")
            .build()
            .expect("union distinct plan");
        let aggregate = LogicalPlanBuilder::from(union)
            .aggregate(Vec::<Expr>::new(), vec![count(lit(1_i64))])
            .expect("aggregate")
            .build()
            .expect("aggregate plan");
        let analyzed = QdrantRelationPushdown
            .analyze(aggregate, &ConfigOptions::default())
            .expect("analyzed plan");

        let LogicalPlan::Extension(extension) = &analyzed else {
            panic!("expected qdrant count extension, got {analyzed:?}");
        };
        assert_eq!(extension.node.name(), QDRANT_COUNT_NODE_NAME);
        assert_eq!(subtree_class(&analyzed).expect("subtree class"), QdrantSubtreeClass {
            source:      QdrantSourceClass::SingleQdrant,
            topology:    QdrantTopologyClass::Leaf,
            composition: QdrantCompositionClass::Atomic,
            kernel:      QdrantKernelClass::ExactSelf,
        });
    }

    #[test]
    fn analyzer_rewrites_facet_over_mergeable_child_kernel_to_qdrant_facet() {
        let union = LogicalPlanBuilder::from(payload_filtered_scan_plan("vectors", "row-1"))
            .union_distinct(payload_filtered_scan_plan("vectors", "row-2"))
            .expect("union distinct")
            .build()
            .expect("union distinct plan");
        let aggregate = LogicalPlanBuilder::from(union)
            .aggregate(vec![payload_path("tag")], vec![count(lit(1_i64))])
            .expect("aggregate")
            .sort(vec![count(lit(1_i64)).sort(false, false)])
            .expect("sort")
            .limit(0, Some(5))
            .expect("limit")
            .build()
            .expect("facet plan");
        let analyzed = QdrantRelationPushdown
            .analyze(aggregate, &ConfigOptions::default())
            .expect("analyzed plan");

        let LogicalPlan::Extension(extension) = &analyzed else {
            panic!("expected qdrant facet extension, got {analyzed:?}");
        };
        assert_eq!(extension.node.name(), QDRANT_FACET_NODE_NAME);
        assert_eq!(subtree_class(&analyzed).expect("subtree class"), QdrantSubtreeClass {
            source:      QdrantSourceClass::SingleQdrant,
            topology:    QdrantTopologyClass::Leaf,
            composition: QdrantCompositionClass::Atomic,
            kernel:      QdrantKernelClass::ExactSelf,
        });
    }
}
