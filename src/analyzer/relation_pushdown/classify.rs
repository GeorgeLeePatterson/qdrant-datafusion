use datafusion::common::Result;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::datasource::source_as_provider;
use datafusion::logical_expr::LogicalPlan;

use super::extract::exact_self_candidate;
use super::{
    QDRANT_COUNT_NODE_NAME, QDRANT_FACET_NODE_NAME, QdrantCompositionClass, QdrantKernelClass,
    QdrantSourceClass, QdrantSubtreeClass, QdrantSubtreeStatus, QdrantTableProvider,
    QdrantTopologyClass, mergeable,
};
use crate::pushdown::QdrantPayloadPath;

pub(super) fn subtree_status(plan: &LogicalPlan) -> Result<QdrantSubtreeStatus> {
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

pub(super) fn topology_class(plan: &LogicalPlan) -> QdrantTopologyClass {
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
    if mergeable::raw_set_join(plan) {
        return Ok(QdrantCompositionClass::Mergeable);
    }
    if mergeable::raw_union_distinct(plan)? {
        return Ok(QdrantCompositionClass::Mergeable);
    }
    if matches!(plan, LogicalPlan::Union(_)) {
        return Ok(match kernel {
            QdrantKernelClass::ExactChild | QdrantKernelClass::ExactChildren => {
                QdrantCompositionClass::Batchable
            }
            QdrantKernelClass::None if mergeable::raw_union(plan)? => {
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
    if exact_self
        || matches!(
            plan,
            LogicalPlan::Extension(extension)
                if matches!(extension.node.name(), QDRANT_COUNT_NODE_NAME | QDRANT_FACET_NODE_NAME)
        )
    {
        return QdrantKernelClass::ExactSelf;
    }
    combine_kernels(plan.inputs().into_iter().map(|child| kernel_class(child, false)))
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
    let direct_local_shell = match plan {
        LogicalPlan::Projection(projection) => {
            matches!(
                projection.input.as_ref(),
                LogicalPlan::TableScan(_) | LogicalPlan::Extension(_)
            )
        }
        LogicalPlan::Window(window) => {
            matches!(window.input.as_ref(), LogicalPlan::TableScan(_) | LogicalPlan::Extension(_))
        }
        _ => false,
    };
    if !direct_local_shell {
        return Ok(false);
    }
    let mut found = false;
    let _ = plan.apply_expressions(|expr| {
        if expr.exists(|expr| Ok(QdrantPayloadPath::from_logical_expr(expr).is_some()))? {
            found = true;
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    })?;
    Ok(found)
}
