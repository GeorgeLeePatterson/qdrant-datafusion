use std::collections::HashSet;
use std::sync::Arc;

use datafusion::common::{NullEquality, Result, ScalarValue};
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::utils::{conjunction, disjunction};
use datafusion::logical_expr::{Expr, JoinType, LogicalPlan, LogicalPlanBuilder};
use qdrant_client::qdrant::PointId;
use qdrant_client::qdrant::point_id::PointIdOptions;

use super::super::common::QdrantSource;
use super::{QdrantFilters, QdrantTableProvider, RawQdrantSetJoin, RawQdrantUnion};

pub(super) fn redundant_raw_qdrant_distinct_plan(plan: &LogicalPlan) -> Option<LogicalPlan> {
    let LogicalPlan::Distinct(datafusion::logical_expr::Distinct::All(input)) = plan else {
        return None;
    };
    QdrantSource::from_plan(input.as_ref()).map(|_| input.as_ref().clone())
}

fn full_row_join_keys(join: &datafusion::logical_expr::logical_plan::Join) -> bool {
    let left_fields = join.left.schema().fields();
    let right_fields = join.right.schema().fields();
    join.on.len() == left_fields.len()
        && left_fields.len() == right_fields.len()
        && join.on.iter().zip(left_fields.iter().zip(right_fields.iter())).all(
            |((left, right), (left_field, right_field))| match (left, right) {
                (Expr::Column(left), Expr::Column(right)) => {
                    left.name == *left_field.name() && right.name == *right_field.name()
                }
                _ => false,
            },
        )
}

impl RawQdrantUnion {
    pub(super) fn from_plan(plan: &LogicalPlan) -> Result<Option<Self>> {
        let LogicalPlan::Union(union) = plan else {
            return Ok(None);
        };
        let branches = union
            .inputs
            .iter()
            .map(|child| Self::branch(child.as_ref()))
            .collect::<Result<Vec<_>>>()?;
        let mut branches = branches.into_iter();
        let Some(first) = branches.next() else {
            return Ok(None);
        };
        let Some(BranchInfo { source, filter, ids }) = first else {
            return Ok(None);
        };
        let collection = source.collection.clone();
        let client = source.client;
        let schema = source.schema;
        let payload_schema = source.payload_schema;
        let mut branch_filters = vec![filter];
        let mut branch_ids = vec![ids];
        for branch in branches {
            let Some(BranchInfo { source, filter, ids }) = branch else {
                return Ok(None);
            };
            if source.collection != collection {
                return Ok(None);
            }
            branch_filters.push(filter);
            branch_ids.push(ids);
        }
        Ok(Some(Self {
            collection,
            client,
            schema,
            payload_schema,
            branches: branch_filters,
            branch_ids,
        }))
    }

    pub(super) fn from_distinct_plan(plan: &LogicalPlan) -> Result<Option<Self>> {
        let LogicalPlan::Distinct(datafusion::logical_expr::Distinct::All(input)) = plan else {
            return Ok(None);
        };
        Self::from_plan(input.as_ref())
    }

    fn branch(plan: &LogicalPlan) -> Result<Option<BranchInfo>> {
        let Some(source) = QdrantSource::from_plan(plan) else {
            return Ok(None);
        };
        if !source.filters.iter().all(|filter| {
            QdrantFilters::supports_exact(&source.schema, &source.payload_schema, filter)
        }) {
            return Ok(None);
        }
        let filters =
            QdrantFilters::try_new(&source.schema, &source.payload_schema, &source.filters)?;
        Ok(Some(BranchInfo {
            filter: conjunction(source.filters.clone()),
            ids: filters.possible_point_ids(),
            source,
        }))
    }

    pub(super) fn can_union_all_merge(&self) -> bool {
        self.branches.iter().all(Option::is_some) && self.point_id_sets_are_disjoint()
    }

    pub(super) fn merged_plan(self, require_disjoint: bool) -> Result<Option<LogicalPlan>> {
        if require_disjoint && !self.can_union_all_merge() {
            return Ok(None);
        }
        let filter = self.merged_filter();
        let provider = Arc::new(QdrantTableProvider::new_for_planner(
            self.collection.clone(),
            self.client,
            self.schema,
            self.payload_schema,
        ));
        let builder =
            LogicalPlanBuilder::scan(self.collection, provider_as_source(provider), None)?;
        match filter {
            Some(filter) => builder.filter(filter)?.build().map(Some),
            None => builder.build().map(Some),
        }
    }

    fn merged_filter(&self) -> Option<Expr> {
        (!self.branches.iter().any(Option::is_none))
            .then(|| disjunction(self.branches.iter().flatten().cloned()))
            .flatten()
    }

    fn point_id_sets_are_disjoint(&self) -> bool {
        let Some(id_sets) = self.branch_ids.iter().cloned().collect::<Option<Vec<Vec<PointId>>>>()
        else {
            return false;
        };
        let mut seen = HashSet::new();
        id_sets.into_iter().all(|ids| {
            ids.into_iter()
                .map(|id| Self::point_id_key(&id))
                .collect::<HashSet<_>>()
                .into_iter()
                .all(|key| seen.insert(key))
        })
    }

    fn point_id_key(id: &PointId) -> String {
        match id.point_id_options.as_ref() {
            Some(PointIdOptions::Num(value)) => format!("num:{value}"),
            Some(PointIdOptions::Uuid(value)) => format!("uuid:{value}"),
            None => "missing".to_owned(),
        }
    }
}

impl RawQdrantSetJoin {
    pub(super) fn from_plan(plan: &LogicalPlan) -> Option<Self> {
        let LogicalPlan::Join(join) = plan else {
            return None;
        };
        if join.filter.is_some()
            || join.null_aware
            || join.null_equality != NullEquality::NullEqualsNull
            || !matches!(join.join_type, JoinType::LeftSemi | JoinType::LeftAnti)
            || !full_row_join_keys(join)
        {
            return None;
        }
        let left_source = QdrantSource::from_set_branch_plan(join.left.as_ref())?;
        let right_source = QdrantSource::from_set_branch_plan(join.right.as_ref())?;
        if left_source.collection != right_source.collection {
            return None;
        }
        if !left_source.filters.iter().all(|filter| {
            QdrantFilters::supports_exact(&left_source.schema, &left_source.payload_schema, filter)
        }) || !right_source.filters.iter().all(|filter| {
            QdrantFilters::supports_exact(
                &right_source.schema,
                &right_source.payload_schema,
                filter,
            )
        }) {
            return None;
        }
        Some(Self {
            collection: left_source.collection,
            client: left_source.client,
            schema: left_source.schema,
            payload_schema: left_source.payload_schema,
            left_filter: conjunction(left_source.filters),
            right_filter: conjunction(right_source.filters),
            join_type: join.join_type,
        })
    }

    pub(super) fn merged_plan(self) -> Result<LogicalPlan> {
        let filter = self.merged_filter();
        let provider = Arc::new(QdrantTableProvider::new_for_planner(
            self.collection.clone(),
            self.client,
            self.schema,
            self.payload_schema,
        ));
        let builder =
            LogicalPlanBuilder::scan(self.collection, provider_as_source(provider), None)?;
        match filter {
            Some(filter) => builder.filter(filter)?.build(),
            None => builder.build(),
        }
    }

    fn merged_filter(&self) -> Option<Expr> {
        match self.join_type {
            JoinType::LeftSemi => conjunction(
                [self.left_filter.clone(), self.right_filter.clone()].into_iter().flatten(),
            ),
            JoinType::LeftAnti => match (&self.left_filter, &self.right_filter) {
                (_, None) => Some(Expr::Literal(ScalarValue::Boolean(Some(false)), None)),
                (None, Some(right)) => Some(Expr::Not(Box::new(right.clone()))),
                (Some(left), Some(right)) => {
                    conjunction([left.clone(), Expr::Not(Box::new(right.clone()))])
                }
            },
            _ => None,
        }
    }
}

impl QdrantSource {
    fn from_set_branch_plan(plan: &LogicalPlan) -> Option<Self> {
        let mut plan = plan;
        loop {
            match plan {
                LogicalPlan::SubqueryAlias(alias) => {
                    plan = alias.input.as_ref();
                }
                LogicalPlan::Distinct(datafusion::logical_expr::Distinct::All(input)) => {
                    plan = input.as_ref();
                }
                _ => return Self::from_plan(plan),
            }
        }
    }
}

struct BranchInfo {
    source: QdrantSource,
    filter: Option<Expr>,
    ids: Option<Vec<PointId>>,
}
