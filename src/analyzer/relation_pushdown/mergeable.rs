use std::sync::Arc;

use datafusion::common::{NullEquality, Result, ScalarValue};
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::utils::{conjunction, disjunction};
use datafusion::logical_expr::{Expr, JoinType, LogicalPlan, LogicalPlanBuilder};
use qdrant_client::qdrant::PointId;
use qdrant_client::qdrant::point_id::PointIdOptions;

use super::super::common::{QdrantSource, qdrant_source};
use super::{QdrantFilters, QdrantTableProvider, RawQdrantSetJoin, RawQdrantUnion};

pub(super) fn raw_union(plan: &LogicalPlan) -> Result<bool> {
    let Some(union) = raw_qdrant_union(plan)? else {
        return Ok(false);
    };
    let Some(branch_ids) = union.branch_ids.into_iter().collect::<Option<Vec<Vec<PointId>>>>()
    else {
        return Ok(false);
    };
    Ok(union.branches.iter().all(Option::is_some) && point_id_sets_are_disjoint(&branch_ids))
}

pub(super) fn raw_union_distinct(plan: &LogicalPlan) -> Result<bool> {
    let LogicalPlan::Distinct(datafusion::logical_expr::Distinct::All(input)) = plan else {
        return Ok(false);
    };
    raw_qdrant_union(input.as_ref()).map(|union| union.is_some())
}

pub(super) fn raw_set_join(plan: &LogicalPlan) -> bool { raw_qdrant_set_join(plan).is_some() }

pub(super) fn union_plan(plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
    let (union, require_disjoint) = match plan {
        LogicalPlan::Union(_) => (raw_qdrant_union(plan)?, true),
        LogicalPlan::Distinct(datafusion::logical_expr::Distinct::All(input)) => {
            (raw_qdrant_union(input.as_ref())?, false)
        }
        _ => return Ok(None),
    };
    let Some(union) = union else {
        return Ok(None);
    };
    let filter = merged_branch_filter(&union.branches);
    if require_disjoint {
        let Some(branch_ids) =
            union.branch_ids.iter().cloned().collect::<Option<Vec<Vec<PointId>>>>()
        else {
            return Ok(None);
        };
        if !union.branches.iter().all(Option::is_some) || !point_id_sets_are_disjoint(&branch_ids) {
            return Ok(None);
        }
    }
    let provider = Arc::new(QdrantTableProvider::new_for_planner(
        union.collection.clone(),
        union.client,
        union.schema,
        union.payload_schema,
    ));
    let builder = LogicalPlanBuilder::scan(union.collection, provider_as_source(provider), None)?;
    match filter {
        Some(filter) => builder.filter(filter)?.build().map(Some),
        None => builder.build().map(Some),
    }
}

pub(super) fn set_join_plan(plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
    let Some(join) = raw_qdrant_set_join(plan) else {
        return Ok(None);
    };
    let filter = match join.join_type {
        JoinType::LeftSemi => {
            conjunction([join.left_filter, join.right_filter].into_iter().flatten())
        }
        JoinType::LeftAnti => except_filter(join.left_filter, join.right_filter),
        _ => return Ok(None),
    };
    let provider = Arc::new(QdrantTableProvider::new_for_planner(
        join.collection.clone(),
        join.client,
        join.schema,
        join.payload_schema,
    ));
    let builder = LogicalPlanBuilder::scan(join.collection, provider_as_source(provider), None)?;
    match filter {
        Some(filter) => builder.filter(filter)?.build().map(Some),
        None => builder.build().map(Some),
    }
}

pub(super) fn redundant_raw_qdrant_distinct_plan(plan: &LogicalPlan) -> Option<LogicalPlan> {
    let LogicalPlan::Distinct(datafusion::logical_expr::Distinct::All(input)) = plan else {
        return None;
    };
    qdrant_source(input.as_ref()).map(|_| input.as_ref().clone())
}

fn raw_qdrant_union(plan: &LogicalPlan) -> Result<Option<RawQdrantUnion>> {
    let LogicalPlan::Union(union) = plan else {
        return Ok(None);
    };
    let mut collection = None::<String>;
    let mut client = None;
    let mut schema = None;
    let mut payload_schema = None;
    let mut branches = vec![];
    let mut branch_ids = vec![];
    for child in &union.inputs {
        let Some(source) = qdrant_source(child.as_ref()) else {
            return Ok(None);
        };
        match &collection {
            None => collection = Some(source.collection.clone()),
            Some(current) if current == &source.collection => {}
            Some(_) => return Ok(None),
        }
        if !source.filters.iter().all(|filter| {
            QdrantFilters::supports_exact(&source.schema, &source.payload_schema, filter)
        }) {
            return Ok(None);
        }
        let filters =
            QdrantFilters::try_new(&source.schema, &source.payload_schema, &source.filters)?;
        branches.push(conjunction(source.filters));
        branch_ids.push(filters.possible_point_ids());
        if client.is_none() {
            client = Some(source.client);
        }
        if schema.is_none() {
            schema = Some(source.schema);
        }
        if payload_schema.is_none() {
            payload_schema = Some(source.payload_schema);
        }
    }
    Ok(match (collection, client, schema, payload_schema) {
        (Some(collection), Some(client), Some(schema), Some(payload_schema)) => {
            Some(RawQdrantUnion {
                collection,
                client,
                schema,
                payload_schema,
                branches,
                branch_ids,
            })
        }
        _ => None,
    })
}

fn raw_qdrant_set_join(plan: &LogicalPlan) -> Option<RawQdrantSetJoin> {
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
    let left_source = raw_qdrant_set_branch_source(join.left.as_ref())?;
    let right_source = raw_qdrant_set_branch_source(join.right.as_ref())?;
    if left_source.collection != right_source.collection {
        return None;
    }
    if !left_source.filters.iter().all(|filter| {
        QdrantFilters::supports_exact(&left_source.schema, &left_source.payload_schema, filter)
    }) || !right_source.filters.iter().all(|filter| {
        QdrantFilters::supports_exact(&right_source.schema, &right_source.payload_schema, filter)
    }) {
        return None;
    }
    Some(RawQdrantSetJoin {
        collection:     left_source.collection,
        client:         left_source.client,
        schema:         left_source.schema,
        payload_schema: left_source.payload_schema,
        left_filter:    conjunction(left_source.filters),
        right_filter:   conjunction(right_source.filters),
        join_type:      join.join_type,
    })
}

fn raw_qdrant_set_branch_source(plan: &LogicalPlan) -> Option<QdrantSource> {
    let mut plan = plan;
    loop {
        match plan {
            LogicalPlan::SubqueryAlias(alias) => {
                plan = alias.input.as_ref();
            }
            LogicalPlan::Distinct(datafusion::logical_expr::Distinct::All(input)) => {
                plan = input.as_ref();
            }
            _ => return qdrant_source(plan),
        }
    }
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

fn point_id_sets_are_disjoint(id_sets: &[Vec<PointId>]) -> bool {
    let mut seen = vec![];
    for ids in id_sets {
        let mut branch_seen = vec![];
        for id in ids {
            let key = point_id_key(id);
            if branch_seen.iter().any(|seen| seen == &key) {
                continue;
            }
            if seen.iter().any(|seen| seen == &key) {
                return false;
            }
            branch_seen.push(key);
        }
        seen.extend(branch_seen);
    }
    true
}

fn point_id_key(id: &PointId) -> String {
    match id.point_id_options.as_ref() {
        Some(PointIdOptions::Num(value)) => format!("num:{value}"),
        Some(PointIdOptions::Uuid(value)) => format!("uuid:{value}"),
        None => "missing".to_owned(),
    }
}

fn merged_branch_filter(branches: &[Option<Expr>]) -> Option<Expr> {
    if branches.iter().any(Option::is_none) {
        return None;
    }
    disjunction(branches.iter().flatten().cloned())
}

fn except_filter(left: Option<Expr>, right: Option<Expr>) -> Option<Expr> {
    match (left, right) {
        (_, None) => Some(Expr::Literal(ScalarValue::Boolean(Some(false)), None)),
        (None, Some(right)) => Some(Expr::Not(Box::new(right))),
        (Some(left), Some(right)) => conjunction([left, Expr::Not(Box::new(right))]),
    }
}
