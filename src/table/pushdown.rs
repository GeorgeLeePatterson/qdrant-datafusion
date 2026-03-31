use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::Expr;
use qdrant_client::qdrant::PointId;

use crate::arrow::schema::{PAYLOAD_FIELD_NAME, QdrantFieldBinding, UNNAMED_VECTOR_FIELD_NAME};
use crate::pushdown::filter::QdrantFilters;
use crate::pushdown::{QdrantPayloadField, QdrantPayloadSchema};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum QdrantVectorSelector {
    None,
    All,
    Named(Vec<String>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QdrantPayloadSelector {
    None,
    Full,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum QdrantOrdering {
    ById,
    ByPayload(QdrantPayloadOrdering),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct QdrantPayloadOrdering {
    pub(crate) field:      String,
    pub(crate) descending: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum QdrantOrderValue {
    Integer(i64),
    Float(f64),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum QdrantContinuation {
    Offset(Option<PointId>),
    Ordered(QdrantOrderedContinuation),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct QdrantOrderedContinuation {
    pub(crate) ordering:     QdrantPayloadOrdering,
    pub(crate) start_from:   Option<QdrantOrderValue>,
    pub(crate) boundary_ids: Vec<PointId>,
}

#[derive(Debug, Clone)]
pub(crate) struct QdrantScanSpec {
    pub(crate) schema:     SchemaRef,
    pub(crate) projection: Option<Vec<usize>>,
    pub(crate) vectors:    QdrantVectorSelector,
    pub(crate) payload:    QdrantPayloadSelector,
    pub(crate) filters:    QdrantFilters,
    pub(crate) ordering:   QdrantOrdering,
    pub(crate) limit:      Option<usize>,
}

impl QdrantPayloadSchema {
    pub(crate) fn ordering_for(
        &self,
        field: &str,
        descending: bool,
    ) -> Option<QdrantPayloadOrdering> {
        match self.field(field) {
            Some(
                QdrantPayloadField::Integer { range: true, .. }
                | QdrantPayloadField::Float
                | QdrantPayloadField::Datetime,
            ) => Some(QdrantPayloadOrdering { field: field.to_owned(), descending }),
            _ => None,
        }
    }
}

impl QdrantScanSpec {
    pub(crate) fn try_new(
        base_schema: &SchemaRef,
        payload_schema: &QdrantPayloadSchema,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Self> {
        let schema = match projection {
            Some(indices) if !indices.is_empty() => Arc::new(base_schema.project(indices)?),
            _ => Arc::clone(base_schema),
        };
        let vector_names = schema
            .fields()
            .iter()
            .filter(|field| QdrantFieldBinding::from_field(field).is_vector())
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        let vectors = if vector_names.is_empty() {
            QdrantVectorSelector::None
        } else if vector_names.len() == 1 && vector_names[0] == UNNAMED_VECTOR_FIELD_NAME {
            QdrantVectorSelector::All
        } else {
            QdrantVectorSelector::Named(vector_names)
        };
        let payload = if schema.fields().iter().any(|field| field.name() == PAYLOAD_FIELD_NAME) {
            QdrantPayloadSelector::Full
        } else {
            QdrantPayloadSelector::None
        };
        Ok(Self {
            schema,
            projection: projection.cloned(),
            vectors,
            payload,
            filters: QdrantFilters::try_new(base_schema, payload_schema, filters)?,
            ordering: QdrantOrdering::ById,
            limit,
        })
    }

    pub(crate) fn initial_continuation(&self) -> QdrantContinuation {
        match &self.ordering {
            QdrantOrdering::ById => QdrantContinuation::Offset(None),
            QdrantOrdering::ByPayload(ordering) => {
                QdrantContinuation::Ordered(QdrantOrderedContinuation {
                    ordering:     ordering.clone(),
                    start_from:   None,
                    boundary_ids: vec![],
                })
            }
        }
    }
}
