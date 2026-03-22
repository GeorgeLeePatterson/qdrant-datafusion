use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result as DataFusionResult;
use datafusion::prelude::Expr;
use qdrant_client::qdrant::{PayloadSchemaInfo, PayloadSchemaType, PointId, payload_index_params};

use crate::arrow::schema::{
    PAYLOAD_FIELD_NAME, UNNAMED_VECTOR_FIELD_NAME, dense_vector_width, is_multi_vector_field,
    is_sparse_vector_field,
};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QdrantPayloadField {
    Integer,
    Float,
    Datetime,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct QdrantPayloadSchema {
    fields: HashMap<String, QdrantPayloadField>,
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
    pub(crate) filters:    Vec<Expr>,
    pub(crate) ordering:   QdrantOrdering,
    pub(crate) limit:      Option<usize>,
}

impl QdrantScanSpec {
    pub(crate) fn try_new(
        base_schema: &SchemaRef,
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
            .filter(|field| {
                dense_vector_width(field).is_some()
                    || is_multi_vector_field(field)
                    || is_sparse_vector_field(field)
            })
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
            filters: filters.to_vec(),
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

impl From<HashMap<String, PayloadSchemaInfo>> for QdrantPayloadSchema {
    fn from(payload_schema: HashMap<String, PayloadSchemaInfo>) -> Self {
        let fields = payload_schema
            .into_iter()
            .filter_map(|(field_name, info)| {
                let data_type = PayloadSchemaType::try_from(info.data_type).ok()?;
                let params = info.params?.index_params?;
                let field = match (data_type, params) {
                    (
                        PayloadSchemaType::Integer,
                        payload_index_params::IndexParams::IntegerIndexParams(params),
                    ) if params.range.unwrap_or(true) => QdrantPayloadField::Integer,
                    (
                        PayloadSchemaType::Float,
                        payload_index_params::IndexParams::FloatIndexParams(_),
                    ) => QdrantPayloadField::Float,
                    (
                        PayloadSchemaType::Datetime,
                        payload_index_params::IndexParams::DatetimeIndexParams(_),
                    ) => QdrantPayloadField::Datetime,
                    _ => return None,
                };
                Some((field_name, field))
            })
            .collect();
        Self { fields }
    }
}

impl QdrantPayloadSchema {
    pub(crate) fn ordering_for(
        &self,
        field: &str,
        descending: bool,
    ) -> Option<QdrantPayloadOrdering> {
        if !self.fields.contains_key(field) {
            return None;
        }
        Some(QdrantPayloadOrdering { field: field.to_owned(), descending })
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use qdrant_client::qdrant::{FloatIndexParams, IntegerIndexParams};

    use super::*;
    use crate::arrow::schema::ID_FIELD_NAME;

    fn schema(fields: Vec<Field>) -> SchemaRef { Arc::new(Schema::new(fields)) }

    #[test]
    fn scan_spec_uses_all_for_unnamed_vector_contract() {
        let schema = schema(vec![
            Field::new(ID_FIELD_NAME, DataType::Utf8, false),
            Field::new(
                UNNAMED_VECTOR_FIELD_NAME,
                DataType::new_fixed_size_list(DataType::Float32, 3, false),
                true,
            ),
        ]);

        let spec = QdrantScanSpec::try_new(&schema, None, &[], None).expect("scan spec");

        assert_eq!(spec.vectors, QdrantVectorSelector::All);
    }

    #[test]
    fn scan_spec_ignores_non_vector_columns() {
        let schema = schema(vec![
            Field::new(ID_FIELD_NAME, DataType::Utf8, false),
            Field::new("score", DataType::Float32, true),
            Field::new(
                "embedding",
                DataType::new_fixed_size_list(DataType::Float32, 3, false),
                true,
            ),
        ]);

        let spec = QdrantScanSpec::try_new(&schema, None, &[], None).expect("scan spec");

        assert_eq!(spec.vectors, QdrantVectorSelector::Named(vec!["embedding".to_owned()]),);
    }

    #[test]
    fn scan_spec_tracks_projection_payload_limit_and_continuation() {
        let schema = schema(vec![
            Field::new(ID_FIELD_NAME, DataType::Utf8, false),
            Field::new(PAYLOAD_FIELD_NAME, DataType::Utf8, true),
            Field::new(
                "embedding",
                DataType::new_fixed_size_list(DataType::Float32, 3, false),
                true,
            ),
        ]);

        let projection = vec![1, 2];
        let spec =
            QdrantScanSpec::try_new(&schema, Some(&projection), &[], Some(7)).expect("scan spec");

        assert_eq!(spec.projection, Some(projection));
        assert_eq!(spec.payload, QdrantPayloadSelector::Full);
        assert_eq!(spec.limit, Some(7));
        assert_eq!(spec.initial_continuation(), QdrantContinuation::Offset(None));
    }

    #[test]
    fn payload_schema_keeps_orderable_scalar_indexes_only() {
        let schema = QdrantPayloadSchema::from(HashMap::from([
            ("rank".to_owned(), PayloadSchemaInfo {
                data_type: PayloadSchemaType::Integer as i32,
                params:    Some(qdrant_client::qdrant::PayloadIndexParams {
                    index_params: Some(payload_index_params::IndexParams::IntegerIndexParams(
                        IntegerIndexParams { range: Some(true), ..Default::default() },
                    )),
                }),
                points:    None,
            }),
            ("score".to_owned(), PayloadSchemaInfo {
                data_type: PayloadSchemaType::Float as i32,
                params:    Some(qdrant_client::qdrant::PayloadIndexParams {
                    index_params: Some(payload_index_params::IndexParams::FloatIndexParams(
                        FloatIndexParams::default(),
                    )),
                }),
                points:    None,
            }),
            ("lookup_only".to_owned(), PayloadSchemaInfo {
                data_type: PayloadSchemaType::Integer as i32,
                params:    Some(qdrant_client::qdrant::PayloadIndexParams {
                    index_params: Some(payload_index_params::IndexParams::IntegerIndexParams(
                        IntegerIndexParams { range: Some(false), ..Default::default() },
                    )),
                }),
                points:    None,
            }),
        ]));

        assert_eq!(
            schema.ordering_for("rank", false),
            Some(QdrantPayloadOrdering { field: "rank".to_owned(), descending: false }),
        );
        assert_eq!(
            schema.ordering_for("score", true),
            Some(QdrantPayloadOrdering { field: "score".to_owned(), descending: true }),
        );
        assert_eq!(schema.ordering_for("lookup_only", false), None);
        assert_eq!(schema.ordering_for("missing", false), None);
    }
}
