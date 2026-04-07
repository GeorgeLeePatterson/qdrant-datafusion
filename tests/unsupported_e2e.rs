#![allow(unused_crate_dependencies)]

mod common;
#[path = "catalog/mod.rs"]
mod sql_catalog;

const TRACING_DIRECTIVES: &[(&str, &str)] =
    &[("testcontainers", "debug"), ("hyper", "error"), ("tonic", "error")];

#[cfg(feature = "test-utils")]
e2e_test!(
    unsupported_scan_projection_queries,
    tests::test_unsupported_scan_projection_queries,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    unsupported_scan_filter_queries,
    tests::test_unsupported_scan_filter_queries,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    unsupported_scan_ordering_queries,
    tests::test_unsupported_scan_ordering_queries,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    unsupported_scan_aggregate_queries,
    tests::test_unsupported_scan_aggregate_queries,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    unsupported_write_append_queries,
    tests::test_unsupported_write_append_queries,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    unsupported_query_nearest_queries,
    tests::test_unsupported_query_nearest_queries,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    unsupported_query_sample_queries,
    tests::test_unsupported_query_sample_queries,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    unsupported_query_recommend_queries,
    tests::test_unsupported_query_recommend_queries,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    unsupported_query_discover_queries,
    tests::test_unsupported_query_discover_queries,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    unsupported_query_context_queries,
    tests::test_unsupported_query_context_queries,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    unsupported_query_mmr_queries,
    tests::test_unsupported_query_mmr_queries,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    unsupported_query_relevance_queries,
    tests::test_unsupported_query_relevance_queries,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    unsupported_query_grouped_queries,
    tests::test_unsupported_query_grouped_queries,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    unsupported_coordination_formula_queries,
    tests::test_unsupported_coordination_formula_queries,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    unsupported_coordination_fusion_queries,
    tests::test_unsupported_coordination_fusion_queries,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{ArrayRef, FixedSizeListArray, Float32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::catalog::memory::MemTable;
    use datafusion::prelude::*;
    use qdrant_client::Qdrant;
    use qdrant_client::qdrant::{
        CreateCollectionBuilder, CreateFieldIndexCollectionBuilder, Distance, FieldType,
        NamedVectors, PointStruct, TextIndexParamsBuilder, TokenizerType, UpsertPointsBuilder,
        Vector, VectorParamsBuilder, VectorsConfigBuilder, payload_index_params,
    };
    use qdrant_datafusion::context::QdrantSessionContext;
    use qdrant_datafusion::error::Result;
    use qdrant_datafusion::table::QdrantTableProvider;
    use qdrant_datafusion::test_utils::QdrantContainer;

    use crate::sql_catalog::{UnsupportedSqlCase, unsupported as sql};

    fn create_qdrant_client(c: &Arc<QdrantContainer>) -> Result<Qdrant> {
        Qdrant::from_url(&c.get_url()).api_key(c.get_api_key()).build().map_err(Into::into)
    }

    fn dense_insert_vector_array(values: &[f32]) -> ArrayRef {
        Arc::new(FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::Float32, false)),
            1,
            Arc::new(Float32Array::from(values.to_vec())),
            None,
        ))
    }

    fn dense_insert_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("payload", DataType::Utf8, true),
            Field::new("vector", DataType::new_fixed_size_list(DataType::Float32, 1, false), true),
        ]));
        RecordBatch::try_new(schema, vec![
            Arc::new(StringArray::from(vec!["1", "2"])),
            Arc::new(StringArray::from(vec![Some(r#"{"rank":10}"#), Some(r#"{"rank":20}"#)])),
            dense_insert_vector_array(&[0.1_f32, 0.9_f32]),
        ])
        .expect("dense insert batch")
    }

    async fn create_scalar_collection(client: &Qdrant, collection_name: &str) -> Result<()> {
        let _ = client
            .create_collection(
                CreateCollectionBuilder::new(collection_name)
                    .vectors_config(VectorParamsBuilder::new(1, Distance::Dot)),
            )
            .await?;
        Ok(())
    }

    async fn create_payload_index<IndexParams>(
        client: &Qdrant,
        collection_name: &str,
        field_name: &str,
        field_type: FieldType,
        index_params: IndexParams,
    ) -> Result<()>
    where
        IndexParams: Into<payload_index_params::IndexParams>,
    {
        drop(
            client
                .create_field_index(
                    CreateFieldIndexCollectionBuilder::new(collection_name, field_name, field_type)
                        .wait(true)
                        .field_index_params(index_params),
                )
                .await?,
        );
        Ok(())
    }

    async fn create_scan_boundary_context(
        c: &Arc<QdrantContainer>,
        collection_name: &str,
    ) -> Result<QdrantSessionContext> {
        let client = create_qdrant_client(c)?;
        create_scalar_collection(&client, collection_name).await?;
        create_payload_index(
            &client,
            collection_name,
            "rank",
            FieldType::Integer,
            qdrant_client::qdrant::IntegerIndexParamsBuilder::new(true, true).build(),
        )
        .await?;
        create_payload_index(
            &client,
            collection_name,
            "description",
            FieldType::Text,
            TextIndexParamsBuilder::new(TokenizerType::Word).phrase_matching(true).build(),
        )
        .await?;

        let mut payload1 = qdrant_client::Payload::new();
        payload1.insert("rank", 30_i64);
        payload1.insert("description", "good cheap coffee");
        let mut payload2 = qdrant_client::Payload::new();
        payload2.insert("rank", 10_i64);
        payload2.insert("description", "time is a flat circle");
        let mut payload3 = qdrant_client::Payload::new();
        payload3.insert("rank", 20_i64);
        payload3.insert("description", "good food nearby");

        let points = vec![
            PointStruct::new(1, Vector::new_dense(vec![0.0]), payload1),
            PointStruct::new(2, Vector::new_dense(vec![0.0]), payload2),
            PointStruct::new(3, Vector::new_dense(vec![0.0]), payload3),
        ];
        drop(client.upsert_points(UpsertPointsBuilder::new(collection_name, points)).await?);

        let table_provider = QdrantTableProvider::try_new(client.clone(), collection_name).await?;
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(ctx.session_context().register_table("vectors", Arc::new(table_provider))?);
        Ok(ctx)
    }

    async fn create_write_context(
        c: &Arc<QdrantContainer>,
        collection_name: &str,
    ) -> Result<QdrantSessionContext> {
        let client = create_qdrant_client(c)?;
        create_scalar_collection(&client, collection_name).await?;

        let table_provider = QdrantTableProvider::try_new(client.clone(), collection_name).await?;
        let staging =
            MemTable::try_new(dense_insert_batch().schema(), vec![vec![dense_insert_batch()]])?;
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(ctx.session_context().register_table("vectors", Arc::new(table_provider))?);
        drop(ctx.session_context().register_table("staging", Arc::new(staging))?);
        Ok(ctx)
    }

    async fn create_vector_query_context(
        c: &Arc<QdrantContainer>,
        collection_name: &str,
    ) -> Result<QdrantSessionContext> {
        let client = create_qdrant_client(c)?;

        let mut vectors_config = VectorsConfigBuilder::default();
        let _ = vectors_config
            .add_named_vector_params("vector", VectorParamsBuilder::new(2, Distance::Dot).build());
        let _ = client
            .create_collection(
                CreateCollectionBuilder::new(collection_name).vectors_config(vectors_config),
            )
            .await?;

        let points = vec![
            PointStruct::new(
                1,
                NamedVectors::default().add_vector("vector", vec![1.0, 0.0]),
                qdrant_client::Payload::default(),
            ),
            PointStruct::new(
                2,
                NamedVectors::default().add_vector("vector", vec![0.5, 0.5]),
                qdrant_client::Payload::default(),
            ),
            PointStruct::new(
                3,
                NamedVectors::default().add_vector("vector", vec![0.0, 1.0]),
                qdrant_client::Payload::default(),
            ),
        ];
        drop(client.upsert_points(UpsertPointsBuilder::new(collection_name, points)).await?);

        let table_provider = QdrantTableProvider::try_new(client.clone(), collection_name).await?;
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(ctx.session_context().register_table("vectors", Arc::new(table_provider))?);
        Ok(ctx)
    }

    async fn create_dual_vector_query_context(
        c: &Arc<QdrantContainer>,
        collection_name: &str,
    ) -> Result<QdrantSessionContext> {
        let client = create_qdrant_client(c)?;

        let mut vectors_config = VectorsConfigBuilder::default();
        let _ = vectors_config.add_named_vector_params(
            "embedding",
            VectorParamsBuilder::new(2, Distance::Dot).build(),
        );
        let _ = vectors_config
            .add_named_vector_params("aux", VectorParamsBuilder::new(2, Distance::Dot).build());
        let _ = client
            .create_collection(
                CreateCollectionBuilder::new(collection_name).vectors_config(vectors_config),
            )
            .await?;

        let points = vec![
            PointStruct::new(
                1,
                NamedVectors::default()
                    .add_vector("embedding", vec![1.0, 0.0])
                    .add_vector("aux", vec![0.0, 1.0]),
                qdrant_client::Payload::new(),
            ),
            PointStruct::new(
                2,
                NamedVectors::default()
                    .add_vector("embedding", vec![0.4, 0.0])
                    .add_vector("aux", vec![0.0, 0.4]),
                qdrant_client::Payload::new(),
            ),
            PointStruct::new(
                3,
                NamedVectors::default()
                    .add_vector("embedding", vec![0.0, 1.0])
                    .add_vector("aux", vec![0.0, 0.2]),
                qdrant_client::Payload::new(),
            ),
        ];
        drop(client.upsert_points(UpsertPointsBuilder::new(collection_name, points)).await?);

        let table_provider = QdrantTableProvider::try_new(client.clone(), collection_name).await?;
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(ctx.session_context().register_table("vectors", Arc::new(table_provider))?);
        Ok(ctx)
    }

    async fn create_grouped_query_context(
        c: &Arc<QdrantContainer>,
        collection_name: &str,
    ) -> Result<QdrantSessionContext> {
        let client = create_qdrant_client(c)?;

        let mut vectors_config = VectorsConfigBuilder::default();
        let _ = vectors_config.add_named_vector_params(
            "embedding",
            VectorParamsBuilder::new(2, Distance::Dot).build(),
        );
        let _ = client
            .create_collection(
                CreateCollectionBuilder::new(collection_name).vectors_config(vectors_config),
            )
            .await?;
        create_payload_index(
            &client,
            collection_name,
            "tag",
            FieldType::Keyword,
            qdrant_client::qdrant::KeywordIndexParamsBuilder::default().build(),
        )
        .await?;

        let mut payload1 = qdrant_client::Payload::new();
        payload1.insert("tag", "red");
        let mut payload2 = qdrant_client::Payload::new();
        payload2.insert("tag", "red");
        let mut payload3 = qdrant_client::Payload::new();
        payload3.insert("tag", "blue");

        let points = vec![
            PointStruct::new(
                1,
                NamedVectors::default().add_vector("embedding", vec![1.0, 0.0]),
                payload1,
            ),
            PointStruct::new(
                2,
                NamedVectors::default().add_vector("embedding", vec![0.4, 0.0]),
                payload2,
            ),
            PointStruct::new(
                3,
                NamedVectors::default().add_vector("embedding", vec![0.9, 0.0]),
                payload3,
            ),
        ];
        drop(client.upsert_points(UpsertPointsBuilder::new(collection_name, points)).await?);

        let table_provider = QdrantTableProvider::try_new(client.clone(), collection_name).await?;
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(ctx.session_context().register_table("vectors", Arc::new(table_provider))?);
        Ok(ctx)
    }

    async fn assert_unsupported_query(
        ctx: &QdrantSessionContext,
        case: UnsupportedSqlCase,
    ) -> Result<()> {
        let err = match ctx.sql(case.case.sql).await {
            Err(error) => error,
            Ok(dataframe) => match dataframe.clone().create_physical_plan().await {
                Err(error) => error,
                Ok(_) => match dataframe.collect().await {
                    Err(error) => error,
                    Ok(batches) => {
                        panic!(
                            "unsupported case unexpectedly succeeded: {} kind={} tracker={:?} \
                             boundary={} sql={} batches={batches:?}",
                            case.case.id,
                            case.kind.label(),
                            case.tracker,
                            case.boundary,
                            case.case.sql,
                        );
                    }
                },
            },
        };

        let message = err.to_string();
        assert!(
            message.contains(case.error_contains),
            "case={} kind={} tracker={:?} boundary={} sql={} expected_error_contains={} \
             actual_error={message}",
            case.case.id,
            case.kind.label(),
            case.tracker,
            case.boundary,
            case.case.sql,
            case.error_contains,
        );
        Ok(())
    }

    pub(super) async fn test_unsupported_scan_projection_queries(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let ctx =
            create_scan_boundary_context(&c, "test_unsupported_scan_projection_queries").await?;
        for case in sql::scan::projection::ALL {
            assert_unsupported_query(&ctx, *case).await?;
        }
        Ok(())
    }

    pub(super) async fn test_unsupported_scan_filter_queries(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let ctx = create_scan_boundary_context(&c, "test_unsupported_scan_filter_queries").await?;
        for case in sql::scan::filters::ALL {
            assert_unsupported_query(&ctx, *case).await?;
        }
        Ok(())
    }

    pub(super) async fn test_unsupported_scan_ordering_queries(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let ctx =
            create_scan_boundary_context(&c, "test_unsupported_scan_ordering_queries").await?;
        for case in sql::scan::ordering::ALL {
            assert_unsupported_query(&ctx, *case).await?;
        }
        Ok(())
    }

    pub(super) async fn test_unsupported_scan_aggregate_queries(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let ctx =
            create_scan_boundary_context(&c, "test_unsupported_scan_aggregate_queries").await?;
        for case in sql::scan::aggregates::ALL {
            assert_unsupported_query(&ctx, *case).await?;
        }
        Ok(())
    }

    pub(super) async fn test_unsupported_write_append_queries(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let ctx = create_write_context(&c, "test_unsupported_write_append_queries").await?;
        for case in sql::writes::append::ALL {
            assert_unsupported_query(&ctx, *case).await?;
        }
        Ok(())
    }

    pub(super) async fn test_unsupported_query_nearest_queries(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let ctx = create_vector_query_context(&c, "test_unsupported_query_nearest_queries").await?;
        for case in sql::query::nearest::ALL {
            assert_unsupported_query(&ctx, *case).await?;
        }
        Ok(())
    }

    pub(super) async fn test_unsupported_query_sample_queries(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let ctx = create_vector_query_context(&c, "test_unsupported_query_sample_queries").await?;
        for case in sql::query::sample::ALL {
            assert_unsupported_query(&ctx, *case).await?;
        }
        Ok(())
    }

    pub(super) async fn test_unsupported_query_recommend_queries(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let ctx = create_dual_vector_query_context(&c, "test_unsupported_query_recommend_queries")
            .await?;
        for case in sql::query::recommend::ALL {
            assert_unsupported_query(&ctx, *case).await?;
        }
        Ok(())
    }

    pub(super) async fn test_unsupported_query_discover_queries(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let ctx =
            create_dual_vector_query_context(&c, "test_unsupported_query_discover_queries").await?;
        for case in sql::query::discover::ALL {
            assert_unsupported_query(&ctx, *case).await?;
        }
        Ok(())
    }

    pub(super) async fn test_unsupported_query_context_queries(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let ctx =
            create_dual_vector_query_context(&c, "test_unsupported_query_context_queries").await?;
        for case in sql::query::context::ALL {
            assert_unsupported_query(&ctx, *case).await?;
        }
        Ok(())
    }

    pub(super) async fn test_unsupported_query_mmr_queries(c: Arc<QdrantContainer>) -> Result<()> {
        let ctx =
            create_dual_vector_query_context(&c, "test_unsupported_query_mmr_queries").await?;
        for case in sql::query::mmr::ALL {
            assert_unsupported_query(&ctx, *case).await?;
        }
        Ok(())
    }

    pub(super) async fn test_unsupported_query_relevance_queries(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let ctx = create_dual_vector_query_context(&c, "test_unsupported_query_relevance_queries")
            .await?;
        for case in sql::query::relevance::ALL {
            assert_unsupported_query(&ctx, *case).await?;
        }
        Ok(())
    }

    pub(super) async fn test_unsupported_query_grouped_queries(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let ctx =
            create_grouped_query_context(&c, "test_unsupported_query_grouped_queries").await?;
        for case in sql::query::grouped::ALL {
            assert_unsupported_query(&ctx, *case).await?;
        }
        Ok(())
    }

    pub(super) async fn test_unsupported_coordination_formula_queries(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        if sql::coordination::formula::ALL.is_empty() {
            return Ok(());
        }
        let ctx =
            create_dual_vector_query_context(&c, "test_unsupported_coordination_formula_queries")
                .await?;
        for case in sql::coordination::formula::ALL {
            assert_unsupported_query(&ctx, *case).await?;
        }
        Ok(())
    }

    pub(super) async fn test_unsupported_coordination_fusion_queries(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let ctx =
            create_dual_vector_query_context(&c, "test_unsupported_coordination_fusion_queries")
                .await?;
        for case in sql::coordination::fusion::ALL {
            assert_unsupported_query(&ctx, *case).await?;
        }
        Ok(())
    }
}
