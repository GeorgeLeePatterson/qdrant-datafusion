#![allow(unused_crate_dependencies)]

mod common;
#[path = "catalog/mod.rs"]
mod sql_catalog;

const TRACING_DIRECTIVES: &[(&str, &str)] =
    &[("testcontainers", "debug"), ("hyper", "error"), ("tonic", "error")];

#[cfg(feature = "test-utils")]
e2e_test!(table_provider_named, tests::test_table_provider_named, TRACING_DIRECTIVES, None);

#[cfg(feature = "test-utils")]
e2e_test!(table_provider_unnamed, tests::test_table_provider_unnamed, TRACING_DIRECTIVES, None);

#[cfg(feature = "test-utils")]
e2e_test!(
    table_provider_scrolls_full_scan,
    tests::test_table_provider_scrolls_full_scan,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    table_provider_orders_by_payload_field,
    tests::test_table_provider_orders_by_payload_field,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    table_provider_orders_by_aliased_payload_field,
    tests::test_table_provider_orders_by_aliased_payload_field,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    table_provider_projects_typed_payload_fields,
    tests::test_table_provider_projects_typed_payload_fields,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    table_provider_payload_empty_and_values_count,
    tests::test_table_provider_payload_empty_and_values_count,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    table_provider_payload_geo_distance,
    tests::test_table_provider_payload_geo_distance,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    table_provider_payload_text_predicates,
    tests::test_table_provider_payload_text_predicates,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    table_provider_payload_nested_predicate,
    tests::test_table_provider_payload_nested_predicate,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    table_provider_insert_into_appends_rows,
    tests::test_table_provider_insert_into_appends_rows,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    table_provider_insert_into_preserves_existing_ids,
    tests::test_table_provider_insert_into_preserves_existing_ids,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    table_provider_replace_into_overwrites_existing_ids,
    tests::test_table_provider_replace_into_overwrites_existing_ids,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    table_provider_delete_from_removes_matching_rows,
    tests::test_table_provider_delete_from_removes_matching_rows,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    table_provider_supported_scan_catalog_queries,
    tests::test_table_provider_supported_scan_catalog_queries,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    table_provider_supported_write_catalog_queries,
    tests::test_table_provider_supported_write_catalog_queries,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    table_provider_filters_by_id_and_vector_presence,
    tests::test_table_provider_filters_by_id_and_vector_presence,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    table_provider_filters_by_payload_field,
    tests::test_table_provider_filters_by_payload_field,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    table_provider_distinguishes_empty_string_from_null,
    tests::test_table_provider_distinguishes_empty_string_from_null,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    table_provider_pushes_down_count_star,
    tests::test_table_provider_pushes_down_count_star,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    table_provider_pushes_down_keyword_facet,
    tests::test_table_provider_pushes_down_keyword_facet,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    table_provider_pushes_down_integer_facet,
    tests::test_table_provider_pushes_down_integer_facet,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    table_provider_pushes_down_bool_facet,
    tests::test_table_provider_pushes_down_bool_facet,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    prepared_session_sql_nearest_query_without_limit,
    tests::test_prepared_session_sql_nearest_query_without_limit,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    prepared_session_sql_nearest_query,
    tests::test_prepared_session_sql_nearest_query,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    prepared_session_sql_nearest_query_local_projection_shell,
    tests::test_prepared_session_sql_nearest_query_local_projection_shell,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    prepared_session_sql_nearest_query_local_filter_shell,
    tests::test_prepared_session_sql_nearest_query_local_filter_shell,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    prepared_session_sql_nearest_query_local_filter_and_projection_shell,
    tests::test_prepared_session_sql_nearest_query_local_filter_and_projection_shell,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    prepared_session_sql_sample_query,
    tests::test_prepared_session_sql_sample_query,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    prepared_session_sql_recommend_query,
    tests::test_prepared_session_sql_recommend_query,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    prepared_session_sql_discover_query,
    tests::test_prepared_session_sql_discover_query,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    prepared_session_sql_context_query,
    tests::test_prepared_session_sql_context_query,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    prepared_session_sql_nearest_with_mmr_query,
    tests::test_prepared_session_sql_nearest_with_mmr_query,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    prepared_session_sql_relevance_feedback_query,
    tests::test_prepared_session_sql_relevance_feedback_query,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    prepared_session_sql_supported_nearest_catalog_queries,
    tests::test_prepared_session_sql_supported_nearest_catalog_queries,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    prepared_session_sql_supported_query_family_catalog_queries,
    tests::test_prepared_session_sql_supported_query_family_catalog_queries,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    prepared_session_sql_grouped_nearest_query,
    tests::test_prepared_session_sql_grouped_nearest_query,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    prepared_session_sql_grouped_query_family_queries,
    tests::test_prepared_session_sql_grouped_query_family_queries,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    nearest_query_projects_payload_path,
    tests::test_nearest_query_projects_payload_path,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    coordinated_formula_sql_variants,
    tests::test_coordinated_formula_sql_variants,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    explicit_fusion_sql_variants,
    tests::test_explicit_fusion_sql_variants,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    qdrant_raw_ordered_scroll_integer_contracts,
    tests::test_qdrant_raw_ordered_scroll_integer_contracts,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    qdrant_raw_ordered_scroll_float_contracts,
    tests::test_qdrant_raw_ordered_scroll_float_contracts,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    qdrant_raw_ordered_scroll_datetime_contracts,
    tests::test_qdrant_raw_ordered_scroll_datetime_contracts,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    qdrant_raw_ordered_scroll_rejects_integer_index_without_range,
    tests::test_qdrant_raw_ordered_scroll_rejects_integer_index_without_range,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    qdrant_raw_payload_null_contracts,
    tests::test_qdrant_raw_payload_null_contracts,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    qdrant_raw_payload_empty_contracts,
    tests::test_qdrant_raw_payload_empty_contracts,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(
    qdrant_raw_integer_facet_contracts,
    tests::test_qdrant_raw_integer_facet_contracts,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use datafusion::arrow::array::types::Float32Type;
    use datafusion::arrow::array::{
        Array, ArrayRef, BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int64Array,
        StringArray, StructArray, UInt64Array,
    };
    use datafusion::arrow::datatypes::DataType;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::catalog::memory::MemTable;
    use datafusion::prelude::*;
    use ndarrow::{
        csr_matrix_batch_iter, fixed_size_list_as_array2, fixed_size_list_as_array2_masked,
        variable_shape_tensor_iter,
    };
    use qdrant_client::Qdrant;
    use qdrant_client::qdrant::{
        Condition, CreateCollectionBuilder, CreateFieldIndexCollectionBuilder, Direction, Distance,
        FacetCountsBuilder, FieldType, Filter, FloatIndexParamsBuilder, GeoIndexParamsBuilder,
        MultiVectorComparator, MultiVectorConfig, NamedVectors, OrderByBuilder, PayloadSchemaType,
        PointStruct, RetrievedPoint, ScrollPointsBuilder, SetPayloadPointsBuilder,
        SparseVectorParamsBuilder, SparseVectorsConfigBuilder, TextIndexParamsBuilder,
        TokenizerType, UpsertPointsBuilder, Value, Vector, VectorParamsBuilder,
        VectorsConfigBuilder, facet_value, order_value, payload_index_params, point_id, start_from,
    };
    use qdrant_datafusion::arrow::schema::QdrantFieldBinding;
    use qdrant_datafusion::context::QdrantSessionContext;
    use qdrant_datafusion::error::Result;
    use qdrant_datafusion::table::QdrantTableProvider;
    use qdrant_datafusion::test_utils::QdrantContainer;

    use crate::sql_catalog::{SqlCase, supported as sql};

    const SUPPORTED_SCAN_PROJECTION_CASES: &[SqlCase] = &[
        sql::scan::projection::HINTED_PAYLOAD_ARITHMETIC,
        sql::scan::projection::RAW_PAYLOAD_ARITHMETIC,
        sql::scan::projection::SUBQUERY_RAW_PAYLOAD_ARITHMETIC,
        sql::scan::projection::CTE_RAW_PAYLOAD_ARITHMETIC,
        sql::scan::projection::UNION_ALL_RAW_PAYLOAD_ARITHMETIC,
        sql::scan::projection::HINTED_PAYLOAD_FUNCTION,
        sql::scan::projection::RAW_PAYLOAD_FUNCTION,
        sql::scan::projection::RAW_PAYLOAD_DIVISION,
        sql::scan::projection::CASE_HINTED_PAYLOAD,
        sql::scan::projection::COALESCE_HINTED_PAYLOAD,
        sql::scan::projection::RAW_PAYLOAD_CASE,
        sql::scan::projection::UNION_ALL_HINTED_PAYLOAD,
        sql::scan::projection::UNION_DISTINCT_HINTED_PAYLOAD,
        sql::scan::projection::INTERSECT_HINTED_PAYLOAD,
        sql::scan::projection::EXCEPT_HINTED_PAYLOAD,
        sql::scan::projection::WINDOW_OVER_TYPED_SUBQUERY,
        sql::scan::projection::SCALAR_SUBQUERY_TYPED,
        sql::scan::projection::SELF_JOIN_TYPED,
        sql::scan::projection::LEFT_JOIN_TYPED,
        sql::scan::projection::INNER_JOIN_USING,
        sql::scan::projection::RIGHT_JOIN_TYPED,
        sql::scan::projection::FULL_JOIN_TYPED,
        sql::scan::projection::CROSS_JOIN_TYPED,
        sql::scan::projection::LEFT_SEMI_JOIN_TYPED,
        sql::scan::projection::LEFT_ANTI_JOIN_TYPED,
        sql::scan::projection::RIGHT_SEMI_JOIN_TYPED,
        sql::scan::projection::RIGHT_ANTI_JOIN_TYPED,
        sql::scan::projection::SUBQUERY,
        sql::scan::projection::CTE,
        sql::scan::projection::UNNEST,
        sql::scan::projection::WINDOW,
    ];

    const SUPPORTED_SCAN_ORDERING_CASES: &[SqlCase] = &[
        sql::scan::ordering::HINTED_PAYLOAD_ARITHMETIC_DESC,
        sql::scan::ordering::HINTED_PAYLOAD_FUNCTION,
        sql::scan::ordering::CASE_HINTED_PAYLOAD,
        sql::scan::ordering::ORDINAL_TYPED,
        sql::scan::ordering::NULLS_LAST_TYPED,
        sql::scan::ordering::NULLS_FIRST_TYPED,
        sql::scan::ordering::MULTI_KEY_LOCAL,
        sql::scan::ordering::SUBQUERY,
        sql::scan::ordering::SUBQUERY_RAW_PAYLOAD_ARITHMETIC,
        sql::scan::ordering::CTE_DESC,
        sql::scan::ordering::CTE_RAW_PAYLOAD_ARITHMETIC,
        sql::scan::ordering::WINDOW_RAW_PAYLOAD_ARITHMETIC,
    ];

    const SUPPORTED_SCAN_FILTER_CASES: &[SqlCase] = &[
        sql::scan::filters::RAW_PAYLOAD_ARITHMETIC,
        sql::scan::filters::SUBQUERY_RAW_PAYLOAD_ARITHMETIC,
        sql::scan::filters::CTE_RAW_PAYLOAD_ARITHMETIC,
        sql::scan::filters::UNION_ALL_RAW_PAYLOAD_ARITHMETIC,
        sql::scan::filters::RAW_PAYLOAD_FUNCTION,
        sql::scan::filters::RAW_PAYLOAD_DIVISION,
        sql::scan::filters::HINTED_PAYLOAD_ARITHMETIC,
        sql::scan::filters::HINTED_PAYLOAD_FUNCTION,
        sql::scan::filters::CASE_HINTED_PAYLOAD,
        sql::scan::filters::IN_SUBQUERY,
        sql::scan::filters::EXISTS_CORRELATED,
        sql::scan::filters::SCALAR_SUBQUERY,
        sql::scan::filters::PAYLOAD_CAST_BETWEEN,
        sql::scan::filters::TEXT_MATCH_WRAPPED,
        sql::scan::filters::TEXT_MATCH_CASE,
        sql::scan::filters::PHRASE_MATCH_WRAPPED,
        sql::scan::filters::TEXT_ANY,
        sql::scan::filters::VALUES_COUNT_GE_ONE,
        sql::scan::filters::GEO_BBOX,
        sql::scan::filters::GEO_POLYGON,
        sql::scan::filters::SUBQUERY,
        sql::scan::filters::CTE,
        sql::scan::filters::TEXT_ANY_SUBQUERY,
        sql::scan::filters::GEO_BBOX_SUBQUERY,
        sql::scan::filters::GEO_POLYGON_SUBQUERY,
    ];

    const SUPPORTED_SCAN_AGGREGATE_CASES: &[SqlCase] = &[
        sql::scan::aggregates::AVG_HINTED_PAYLOAD,
        sql::scan::aggregates::RAW_PAYLOAD_ARITHMETIC,
        sql::scan::aggregates::SUBQUERY_RAW_PAYLOAD_ARITHMETIC,
        sql::scan::aggregates::CTE_RAW_PAYLOAD_ARITHMETIC,
        sql::scan::aggregates::SUM_HINTED_PAYLOAD_ARITHMETIC,
        sql::scan::aggregates::RAW_PAYLOAD_FUNCTION,
        sql::scan::aggregates::RAW_PAYLOAD_DIVISION,
        sql::scan::aggregates::CASE_HINTED_PAYLOAD,
        sql::scan::aggregates::HAVING_LOCAL_TYPED,
        sql::scan::aggregates::HAVING_FACET,
        sql::scan::aggregates::WINDOW_OVER_FACET_SUBQUERY,
        sql::scan::aggregates::SUBQUERY,
        sql::scan::aggregates::CTE,
    ];

    fn create_qdrant_client(c: &Arc<QdrantContainer>) -> Result<Qdrant> {
        Qdrant::from_url(&c.get_url()).api_key(c.get_api_key()).build().map_err(Into::into)
    }

    fn field_names(schema: &datafusion::arrow::datatypes::Schema) -> Vec<&str> {
        schema.fields().iter().map(|field| field.name().as_str()).collect()
    }

    fn assert_f32_eq(left: f32, right: f32) {
        assert!((left - right).abs() < 1.0e-6, "left={left}, right={right}");
    }

    fn assert_scored_rows_eq(left: &[(u64, f32)], right: &[(u64, f32)]) {
        assert_eq!(left.len(), right.len(), "left={left:?}, right={right:?}");
        for ((left_id, left_score), (right_id, right_score)) in left.iter().zip(right) {
            assert_eq!(left_id, right_id, "left={left:?}, right={right:?}");
            assert_f32_eq(*left_score, *right_score);
        }
    }

    async fn assert_supported_scan_catalog_case(
        ctx: &QdrantSessionContext,
        case: SqlCase,
    ) -> Result<()> {
        let (_batches, display) =
            assert_supported_query_collects(ctx, case.sql).await.map_err(|error| {
                datafusion::error::DataFusionError::Execution(format!(
                    "supported scan catalog case={} sql={} error={error}",
                    case.id, case.sql
                ))
            })?;
        assert!(
            display.contains("QdrantScanExec")
                || display.contains("QdrantCountExec")
                || display.contains("QdrantFacetExec")
                || display.contains("ProjectionExec")
                || display.contains("AggregateExec")
                || display.contains("SortExec")
                || display.contains("UnnestExec")
                || display.contains("WindowAggExec")
                || display.contains("CrossJoinExec")
                || display.contains("NestedLoopJoinExec"),
            "case={} sql={} display={display}",
            case.id,
            case.sql,
        );
        Ok(())
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
        create_payload_index(
            &client,
            collection_name,
            "rank",
            FieldType::Integer,
            qdrant_client::qdrant::IntegerIndexParamsBuilder::new(true, true).build(),
        )
        .await?;

        let mut payload1 = qdrant_client::Payload::new();
        payload1.insert("rank", 30_i64);
        let mut payload2 = qdrant_client::Payload::new();
        payload2.insert("rank", 20_i64);
        let mut payload3 = qdrant_client::Payload::new();
        payload3.insert("rank", 10_i64);
        let points = vec![
            PointStruct::new(
                1,
                NamedVectors::default()
                    .add_vector("embedding", vec![1.0, 0.0])
                    .add_vector("aux", vec![0.0, 1.0]),
                payload1,
            ),
            PointStruct::new(
                2,
                NamedVectors::default()
                    .add_vector("embedding", vec![0.4, 0.0])
                    .add_vector("aux", vec![0.0, 0.4]),
                payload2,
            ),
            PointStruct::new(
                3,
                NamedVectors::default()
                    .add_vector("embedding", vec![0.0, 1.0])
                    .add_vector("aux", vec![0.0, 0.2]),
                payload3,
            ),
        ];
        drop(client.upsert_points(UpsertPointsBuilder::new(collection_name, points)).await?);

        let table_provider = QdrantTableProvider::try_new(client.clone(), collection_name).await?;
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(ctx.session_context().register_table("vectors", Arc::new(table_provider))?);
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

    async fn create_grouped_nearest_query_context(
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
        let mut payload4 = qdrant_client::Payload::new();
        payload4.insert("tag", "green");

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
            PointStruct::new(
                4,
                NamedVectors::default().add_vector("embedding", vec![0.1, 0.0]),
                payload4,
            ),
        ];
        drop(client.upsert_points(UpsertPointsBuilder::new(collection_name, points)).await?);

        let table_provider = QdrantTableProvider::try_new(client.clone(), collection_name).await?;
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(ctx.session_context().register_table("vectors", Arc::new(table_provider))?);
        Ok(ctx)
    }

    async fn create_catalog_scan_context(
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
            "tag",
            FieldType::Keyword,
            qdrant_client::qdrant::KeywordIndexParamsBuilder::default().build(),
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
        create_payload_index(
            &client,
            collection_name,
            "location",
            FieldType::Geo,
            GeoIndexParamsBuilder::default().build(),
        )
        .await?;
        create_payload_index(
            &client,
            collection_name,
            "metadata.rank",
            FieldType::Integer,
            qdrant_client::qdrant::IntegerIndexParamsBuilder::new(true, true).build(),
        )
        .await?;
        create_payload_index(
            &client,
            collection_name,
            "metadata.tag",
            FieldType::Keyword,
            qdrant_client::qdrant::KeywordIndexParamsBuilder::default().build(),
        )
        .await?;

        let mut payload1 = qdrant_client::Payload::new();
        payload1.insert("rank", 30_i64);
        payload1.insert("tag", "red");
        payload1.insert("description", "good cheap coffee");
        payload1.insert("location", serde_json::json!({"lon": 0.0, "lat": 0.0}));
        payload1.insert(
            "metadata",
            serde_json::json!([
                {"rank": 30, "tag": "red"},
                {"rank": 5, "tag": "blue"}
            ]),
        );
        let mut payload2 = qdrant_client::Payload::new();
        payload2.insert("rank", 10_i64);
        payload2.insert("tag", "blue");
        payload2.insert("description", "time is a flat circle");
        payload2.insert("location", serde_json::json!({"lon": 0.0, "lat": 1.0}));
        payload2.insert(
            "metadata",
            serde_json::json!([
                {"rank": 10, "tag": "blue"}
            ]),
        );
        let mut payload3 = qdrant_client::Payload::new();
        payload3.insert("rank", 20_i64);
        payload3.insert("tag", "blue");
        payload3.insert("description", "good food nearby");
        payload3.insert("location", serde_json::json!({"lon": 0.0, "lat": 2.0}));
        payload3.insert(
            "metadata",
            serde_json::json!([
                {"rank": 20, "tag": "red"},
                {"rank": 20, "tag": "green"}
            ]),
        );

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

    async fn collect_scored_rows(
        ctx: &QdrantSessionContext,
        sql: &str,
    ) -> Result<(Vec<(u64, f32)>, String)> {
        let dataframe = ctx.sql(sql).await?;
        let plan = dataframe.clone().create_physical_plan().await?;
        let display =
            datafusion::physical_plan::displayable(plan.as_ref()).indent(true).to_string();
        let batches = dataframe.collect().await.map_err(|err| {
            datafusion::error::DataFusionError::Execution(format!(
                "failed to collect SQL `{sql}` with physical plan:\n{display}\nerror: {err}"
            ))
        })?;
        let rows = batches
            .iter()
            .flat_map(|batch| {
                let ids = batch
                    .column(batch.schema().index_of("id").expect("id column"))
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("id string array");
                let scores = batch
                    .column(batch.schema().index_of("score").expect("score column"))
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .expect("score float32 array");
                (0..batch.num_rows())
                    .map(|row| {
                        (ids.value(row).parse::<u64>().expect("numeric id"), scores.value(row))
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        Ok((rows, display))
    }

    async fn collect_grouped_scored_rows(
        ctx: &QdrantSessionContext,
        sql: &str,
    ) -> Result<(Vec<(String, u64, f32)>, String)> {
        let dataframe = ctx.sql(sql).await?;
        let plan = dataframe.clone().create_physical_plan().await?;
        let display =
            datafusion::physical_plan::displayable(plan.as_ref()).indent(true).to_string();
        let batches = dataframe.collect().await.map_err(|err| {
            datafusion::error::DataFusionError::Execution(format!(
                "failed to collect SQL `{sql}` with physical plan:
{display}
error: {err}"
            ))
        })?;
        let rows = batches
            .iter()
            .flat_map(|batch| {
                let ids = batch
                    .column(batch.schema().index_of("id").expect("id column"))
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("id string array");
                let tags = batch
                    .column(batch.schema().index_of("tag").expect("tag column"))
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("tag string array");
                let scores = batch
                    .column(batch.schema().index_of("score").expect("score column"))
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .expect("score float32 array");
                (0..batch.num_rows())
                    .map(|row| {
                        (
                            tags.value(row).to_owned(),
                            ids.value(row).parse::<u64>().expect("numeric id"),
                            scores.value(row),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        Ok((rows, display))
    }

    async fn collect_id_rows(ctx: &QdrantSessionContext, sql: &str) -> Result<(Vec<u64>, String)> {
        let dataframe = ctx.sql(sql).await?;
        let plan = dataframe.clone().create_physical_plan().await?;
        let display =
            datafusion::physical_plan::displayable(plan.as_ref()).indent(true).to_string();
        let batches = dataframe.collect().await.map_err(|err| {
            datafusion::error::DataFusionError::Execution(format!(
                "failed to collect SQL `{sql}` with physical plan:\n{display}\nerror: {err}"
            ))
        })?;
        let rows = batches
            .iter()
            .flat_map(|batch| {
                let ids = batch
                    .column(batch.schema().index_of("id").expect("id column"))
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("id string array");
                (0..batch.num_rows())
                    .map(|row| ids.value(row).parse::<u64>().expect("numeric id"))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        Ok((rows, display))
    }

    async fn assert_pushed_id_rows(
        ctx: &QdrantSessionContext,
        sql: &str,
        expected_ids: &[u64],
    ) -> Result<()> {
        let (actual_ids, display) = collect_id_rows(ctx, sql).await?;
        assert_eq!(actual_ids, expected_ids, "{display}");
        assert!(!display.contains("FilterExec"), "{display}");
        Ok(())
    }

    async fn collect_i64_rows(
        ctx: &QdrantSessionContext,
        sql: &str,
        column: &str,
    ) -> Result<(Vec<i64>, String)> {
        let dataframe = ctx.sql(sql).await?;
        let plan = dataframe.clone().create_physical_plan().await?;
        let display =
            datafusion::physical_plan::displayable(plan.as_ref()).indent(true).to_string();
        let batches = dataframe.collect().await.map_err(|err| {
            datafusion::error::DataFusionError::Execution(format!(
                "failed to collect SQL `{sql}` with physical plan:\n{display}\nerror: {err}"
            ))
        })?;
        let rows =
            batches.iter().flat_map(|batch| batch_i64_values(batch, column)).collect::<Vec<_>>();
        Ok((rows, display))
    }

    async fn assert_supported_query_collects(
        ctx: &QdrantSessionContext,
        sql: &str,
    ) -> Result<(Vec<RecordBatch>, String)> {
        let dataframe = ctx.sql(sql).await?;
        let plan = dataframe.clone().create_physical_plan().await?;
        let display =
            datafusion::physical_plan::displayable(plan.as_ref()).indent(true).to_string();
        let batches = dataframe.collect().await.map_err(|err| {
            datafusion::error::DataFusionError::Execution(format!(
                "failed to collect supported SQL `{sql}` with physical plan:\n{display}\nerror: \
                 {err}"
            ))
        })?;
        Ok((batches, display))
    }

    fn batch_u64_ids(batch: &RecordBatch, column: &str) -> Vec<u64> {
        batch
            .column(batch.schema().index_of(column).expect("id column"))
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("id string array")
            .iter()
            .map(|value| value.expect("non-null id").parse::<u64>().expect("numeric id"))
            .collect()
    }

    fn batch_i64_values(batch: &RecordBatch, column: &str) -> Vec<i64> {
        batch
            .column(batch.schema().index_of(column).expect("int64 column"))
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 array")
            .iter()
            .map(|value| value.expect("non-null int64 value"))
            .collect()
    }

    fn batch_u64_values(batch: &RecordBatch, column: &str) -> Vec<u64> {
        batch
            .column(batch.schema().index_of(column).expect("uint64 column"))
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("uint64 array")
            .iter()
            .map(|value| value.expect("non-null uint64 value"))
            .collect()
    }

    fn batch_optional_i64_values(batch: &RecordBatch, column: &str) -> Vec<Option<i64>> {
        batch
            .column(batch.schema().index_of(column).expect("int64 column"))
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 array")
            .iter()
            .collect()
    }

    fn batch_f64_values(batch: &RecordBatch, column: &str) -> Vec<f64> {
        batch
            .column(batch.schema().index_of(column).expect("float64 column"))
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("float64 array")
            .iter()
            .map(|value| value.expect("non-null float64 value"))
            .collect()
    }

    fn batch_stringified_scalar_values(batch: &RecordBatch, column: &str) -> Vec<String> {
        let column = batch.column(batch.schema().index_of(column).expect("scalar column"));
        match column.data_type() {
            DataType::Int64 => column
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 array")
                .iter()
                .map(|value| value.expect("non-null int64 value").to_string())
                .collect(),
            DataType::Utf8 => column
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("string array")
                .iter()
                .map(|value| value.expect("non-null string value").to_owned())
                .collect(),
            other => panic!("unexpected scalar column type: {other:?}"),
        }
    }

    fn batch_bool_values(batch: &RecordBatch, column: &str) -> Vec<bool> {
        batch
            .column(batch.schema().index_of(column).expect("bool column"))
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("bool array")
            .iter()
            .map(|value| value.expect("non-null bool value"))
            .collect()
    }

    fn assert_typed_payload_projection_batch(batch: &RecordBatch) {
        let ids = batch_u64_ids(batch, "id");
        let ranks = batch_i64_values(batch, "rank");
        let active = batch_bool_values(batch, "active");
        let tags = batch_string_values(batch, "tag");
        let next_ranks = batch_i64_values(batch, "next_rank");
        let hinted_ranks = batch_i64_values(batch, "hinted_rank");
        let hinted_next_ranks = batch_i64_values(batch, "hinted_next_rank");

        assert_eq!(ids, vec![1, 2]);
        assert_eq!(ranks, vec![10, 20]);
        assert_eq!(active, vec![true, false]);
        assert_eq!(tags, vec!["red".to_owned(), "blue".to_owned()]);
        assert_eq!(next_ranks, vec![11, 21]);
        assert_eq!(hinted_ranks, vec![10, 20]);
        assert_eq!(hinted_next_ranks, vec![11, 21]);
    }

    fn batch_string_values(batch: &RecordBatch, column: &str) -> Vec<String> {
        batch
            .column(batch.schema().index_of(column).expect("string column"))
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string array")
            .iter()
            .map(|value| value.expect("non-null string value").to_owned())
            .collect()
    }

    async fn create_scalar_collection(client: &Qdrant, collection_name: &str) -> Result<()> {
        create_scalar_collection_with_shards(client, collection_name, 1).await
    }

    async fn create_scalar_collection_with_shards(
        client: &Qdrant,
        collection_name: &str,
        shard_number: u32,
    ) -> Result<()> {
        let _ = client
            .create_collection(
                CreateCollectionBuilder::new(collection_name)
                    .vectors_config(VectorParamsBuilder::new(1, Distance::Dot))
                    .shard_number(shard_number),
            )
            .await?;
        Ok(())
    }

    async fn assert_single_peer_ordered_scroll_contract(
        client: &Qdrant,
        collection_name: &str,
        minimum_local_shards: usize,
    ) -> Result<()> {
        let info = client.collection_cluster_info(collection_name).await?;
        assert!(info.remote_shards.is_empty(), "{info:?}");
        assert!(info.shard_transfers.is_empty(), "{info:?}");
        assert!(info.resharding_operations.is_empty(), "{info:?}");
        assert!(info.local_shards.len() >= minimum_local_shards, "{info:?}");
        Ok(())
    }

    fn dense_insert_vector_array(values: &[f32]) -> ArrayRef {
        Arc::new(FixedSizeListArray::new(
            Arc::new(datafusion::arrow::datatypes::Field::new("item", DataType::Float32, false)),
            1,
            Arc::new(Float32Array::from(values.to_vec())),
            None,
        ))
    }

    fn dense_insert_batch() -> RecordBatch {
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new("id", DataType::Utf8, false),
            datafusion::arrow::datatypes::Field::new("payload", DataType::Utf8, true),
            datafusion::arrow::datatypes::Field::new(
                "vector",
                DataType::new_fixed_size_list(DataType::Float32, 1, false),
                true,
            ),
        ]));
        RecordBatch::try_new(schema, vec![
            Arc::new(StringArray::from(vec!["1", "2"])),
            Arc::new(StringArray::from(vec![Some(r#"{"rank":10}"#), Some(r#"{"rank":20}"#)])),
            dense_insert_vector_array(&[0.1_f32, 0.9_f32]),
        ])
        .expect("dense insert batch")
    }

    async fn create_write_context(
        c: &Arc<QdrantContainer>,
        collection_name: &str,
        existing_points: Vec<PointStruct>,
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
        if !existing_points.is_empty() {
            drop(
                client
                    .upsert_points(UpsertPointsBuilder::new(collection_name, existing_points))
                    .await?,
            );
        }

        let table_provider = QdrantTableProvider::try_new(client.clone(), collection_name).await?;
        let staging =
            MemTable::try_new(dense_insert_batch().schema(), vec![vec![dense_insert_batch()]])?;
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(ctx.session_context().register_table("vectors", Arc::new(table_provider))?);
        drop(ctx.session_context().register_table("staging", Arc::new(staging))?);
        Ok(ctx)
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

    fn scalar_point(id: u64, field_name: &str, value: impl Into<Value>) -> PointStruct {
        let mut payload = qdrant_client::Payload::new();
        payload.insert(field_name, value);
        PointStruct::new(id, Vector::new_dense(vec![0.0]), payload)
    }

    fn point_num(point: &RetrievedPoint) -> u64 {
        match point.id.as_ref().and_then(|id| id.point_id_options.as_ref()) {
            Some(point_id::PointIdOptions::Num(id)) => *id,
            _ => panic!("expected numeric point id"),
        }
    }

    fn int_order_value(point: &RetrievedPoint) -> i64 {
        match point.order_value.as_ref().and_then(|value| value.variant.as_ref()) {
            Some(order_value::Variant::Int(value)) => *value,
            _ => panic!("expected integer order value"),
        }
    }

    fn float_order_value(point: &RetrievedPoint) -> f64 {
        match point.order_value.as_ref().and_then(|value| value.variant.as_ref()) {
            Some(order_value::Variant::Float(value)) => *value,
            _ => panic!("expected float order value"),
        }
    }

    async fn scroll_ordered_page(
        client: &Qdrant,
        collection_name: &str,
        field_name: &str,
        direction: Direction,
        start_from: Option<start_from::Value>,
        exclude_ids: &[u64],
        limit: u32,
    ) -> Result<qdrant_client::qdrant::ScrollResponse> {
        let order_by = match start_from {
            Some(start_from) => {
                OrderByBuilder::new(field_name).direction(direction as i32).start_from(start_from)
            }
            None => OrderByBuilder::new(field_name).direction(direction as i32),
        };
        let mut request = ScrollPointsBuilder::new(collection_name)
            .limit(limit)
            .with_payload(true)
            .with_vectors(false)
            .order_by(order_by);
        if !exclude_ids.is_empty() {
            request =
                request.filter(Filter::must_not([Condition::has_id(exclude_ids.iter().copied())]));
        }
        client.scroll(request).await.map_err(Into::into)
    }

    async fn collect_ordered_pages<T, EXTRACT, START>(
        client: &Qdrant,
        collection_name: &str,
        field_name: &str,
        direction: Direction,
        limit: u32,
        mut extract: EXTRACT,
        mut next_start_from: START,
    ) -> Result<Vec<(u64, T)>>
    where
        T: Clone + PartialEq,
        EXTRACT: FnMut(&RetrievedPoint) -> T,
        START: FnMut(T) -> start_from::Value,
    {
        let mut start_from = None;
        let mut boundary_value = None;
        let mut boundary_ids = BTreeSet::new();
        let mut collected = vec![];

        for _ in 0..16 {
            let response = scroll_ordered_page(
                client,
                collection_name,
                field_name,
                direction,
                start_from.clone(),
                &boundary_ids.iter().copied().collect::<Vec<_>>(),
                limit,
            )
            .await?;
            assert!(response.next_page_offset.is_none());
            if response.result.is_empty() {
                return Ok(collected);
            }

            let page = response
                .result
                .iter()
                .map(|point| (point_num(point), extract(point)))
                .collect::<Vec<_>>();
            let last_value = page.last().expect("ordered page").1.clone();
            let page_boundary_ids = page
                .iter()
                .filter(|(_, value)| *value == last_value)
                .map(|(id, _)| *id)
                .collect::<BTreeSet<_>>();
            if boundary_value.as_ref() == Some(&last_value) {
                boundary_ids.extend(page_boundary_ids);
            } else {
                boundary_value = Some(last_value.clone());
                boundary_ids = page_boundary_ids;
            }
            start_from = Some(next_start_from(last_value));
            collected.extend(page);
        }

        panic!("ordered scroll did not terminate");
    }

    #[expect(clippy::too_many_lines)]
    pub(super) async fn test_table_provider_named(c: Arc<QdrantContainer>) -> Result<()> {
        let client = create_qdrant_client(&c)?;
        let collection_name = "test_named_canonical";

        let mut vectors_config = VectorsConfigBuilder::default();
        let _ = vectors_config.add_named_vector_params(
            "text_embedding",
            VectorParamsBuilder::new(3, Distance::Dot).build(),
        );
        let _ = vectors_config.add_named_vector_params(
            "multi_embedding",
            VectorParamsBuilder::new(2, Distance::Dot)
                .multivector_config(MultiVectorConfig {
                    comparator: MultiVectorComparator::MaxSim.into(),
                })
                .build(),
        );

        let mut sparse_config = SparseVectorsConfigBuilder::default();
        let _ =
            sparse_config.add_named_vector_params("keywords", SparseVectorParamsBuilder::default());

        let _ = client
            .create_collection(
                CreateCollectionBuilder::new(collection_name)
                    .vectors_config(vectors_config)
                    .sparse_vectors_config(sparse_config),
            )
            .await?;

        let mut payload1 = qdrant_client::Payload::new();
        payload1.insert("title", "Point 1");
        let mut payload2 = qdrant_client::Payload::new();
        payload2.insert("title", "Point 2");
        let mut payload3 = qdrant_client::Payload::new();
        payload3.insert("title", "Point 3");

        let mut vectors1 = NamedVectors::default();
        vectors1 = vectors1.add_vector("text_embedding", Vector::new_dense(vec![0.1, 0.2, 0.3]));
        vectors1 = vectors1
            .add_vector("multi_embedding", Vector::new_multi(vec![vec![1.0, 2.0], vec![3.0, 4.0]]));
        vectors1 = vectors1.add_vector("keywords", Vector::new_sparse(vec![0, 5], vec![0.5, 1.5]));

        let mut vectors2 = NamedVectors::default();
        vectors2 = vectors2.add_vector("text_embedding", Vector::new_dense(vec![0.4, 0.5, 0.6]));

        let mut vectors3 = NamedVectors::default();
        vectors3 = vectors3.add_vector("multi_embedding", Vector::new_multi(vec![vec![7.0, 8.0]]));
        vectors3 = vectors3.add_vector("keywords", Vector::new_sparse(vec![2], vec![0.9]));

        let points = vec![
            PointStruct::new(1, vectors1, payload1),
            PointStruct::new(2, vectors2, payload2),
            PointStruct::new(3, vectors3, payload3),
        ];
        drop(client.upsert_points(UpsertPointsBuilder::new(collection_name, points)).await?);

        let table_provider = QdrantTableProvider::try_new(client.clone(), collection_name).await?;
        let ctx = SessionContext::new();
        drop(ctx.register_table("docs", Arc::new(table_provider))?);

        let batches = ctx.sql(sql::scan::projection::DOCS_FULL.sql).await?.collect().await?;
        let batch = batches.into_iter().next().expect("single batch");
        let schema = batch.schema();

        assert_eq!(batch.num_rows(), 3);
        assert_eq!(field_names(schema.as_ref()), vec![
            "id",
            "payload",
            "text_embedding",
            "multi_embedding",
            "keywords"
        ],);

        let payload = batch
            .column(schema.index_of("payload").expect("payload index"))
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("payload string array");
        assert_eq!(payload.null_count(), 0);

        let dense_field = schema.field_with_name("text_embedding").expect("dense field present");
        assert!(dense_field.is_nullable());
        assert_eq!(QdrantFieldBinding::from_field(dense_field).dense_vector_width(), Some(3));
        let dense_array = batch
            .column(schema.index_of("text_embedding").expect("dense index"))
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .expect("dense vector array");
        let (dense_view, dense_nulls) =
            fixed_size_list_as_array2_masked::<Float32Type>(dense_array)
                .expect("dense masked view");
        let dense_nulls = dense_nulls.expect("dense null buffer");
        assert_eq!(dense_array.null_count(), 1);
        assert_eq!(dense_view.shape(), &[3, 3]);
        assert!(dense_nulls.is_valid(0));
        assert!(dense_nulls.is_valid(1));
        assert!(dense_nulls.is_null(2));
        assert_f32_eq(dense_view[[0, 0]], 0.1);
        assert_f32_eq(dense_view[[1, 2]], 0.6);

        let multi_field =
            schema.field_with_name("multi_embedding").expect("multivector field present");
        assert!(multi_field.is_nullable());
        assert_eq!(QdrantFieldBinding::from_field(multi_field).multivector_width(), Some(2));
        let multi_array = batch
            .column(schema.index_of("multi_embedding").expect("multivector index"))
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("multivector struct array");
        assert_eq!(multi_array.null_count(), 1);
        assert!(!multi_array.is_null(0));
        assert!(multi_array.is_null(1));
        assert!(!multi_array.is_null(2));

        let sparse_field = schema.field_with_name("keywords").expect("sparse field present");
        assert!(sparse_field.is_nullable());
        assert_eq!(QdrantFieldBinding::from_field(sparse_field), QdrantFieldBinding::Sparse);
        let sparse_array = batch
            .column(schema.index_of("keywords").expect("sparse index"))
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("sparse struct array");
        assert_eq!(sparse_array.null_count(), 1);
        assert!(!sparse_array.is_null(0));
        assert!(sparse_array.is_null(1));
        assert!(!sparse_array.is_null(2));

        let dense_batches =
            ctx.sql(sql::scan::projection::DOCS_TEXT_NONNULL.sql).await?.collect().await?;
        let dense_batch = dense_batches.into_iter().next().expect("dense batch");
        let dense_array = dense_batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .expect("filtered dense array");
        let dense_view =
            fixed_size_list_as_array2::<Float32Type>(dense_array).expect("dense ndarray view");
        assert_eq!(dense_view.shape(), &[2, 3]);
        assert_f32_eq(dense_view[[0, 0]], 0.1);
        assert_f32_eq(dense_view[[1, 2]], 0.6);

        let multi_batches =
            ctx.sql(sql::scan::projection::DOCS_MULTI_NONNULL.sql).await?.collect().await?;
        let multi_batch = multi_batches.into_iter().next().expect("multivector batch");
        let multi_field = multi_batch.schema().field(0).clone();
        let multi_array = multi_batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("filtered multivector array");
        let multi_rows = variable_shape_tensor_iter::<Float32Type>(&multi_field, multi_array)
            .expect("multivector iterator")
            .collect::<std::result::Result<Vec<_>, _>>()
            .expect("valid multivector rows");
        assert_eq!(multi_rows[0].1.shape(), &[2, 2]);
        assert_f32_eq(multi_rows[0].1[[1, 1]], 4.0);
        assert_eq!(multi_rows[1].1.shape(), &[1, 2]);
        assert_f32_eq(multi_rows[1].1[[0, 1]], 8.0);

        let sparse_batches =
            ctx.sql(sql::scan::projection::DOCS_KEYWORDS_NONNULL.sql).await?.collect().await?;
        let sparse_batch = sparse_batches.into_iter().next().expect("sparse batch");
        let sparse_field = sparse_batch.schema().field(0).clone();
        let sparse_array = sparse_batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("filtered sparse array");
        let sparse_rows = csr_matrix_batch_iter::<Float32Type>(&sparse_field, sparse_array)
            .expect("sparse iterator")
            .collect::<std::result::Result<Vec<_>, _>>()
            .expect("valid sparse rows");
        assert_eq!(sparse_rows[0].1.nrows, 1);
        assert_eq!(sparse_rows[0].1.ncols, 6);
        assert_eq!(sparse_rows[0].1.col_indices, &[0, 5]);
        assert_eq!(sparse_rows[0].1.values, &[0.5, 1.5]);
        assert_eq!(sparse_rows[1].1.nrows, 1);
        assert_eq!(sparse_rows[1].1.ncols, 3);
        assert_eq!(sparse_rows[1].1.col_indices, &[2]);
        assert_eq!(sparse_rows[1].1.values, &[0.9]);

        Ok(())
    }

    pub(super) async fn test_table_provider_scrolls_full_scan(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let client = create_qdrant_client(&c)?;
        let collection_name = "test_scroll_full_scan";

        let _ = client
            .create_collection(
                CreateCollectionBuilder::new(collection_name)
                    .vectors_config(VectorParamsBuilder::new(3, Distance::Dot)),
            )
            .await?;

        let points = (1_u16..=12)
            .map(|id| {
                PointStruct::new(
                    u64::from(id),
                    Vector::new_dense(vec![
                        f32::from(id),
                        f32::from(id) + 0.5,
                        f32::from(id) + 1.0,
                    ]),
                    qdrant_client::Payload::new(),
                )
            })
            .collect::<Vec<_>>();
        drop(client.upsert_points(UpsertPointsBuilder::new(collection_name, points)).await?);

        let table_provider = QdrantTableProvider::try_new(client.clone(), collection_name).await?;
        let ctx = SessionContext::new();
        drop(ctx.register_table("vectors", Arc::new(table_provider))?);

        let batches = ctx.sql(sql::scan::projection::VECTORS_ALL_IDS.sql).await?.collect().await?;
        let mut ids = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("id string array")
                    .iter()
                    .map(|value| value.expect("non-null id").parse::<u64>().expect("numeric id"))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        ids.sort_unstable();

        assert_eq!(ids, (1_u64..=12).collect::<Vec<_>>());

        Ok(())
    }

    pub(super) async fn test_table_provider_orders_by_payload_field(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let client = create_qdrant_client(&c)?;
        let collection_name = "test_order_by_payload_field";
        create_scalar_collection(&client, collection_name).await?;
        create_payload_index(
            &client,
            collection_name,
            "rank",
            FieldType::Integer,
            qdrant_client::qdrant::IntegerIndexParamsBuilder::new(true, true).build(),
        )
        .await?;
        let points = vec![
            scalar_point(1, "rank", 30_i64),
            scalar_point(2, "rank", 10_i64),
            scalar_point(3, "rank", 20_i64),
        ];
        drop(client.upsert_points(UpsertPointsBuilder::new(collection_name, points)).await?);

        let table_provider = QdrantTableProvider::try_new(client.clone(), collection_name).await?;
        let ctx = SessionContext::new();
        drop(ctx.register_table("vectors", Arc::new(table_provider))?);

        let batches = ctx.sql(sql::scan::ordering::PAYLOAD.sql).await?.collect().await?;
        let ids = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("id string array")
                    .iter()
                    .map(|value| value.expect("non-null id").parse::<u64>().expect("numeric id"))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        assert_eq!(ids, vec![2, 3, 1]);

        Ok(())
    }

    pub(super) async fn test_table_provider_orders_by_aliased_payload_field(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let client = create_qdrant_client(&c)?;
        let collection_name = "test_order_by_aliased_payload_field";
        create_scalar_collection(&client, collection_name).await?;
        create_payload_index(
            &client,
            collection_name,
            "rank",
            FieldType::Integer,
            qdrant_client::qdrant::IntegerIndexParamsBuilder::new(true, true).build(),
        )
        .await?;
        let points = vec![
            scalar_point(1, "rank", 30_i64),
            scalar_point(2, "rank", 10_i64),
            scalar_point(3, "rank", 20_i64),
        ];
        drop(client.upsert_points(UpsertPointsBuilder::new(collection_name, points)).await?);

        let table_provider = QdrantTableProvider::try_new(client.clone(), collection_name).await?;
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(ctx.session_context().register_table("vectors", Arc::new(table_provider))?);

        let (ids, display) =
            collect_id_rows(&ctx, sql::scan::ordering::ALIASED_PAYLOAD.sql).await?;

        assert_eq!(ids, vec![2, 3, 1]);
        assert!(display.contains("QdrantScanExec"), "{display}");
        assert!(!display.contains("SortExec"), "{display}");

        Ok(())
    }

    pub(super) async fn test_table_provider_insert_into_appends_rows(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let collection_name = "test_insert_into_appends_rows";
        let ctx = create_write_context(&c, collection_name, vec![]).await?;

        let insert_batches =
            ctx.sql(sql::writes::append::INSERT_SELECT.sql).await?.collect().await?;
        let insert_batch = insert_batches.into_iter().next().expect("insert result batch");
        let inserted = insert_batch
            .column(insert_batch.schema().index_of("count").expect("count column"))
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("count array");
        assert_eq!(inserted.value(0), 2);

        let ranks = ctx.sql(sql::scan::projection::INSERT_VERIFY.sql).await?.collect().await?;
        let rank_batch = ranks.into_iter().next().expect("rank batch");
        assert_eq!(batch_u64_ids(&rank_batch, "id"), vec![1, 2]);
        assert_eq!(batch_i64_values(&rank_batch, "rank"), vec![10, 20]);

        let nearest_sql = "SELECT id, qdrant_nearest_score(vector, 1.0) AS score FROM vectors \
                           ORDER BY score DESC LIMIT 2";
        let (ids, display) = collect_id_rows(&ctx, nearest_sql).await?;
        assert_eq!(ids, vec![2, 1], "{display}");
        assert!(display.contains("QdrantQueryExec"), "{display}");

        Ok(())
    }

    pub(super) async fn test_table_provider_insert_into_preserves_existing_ids(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let collection_name = "test_insert_into_preserves_existing_ids";
        let mut existing_payload = qdrant_client::Payload::new();
        existing_payload.insert("rank", 99_i64);
        let ctx = create_write_context(&c, collection_name, vec![PointStruct::new(
            1,
            Vector::new_dense(vec![0.2]),
            existing_payload,
        )])
        .await?;

        let _batches = ctx.sql(sql::writes::append::INSERT_SELECT.sql).await?.collect().await?;

        let ranks = ctx.sql(sql::scan::projection::INSERT_VERIFY.sql).await?.collect().await?;
        let rank_batch = ranks.into_iter().next().expect("rank batch");
        assert_eq!(batch_u64_ids(&rank_batch, "id"), vec![1, 2]);
        assert_eq!(batch_i64_values(&rank_batch, "rank"), vec![99, 20]);

        Ok(())
    }

    pub(super) async fn test_table_provider_replace_into_overwrites_existing_ids(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let collection_name = "test_replace_into_overwrites_existing_ids";
        let mut existing_payload = qdrant_client::Payload::new();
        existing_payload.insert("rank", 99_i64);
        let ctx = create_write_context(&c, collection_name, vec![PointStruct::new(
            1,
            Vector::new_dense(vec![0.2]),
            existing_payload,
        )])
        .await?;

        let _batches = ctx.sql(sql::writes::replace::INSERT_SELECT.sql).await?.collect().await?;

        let ranks = ctx.sql(sql::scan::projection::INSERT_VERIFY.sql).await?.collect().await?;
        let rank_batch = ranks.into_iter().next().expect("rank batch");
        assert_eq!(batch_u64_ids(&rank_batch, "id"), vec![1, 2]);
        assert_eq!(batch_i64_values(&rank_batch, "rank"), vec![10, 20]);

        Ok(())
    }

    pub(super) async fn test_table_provider_delete_from_removes_matching_rows(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let ctx = create_catalog_scan_context(&c, "test_delete_from_removes_matching_rows").await?;

        let delete_batches =
            ctx.sql(sql::writes::delete::PAYLOAD_RANGE.sql).await?.collect().await?;
        let delete_batch = delete_batches.into_iter().next().expect("delete result batch");
        assert_eq!(batch_u64_values(&delete_batch, "count"), vec![2]);

        let remaining = ctx.sql(sql::scan::projection::INSERT_VERIFY.sql).await?.collect().await?;
        let remaining_batch = remaining.into_iter().next().expect("remaining batch");
        assert_eq!(batch_u64_ids(&remaining_batch, "id"), vec![2]);
        assert_eq!(batch_i64_values(&remaining_batch, "rank"), vec![10]);

        Ok(())
    }

    pub(super) async fn test_table_provider_supported_scan_catalog_queries(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let ctx = create_catalog_scan_context(&c, "test_supported_scan_catalog_queries").await?;
        for cases in [
            SUPPORTED_SCAN_PROJECTION_CASES,
            SUPPORTED_SCAN_ORDERING_CASES,
            SUPPORTED_SCAN_FILTER_CASES,
            SUPPORTED_SCAN_AGGREGATE_CASES,
        ] {
            for &case in cases {
                assert_supported_scan_catalog_case(&ctx, case).await?;
            }
        }

        Ok(())
    }

    async fn assert_supported_append_write_case(
        c: &Arc<QdrantContainer>,
        case: SqlCase,
    ) -> Result<()> {
        let collection_name = format!("test_supported_write_append_{}", case.id.replace('.', "_"));
        let ctx = create_write_context(c, &collection_name, vec![]).await?;
        let (_batches, display) =
            assert_supported_query_collects(&ctx, case.sql).await.map_err(|error| {
                datafusion::error::DataFusionError::Execution(format!(
                    "supported write catalog case={} sql={} error={error}",
                    case.id, case.sql
                ))
            })?;
        assert!(display.contains("DataSinkExec"), "case={} display={display}", case.id);

        let ranks = ctx.sql(sql::scan::projection::INSERT_VERIFY.sql).await?.collect().await?;
        let rank_batch = ranks.into_iter().next().expect("rank batch");
        let expected_ids =
            if case.id == sql::writes::append::RIGHT_SEMI_JOIN.id { vec![1] } else { vec![1, 2] };
        assert_eq!(batch_u64_ids(&rank_batch, "id"), expected_ids, "case={}", case.id);
        let expected_ranks = if case.id == sql::writes::append::TARGET_COLUMNS_OMIT_PAYLOAD.id {
            vec![None, None]
        } else if case.id == sql::writes::append::RIGHT_SEMI_JOIN.id {
            vec![Some(10)]
        } else {
            vec![Some(10), Some(20)]
        };
        assert_eq!(
            batch_optional_i64_values(&rank_batch, "rank"),
            expected_ranks,
            "case={}",
            case.id
        );

        Ok(())
    }

    async fn assert_supported_overwrite_write_case(
        c: &Arc<QdrantContainer>,
        case: SqlCase,
    ) -> Result<()> {
        let collection_name =
            format!("test_supported_write_overwrite_{}", case.id.replace('.', "_"));
        let mut existing_payload = qdrant_client::Payload::new();
        existing_payload.insert("rank", 99_i64);
        let ctx = create_write_context(c, &collection_name, vec![PointStruct::new(
            9,
            Vector::new_dense(vec![0.2]),
            existing_payload,
        )])
        .await?;
        let (_batches, display) =
            assert_supported_query_collects(&ctx, case.sql).await.map_err(|error| {
                datafusion::error::DataFusionError::Execution(format!(
                    "supported overwrite catalog case={} sql={} error={error}",
                    case.id, case.sql
                ))
            })?;
        assert!(display.contains("DataSinkExec"), "case={} display={display}", case.id);

        let ranks = ctx.sql(sql::scan::projection::INSERT_VERIFY.sql).await?.collect().await?;
        let rank_batch = ranks.into_iter().next().expect("rank batch");
        assert_eq!(batch_u64_ids(&rank_batch, "id"), vec![1, 2], "case={}", case.id);
        let expected_ranks = if case.id == sql::writes::overwrite::TARGET_COLUMNS_OMIT_PAYLOAD.id {
            vec![None, None]
        } else {
            vec![Some(10), Some(20)]
        };
        assert_eq!(
            batch_optional_i64_values(&rank_batch, "rank"),
            expected_ranks,
            "case={}",
            case.id
        );

        Ok(())
    }

    async fn assert_supported_replace_write_case(
        c: &Arc<QdrantContainer>,
        case: SqlCase,
    ) -> Result<()> {
        let collection_name = format!("test_supported_write_replace_{}", case.id.replace('.', "_"));
        let mut existing_payload = qdrant_client::Payload::new();
        existing_payload.insert("rank", 99_i64);
        let ctx = create_write_context(c, &collection_name, vec![PointStruct::new(
            1,
            Vector::new_dense(vec![0.2]),
            existing_payload,
        )])
        .await?;
        let (_batches, display) =
            assert_supported_query_collects(&ctx, case.sql).await.map_err(|error| {
                datafusion::error::DataFusionError::Execution(format!(
                    "supported replace catalog case={} sql={} error={error}",
                    case.id, case.sql
                ))
            })?;
        assert!(display.contains("DataSinkExec"), "case={} display={display}", case.id);

        let ranks = ctx.sql(sql::scan::projection::INSERT_VERIFY.sql).await?.collect().await?;
        let rank_batch = ranks.into_iter().next().expect("rank batch");
        let expected_ids =
            if case.id == sql::writes::replace::RIGHT_SEMI_JOIN.id { vec![1] } else { vec![1, 2] };
        assert_eq!(batch_u64_ids(&rank_batch, "id"), expected_ids, "case={}", case.id);
        let expected_ranks = if case.id == sql::writes::replace::TARGET_COLUMNS_OMIT_PAYLOAD.id {
            vec![None, None]
        } else if case.id == sql::writes::replace::RIGHT_SEMI_JOIN.id {
            vec![Some(10)]
        } else {
            vec![Some(10), Some(20)]
        };
        assert_eq!(
            batch_optional_i64_values(&rank_batch, "rank"),
            expected_ranks,
            "case={}",
            case.id
        );

        Ok(())
    }

    async fn assert_supported_delete_write_case(
        c: &Arc<QdrantContainer>,
        case: SqlCase,
    ) -> Result<()> {
        let collection_name = format!("test_supported_write_delete_{}", case.id.replace('.', "_"));
        let ctx = create_catalog_scan_context(c, &collection_name).await?;
        let (batches, display) =
            assert_supported_query_collects(&ctx, case.sql).await.map_err(|error| {
                datafusion::error::DataFusionError::Execution(format!(
                    "supported delete catalog case={} sql={} error={error}",
                    case.id, case.sql
                ))
            })?;
        assert!(display.contains("QdrantDeleteExec"), "case={} display={display}", case.id);

        let delete_batch = batches.into_iter().next().expect("delete result batch");
        let remaining = ctx.sql(sql::scan::projection::INSERT_VERIFY.sql).await?.collect().await?;
        let remaining_ids =
            remaining.into_iter().next().map_or_else(Vec::new, |batch| batch_u64_ids(&batch, "id"));

        let (expected_count, expected_remaining_ids) = match case.id {
            id if id == sql::writes::delete::DELETE_ALL.id => (3, vec![]),
            id if id == sql::writes::delete::ID_EQ.id => (1, vec![2, 3]),
            id if id == sql::writes::delete::PAYLOAD_RANGE.id => (2, vec![2]),
            id if id == sql::writes::delete::TAG_IN.id => (2, vec![1]),
            id if id == sql::writes::delete::TEXT_MATCH.id => (2, vec![2]),
            id if id == sql::writes::delete::GEO_BBOX.id => (1, vec![2, 3]),
            id if id == sql::writes::delete::NESTED_MATCH.id => (1, vec![1, 2]),
            id if id == sql::writes::delete::VALUES_COUNT.id => (2, vec![2]),
            id if id == sql::writes::delete::NO_MATCH.id => (0, vec![1, 2, 3]),
            _ => panic!("unexpected delete catalog case {}", case.id),
        };

        assert_eq!(
            batch_u64_values(&delete_batch, "count"),
            vec![expected_count],
            "case={}",
            case.id
        );
        assert_eq!(remaining_ids, expected_remaining_ids, "case={}", case.id);

        Ok(())
    }

    pub(super) async fn test_table_provider_supported_write_catalog_queries(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        for case in sql::writes::append::ALL {
            assert_supported_append_write_case(&c, *case).await?;
        }

        for case in sql::writes::overwrite::ALL {
            assert_supported_overwrite_write_case(&c, *case).await?;
        }

        for case in sql::writes::replace::ALL {
            assert_supported_replace_write_case(&c, *case).await?;
        }

        for case in sql::writes::delete::ALL {
            assert_supported_delete_write_case(&c, *case).await?;
        }

        Ok(())
    }

    pub(super) async fn test_table_provider_projects_typed_payload_fields(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let client = create_qdrant_client(&c)?;
        let collection_name = "test_projects_typed_payload_fields";
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
            "active",
            FieldType::Bool,
            qdrant_client::qdrant::BoolIndexParamsBuilder::default().build(),
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
        payload1.insert("rank", 10_i64);
        payload1.insert("active", true);
        payload1.insert("tag", "red");
        let mut payload2 = qdrant_client::Payload::new();
        payload2.insert("rank", 20_i64);
        payload2.insert("active", false);
        payload2.insert("tag", "blue");

        let points = vec![
            PointStruct::new(1, Vector::new_dense(vec![0.0]), payload1),
            PointStruct::new(2, Vector::new_dense(vec![0.0]), payload2),
        ];
        drop(client.upsert_points(UpsertPointsBuilder::new(collection_name, points)).await?);

        let table_provider = QdrantTableProvider::try_new(client.clone(), collection_name).await?;
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(ctx.session_context().register_table("vectors", Arc::new(table_provider))?);

        let dataframe = ctx.sql(sql::scan::projection::TYPED_PAYLOAD_FIELDS.sql).await?;
        let batches = dataframe.collect().await?;
        let batch = batches.into_iter().next().expect("typed payload batch");
        assert_typed_payload_projection_batch(&batch);

        let filtered = ctx.sql(sql::scan::filters::PAYLOAD_HINTED_GTE.sql).await?.collect().await?;
        let filtered_batch = filtered.into_iter().next().expect("filtered typed payload batch");
        assert_eq!(batch_u64_ids(&filtered_batch, "id"), vec![2]);

        let cast_filtered =
            ctx.sql(sql::scan::filters::PAYLOAD_CAST_GTE.sql).await?.collect().await?;
        let cast_filtered_batch =
            cast_filtered.into_iter().next().expect("filtered exact-cast payload batch");
        assert_eq!(batch_u64_ids(&cast_filtered_batch, "id"), vec![2]);

        let (cast_sorted_ids, cast_sorted_display) =
            collect_id_rows(&ctx, sql::scan::ordering::CAST_PAYLOAD_DESC.sql).await?;
        assert_eq!(cast_sorted_ids, vec![2, 1], "{cast_sorted_display}");

        let (query_sorted_ids, query_sorted_display) =
            collect_id_rows(&ctx, sql::scan::ordering::ORDER_BY_SCORE_CAST.sql).await?;
        assert_eq!(query_sorted_ids, vec![2, 1], "{query_sorted_display}");

        Ok(())
    }

    pub(super) async fn test_table_provider_payload_empty_and_values_count(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let client = create_qdrant_client(&c)?;
        let collection_name = "test_payload_empty_and_values_count";
        create_scalar_collection(&client, collection_name).await?;

        let mut missing = qdrant_client::Payload::new();
        missing.insert("kind", "missing");

        let mut nulls = qdrant_client::Payload::new();
        nulls.insert("kind", "null");
        nulls.insert("list", serde_json::Value::Null);

        let mut empties = qdrant_client::Payload::new();
        empties.insert("kind", "empty");
        empties.insert("list", serde_json::json!([]));

        let mut values = qdrant_client::Payload::new();
        values.insert("kind", "value");
        values.insert("list", serde_json::json!([1]));

        let mut multi = qdrant_client::Payload::new();
        multi.insert("kind", "multi");
        multi.insert("list", serde_json::json!([1, 2]));

        let points = vec![
            PointStruct::new(1, Vector::new_dense(vec![0.0]), missing),
            PointStruct::new(2, Vector::new_dense(vec![0.0]), nulls),
            PointStruct::new(3, Vector::new_dense(vec![0.0]), empties),
            PointStruct::new(4, Vector::new_dense(vec![0.0]), values),
            PointStruct::new(5, Vector::new_dense(vec![0.0]), multi),
        ];
        drop(client.upsert_points(UpsertPointsBuilder::new(collection_name, points)).await?);

        let table_provider = QdrantTableProvider::try_new(client.clone(), collection_name).await?;
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(ctx.session_context().register_table("vectors", Arc::new(table_provider))?);

        let projection =
            ctx.sql(sql::scan::projection::EMPTY_AND_COUNT_VALUES.sql).await?.collect().await?;
        let projection_batch = projection.into_iter().next().expect("projection batch");
        assert_eq!(batch_u64_ids(&projection_batch, "id"), vec![1, 2, 3, 4, 5]);
        assert_eq!(batch_bool_values(&projection_batch, "list_exists"), vec![
            false, true, true, true, true
        ]);
        assert_eq!(batch_bool_values(&projection_batch, "list_missing"), vec![
            true, false, false, false, false
        ]);
        assert_eq!(batch_bool_values(&projection_batch, "list_null"), vec![
            false, true, false, false, false
        ]);
        assert_eq!(batch_bool_values(&projection_batch, "list_empty"), vec![
            true, true, true, false, false
        ]);
        assert_eq!(batch_bool_values(&projection_batch, "list_has_values"), vec![
            false, false, false, true, true
        ]);
        assert_eq!(batch_optional_i64_values(&projection_batch, "list_count"), vec![
            None,
            Some(0),
            Some(0),
            Some(1),
            Some(2)
        ]);

        assert_pushed_id_rows(&ctx, sql::scan::filters::EMPTY.sql, &[1, 2, 3]).await?;
        assert_pushed_id_rows(&ctx, sql::scan::filters::EXISTS.sql, &[2, 3, 4, 5]).await?;
        assert_pushed_id_rows(&ctx, sql::scan::filters::NOT_EXISTS.sql, &[1]).await?;
        assert_pushed_id_rows(&ctx, sql::scan::filters::IS_MISSING.sql, &[1]).await?;
        assert_pushed_id_rows(&ctx, sql::scan::filters::IS_NULL_EXPLICIT.sql, &[2]).await?;
        assert_pushed_id_rows(&ctx, sql::scan::filters::VALUES_COUNT_ZERO.sql, &[2, 3]).await?;
        assert_pushed_id_rows(&ctx, sql::scan::filters::VALUES_COUNT_GE_ONE.sql, &[4, 5]).await?;
        assert_pushed_id_rows(&ctx, sql::scan::filters::HAS_VALUES.sql, &[4, 5]).await?;
        assert_pushed_id_rows(&ctx, sql::scan::filters::VALUES_COUNT_NOT_ZERO.sql, &[4, 5]).await?;
        assert_pushed_id_rows(&ctx, sql::scan::filters::VALUES_COUNT_LT_ONE.sql, &[2, 3]).await?;
        assert_pushed_id_rows(&ctx, sql::scan::filters::VALUES_COUNT_GT_ONE.sql, &[5]).await?;
        assert_pushed_id_rows(&ctx, sql::scan::filters::VALUES_COUNT_BETWEEN.sql, &[2, 3, 4])
            .await?;
        assert_pushed_id_rows(&ctx, sql::scan::filters::VALUES_COUNT_NOT_BETWEEN.sql, &[5]).await?;

        let count_aggregate =
            ctx.sql(sql::scan::aggregates::AVG_VALUES_COUNT.sql).await?.collect().await?;
        let count_aggregate_batch = count_aggregate.into_iter().next().expect("aggregate batch");
        let avg_count = batch_f64_values(&count_aggregate_batch, "avg_list_count");
        assert_eq!(avg_count, vec![0.75]);

        let null_missing =
            ctx.sql(sql::scan::aggregates::SUM_NULL_AND_MISSING_FLAGS.sql).await?.collect().await?;
        let null_missing_batch = null_missing.into_iter().next().expect("aggregate batch");
        assert_eq!(batch_i64_values(&null_missing_batch, "null_total"), vec![1]);
        assert_eq!(batch_i64_values(&null_missing_batch, "missing_total"), vec![1]);

        Ok(())
    }

    pub(super) async fn test_table_provider_payload_geo_distance(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let client = create_qdrant_client(&c)?;
        let collection_name = "test_payload_geo_distance";
        create_scalar_collection(&client, collection_name).await?;
        create_payload_index(
            &client,
            collection_name,
            "location",
            FieldType::Geo,
            GeoIndexParamsBuilder::default().build(),
        )
        .await?;

        let mut center = qdrant_client::Payload::new();
        center.insert("location", serde_json::json!({"lon": 0.0, "lat": 0.0}));

        let mut near = qdrant_client::Payload::new();
        near.insert("location", serde_json::json!({"lon": 0.0, "lat": 1.0}));

        let mut far = qdrant_client::Payload::new();
        far.insert("location", serde_json::json!({"lon": 0.0, "lat": 2.0}));

        let points = vec![
            PointStruct::new(1, Vector::new_dense(vec![0.0]), center),
            PointStruct::new(2, Vector::new_dense(vec![0.0]), near),
            PointStruct::new(3, Vector::new_dense(vec![0.0]), far),
        ];
        drop(client.upsert_points(UpsertPointsBuilder::new(collection_name, points)).await?);

        let table_provider = QdrantTableProvider::try_new(client.clone(), collection_name).await?;
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(ctx.session_context().register_table("vectors", Arc::new(table_provider))?);

        let projection =
            ctx.sql(sql::scan::projection::GEO_DISTANCE_VALUES.sql).await?.collect().await?;
        let projection_batch = projection.into_iter().next().expect("projection batch");
        assert_eq!(batch_u64_ids(&projection_batch, "id"), vec![1, 2, 3]);
        let distances = batch_i64_values(&projection_batch, "distance");
        assert_eq!(distances[0], 0);
        assert!((110_000..=112_500).contains(&distances[1]), "{distances:?}");
        assert!((220_000..=223_000).contains(&distances[2]), "{distances:?}");

        let (near_ids, near_display) =
            collect_id_rows(&ctx, sql::scan::filters::GEO_RADIUS.sql).await?;
        assert_eq!(near_ids, vec![1], "{near_display}");
        assert!(!near_display.contains("FilterExec"), "{near_display}");

        let (far_ids, far_display) =
            collect_id_rows(&ctx, sql::scan::filters::GEO_RESIDUAL.sql).await?;
        assert_eq!(far_ids, vec![2, 3], "{far_display}");
        assert!(far_display.contains("FilterExec"), "{far_display}");

        let (bbox_ids, bbox_display) =
            collect_id_rows(&ctx, sql::scan::filters::GEO_BBOX.sql).await?;
        assert_eq!(bbox_ids, vec![1, 2], "{bbox_display}");
        assert!(!bbox_display.contains("FilterExec"), "{bbox_display}");

        let (polygon_ids, polygon_display) =
            collect_id_rows(&ctx, sql::scan::filters::GEO_POLYGON.sql).await?;
        assert_eq!(polygon_ids, vec![1, 2], "{polygon_display}");
        assert!(!polygon_display.contains("FilterExec"), "{polygon_display}");

        Ok(())
    }

    pub(super) async fn test_table_provider_payload_text_predicates(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let client = create_qdrant_client(&c)?;
        let collection_name = "test_payload_text_predicates";
        create_scalar_collection(&client, collection_name).await?;
        create_payload_index(
            &client,
            collection_name,
            "description",
            FieldType::Text,
            TextIndexParamsBuilder::new(TokenizerType::Word).phrase_matching(true).build(),
        )
        .await?;

        let mut first = qdrant_client::Payload::new();
        first.insert("description", "good cheap coffee");
        let mut second = qdrant_client::Payload::new();
        second.insert("description", "time is a flat circle");
        let mut third = qdrant_client::Payload::new();
        third.insert("description", "good food nearby");
        let mut fourth = qdrant_client::Payload::new();
        fourth.insert("description", "machine time notes");

        let points = vec![
            PointStruct::new(1, Vector::new_dense(vec![0.0]), first),
            PointStruct::new(2, Vector::new_dense(vec![0.0]), second),
            PointStruct::new(3, Vector::new_dense(vec![0.0]), third),
            PointStruct::new(4, Vector::new_dense(vec![0.0]), fourth),
        ];
        drop(client.upsert_points(UpsertPointsBuilder::new(collection_name, points)).await?);

        let table_provider = QdrantTableProvider::try_new(client.clone(), collection_name).await?;
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(ctx.session_context().register_table("vectors", Arc::new(table_provider))?);

        let (text_ids, text_display) =
            collect_id_rows(&ctx, sql::scan::filters::TEXT_MATCH.sql).await?;
        assert_eq!(text_ids, vec![1], "{text_display}");
        assert!(!text_display.contains("FilterExec"), "{text_display}");

        let (phrase_ids, phrase_display) =
            collect_id_rows(&ctx, sql::scan::filters::PHRASE_MATCH.sql).await?;
        assert_eq!(phrase_ids, vec![2], "{phrase_display}");
        assert!(!phrase_display.contains("FilterExec"), "{phrase_display}");

        let (text_any_ids, text_any_display) =
            collect_id_rows(&ctx, sql::scan::filters::TEXT_ANY.sql).await?;
        assert_eq!(text_any_ids, vec![1, 3], "{text_any_display}");
        assert!(!text_any_display.contains("FilterExec"), "{text_any_display}");

        Ok(())
    }

    pub(super) async fn test_table_provider_payload_nested_predicate(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let ctx = create_catalog_scan_context(&c, "test_payload_nested_predicate").await?;

        let (ids, display) = collect_id_rows(&ctx, sql::scan::filters::NESTED_MATCH.sql).await?;
        assert_eq!(ids, vec![1, 3], "{display}");
        assert!(!display.contains("FilterExec"), "{display}");

        Ok(())
    }

    pub(super) async fn test_table_provider_pushes_down_bool_facet(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let client = create_qdrant_client(&c)?;
        let collection_name = "test_bool_facet_pushdown";
        create_scalar_collection(&client, collection_name).await?;
        create_payload_index(
            &client,
            collection_name,
            "active",
            FieldType::Bool,
            qdrant_client::qdrant::BoolIndexParamsBuilder::default().build(),
        )
        .await?;

        let mut payload1 = qdrant_client::Payload::new();
        payload1.insert("active", true);
        let mut payload2 = qdrant_client::Payload::new();
        payload2.insert("active", false);
        let mut payload3 = qdrant_client::Payload::new();
        payload3.insert("active", true);
        let mut payload4 = qdrant_client::Payload::new();
        payload4.insert("active", true);

        let points = vec![
            PointStruct::new(1, Vector::new_dense(vec![0.0]), payload1),
            PointStruct::new(2, Vector::new_dense(vec![0.0]), payload2),
            PointStruct::new(3, Vector::new_dense(vec![0.0]), payload3),
            PointStruct::new(4, Vector::new_dense(vec![0.0]), payload4),
        ];
        drop(client.upsert_points(UpsertPointsBuilder::new(collection_name, points)).await?);

        let table_provider = QdrantTableProvider::try_new(client.clone(), collection_name).await?;
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(ctx.session_context().register_table("vectors", Arc::new(table_provider))?);

        let dataframe = ctx.sql(sql::scan::aggregates::BOOL_FACET.sql).await?;
        let plan = dataframe.clone().create_physical_plan().await?;
        let display =
            datafusion::physical_plan::displayable(plan.as_ref()).indent(true).to_string();
        let batches = dataframe.collect().await?;
        let rows = batches
            .iter()
            .flat_map(|batch| {
                let active = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("active bool array");
                let totals = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("count int64 array");
                (0..batch.num_rows())
                    .map(|row| (active.value(row), totals.value(row)))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        assert_eq!(rows, vec![(true, 3), (false, 1)]);
        assert!(display.contains("QdrantFacetExec"), "{display}");
        assert!(!display.contains("AggregateExec"), "{display}");
        assert!(!display.contains("SortExec"), "{display}");
        assert!(!display.contains("GlobalLimitExec"), "{display}");
        assert!(!display.contains("LocalLimitExec"), "{display}");

        Ok(())
    }

    pub(super) async fn test_prepared_session_sql_nearest_query_without_limit(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let client = create_qdrant_client(&c)?;
        let collection_name = "test_session_context_nearest_query_without_limit";

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

        let sql = sql::query::nearest::WITHOUT_LIMIT.sql;
        let (rows, display) = collect_scored_rows(&ctx, sql).await?;

        assert_eq!(rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![1, 2, 3]);
        assert_f32_eq(rows[0].1, 1.0);
        assert!(!display.contains(", limit="), "{display}");
        assert!(display.contains("QdrantQueryExec"), "{display}");

        Ok(())
    }

    pub(super) async fn test_prepared_session_sql_nearest_query_local_projection_shell(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let client = create_qdrant_client(&c)?;
        let collection_name = "test_session_context_nearest_query_local_projection_shell";

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

        let sql = sql::query::nearest::LOCAL_PROJECTION.sql;
        let (rows, display) = collect_scored_rows(&ctx, sql).await?;

        assert_eq!(rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![1, 2, 3]);
        assert_f32_eq(rows[0].1, 2.0);
        assert_f32_eq(rows[1].1, 1.5);
        assert_f32_eq(rows[2].1, 1.0);
        assert!(display.contains("QdrantQueryExec"), "{display}");
        assert!(display.contains("ProjectionExec"), "{display}");

        Ok(())
    }

    pub(super) async fn test_prepared_session_sql_nearest_query_local_filter_shell(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let client = create_qdrant_client(&c)?;
        let collection_name = "test_session_context_nearest_query_local_filter_shell";

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

        let sql = sql::query::nearest::LOCAL_FILTER.sql;
        let (ids, display) = collect_id_rows(&ctx, sql).await?;

        assert_eq!(ids, vec![2, 3]);
        assert!(display.contains("QdrantQueryExec"), "{display}");
        assert!(display.contains("FilterExec"), "{display}");

        Ok(())
    }

    pub(super) async fn test_prepared_session_sql_nearest_query_local_filter_and_projection_shell(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let client = create_qdrant_client(&c)?;
        let collection_name =
            "test_session_context_nearest_query_local_filter_and_projection_shell";

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

        let sql = sql::query::nearest::LOCAL_FILTER_AND_PROJECTION.sql;
        let (rows, display) = collect_scored_rows(&ctx, sql).await?;

        assert_eq!(rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![2, 3]);
        assert_f32_eq(rows[0].1, 0.5);
        assert_f32_eq(rows[1].1, 0.0);
        assert!(display.contains("QdrantQueryExec"), "{display}");
        assert!(display.contains("FilterExec"), "{display}");
        assert!(display.contains("ProjectionExec"), "{display}");

        Ok(())
    }

    pub(super) async fn test_prepared_session_sql_nearest_query(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let client = create_qdrant_client(&c)?;
        let collection_name = "test_session_context_nearest_query";

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
                    .add_vector("aux", vec![1.0, 0.0]),
                qdrant_client::Payload::new(),
            ),
            PointStruct::new(
                3,
                NamedVectors::default()
                    .add_vector("embedding", vec![0.0, 1.0])
                    .add_vector("aux", vec![1.0, 0.0]),
                qdrant_client::Payload::new(),
            ),
        ];
        drop(client.upsert_points(UpsertPointsBuilder::new(collection_name, points)).await?);

        let table_provider = QdrantTableProvider::try_new(client.clone(), collection_name).await?;
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(ctx.session_context().register_table("vectors", Arc::new(table_provider))?);

        let dataframe = ctx.sql(sql::query::nearest::PAYLOAD_PROJECTION.sql).await?;
        let plan = dataframe.clone().create_physical_plan().await?;
        let display =
            datafusion::physical_plan::displayable(plan.as_ref()).indent(true).to_string();
        let batches = dataframe.collect().await?;
        let batch = batches.into_iter().next().expect("nearest batch");

        let ids = batch
            .column(batch.schema().index_of("id").expect("id column"))
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("id string array")
            .iter()
            .map(|value| value.expect("non-null id").parse::<u64>().expect("numeric id"))
            .collect::<Vec<_>>();
        let scores = batch
            .column(batch.schema().index_of("score").expect("score column"))
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("score float32 array")
            .iter()
            .map(|value| value.expect("non-null score"))
            .collect::<Vec<_>>();

        assert_eq!(ids, vec![1, 2]);
        assert_eq!(field_names(batch.schema().as_ref()), vec![
            "id",
            "payload",
            "embedding",
            "aux",
            "score"
        ],);
        assert_f32_eq(scores[0], 1.0);
        assert_f32_eq(scores[1], 0.4);
        assert!(display.contains("QdrantQueryExec"), "{display}");

        Ok(())
    }

    pub(super) async fn test_prepared_session_sql_sample_query(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let client = create_qdrant_client(&c)?;
        let collection_name = "test_session_context_sample_query";

        create_scalar_collection(&client, collection_name).await?;

        let points = vec![
            PointStruct::new(1, Vector::new_dense(vec![0.1]), qdrant_client::Payload::new()),
            PointStruct::new(2, Vector::new_dense(vec![0.2]), qdrant_client::Payload::new()),
            PointStruct::new(3, Vector::new_dense(vec![0.3]), qdrant_client::Payload::new()),
            PointStruct::new(4, Vector::new_dense(vec![0.4]), qdrant_client::Payload::new()),
            PointStruct::new(5, Vector::new_dense(vec![0.5]), qdrant_client::Payload::new()),
        ];
        drop(client.upsert_points(UpsertPointsBuilder::new(collection_name, points)).await?);

        let table_provider = QdrantTableProvider::try_new(client.clone(), collection_name).await?;
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(ctx.session_context().register_table("vectors", Arc::new(table_provider))?);

        let sql_variants = [sql::query::sample::EXPLICIT.sql, sql::query::sample::DEFAULT.sql];

        for sql in sql_variants {
            let (rows, display) = collect_scored_rows(&ctx, sql).await?;
            assert_eq!(rows.len(), 2, "sql={sql}, rows={rows:?}");
            let ids = rows.iter().map(|(id, _)| *id).collect::<BTreeSet<_>>();
            assert_eq!(ids.len(), 2, "sql={sql}, rows={rows:?}");
            assert!(ids.iter().all(|id| (1..=5).contains(id)), "sql={sql}, rows={rows:?}");
            assert!(display.contains("QdrantQueryExec"), "{display}");
        }

        Ok(())
    }

    pub(super) async fn test_prepared_session_sql_recommend_query(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let ctx =
            create_dual_vector_query_context(&c, "test_session_context_recommend_query").await?;

        let default_sql = sql::query::recommend::DEFAULT.sql;
        let strategy_sql = sql::query::recommend::STRATEGY.sql;

        let (default_rows, default_display) = collect_scored_rows(&ctx, default_sql).await?;
        let (strategy_rows, strategy_display) = collect_scored_rows(&ctx, strategy_sql).await?;

        assert_scored_rows_eq(&default_rows, &strategy_rows);
        assert_eq!(default_rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![1, 2]);
        assert!(default_display.contains("QdrantQueryExec"), "{default_display}");
        assert!(strategy_display.contains("QdrantQueryExec"), "{strategy_display}");

        Ok(())
    }

    pub(super) async fn test_prepared_session_sql_discover_query(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let ctx =
            create_dual_vector_query_context(&c, "test_session_context_discover_query").await?;

        let sql = sql::query::discover::CANONICAL.sql;
        let (rows, display) = collect_scored_rows(&ctx, sql).await?;

        assert_eq!(rows.len(), 2, "rows={rows:?}");
        assert_eq!(rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![1, 2]);
        assert!(rows[0].1 >= rows[1].1, "rows={rows:?}");
        assert!(display.contains("QdrantQueryExec"), "{display}");

        Ok(())
    }

    pub(super) async fn test_prepared_session_sql_context_query(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let ctx =
            create_dual_vector_query_context(&c, "test_session_context_context_query").await?;

        let sql = sql::query::context::CANONICAL.sql;
        let (rows, display) = collect_scored_rows(&ctx, sql).await?;

        assert_eq!(rows.len(), 2, "rows={rows:?}");
        assert_eq!(rows.iter().map(|(id, _)| *id).collect::<BTreeSet<_>>(), BTreeSet::from([1, 2]));
        assert!(rows.iter().all(|(_, score)| *score <= 0.0), "rows={rows:?}");
        assert!(display.contains("QdrantQueryExec"), "{display}");

        Ok(())
    }

    pub(super) async fn test_prepared_session_sql_nearest_with_mmr_query(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let ctx =
            create_dual_vector_query_context(&c, "test_session_context_nearest_with_mmr_query")
                .await?;

        let sql = sql::query::mmr::CANONICAL.sql;
        let (rows, display) = collect_scored_rows(&ctx, sql).await?;

        assert_eq!(rows.len(), 2, "rows={rows:?}");
        assert_eq!(rows[0].0, 1, "rows={rows:?}");
        assert_eq!(
            rows.iter().map(|(id, _)| *id).collect::<BTreeSet<_>>().len(),
            2,
            "rows={rows:?}"
        );
        assert!(rows.iter().all(|(id, _)| (1..=3).contains(id)), "rows={rows:?}");
        assert!(display.contains("QdrantQueryExec"), "{display}");

        Ok(())
    }

    pub(super) async fn test_prepared_session_sql_relevance_feedback_query(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let ctx =
            create_dual_vector_query_context(&c, "test_session_context_relevance_feedback_query")
                .await?;

        let sql = sql::query::relevance::CANONICAL.sql;
        let (rows, display) = collect_scored_rows(&ctx, sql).await?;

        assert_eq!(rows.len(), 2, "rows={rows:?}");
        assert_eq!(rows[0].0, 1, "rows={rows:?}");
        assert_eq!(
            rows.iter().map(|(id, _)| *id).collect::<BTreeSet<_>>().len(),
            2,
            "rows={rows:?}"
        );
        assert!(rows.iter().all(|(id, _)| (1..=3).contains(id)), "rows={rows:?}");
        assert!(display.contains("QdrantQueryExec"), "{display}");

        Ok(())
    }

    pub(super) async fn test_prepared_session_sql_supported_nearest_catalog_queries(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let ctx = create_vector_query_context(&c, "test_supported_nearest_catalog_queries").await?;
        for case in sql::query::nearest::ALL {
            let (_batches, display) =
                assert_supported_query_collects(&ctx, case.sql).await.map_err(|error| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "supported nearest catalog case={} sql={} error={error}",
                        case.id, case.sql
                    ))
                })?;
            assert!(
                display.contains("QdrantQueryExec")
                    || display.contains("QdrantQueryBatchExec")
                    || display.contains("ProjectionExec")
                    || display.contains("FilterExec")
                    || display.contains("AggregateExec")
                    || display.contains("WindowAggExec")
                    || display.contains("HashJoinExec")
                    || display.contains("JoinExec"),
                "case={} sql={} display={display}",
                case.id,
                case.sql,
            );
        }

        Ok(())
    }

    pub(super) async fn test_prepared_session_sql_supported_query_family_catalog_queries(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let vector_ctx =
            create_vector_query_context(&c, "test_supported_query_family_sample_queries").await?;
        let dual_ctx =
            create_dual_vector_query_context(&c, "test_supported_query_family_dual_queries")
                .await?;
        let grouped_ctx =
            create_grouped_nearest_query_context(&c, "test_supported_query_family_grouped_queries")
                .await?;

        for case in sql::query::sample::ALL {
            let (_batches, display) =
                assert_supported_query_collects(&vector_ctx, case.sql).await.map_err(|error| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "supported query-family sample case={} sql={} error={error}",
                        case.id, case.sql
                    ))
                })?;
            assert!(
                display.contains("QdrantQueryExec")
                    || display.contains("SortExec")
                    || display.contains("AggregateExec")
                    || display.contains("WindowAggExec")
                    || display.contains("HashJoinExec")
                    || display.contains("JoinExec")
                    || display.contains("NestedLoopJoinExec"),
                "{display}"
            );
        }
        for case in sql::query::recommend::ALL
            .iter()
            .chain(sql::query::discover::ALL.iter())
            .chain(sql::query::context::ALL.iter())
            .chain(sql::query::mmr::ALL.iter())
            .chain(sql::query::relevance::ALL.iter())
        {
            let (_batches, display) =
                assert_supported_query_collects(&dual_ctx, case.sql).await.map_err(|error| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "supported query-family dual case={} sql={} error={error}",
                        case.id, case.sql
                    ))
                })?;
            assert!(
                display.contains("QdrantQueryExec")
                    || display.contains("AggregateExec")
                    || display.contains("WindowAggExec")
                    || display.contains("HashJoinExec")
                    || display.contains("JoinExec")
                    || display.contains("NestedLoopJoinExec"),
                "{display}"
            );
        }
        for case in sql::query::grouped::ALL {
            let (_batches, display) =
                assert_supported_query_collects(&grouped_ctx, case.sql).await.map_err(|error| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "supported grouped case={} sql={} error={error}",
                        case.id, case.sql
                    ))
                })?;
            assert!(
                display.contains("QdrantQueryGroupsExec")
                    || display.contains("QdrantQueryExec")
                    || display.contains("WindowAggExec"),
                "{display}"
            );
        }

        Ok(())
    }

    pub(super) async fn test_prepared_session_sql_grouped_nearest_query(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let ctx =
            create_grouped_nearest_query_context(&c, "test_session_context_grouped_nearest_query")
                .await?;

        let ascending_sql = sql::query::grouped::NEAREST_ASC.sql;
        let ascending_without_tiebreak_sql = sql::query::grouped::NEAREST_ASC_NO_TIEBREAK.sql;
        let descending_sql = sql::query::grouped::NEAREST_DESC.sql;

        let (ascending_rows, ascending_display) =
            collect_grouped_scored_rows(&ctx, ascending_sql).await?;
        let (ascending_without_tiebreak_rows, ascending_without_tiebreak_display) =
            collect_grouped_scored_rows(&ctx, ascending_without_tiebreak_sql).await?;
        let (descending_rows, descending_display) =
            collect_grouped_scored_rows(&ctx, descending_sql).await?;

        assert_eq!(
            ascending_rows.iter().map(|(tag, id, _)| (tag.clone(), *id)).collect::<Vec<_>>(),
            vec![("blue".to_owned(), 3), ("green".to_owned(), 4), ("red".to_owned(), 1),],
            "rows={ascending_rows:?}"
        );
        assert_f32_eq(ascending_rows[0].2, 0.9);
        assert_f32_eq(ascending_rows[1].2, 0.1);
        assert_f32_eq(ascending_rows[2].2, 1.0);
        assert!(ascending_display.contains("QdrantQueryGroupsExec"), "{ascending_display}");
        assert!(!ascending_display.contains("SortExec"), "{ascending_display}");

        assert_eq!(
            ascending_without_tiebreak_rows, ascending_rows,
            "rows={ascending_without_tiebreak_rows:?}"
        );
        assert!(
            ascending_without_tiebreak_display.contains("QdrantQueryGroupsExec"),
            "{ascending_without_tiebreak_display}"
        );
        assert!(
            !ascending_without_tiebreak_display.contains("SortExec"),
            "{ascending_without_tiebreak_display}"
        );

        assert_eq!(
            descending_rows.iter().map(|(tag, id, _)| (tag.clone(), *id)).collect::<Vec<_>>(),
            vec![("red".to_owned(), 1), ("green".to_owned(), 4)],
            "rows={descending_rows:?}"
        );
        assert_f32_eq(descending_rows[0].2, 1.0);
        assert_f32_eq(descending_rows[1].2, 0.1);
        assert!(descending_display.contains("QdrantQueryGroupsExec"), "{descending_display}");
        assert!(!descending_display.contains(", limit="), "{descending_display}");
        assert!(descending_display.contains("GlobalLimitExec"), "{descending_display}");

        Ok(())
    }

    pub(super) async fn test_prepared_session_sql_grouped_query_family_queries(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let ctx = create_grouped_nearest_query_context(
            &c,
            "test_session_context_grouped_query_family_queries",
        )
        .await?;

        let grouped_cases = [
            ("recommend", sql::query::grouped::RECOMMEND.sql),
            ("discover", sql::query::grouped::DISCOVER.sql),
            ("context", sql::query::grouped::CONTEXT.sql),
            ("sample", sql::query::grouped::SAMPLE.sql),
            ("mmr", sql::query::grouped::MMR.sql),
            ("relevance", sql::query::grouped::RELEVANCE.sql),
        ];

        for (label, sql) in grouped_cases {
            let (rows, display) = collect_grouped_scored_rows(&ctx, sql).await?;
            let tag_ids = rows.iter().map(|(tag, id, _)| (tag.clone(), *id)).collect::<Vec<_>>();
            let tags = rows.iter().map(|(tag, _, _)| tag.clone()).collect::<Vec<_>>();
            assert_eq!(
                tags,
                vec!["blue".to_owned(), "green".to_owned(), "red".to_owned()],
                "label={label} rows={rows:?}"
            );
            assert_eq!(
                tag_ids[0..2],
                [("blue".to_owned(), 3), ("green".to_owned(), 4)],
                "label={label} rows={rows:?}"
            );
            assert_eq!(tag_ids[2].0, "red", "label={label} rows={rows:?}");
            assert!(matches!(tag_ids[2].1, 1 | 2), "label={label} rows={rows:?}");
            assert!(display.contains("QdrantQueryGroupsExec"), "label={label} display={display}");
            assert!(!display.contains("SortExec"), "label={label} display={display}");
        }

        Ok(())
    }

    pub(super) async fn test_nearest_query_projects_payload_path(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let client = create_qdrant_client(&c)?;
        let collection_name = "test_nearest_query_projects_payload_path";

        let mut vectors_config = VectorsConfigBuilder::default();
        let _ = vectors_config
            .add_named_vector_params("vector", VectorParamsBuilder::new(2, Distance::Dot).build());
        let _ = client
            .create_collection(
                CreateCollectionBuilder::new(collection_name).vectors_config(vectors_config),
            )
            .await?;

        create_payload_index(
            &client,
            collection_name,
            "rank",
            FieldType::Integer,
            qdrant_client::qdrant::IntegerIndexParamsBuilder::new(true, true).build(),
        )
        .await?;

        let mut payload1 = qdrant_client::Payload::new();
        payload1.insert("rank", 30_i64);
        let mut payload2 = qdrant_client::Payload::new();
        payload2.insert("rank", 20_i64);
        let mut payload3 = qdrant_client::Payload::new();
        payload3.insert("rank", 10_i64);

        let points = vec![
            PointStruct::new(
                1,
                NamedVectors::default().add_vector("vector", vec![1.0, 0.0]),
                payload1,
            ),
            PointStruct::new(
                2,
                NamedVectors::default().add_vector("vector", vec![0.4, 0.0]),
                payload2,
            ),
            PointStruct::new(
                3,
                NamedVectors::default().add_vector("vector", vec![0.0, 1.0]),
                payload3,
            ),
        ];
        drop(client.upsert_points(UpsertPointsBuilder::new(collection_name, points)).await?);

        let table_provider = QdrantTableProvider::try_new(client.clone(), collection_name).await?;
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(ctx.session_context().register_table("vectors", Arc::new(table_provider))?);

        let dataframe = ctx.sql(sql::scan::projection::NEAREST_PAYLOAD_PATH.sql).await?;
        let plan = dataframe.clone().create_physical_plan().await?;
        let display =
            datafusion::physical_plan::displayable(plan.as_ref()).indent(true).to_string();
        let batches = dataframe.collect().await?;
        let batch = batches.into_iter().next().expect("nearest payload batch");

        let ids = batch
            .column(batch.schema().index_of("id").expect("id column"))
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("id string array")
            .iter()
            .map(|value| value.expect("non-null id").parse::<u64>().expect("numeric id"))
            .collect::<Vec<_>>();
        let scores = batch
            .column(batch.schema().index_of("score").expect("score column"))
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("score float32 array")
            .iter()
            .map(|value| value.expect("non-null score"))
            .collect::<Vec<_>>();
        let ranks = batch_stringified_scalar_values(&batch, "rank");

        assert_eq!(ids, vec![1, 2]);
        assert_eq!(ranks, vec!["30".to_owned(), "20".to_owned()]);
        assert_f32_eq(scores[0], 1.0);
        assert_f32_eq(scores[1], 0.4);
        assert!(display.contains("QdrantQueryExec"), "{display}");

        let (cast_ranks, cast_display) =
            collect_i64_rows(&ctx, sql::scan::projection::NEAREST_PAYLOAD_PATH_CAST.sql, "rank")
                .await?;

        assert_eq!(cast_ranks, vec![30, 20, 10]);
        assert!(cast_display.contains("QdrantQueryExec"), "{cast_display}");

        Ok(())
    }

    pub(super) async fn test_coordinated_formula_sql_variants(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let ctx =
            create_dual_vector_query_context(&c, "test_coordinated_formula_sql_variants").await?;

        let without_limit_sql = sql::coordination::formula::WITHOUT_LIMIT.sql;
        let canonical_sql = sql::coordination::formula::CANONICAL.sql;
        let alias_wrapped_sql = sql::coordination::formula::ALIAS_WRAPPED.sql;
        let redundant_sort_sql = sql::coordination::formula::REDUNDANT_SORT.sql;
        let alias_threaded_sql = sql::coordination::formula::ALIAS_THREADING.sql;
        let sort_only_sql = sql::coordination::formula::SORT_ONLY.sql;
        let left_join_sql = sql::coordination::formula::LEFT_JOIN.sql;
        let cross_join_sql = sql::coordination::formula::CROSS_JOIN.sql;
        let inner_join_qdrant_leaf_sql =
            sql::coordination::formula::INNER_JOIN_QDRANT_ONLY_LEAF.sql;

        let (without_limit_rows, without_limit_display) =
            collect_scored_rows(&ctx, without_limit_sql).await?;
        let (canonical_rows, canonical_display) = collect_scored_rows(&ctx, canonical_sql).await?;
        let (alias_rows, alias_display) = collect_scored_rows(&ctx, alias_wrapped_sql).await?;
        let (redundant_sort_rows, redundant_sort_display) =
            collect_scored_rows(&ctx, redundant_sort_sql).await?;
        let (alias_threaded_rows, alias_threaded_display) =
            collect_scored_rows(&ctx, alias_threaded_sql).await?;
        let (sort_only_rows, sort_only_display) = collect_id_rows(&ctx, sort_only_sql).await?;
        let (left_join_rows, left_join_display) = collect_scored_rows(&ctx, left_join_sql).await?;
        let (cross_join_rows, cross_join_display) =
            collect_scored_rows(&ctx, cross_join_sql).await?;
        let (inner_join_qdrant_leaf_rows, inner_join_qdrant_leaf_display) =
            collect_scored_rows(&ctx, inner_join_qdrant_leaf_sql).await?;

        assert_eq!(without_limit_rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![1, 2, 3]);
        assert_f32_eq(without_limit_rows[0].1, 2.0);
        assert_f32_eq(without_limit_rows[1].1, 0.8);
        assert_f32_eq(without_limit_rows[2].1, 0.2);
        assert_scored_rows_eq(&canonical_rows, &alias_rows);
        assert_scored_rows_eq(&canonical_rows, &redundant_sort_rows);
        assert_scored_rows_eq(&canonical_rows, &alias_threaded_rows);
        assert_scored_rows_eq(&canonical_rows, &left_join_rows);
        assert_eq!(sort_only_rows, canonical_rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),);
        assert_eq!(cross_join_rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![1, 1]);
        assert_f32_eq(cross_join_rows[0].1, 2.0);
        assert_f32_eq(cross_join_rows[1].1, 1.4);
        assert_eq!(
            inner_join_qdrant_leaf_rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_f32_eq(inner_join_qdrant_leaf_rows[0].1, 2.0);
        assert_f32_eq(inner_join_qdrant_leaf_rows[1].1, 1.4);

        assert_eq!(
            without_limit_display.matches("QdrantQueryExec").count(),
            1,
            "{without_limit_display}"
        );
        assert!(without_limit_display.contains("prefetch=2"), "{without_limit_display}");
        assert!(!without_limit_display.contains("JoinExec"), "{without_limit_display}");
        assert!(!without_limit_display.contains("HashJoinExec"), "{without_limit_display}");

        for display in
            [&canonical_display, &alias_display, &redundant_sort_display, &alias_threaded_display]
        {
            assert_eq!(display.matches("QdrantQueryExec").count(), 1, "{display}");
            assert!(display.contains("prefetch=2"), "{display}");
            assert!(!display.contains("JoinExec"), "{display}");
            assert!(!display.contains("HashJoinExec"), "{display}");
        }

        assert_eq!(sort_only_display.matches("QdrantQueryExec").count(), 1, "{sort_only_display}");
        assert!(sort_only_display.contains("prefetch=2"), "{sort_only_display}");
        assert!(!sort_only_display.contains("JoinExec"), "{sort_only_display}");
        assert!(!sort_only_display.contains("HashJoinExec"), "{sort_only_display}");
        assert_eq!(left_join_display.matches("QdrantQueryExec").count(), 2, "{left_join_display}");
        assert!(left_join_display.contains("HashJoinExec"), "{left_join_display}");
        assert_eq!(
            cross_join_display.matches("QdrantQueryExec").count(),
            2,
            "{cross_join_display}"
        );
        assert!(cross_join_display.contains("CrossJoinExec"), "{cross_join_display}");
        assert_eq!(
            inner_join_qdrant_leaf_display.matches("QdrantQueryExec").count(),
            2,
            "{inner_join_qdrant_leaf_display}"
        );
        assert!(
            inner_join_qdrant_leaf_display.contains("JoinExec")
                || inner_join_qdrant_leaf_display.contains("HashJoinExec"),
            "{inner_join_qdrant_leaf_display}"
        );
        assert!(
            inner_join_qdrant_leaf_display.contains("__qdrant_formula_score"),
            "{inner_join_qdrant_leaf_display}"
        );

        Ok(())
    }

    pub(super) async fn test_explicit_fusion_sql_variants(c: Arc<QdrantContainer>) -> Result<()> {
        let ctx = create_dual_vector_query_context(&c, "test_explicit_fusion_sql_variants").await?;

        let without_limit_sql = sql::coordination::fusion::WITHOUT_LIMIT.sql;
        let canonical_sql = sql::coordination::fusion::CANONICAL.sql;
        let alias_wrapped_sql = sql::coordination::fusion::ALIAS_WRAPPED.sql;
        let alias_threaded_sql = sql::coordination::fusion::ALIAS_THREADING.sql;
        let inner_join_sql = sql::coordination::fusion::INNER_JOIN.sql;
        let left_join_sql = sql::coordination::fusion::LEFT_JOIN.sql;
        let right_join_sql = sql::coordination::fusion::RIGHT_JOIN.sql;
        let dbsf_canonical_sql =
            "SELECT id, qdrant_fusion_score('DBSF', dense.score, sparse.score) AS score FROM \
             (SELECT id, qdrant_nearest_score(embedding, 1.0, 0.0) AS score FROM vectors ORDER BY \
             score DESC LIMIT 5) dense FULL OUTER JOIN (SELECT id, qdrant_nearest_score(aux, 0.0, \
             1.0) AS score FROM vectors ORDER BY score DESC LIMIT 5) sparse USING (id) ORDER BY \
             score DESC LIMIT 2";
        let dbsf_inner_join_sql = sql::coordination::fusion::DBSF_INNER_JOIN.sql;
        let dbsf_left_join_sql = sql::coordination::fusion::DBSF_LEFT_JOIN.sql;
        let dbsf_right_join_sql = sql::coordination::fusion::DBSF_RIGHT_JOIN.sql;

        let (without_limit_rows, without_limit_display) =
            collect_scored_rows(&ctx, without_limit_sql).await?;
        let (canonical_rows, canonical_display) = collect_scored_rows(&ctx, canonical_sql).await?;
        let (alias_rows, alias_display) = collect_scored_rows(&ctx, alias_wrapped_sql).await?;
        let (alias_threaded_rows, alias_threaded_display) =
            collect_scored_rows(&ctx, alias_threaded_sql).await?;
        let (inner_join_rows, inner_join_display) =
            collect_scored_rows(&ctx, inner_join_sql).await?;
        let (left_join_rows, left_join_display) = collect_scored_rows(&ctx, left_join_sql).await?;
        let (right_join_rows, right_join_display) =
            collect_scored_rows(&ctx, right_join_sql).await?;
        let (dbsf_canonical_rows, dbsf_canonical_display) =
            collect_scored_rows(&ctx, dbsf_canonical_sql).await?;
        let (dbsf_inner_join_rows, dbsf_inner_join_display) =
            collect_scored_rows(&ctx, dbsf_inner_join_sql).await?;
        let (dbsf_left_join_rows, dbsf_left_join_display) =
            collect_scored_rows(&ctx, dbsf_left_join_sql).await?;
        let (dbsf_right_join_rows, dbsf_right_join_display) =
            collect_scored_rows(&ctx, dbsf_right_join_sql).await?;

        assert_eq!(without_limit_rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![1, 2, 3]);
        assert_f32_eq(without_limit_rows[0].1, canonical_rows[0].1);
        assert_f32_eq(without_limit_rows[1].1, canonical_rows[1].1);
        assert!(without_limit_rows[2].1 <= without_limit_rows[1].1);
        assert_scored_rows_eq(&canonical_rows, &alias_rows);
        assert_scored_rows_eq(&canonical_rows, &alias_threaded_rows);
        assert_eq!(inner_join_rows, canonical_rows);
        assert_eq!(left_join_rows, canonical_rows);
        assert_eq!(right_join_rows, canonical_rows);
        assert_eq!(dbsf_inner_join_rows, dbsf_canonical_rows);
        assert_eq!(dbsf_left_join_rows, dbsf_canonical_rows);
        assert_eq!(dbsf_right_join_rows, dbsf_canonical_rows);

        for display in [
            &without_limit_display,
            &canonical_display,
            &alias_display,
            &alias_threaded_display,
            &dbsf_canonical_display,
        ] {
            assert_eq!(display.matches("QdrantQueryExec").count(), 1, "{display}");
            assert!(display.contains("prefetch=2"), "{display}");
            assert!(!display.contains("JoinExec"), "{display}");
            assert!(!display.contains("HashJoinExec"), "{display}");
        }
        for display in [
            &inner_join_display,
            &left_join_display,
            &right_join_display,
            &dbsf_inner_join_display,
            &dbsf_left_join_display,
            &dbsf_right_join_display,
        ] {
            assert_eq!(display.matches("QdrantQueryExec").count(), 2, "{display}");
            assert!(display.contains("JoinExec") || display.contains("HashJoinExec"), "{display}");
            assert!(!display.contains("prefetch=2"), "{display}");
        }

        Ok(())
    }

    pub(super) async fn test_table_provider_pushes_down_integer_facet(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let client = create_qdrant_client(&c)?;
        let collection_name = "test_integer_facet_pushdown";
        create_scalar_collection(&client, collection_name).await?;
        create_payload_index(
            &client,
            collection_name,
            "rank",
            FieldType::Integer,
            qdrant_client::qdrant::IntegerIndexParamsBuilder::new(true, false).build(),
        )
        .await?;

        let mut payload1 = qdrant_client::Payload::new();
        payload1.insert("rank", 20_i64);
        let mut payload2 = qdrant_client::Payload::new();
        payload2.insert("rank", 10_i64);
        let mut payload3 = qdrant_client::Payload::new();
        payload3.insert("rank", 20_i64);
        let mut payload4 = qdrant_client::Payload::new();
        payload4.insert("rank", 20_i64);
        let mut payload5 = qdrant_client::Payload::new();
        payload5.insert("rank", 10_i64);
        let mut payload6 = qdrant_client::Payload::new();
        payload6.insert("rank", 30_i64);

        let points = vec![
            PointStruct::new(1, Vector::new_dense(vec![0.0]), payload1),
            PointStruct::new(2, Vector::new_dense(vec![0.0]), payload2),
            PointStruct::new(3, Vector::new_dense(vec![0.0]), payload3),
            PointStruct::new(4, Vector::new_dense(vec![0.0]), payload4),
            PointStruct::new(5, Vector::new_dense(vec![0.0]), payload5),
            PointStruct::new(6, Vector::new_dense(vec![0.0]), payload6),
        ];
        drop(client.upsert_points(UpsertPointsBuilder::new(collection_name, points)).await?);

        let table_provider = QdrantTableProvider::try_new(client.clone(), collection_name).await?;
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(ctx.session_context().register_table("vectors", Arc::new(table_provider))?);

        let dataframe = ctx.sql(sql::scan::aggregates::INT_FACET.sql).await?;
        let plan = dataframe.clone().create_physical_plan().await?;
        let display =
            datafusion::physical_plan::displayable(plan.as_ref()).indent(true).to_string();
        let batches = dataframe.collect().await?;
        let rows = batches
            .iter()
            .flat_map(|batch| {
                let ranks = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("rank int64 array");
                let totals = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("count int64 array");
                (0..batch.num_rows())
                    .map(|row| (ranks.value(row), totals.value(row)))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        assert_eq!(rows, vec![(20, 3), (10, 2)]);
        assert!(display.contains("QdrantFacetExec"), "{display}");
        assert!(!display.contains("AggregateExec"), "{display}");
        assert!(!display.contains("SortExec"), "{display}");
        assert!(!display.contains("GlobalLimitExec"), "{display}");
        assert!(!display.contains("LocalLimitExec"), "{display}");

        Ok(())
    }

    pub(super) async fn test_table_provider_filters_by_id_and_vector_presence(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let client = create_qdrant_client(&c)?;
        let collection_name = "test_filter_id_and_vector_presence";

        let mut vectors_config = VectorsConfigBuilder::default();
        let _ = vectors_config.add_named_vector_params(
            "text_embedding",
            VectorParamsBuilder::new(3, Distance::Dot).build(),
        );
        let _ = vectors_config.add_named_vector_params(
            "multi_embedding",
            VectorParamsBuilder::new(2, Distance::Dot)
                .multivector_config(MultiVectorConfig {
                    comparator: MultiVectorComparator::MaxSim.into(),
                })
                .build(),
        );

        let _ = client
            .create_collection(
                CreateCollectionBuilder::new(collection_name).vectors_config(vectors_config),
            )
            .await?;

        let mut vectors1 = NamedVectors::default();
        vectors1 = vectors1.add_vector("text_embedding", Vector::new_dense(vec![0.1, 0.2, 0.3]));
        vectors1 = vectors1
            .add_vector("multi_embedding", Vector::new_multi(vec![vec![1.0, 2.0], vec![3.0, 4.0]]));

        let mut vectors2 = NamedVectors::default();
        vectors2 = vectors2.add_vector("text_embedding", Vector::new_dense(vec![0.4, 0.5, 0.6]));

        let mut vectors3 = NamedVectors::default();
        vectors3 = vectors3.add_vector("multi_embedding", Vector::new_multi(vec![vec![7.0, 8.0]]));

        let points = vec![
            PointStruct::new(1, vectors1, qdrant_client::Payload::new()),
            PointStruct::new(2, vectors2, qdrant_client::Payload::new()),
            PointStruct::new(3, vectors3, qdrant_client::Payload::new()),
        ];
        drop(client.upsert_points(UpsertPointsBuilder::new(collection_name, points)).await?);

        let table_provider = QdrantTableProvider::try_new(client.clone(), collection_name).await?;
        let ctx = SessionContext::new();
        drop(ctx.register_table("docs", Arc::new(table_provider))?);

        let id_batches = ctx.sql(sql::scan::filters::IDS_IN.sql).await?.collect().await?;
        let ids = id_batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("id string array")
                    .iter()
                    .map(|value| value.expect("non-null id").parse::<u64>().expect("numeric id"))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(ids, vec![1, 3]);

        let missing_batches =
            ctx.sql(sql::scan::filters::VECTOR_IS_NULL.sql).await?.collect().await?;
        let missing_ids = missing_batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("id string array")
                    .iter()
                    .map(|value| value.expect("non-null id").parse::<u64>().expect("numeric id"))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(missing_ids, vec![3]);

        let mixed_batches =
            ctx.sql(sql::scan::filters::VECTOR_NONNULL_AND_MULTI_NULL.sql).await?.collect().await?;
        let mixed_ids = mixed_batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("id string array")
                    .iter()
                    .map(|value| value.expect("non-null id").parse::<u64>().expect("numeric id"))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(mixed_ids, vec![2]);

        Ok(())
    }

    #[expect(clippy::too_many_lines)]
    pub(super) async fn test_table_provider_filters_by_payload_field(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let client = create_qdrant_client(&c)?;
        let collection_name = "test_filter_payload_field";
        create_scalar_collection(&client, collection_name).await?;
        create_payload_index(
            &client,
            collection_name,
            "rank",
            FieldType::Integer,
            qdrant_client::qdrant::IntegerIndexParamsBuilder::new(false, true).build(),
        )
        .await?;
        create_payload_index(
            &client,
            collection_name,
            "score",
            FieldType::Float,
            FloatIndexParamsBuilder::new().build(),
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
        payload1.insert("rank", 30_i64);
        payload1.insert("score", 1.5_f64);
        payload1.insert("tag", "red");
        payload1.insert("remark", serde_json::Value::Null);
        let mut payload2 = qdrant_client::Payload::new();
        payload2.insert("rank", 10_i64);
        payload2.insert("score", 2.25_f64);
        payload2.insert("tag", "green");
        let mut payload3 = qdrant_client::Payload::new();
        payload3.insert("rank", 20_i64);
        payload3.insert("score", 3.5_f64);
        payload3.insert("tag", "blue");
        payload3.insert("remark", serde_json::json!([]));

        let points = vec![
            PointStruct::new(1, Vector::new_dense(vec![0.0]), payload1),
            PointStruct::new(2, Vector::new_dense(vec![0.0]), payload2),
            PointStruct::new(3, Vector::new_dense(vec![0.0]), payload3),
        ];
        drop(client.upsert_points(UpsertPointsBuilder::new(collection_name, points)).await?);

        let table_provider = QdrantTableProvider::try_new(client.clone(), collection_name).await?;
        let ctx = SessionContext::new();
        drop(ctx.register_table("vectors", Arc::new(table_provider))?);

        let rank_batches = ctx.sql(sql::scan::filters::RANK_GTE.sql).await?.collect().await?;
        let rank_ids = rank_batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("id string array")
                    .iter()
                    .map(|value| value.expect("non-null id").parse::<u64>().expect("numeric id"))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(rank_ids, vec![1, 3]);

        let score_batches = ctx.sql(sql::scan::filters::SCORE_IN.sql).await?.collect().await?;
        let score_ids = score_batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("id string array")
                    .iter()
                    .map(|value| value.expect("non-null id").parse::<u64>().expect("numeric id"))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(score_ids, vec![1, 3]);

        let tag_batches = ctx.sql(sql::scan::filters::TAG_NOT_IN.sql).await?.collect().await?;
        let tag_ids = tag_batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("id string array")
                    .iter()
                    .map(|value| value.expect("non-null id").parse::<u64>().expect("numeric id"))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(tag_ids, vec![2, 3]);

        let or_batches = ctx.sql(sql::scan::filters::TAG_OR.sql).await?.collect().await?;
        let or_ids = or_batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("id string array")
                    .iter()
                    .map(|value| value.expect("non-null id").parse::<u64>().expect("numeric id"))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(or_ids, vec![1, 3]);

        let boolean_batches =
            ctx.sql(sql::scan::filters::TAG_OR_ID_AND_NOT_RANK.sql).await?.collect().await?;
        let boolean_ids = boolean_batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("id string array")
                    .iter()
                    .map(|value| value.expect("non-null id").parse::<u64>().expect("numeric id"))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(boolean_ids, vec![2]);

        let null_batches = ctx.sql(sql::scan::filters::REMARK_IS_NULL.sql).await?.collect().await?;
        let null_ids = null_batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("id string array")
                    .iter()
                    .map(|value| value.expect("non-null id").parse::<u64>().expect("numeric id"))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(null_ids, vec![1, 2]);

        let not_null_batches =
            ctx.sql(sql::scan::filters::REMARK_IS_NOT_NULL.sql).await?.collect().await?;
        let not_null_ids = not_null_batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("id string array")
                    .iter()
                    .map(|value| value.expect("non-null id").parse::<u64>().expect("numeric id"))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(not_null_ids, vec![3]);

        Ok(())
    }

    pub(super) async fn test_table_provider_distinguishes_empty_string_from_null(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let client = create_qdrant_client(&c)?;
        let collection_name = "test_payload_empty_scalar_semantics";
        create_scalar_collection(&client, collection_name).await?;
        create_payload_index(
            &client,
            collection_name,
            "tag",
            FieldType::Keyword,
            qdrant_client::qdrant::KeywordIndexParamsBuilder::default().build(),
        )
        .await?;

        let mut payload1 = qdrant_client::Payload::new();
        payload1.insert("tag", "");
        let mut payload3 = qdrant_client::Payload::new();
        payload3.insert("tag", "blue");
        let mut payload4 = qdrant_client::Payload::new();
        payload4.insert("tag", serde_json::Value::Null);

        let points = vec![
            PointStruct::new(1, Vector::new_dense(vec![0.0]), payload1),
            PointStruct::new(2, Vector::new_dense(vec![0.0]), qdrant_client::Payload::new()),
            PointStruct::new(3, Vector::new_dense(vec![0.0]), payload3),
            PointStruct::new(4, Vector::new_dense(vec![0.0]), payload4),
        ];
        drop(client.upsert_points(UpsertPointsBuilder::new(collection_name, points)).await?);

        let table_provider = QdrantTableProvider::try_new(client.clone(), collection_name).await?;
        let ctx = SessionContext::new();
        drop(ctx.register_table("vectors", Arc::new(table_provider))?);

        let empty_batches =
            ctx.sql(sql::scan::filters::EMPTY_STRING_EQ.sql).await?.collect().await?;
        let empty_ids = empty_batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("id string array")
                    .iter()
                    .map(|value| value.expect("non-null id").parse::<u64>().expect("numeric id"))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(empty_ids, vec![1]);

        let null_batches =
            ctx.sql(sql::scan::filters::EMPTY_STRING_IS_NULL.sql).await?.collect().await?;
        let null_ids = null_batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("id string array")
                    .iter()
                    .map(|value| value.expect("non-null id").parse::<u64>().expect("numeric id"))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(null_ids, vec![2, 4]);

        let not_null_batches =
            ctx.sql(sql::scan::filters::EMPTY_STRING_IS_NOT_NULL.sql).await?.collect().await?;
        let not_null_ids = not_null_batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("id string array")
                    .iter()
                    .map(|value| value.expect("non-null id").parse::<u64>().expect("numeric id"))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(not_null_ids, vec![1, 3]);

        Ok(())
    }

    pub(super) async fn test_table_provider_pushes_down_count_star(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let client = create_qdrant_client(&c)?;
        let collection_name = "test_count_pushdown";
        create_scalar_collection(&client, collection_name).await?;
        create_payload_index(
            &client,
            collection_name,
            "rank",
            FieldType::Integer,
            qdrant_client::qdrant::IntegerIndexParamsBuilder::new(false, true).build(),
        )
        .await?;

        let mut payload1 = qdrant_client::Payload::new();
        payload1.insert("rank", 30_i64);
        let mut payload2 = qdrant_client::Payload::new();
        payload2.insert("rank", 10_i64);
        let mut payload3 = qdrant_client::Payload::new();
        payload3.insert("rank", 20_i64);

        let points = vec![
            PointStruct::new(1, Vector::new_dense(vec![0.0]), payload1),
            PointStruct::new(2, Vector::new_dense(vec![0.0]), payload2),
            PointStruct::new(3, Vector::new_dense(vec![0.0]), payload3),
        ];
        drop(client.upsert_points(UpsertPointsBuilder::new(collection_name, points)).await?);

        let table_provider = QdrantTableProvider::try_new(client.clone(), collection_name).await?;
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(ctx.session_context().register_table("vectors", Arc::new(table_provider))?);

        let dataframe = ctx.sql(sql::scan::aggregates::COUNT_RANK_GTE.sql).await?;
        let plan = dataframe.clone().create_physical_plan().await?;
        let display =
            datafusion::physical_plan::displayable(plan.as_ref()).indent(true).to_string();
        let batches = dataframe.collect().await?;
        let values = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("count int64 array")
                    .iter()
                    .map(|value| value.expect("non-null count"))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        assert_eq!(values, vec![2]);
        assert!(display.contains("QdrantCountExec"), "{display}");
        assert!(!display.contains("AggregateExec"), "{display}");

        Ok(())
    }

    pub(super) async fn test_table_provider_pushes_down_keyword_facet(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let client = create_qdrant_client(&c)?;
        let collection_name = "test_keyword_facet_pushdown";
        create_scalar_collection(&client, collection_name).await?;
        create_payload_index(
            &client,
            collection_name,
            "rank",
            FieldType::Integer,
            qdrant_client::qdrant::IntegerIndexParamsBuilder::new(false, true).build(),
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
        payload1.insert("rank", 30_i64);
        payload1.insert("tag", "red");
        let mut payload2 = qdrant_client::Payload::new();
        payload2.insert("rank", 10_i64);
        payload2.insert("tag", "blue");
        let mut payload3 = qdrant_client::Payload::new();
        payload3.insert("rank", 20_i64);
        payload3.insert("tag", "red");
        let mut payload4 = qdrant_client::Payload::new();
        payload4.insert("rank", 5_i64);
        payload4.insert("tag", "green");

        let points = vec![
            PointStruct::new(1, Vector::new_dense(vec![0.0]), payload1),
            PointStruct::new(2, Vector::new_dense(vec![0.0]), payload2),
            PointStruct::new(3, Vector::new_dense(vec![0.0]), payload3),
            PointStruct::new(4, Vector::new_dense(vec![0.0]), payload4),
        ];
        drop(client.upsert_points(UpsertPointsBuilder::new(collection_name, points)).await?);

        let table_provider = QdrantTableProvider::try_new(client.clone(), collection_name).await?;
        let ctx = QdrantSessionContext::from(SessionContext::new());
        drop(ctx.session_context().register_table("vectors", Arc::new(table_provider))?);

        let dataframe = ctx.sql(sql::scan::aggregates::TAG_FACET_WITH_FILTER.sql).await?;
        let plan = dataframe.clone().create_physical_plan().await?;
        let display =
            datafusion::physical_plan::displayable(plan.as_ref()).indent(true).to_string();
        let batches = dataframe.collect().await?;
        let rows = batches
            .iter()
            .flat_map(|batch| {
                let tags = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("tag string array");
                let totals = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("count int64 array");
                (0..batch.num_rows())
                    .map(|row| (tags.value(row).to_owned(), totals.value(row)))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        assert_eq!(rows, vec![("red".to_owned(), 2), ("blue".to_owned(), 1)]);
        assert!(display.contains("QdrantFacetExec"), "{display}");
        assert!(!display.contains("AggregateExec"), "{display}");
        assert!(!display.contains("SortExec"), "{display}");
        assert!(!display.contains("GlobalLimitExec"), "{display}");
        assert!(!display.contains("LocalLimitExec"), "{display}");

        Ok(())
    }

    pub(super) async fn test_table_provider_unnamed(c: Arc<QdrantContainer>) -> Result<()> {
        let client = create_qdrant_client(&c)?;
        let collection_name = "test_unnamed_canonical";

        let _ = client
            .create_collection(
                CreateCollectionBuilder::new(collection_name)
                    .vectors_config(VectorParamsBuilder::new(3, Distance::Dot)),
            )
            .await?;

        let mut payload1 = qdrant_client::Payload::new();
        payload1.insert("title", "Unnamed Point 1");
        let mut payload2 = qdrant_client::Payload::new();
        payload2.insert("title", "Unnamed Point 2");

        let points = vec![
            PointStruct::new(1, Vector::new_dense(vec![0.1, 0.2, 0.3]), payload1),
            PointStruct::new(2, Vector::new_dense(vec![0.4, 0.5, 0.6]), payload2),
        ];
        drop(client.upsert_points(UpsertPointsBuilder::new(collection_name, points)).await?);

        let table_provider = QdrantTableProvider::try_new(client.clone(), collection_name).await?;
        let ctx = SessionContext::new();
        drop(ctx.register_table("vectors", Arc::new(table_provider))?);

        let batches = ctx.sql(sql::scan::projection::UNNAMED_FULL.sql).await?.collect().await?;
        let batch = batches.into_iter().next().expect("single batch");
        let schema = batch.schema();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(field_names(schema.as_ref()), vec!["id", "payload", "vector"]);

        let vector_field = schema.field_with_name("vector").expect("vector field present");
        assert_eq!(QdrantFieldBinding::from_field(vector_field).dense_vector_width(), Some(3));
        let vector_array = batch
            .column(schema.index_of("vector").expect("vector index"))
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .expect("unnamed vector array");
        let vector_view =
            fixed_size_list_as_array2::<Float32Type>(vector_array).expect("vector ndarray view");
        assert_eq!(vector_view.shape(), &[2, 3]);
        assert_f32_eq(vector_view[[0, 1]], 0.2);
        assert_f32_eq(vector_view[[1, 0]], 0.4);

        Ok(())
    }

    pub(super) async fn test_qdrant_raw_ordered_scroll_integer_contracts(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let client = create_qdrant_client(&c)?;
        let integer_collection = "test_ordered_scroll_integer";
        create_scalar_collection_with_shards(&client, integer_collection, 2).await?;
        create_payload_index(
            &client,
            integer_collection,
            "rank",
            FieldType::Integer,
            qdrant_client::qdrant::IntegerIndexParamsBuilder::new(false, true).build(),
        )
        .await?;
        assert_single_peer_ordered_scroll_contract(&client, integer_collection, 2).await?;
        let integer_points = vec![
            scalar_point(1, "rank", 10_i64),
            scalar_point(2, "rank", 10_i64),
            scalar_point(3, "rank", 10_i64),
            scalar_point(4, "rank", 20_i64),
            scalar_point(5, "rank", 20_i64),
            scalar_point(6, "rank", 30_i64),
        ];
        drop(
            client
                .upsert_points(UpsertPointsBuilder::new(integer_collection, integer_points))
                .await?,
        );

        let ascending = collect_ordered_pages(
            &client,
            integer_collection,
            "rank",
            Direction::Asc,
            2,
            int_order_value,
            start_from::Value::Integer,
        )
        .await?;
        let ascending_values = ascending.iter().map(|(_, value)| *value).collect::<Vec<_>>();
        let ascending_ids = ascending.iter().map(|(id, _)| *id).collect::<Vec<_>>();
        assert_eq!(ascending_values, vec![10, 10, 10, 20, 20, 30]);
        assert_eq!(
            ascending_ids.iter().copied().collect::<BTreeSet<_>>(),
            (1_u64..=6).collect::<BTreeSet<_>>(),
        );

        let descending = collect_ordered_pages(
            &client,
            integer_collection,
            "rank",
            Direction::Desc,
            2,
            int_order_value,
            start_from::Value::Integer,
        )
        .await?;
        let descending_values = descending.iter().map(|(_, value)| *value).collect::<Vec<_>>();
        let descending_ids = descending.iter().map(|(id, _)| *id).collect::<Vec<_>>();
        assert_eq!(descending_values, vec![30, 20, 20, 10, 10, 10]);
        assert_eq!(
            descending_ids.iter().copied().collect::<BTreeSet<_>>(),
            (1_u64..=6).collect::<BTreeSet<_>>(),
        );

        Ok(())
    }

    pub(super) async fn test_qdrant_raw_ordered_scroll_float_contracts(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let client = create_qdrant_client(&c)?;
        let float_collection = "test_ordered_scroll_float";
        create_scalar_collection(&client, float_collection).await?;
        create_payload_index(
            &client,
            float_collection,
            "score",
            FieldType::Float,
            FloatIndexParamsBuilder::new().build(),
        )
        .await?;
        let float_points = vec![
            scalar_point(1, "score", 1.5_f64),
            scalar_point(2, "score", 1.5_f64),
            scalar_point(3, "score", 2.25_f64),
            scalar_point(4, "score", 3.5_f64),
        ];
        drop(client.upsert_points(UpsertPointsBuilder::new(float_collection, float_points)).await?);

        let float_page =
            scroll_ordered_page(&client, float_collection, "score", Direction::Asc, None, &[], 3)
                .await?;
        assert!(float_page.next_page_offset.is_none());
        assert_eq!(float_page.result.iter().map(float_order_value).collect::<Vec<_>>(), vec![
            1.5, 1.5, 2.25
        ],);
        let float_pages = collect_ordered_pages(
            &client,
            float_collection,
            "score",
            Direction::Asc,
            2,
            float_order_value,
            start_from::Value::Float,
        )
        .await?;
        let float_values = float_pages.iter().map(|(_, value)| *value).collect::<Vec<_>>();
        let float_ids = float_pages.iter().map(|(id, _)| *id).collect::<Vec<_>>();
        assert_eq!(float_values, vec![1.5, 1.5, 2.25, 3.5]);
        assert_eq!(
            float_ids.iter().copied().collect::<BTreeSet<_>>(),
            (1_u64..=4).collect::<BTreeSet<_>>(),
        );

        Ok(())
    }

    pub(super) async fn test_qdrant_raw_ordered_scroll_datetime_contracts(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let client = create_qdrant_client(&c)?;
        let datetime_collection = "test_ordered_scroll_datetime";
        create_scalar_collection(&client, datetime_collection).await?;
        create_payload_index(
            &client,
            datetime_collection,
            "created_at",
            FieldType::Datetime,
            qdrant_client::qdrant::DatetimeIndexParamsBuilder::default().build(),
        )
        .await?;
        let datetime_points = vec![
            scalar_point(1, "created_at", "2024-01-01T00:00:00Z"),
            scalar_point(2, "created_at", "2024-01-01T00:00:00Z"),
            scalar_point(3, "created_at", "2024-01-02T00:00:00Z"),
            scalar_point(4, "created_at", "2024-01-03T00:00:00Z"),
        ];
        drop(
            client
                .upsert_points(UpsertPointsBuilder::new(datetime_collection, datetime_points))
                .await?,
        );

        let datetime_page = scroll_ordered_page(
            &client,
            datetime_collection,
            "created_at",
            Direction::Asc,
            None,
            &[],
            2,
        )
        .await?;
        assert!(datetime_page.next_page_offset.is_none());
        assert_eq!(datetime_page.result.iter().map(int_order_value).collect::<Vec<_>>(), vec![
            1_704_067_200_000_000,
            1_704_067_200_000_000
        ],);

        let datetime_pages = collect_ordered_pages(
            &client,
            datetime_collection,
            "created_at",
            Direction::Asc,
            2,
            int_order_value,
            start_from::Value::Integer,
        )
        .await?;
        let datetime_values = datetime_pages.iter().map(|(_, value)| *value).collect::<Vec<_>>();
        let datetime_ids = datetime_pages.iter().map(|(id, _)| *id).collect::<Vec<_>>();
        assert_eq!(datetime_values, vec![
            1_704_067_200_000_000,
            1_704_067_200_000_000,
            1_704_153_600_000_000,
            1_704_240_000_000_000,
        ],);
        assert_eq!(
            datetime_ids.iter().copied().collect::<BTreeSet<_>>(),
            (1_u64..=4).collect::<BTreeSet<_>>(),
        );

        Ok(())
    }

    pub(super) async fn test_qdrant_raw_ordered_scroll_rejects_integer_index_without_range(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let client = create_qdrant_client(&c)?;
        let collection_name = "test_ordered_scroll_integer_no_range";
        create_scalar_collection(&client, collection_name).await?;
        create_payload_index(
            &client,
            collection_name,
            "rank",
            FieldType::Integer,
            qdrant_client::qdrant::IntegerIndexParamsBuilder::new(true, false).build(),
        )
        .await?;
        let points = vec![scalar_point(1, "rank", 10_i64), scalar_point(2, "rank", 20_i64)];
        drop(client.upsert_points(UpsertPointsBuilder::new(collection_name, points)).await?);

        let error =
            scroll_ordered_page(&client, collection_name, "rank", Direction::Asc, None, &[], 2)
                .await
                .expect_err("integer order_by should reject indices without range support");
        let message = error.to_string();
        assert!(message.contains("range") || message.contains("order_by"), "{message}");

        Ok(())
    }

    pub(super) async fn test_qdrant_raw_payload_null_contracts(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let client = create_qdrant_client(&c)?;
        let collection_name = "test_payload_null_contracts";
        create_scalar_collection(&client, collection_name).await?;
        create_payload_index(
            &client,
            collection_name,
            "remark",
            FieldType::Keyword,
            qdrant_client::qdrant::KeywordIndexParamsBuilder::default().build(),
        )
        .await?;

        let mut payload1 = qdrant_client::Payload::new();
        payload1.insert("remark", serde_json::Value::Null);
        let mut payload3 = qdrant_client::Payload::new();
        payload3.insert("remark", "ready");

        let points = vec![
            PointStruct::new(1, Vector::new_dense(vec![0.0]), payload1),
            PointStruct::new(2, Vector::new_dense(vec![0.0]), qdrant_client::Payload::new()),
            PointStruct::new(3, Vector::new_dense(vec![0.0]), payload3),
            PointStruct::new(4, Vector::new_dense(vec![0.0]), qdrant_client::Payload::new()),
        ];
        drop(client.upsert_points(UpsertPointsBuilder::new(collection_name, points)).await?);

        let mut payload4 = qdrant_client::Payload::new();
        payload4.insert("remark", serde_json::Value::Null);
        drop(
            client
                .set_payload(
                    SetPayloadPointsBuilder::new(collection_name, payload4)
                        .points_selector([4_u64])
                        .wait(true),
                )
                .await?,
        );

        let all = client
            .scroll(
                ScrollPointsBuilder::new(collection_name)
                    .limit(10)
                    .with_payload(true)
                    .with_vectors(false),
            )
            .await?;
        let all_ids = all.result.iter().map(point_num).collect::<Vec<_>>();
        assert_eq!(all_ids, vec![1, 2, 3, 4]);
        assert!(all.result[0].try_get("remark").is_some_and(Value::is_null));
        assert!(all.result[1].try_get("remark").is_none());
        assert_eq!(
            all.result[2].try_get("remark").and_then(|value| value.as_str().map(String::as_str)),
            Some("ready")
        );
        assert!(all.result[3].try_get("remark").is_some_and(Value::is_null));

        let is_null = client
            .scroll(
                ScrollPointsBuilder::new(collection_name)
                    .limit(10)
                    .with_payload(true)
                    .with_vectors(false)
                    .filter(Filter::all([Condition::is_null("remark")])),
            )
            .await?;
        let is_null_ids = is_null.result.iter().map(point_num).collect::<Vec<_>>();
        assert_eq!(is_null_ids, vec![1, 4]);

        let is_empty = client
            .scroll(
                ScrollPointsBuilder::new(collection_name)
                    .limit(10)
                    .with_payload(true)
                    .with_vectors(false)
                    .filter(Filter::all([Condition::is_empty("remark")])),
            )
            .await?;
        let is_empty_ids = is_empty.result.iter().map(point_num).collect::<Vec<_>>();
        assert_eq!(is_empty_ids, vec![1, 2, 4]);

        let not_null = client
            .scroll(
                ScrollPointsBuilder::new(collection_name)
                    .limit(10)
                    .with_payload(true)
                    .with_vectors(false)
                    .filter(Filter::must_not([Condition::is_null("remark")])),
            )
            .await?;
        let not_null_ids = not_null.result.iter().map(point_num).collect::<Vec<_>>();
        assert_eq!(not_null_ids, vec![2, 3]);

        Ok(())
    }

    #[expect(clippy::too_many_lines)]
    pub(super) async fn test_qdrant_raw_payload_empty_contracts(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let client = create_qdrant_client(&c)?;
        let collection_name = "test_payload_empty_contracts";
        create_scalar_collection(&client, collection_name).await?;

        let mut missing = qdrant_client::Payload::new();
        missing.insert("kind", "missing");

        let mut nulls = qdrant_client::Payload::new();
        nulls.insert("kind", "null");
        nulls.insert("text", serde_json::Value::Null);
        nulls.insert("list", serde_json::Value::Null);
        nulls.insert("obj", serde_json::Value::Null);

        let mut empties = qdrant_client::Payload::new();
        empties.insert("kind", "empty");
        empties.insert("text", "");
        empties.insert("list", serde_json::json!([]));
        empties.insert("obj", serde_json::json!({}));

        let mut values = qdrant_client::Payload::new();
        values.insert("kind", "value");
        values.insert("text", "x");
        values.insert("list", serde_json::json!([1]));
        values.insert("obj", serde_json::json!({"k": 1}));

        let points = vec![
            PointStruct::new(1, Vector::new_dense(vec![0.0]), missing),
            PointStruct::new(2, Vector::new_dense(vec![0.0]), nulls),
            PointStruct::new(3, Vector::new_dense(vec![0.0]), empties),
            PointStruct::new(4, Vector::new_dense(vec![0.0]), values),
        ];
        drop(client.upsert_points(UpsertPointsBuilder::new(collection_name, points)).await?);

        let text_empty = client
            .scroll(
                ScrollPointsBuilder::new(collection_name)
                    .limit(10)
                    .with_payload(true)
                    .with_vectors(false)
                    .filter(Filter::all([Condition::is_empty("text")])),
            )
            .await?;
        let text_empty_ids = text_empty.result.iter().map(point_num).collect::<Vec<_>>();

        let list_empty = client
            .scroll(
                ScrollPointsBuilder::new(collection_name)
                    .limit(10)
                    .with_payload(true)
                    .with_vectors(false)
                    .filter(Filter::all([Condition::is_empty("list")])),
            )
            .await?;
        let list_empty_ids = list_empty.result.iter().map(point_num).collect::<Vec<_>>();

        let obj_empty = client
            .scroll(
                ScrollPointsBuilder::new(collection_name)
                    .limit(10)
                    .with_payload(true)
                    .with_vectors(false)
                    .filter(Filter::all([Condition::is_empty("obj")])),
            )
            .await?;
        let obj_empty_ids = obj_empty.result.iter().map(point_num).collect::<Vec<_>>();

        let text_exists = client
            .scroll(
                ScrollPointsBuilder::new(collection_name)
                    .limit(10)
                    .with_payload(true)
                    .with_vectors(false)
                    .filter(Filter::all([Condition::values_count(
                        "text",
                        qdrant_client::qdrant::ValuesCount { gte: Some(0), ..Default::default() },
                    )])),
            )
            .await?;
        let text_exists_ids = text_exists.result.iter().map(point_num).collect::<Vec<_>>();

        let list_exists = client
            .scroll(
                ScrollPointsBuilder::new(collection_name)
                    .limit(10)
                    .with_payload(true)
                    .with_vectors(false)
                    .filter(Filter::all([Condition::values_count(
                        "list",
                        qdrant_client::qdrant::ValuesCount { gte: Some(0), ..Default::default() },
                    )])),
            )
            .await?;
        let list_exists_ids = list_exists.result.iter().map(point_num).collect::<Vec<_>>();

        let obj_exists = client
            .scroll(
                ScrollPointsBuilder::new(collection_name)
                    .limit(10)
                    .with_payload(true)
                    .with_vectors(false)
                    .filter(Filter::all([Condition::values_count(
                        "obj",
                        qdrant_client::qdrant::ValuesCount { gte: Some(0), ..Default::default() },
                    )])),
            )
            .await?;
        let obj_exists_ids = obj_exists.result.iter().map(point_num).collect::<Vec<_>>();

        let text_zero = client
            .scroll(
                ScrollPointsBuilder::new(collection_name)
                    .limit(10)
                    .with_payload(true)
                    .with_vectors(false)
                    .filter(Filter::all([Condition::values_count(
                        "text",
                        qdrant_client::qdrant::ValuesCount {
                            gte: Some(0),
                            lte: Some(0),
                            ..Default::default()
                        },
                    )])),
            )
            .await?;
        let text_zero_ids = text_zero.result.iter().map(point_num).collect::<Vec<_>>();

        let text_one = client
            .scroll(
                ScrollPointsBuilder::new(collection_name)
                    .limit(10)
                    .with_payload(true)
                    .with_vectors(false)
                    .filter(Filter::all([Condition::values_count(
                        "text",
                        qdrant_client::qdrant::ValuesCount {
                            gte: Some(1),
                            lte: Some(1),
                            ..Default::default()
                        },
                    )])),
            )
            .await?;
        let text_one_ids = text_one.result.iter().map(point_num).collect::<Vec<_>>();

        let list_zero = client
            .scroll(
                ScrollPointsBuilder::new(collection_name)
                    .limit(10)
                    .with_payload(true)
                    .with_vectors(false)
                    .filter(Filter::all([Condition::values_count(
                        "list",
                        qdrant_client::qdrant::ValuesCount {
                            gte: Some(0),
                            lte: Some(0),
                            ..Default::default()
                        },
                    )])),
            )
            .await?;
        let list_zero_ids = list_zero.result.iter().map(point_num).collect::<Vec<_>>();

        let list_one = client
            .scroll(
                ScrollPointsBuilder::new(collection_name)
                    .limit(10)
                    .with_payload(true)
                    .with_vectors(false)
                    .filter(Filter::all([Condition::values_count(
                        "list",
                        qdrant_client::qdrant::ValuesCount {
                            gte: Some(1),
                            lte: Some(1),
                            ..Default::default()
                        },
                    )])),
            )
            .await?;
        let list_one_ids = list_one.result.iter().map(point_num).collect::<Vec<_>>();

        let obj_zero = client
            .scroll(
                ScrollPointsBuilder::new(collection_name)
                    .limit(10)
                    .with_payload(true)
                    .with_vectors(false)
                    .filter(Filter::all([Condition::values_count(
                        "obj",
                        qdrant_client::qdrant::ValuesCount {
                            gte: Some(0),
                            lte: Some(0),
                            ..Default::default()
                        },
                    )])),
            )
            .await?;
        let obj_zero_ids = obj_zero.result.iter().map(point_num).collect::<Vec<_>>();

        let obj_one = client
            .scroll(
                ScrollPointsBuilder::new(collection_name)
                    .limit(10)
                    .with_payload(true)
                    .with_vectors(false)
                    .filter(Filter::all([Condition::values_count(
                        "obj",
                        qdrant_client::qdrant::ValuesCount {
                            gte: Some(1),
                            lte: Some(1),
                            ..Default::default()
                        },
                    )])),
            )
            .await?;
        let obj_one_ids = obj_one.result.iter().map(point_num).collect::<Vec<_>>();

        assert_eq!(text_empty_ids, vec![1, 2]);
        assert_eq!(list_empty_ids, vec![1, 2, 3]);
        assert_eq!(obj_empty_ids, vec![1, 2]);
        assert_eq!(text_exists_ids, vec![2, 3, 4]);
        assert_eq!(list_exists_ids, vec![2, 3, 4]);
        assert_eq!(obj_exists_ids, vec![2, 3, 4]);
        assert_eq!(text_zero_ids, vec![2]);
        assert_eq!(text_one_ids, vec![3, 4]);
        assert_eq!(list_zero_ids, vec![2, 3]);
        assert_eq!(list_one_ids, vec![4]);
        assert_eq!(obj_zero_ids, vec![2]);
        assert_eq!(obj_one_ids, vec![3, 4]);

        Ok(())
    }

    pub(super) async fn test_qdrant_raw_integer_facet_contracts(
        c: Arc<QdrantContainer>,
    ) -> Result<()> {
        let client = create_qdrant_client(&c)?;
        let collection_name = "test_integer_facet_contracts";
        create_scalar_collection(&client, collection_name).await?;
        create_payload_index(
            &client,
            collection_name,
            "rank",
            FieldType::Integer,
            qdrant_client::qdrant::IntegerIndexParamsBuilder::new(true, false).build(),
        )
        .await?;

        let points = vec![
            scalar_point(1, "rank", 20_i64),
            scalar_point(2, "rank", 10_i64),
            scalar_point(3, "rank", 20_i64),
            scalar_point(4, "rank", 20_i64),
            scalar_point(5, "rank", 10_i64),
            scalar_point(6, "rank", 30_i64),
        ];
        drop(client.upsert_points(UpsertPointsBuilder::new(collection_name, points)).await?);

        let info = client.collection_info(collection_name).await?;
        let info = info.result.expect("collection info result");
        let rank_info = info.payload_schema.get("rank").expect("rank payload schema");
        assert_eq!(
            PayloadSchemaType::try_from(rank_info.data_type).ok(),
            Some(PayloadSchemaType::Integer),
        );
        match rank_info.params.as_ref().and_then(|params| params.index_params.as_ref()) {
            Some(payload_index_params::IndexParams::IntegerIndexParams(params)) => {
                assert_eq!(params.lookup, Some(true));
                assert_eq!(params.range, Some(false));
            }
            _ => panic!("expected integer payload index params"),
        }

        let facet = client
            .facet(FacetCountsBuilder::new(collection_name, "rank").exact(true).limit(2))
            .await?;
        let rows = facet
            .hits
            .into_iter()
            .map(|hit| {
                let value = hit.value.and_then(|value| value.variant).expect("facet value");
                let facet_value::Variant::IntegerValue(value) = value else {
                    panic!("expected integer facet value");
                };
                (value, hit.count)
            })
            .collect::<Vec<_>>();

        assert_eq!(rows, vec![(20, 3), (10, 2)]);

        Ok(())
    }
}
