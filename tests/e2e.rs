#![allow(unused_crate_dependencies)]

mod common;

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
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use datafusion::arrow::array::types::Float32Type;
    use datafusion::arrow::array::{Array, FixedSizeListArray, StringArray, StructArray};
    use datafusion::prelude::*;
    use ndarrow::{
        csr_matrix_batch_iter, fixed_size_list_as_array2, fixed_size_list_as_array2_masked,
        variable_shape_tensor_iter,
    };
    use qdrant_client::Qdrant;
    use qdrant_client::qdrant::{
        Condition, CreateCollectionBuilder, CreateFieldIndexCollectionBuilder, Direction, Distance,
        FieldType, Filter, FloatIndexParamsBuilder, MultiVectorComparator, MultiVectorConfig,
        NamedVectors, OrderByBuilder, PointStruct, RetrievedPoint, ScrollPointsBuilder,
        SparseVectorParamsBuilder, SparseVectorsConfigBuilder, UpsertPointsBuilder, Vector,
        VectorParamsBuilder, VectorsConfigBuilder, order_value, payload_index_params, point_id,
        start_from,
    };
    use qdrant_datafusion::arrow::schema::{
        dense_vector_width, is_multi_vector_field, is_sparse_vector_field, multivector_width,
    };
    use qdrant_datafusion::error::Result;
    use qdrant_datafusion::table::QdrantTableProvider;
    use qdrant_datafusion::test_utils::QdrantContainer;

    fn create_qdrant_client(c: &Arc<QdrantContainer>) -> Result<Qdrant> {
        Qdrant::from_url(&c.get_url()).api_key(c.get_api_key()).build().map_err(Into::into)
    }

    fn field_names(schema: &datafusion::arrow::datatypes::Schema) -> Vec<&str> {
        schema.fields().iter().map(|field| field.name().as_str()).collect()
    }

    fn assert_f32_eq(left: f32, right: f32) {
        assert!((left - right).abs() < 1.0e-6, "left={left}, right={right}");
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

    fn scalar_point(
        id: u64,
        field_name: &str,
        value: impl Into<qdrant_client::qdrant::Value>,
    ) -> PointStruct {
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

        let batches = ctx
            .sql(
                "SELECT id, payload, text_embedding, multi_embedding, keywords FROM docs ORDER BY \
                 id",
            )
            .await?
            .collect()
            .await?;
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
        assert_eq!(dense_vector_width(dense_field), Some(3));
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
        assert!(is_multi_vector_field(multi_field));
        assert_eq!(multivector_width(multi_field), Some(2));
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
        assert!(is_sparse_vector_field(sparse_field));
        let sparse_array = batch
            .column(schema.index_of("keywords").expect("sparse index"))
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("sparse struct array");
        assert_eq!(sparse_array.null_count(), 1);
        assert!(!sparse_array.is_null(0));
        assert!(sparse_array.is_null(1));
        assert!(!sparse_array.is_null(2));

        let dense_batches = ctx
            .sql("SELECT text_embedding FROM docs WHERE text_embedding IS NOT NULL ORDER BY id")
            .await?
            .collect()
            .await?;
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

        let multi_batches = ctx
            .sql("SELECT multi_embedding FROM docs WHERE multi_embedding IS NOT NULL ORDER BY id")
            .await?
            .collect()
            .await?;
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

        let sparse_batches = ctx
            .sql("SELECT keywords FROM docs WHERE keywords IS NOT NULL ORDER BY id")
            .await?
            .collect()
            .await?;
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

        let batches = ctx.sql("SELECT id FROM vectors").await?.collect().await?;
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
            qdrant_client::qdrant::IntegerIndexParamsBuilder::new(false, true).build(),
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

        let batches =
            ctx.sql("SELECT id FROM vectors ORDER BY payload:rank").await?.collect().await?;
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

        let id_batches = ctx
            .sql("SELECT id FROM docs WHERE id IN ('1', '3') ORDER BY id")
            .await?
            .collect()
            .await?;
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

        let missing_batches = ctx
            .sql("SELECT id FROM docs WHERE text_embedding IS NULL ORDER BY id")
            .await?
            .collect()
            .await?;
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

        let mixed_batches = ctx
            .sql(
                "SELECT id FROM docs WHERE text_embedding IS NOT NULL AND multi_embedding IS NULL \
                 ORDER BY id",
            )
            .await?
            .collect()
            .await?;
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
        let mut payload2 = qdrant_client::Payload::new();
        payload2.insert("rank", 10_i64);
        payload2.insert("score", 2.25_f64);
        payload2.insert("tag", "green");
        let mut payload3 = qdrant_client::Payload::new();
        payload3.insert("rank", 20_i64);
        payload3.insert("score", 3.5_f64);
        payload3.insert("tag", "blue");

        let points = vec![
            PointStruct::new(1, Vector::new_dense(vec![0.0]), payload1),
            PointStruct::new(2, Vector::new_dense(vec![0.0]), payload2),
            PointStruct::new(3, Vector::new_dense(vec![0.0]), payload3),
        ];
        drop(client.upsert_points(UpsertPointsBuilder::new(collection_name, points)).await?);

        let table_provider = QdrantTableProvider::try_new(client.clone(), collection_name).await?;
        let ctx = SessionContext::new();
        drop(ctx.register_table("vectors", Arc::new(table_provider))?);

        let rank_batches = ctx
            .sql("SELECT id FROM vectors WHERE payload:rank >= 20 ORDER BY id")
            .await?
            .collect()
            .await?;
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

        let score_batches = ctx
            .sql("SELECT id FROM vectors WHERE payload:score IN (1.5, 3.5) ORDER BY id")
            .await?
            .collect()
            .await?;
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

        let tag_batches = ctx
            .sql("SELECT id FROM vectors WHERE payload:tag NOT IN ('red') ORDER BY id")
            .await?
            .collect()
            .await?;
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

        let or_batches = ctx
            .sql(
                "SELECT id FROM vectors WHERE payload:tag = 'red' OR payload:tag = 'blue' ORDER \
                 BY id",
            )
            .await?
            .collect()
            .await?;
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

        let boolean_batches = ctx
            .sql(
                "SELECT id FROM vectors WHERE (payload:tag = 'red' OR id = '2') AND NOT \
                 payload:rank > 20 ORDER BY id",
            )
            .await?
            .collect()
            .await?;
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

        let batches =
            ctx.sql("SELECT id, payload, vector FROM vectors ORDER BY id").await?.collect().await?;
        let batch = batches.into_iter().next().expect("single batch");
        let schema = batch.schema();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(field_names(schema.as_ref()), vec!["id", "payload", "vector"]);

        let vector_field = schema.field_with_name("vector").expect("vector field present");
        assert_eq!(dense_vector_width(vector_field), Some(3));
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
        create_scalar_collection(&client, integer_collection).await?;
        create_payload_index(
            &client,
            integer_collection,
            "rank",
            FieldType::Integer,
            qdrant_client::qdrant::IntegerIndexParamsBuilder::new(false, true).build(),
        )
        .await?;
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
}
