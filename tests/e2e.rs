#![allow(unused_crate_dependencies)]

mod common;

const TRACING_DIRECTIVES: &[(&str, &str)] =
    &[("testcontainers", "debug"), ("hyper", "error"), ("tonic", "error")];

#[cfg(feature = "test-utils")]
e2e_test!(table_provider_named, tests::test_table_provider_named, TRACING_DIRECTIVES, None);

#[cfg(feature = "test-utils")]
e2e_test!(table_provider_unnamed, tests::test_table_provider_unnamed, TRACING_DIRECTIVES, None);

#[cfg(feature = "test-utils")]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::types::Float32Type;
    use datafusion::arrow::array::{Array, FixedSizeListArray, StringArray, StructArray};
    use datafusion::prelude::*;
    use ndarrow::{csr_matrix_batch_iter, fixed_size_list_as_array2, variable_shape_tensor_iter};
    use qdrant_client::Qdrant;
    use qdrant_client::qdrant::{
        CreateCollectionBuilder, Distance, MultiVectorComparator, MultiVectorConfig, NamedVectors,
        PointStruct, SparseVectorParamsBuilder, SparseVectorsConfigBuilder, UpsertPointsBuilder,
        Vector, VectorParamsBuilder, VectorsConfigBuilder,
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

        let mut vectors1 = NamedVectors::default();
        vectors1 = vectors1.add_vector("text_embedding", Vector::new_dense(vec![0.1, 0.2, 0.3]));
        vectors1 = vectors1
            .add_vector("multi_embedding", Vector::new_multi(vec![vec![1.0, 2.0], vec![3.0, 4.0]]));
        vectors1 = vectors1.add_vector("keywords", Vector::new_sparse(vec![0, 5], vec![0.5, 1.5]));

        let mut vectors2 = NamedVectors::default();
        vectors2 = vectors2.add_vector("text_embedding", Vector::new_dense(vec![0.4, 0.5, 0.6]));
        vectors2 = vectors2.add_vector("multi_embedding", Vector::new_multi(vec![vec![5.0, 6.0]]));
        vectors2 =
            vectors2.add_vector("keywords", Vector::new_sparse(vec![1, 3, 4], vec![0.2, 0.3, 0.4]));

        let points =
            vec![PointStruct::new(1, vectors1, payload1), PointStruct::new(2, vectors2, payload2)];
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

        assert_eq!(batch.num_rows(), 2);
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
        assert_eq!(dense_vector_width(dense_field), Some(3));
        let dense_array = batch
            .column(schema.index_of("text_embedding").expect("dense index"))
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .expect("dense vector array");
        let dense_view =
            fixed_size_list_as_array2::<Float32Type>(dense_array).expect("dense ndarray view");
        assert_eq!(dense_view.shape(), &[2, 3]);
        assert_f32_eq(dense_view[[0, 0]], 0.1);
        assert_f32_eq(dense_view[[1, 2]], 0.6);

        let multi_field =
            schema.field_with_name("multi_embedding").expect("multivector field present");
        assert!(is_multi_vector_field(multi_field));
        assert_eq!(multivector_width(multi_field), Some(2));
        let multi_array = batch
            .column(schema.index_of("multi_embedding").expect("multivector index"))
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("multivector struct array");
        let multi_rows = variable_shape_tensor_iter::<Float32Type>(multi_field, multi_array)
            .expect("multivector iterator")
            .collect::<std::result::Result<Vec<_>, _>>()
            .expect("valid multivector rows");
        assert_eq!(multi_rows[0].1.shape(), &[2, 2]);
        assert_f32_eq(multi_rows[0].1[[1, 1]], 4.0);
        assert_eq!(multi_rows[1].1.shape(), &[1, 2]);
        assert_f32_eq(multi_rows[1].1[[0, 1]], 6.0);

        let sparse_field = schema.field_with_name("keywords").expect("sparse field present");
        assert!(is_sparse_vector_field(sparse_field));
        let sparse_array = batch
            .column(schema.index_of("keywords").expect("sparse index"))
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("sparse struct array");
        let sparse_rows = csr_matrix_batch_iter::<Float32Type>(sparse_field, sparse_array)
            .expect("sparse iterator")
            .collect::<std::result::Result<Vec<_>, _>>()
            .expect("valid sparse rows");
        assert_eq!(sparse_rows[0].1.nrows, 1);
        assert_eq!(sparse_rows[0].1.ncols, 6);
        assert_eq!(sparse_rows[0].1.col_indices, &[0, 5]);
        assert_eq!(sparse_rows[0].1.values, &[0.5, 1.5]);
        assert_eq!(sparse_rows[1].1.ncols, 5);
        assert_eq!(sparse_rows[1].1.col_indices, &[1, 3, 4]);
        assert_eq!(sparse_rows[1].1.values, &[0.2, 0.3, 0.4]);

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
}
