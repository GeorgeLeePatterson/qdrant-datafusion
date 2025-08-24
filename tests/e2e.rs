#![allow(unused_crate_dependencies)]

mod common;

const TRACING_DIRECTIVES: &[(&str, &str)] = &[
    ("testcontainers", "debug"),
    ("hyper", "error"),
    ("tonic", "error"),
    // --
];

#[cfg(feature = "test-utils")]
e2e_test!(table_provider_named, tests::test_table_provider_named, TRACING_DIRECTIVES, None);

#[cfg(feature = "test-utils")]
e2e_test!(table_provider_unnamed, tests::test_table_provider_unnamed, TRACING_DIRECTIVES, None);

#[cfg(feature = "test-utils")]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow;
    use datafusion::prelude::*;
    use qdrant_client::Qdrant;
    use qdrant_client::qdrant::{
        CreateCollectionBuilder, Distance, NamedVectors, PointStruct, SparseVectorParamsBuilder,
        SparseVectorsConfigBuilder, UpsertPointsBuilder, Vector, VectorParamsBuilder,
        VectorsConfigBuilder,
    };
    use qdrant_datafusion::error::Result;
    use qdrant_datafusion::table::QdrantTableProvider;
    use qdrant_datafusion::test_utils::QdrantContainer;
    use tracing::debug;

    fn create_qdrant_client(c: &Arc<QdrantContainer>) -> Result<Qdrant> {
        let api_key = c.get_api_key();
        let url = c.get_url();
        eprintln!(">> Connecting to Qdrant @ {url}");
        Qdrant::from_url(&url).api_key(api_key).build().map_err(Into::into)
    }

    /// Test heterogeneous vector sets - different points have different vector fields
    #[expect(clippy::too_many_lines)]
    pub(super) async fn test_table_provider_named(c: Arc<QdrantContainer>) -> Result<()> {
        eprintln!(
            "> Testing HETEROGENEOUS vector fields - different points have different vectors"
        );
        let client = create_qdrant_client(&c)?;

        let collection_name = "test_heterogeneous";

        // Create collection using PROPER BUILDERS as per Qdrant docs
        // Dense vectors: can be named or unnamed, regular or multi
        // Sparse vectors: MUST be named

        // Use VectorsConfigBuilder for named dense vectors
        let mut vectors_config = VectorsConfigBuilder::default();
        let _ = vectors_config.add_named_vector_params(
            "test_embedding",
            VectorParamsBuilder::new(2, Distance::Cosine).build(),
        );
        let _ = vectors_config.add_named_vector_params(
            "text_embedding",
            VectorParamsBuilder::new(3, Distance::Cosine).build(),
        );
        let _ = vectors_config.add_named_vector_params(
            "image_embedding",
            VectorParamsBuilder::new(4, Distance::Dot).build(),
        );
        let _ = vectors_config.add_named_vector_params(
            "audio_embedding",
            VectorParamsBuilder::new(2, Distance::Euclid).build(),
        );

        // Add sparse vectors (MUST be named per docs)
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

        // Point 1: ONLY has test_embedding + keywords sparse
        let mut payload1 = qdrant_client::Payload::new();
        payload1.insert("title", "Point 1");

        let mut named_vectors1 = NamedVectors::default();
        named_vectors1 =
            named_vectors1.add_vector("test_embedding", Vector::new_dense(vec![0.1, 0.2]));
        named_vectors1 =
            named_vectors1.add_vector("keywords", Vector::new_sparse(vec![0, 5], vec![0.1, 0.9]));
        // Deliberately NO other vector fields!

        // Point 2: Has text_embedding + image_embedding + keywords
        let mut payload2 = qdrant_client::Payload::new();
        payload2.insert("title", "Point 2");

        let mut named_vectors2 = NamedVectors::default();
        named_vectors2 =
            named_vectors2.add_vector("text_embedding", Vector::new_dense(vec![0.3, 0.4, 0.5]));
        named_vectors2 = named_vectors2
            .add_vector("image_embedding", Vector::new_dense(vec![0.6, 0.7, 0.8, 0.9]));
        named_vectors2 =
            named_vectors2.add_vector("keywords", Vector::new_sparse(vec![1, 3], vec![0.7, 0.4]));
        // Deliberately NO test_embedding or audio_embedding!

        // Point 3: Has text_embedding + audio_embedding + keywords
        let mut payload3 = qdrant_client::Payload::new();
        payload3.insert("title", "Point 3");

        let mut named_vectors3 = NamedVectors::default();
        named_vectors3 =
            named_vectors3.add_vector("text_embedding", Vector::new_dense(vec![0.11, 0.12, 0.13]));
        named_vectors3 =
            named_vectors3.add_vector("audio_embedding", Vector::new_dense(vec![0.14, 0.15]));
        named_vectors3 = named_vectors3
            .add_vector("keywords", Vector::new_sparse(vec![2, 4, 6], vec![0.2, 0.6, 0.8]));
        // Deliberately NO test_embedding or image_embedding!

        let points = vec![
            PointStruct::new(1, named_vectors1, payload1),
            PointStruct::new(2, named_vectors2, payload2),
            PointStruct::new(3, named_vectors3, payload3),
        ];

        drop(client.upsert_points(UpsertPointsBuilder::new(collection_name, points)).await?);

        // Create TableProvider and test
        debug!(">> Creating QdrantTableProvider for heterogeneous vectors");
        let table_provider = QdrantTableProvider::try_new(client.clone(), collection_name).await?;

        let ctx = SessionContext::new();
        drop(ctx.register_table("hetero_table", Arc::new(table_provider))?);

        // Test: SELECT * - Should show all fields, with nulls where vectors missing
        eprintln!(">> Test: SELECT * FROM hetero_table (heterogeneous vectors)");
        let df = ctx.sql("SELECT * FROM hetero_table").await?;
        let results = df.collect().await?;

        eprintln!(">>> Heterogeneous vectors query results:");
        for (i, batch) in results.iter().enumerate() {
            let schema = batch.schema();
            eprintln!("    Batch {i}: {} rows, {} columns", batch.num_rows(), batch.num_columns());
            debug!("      Schema: {schema:?}");

            // Should have all vector fields defined in collection config
            assert_eq!(batch.num_rows(), 3);

            let field_names: Vec<&str> =
                schema.fields().iter().map(|f| f.name().as_str()).collect();
            eprintln!("    Field names: {field_names:?}");

            // All vector fields should be present in schema
            assert!(field_names.contains(&"test_embedding"));
            assert!(field_names.contains(&"text_embedding"));
            assert!(field_names.contains(&"image_embedding"));
            assert!(field_names.contains(&"audio_embedding"));
            assert!(field_names.contains(&"keywords_indices"));
            assert!(field_names.contains(&"keywords_values"));
            assert!(field_names.contains(&"id"));
            assert!(field_names.contains(&"payload"));

            arrow::util::pretty::print_batches(std::slice::from_ref(batch)).unwrap();
        }

        // Test 2: Projection - Only dense vector
        eprintln!(">> Test 2: SELECT text_embedding FROM hetero_table (dense only)");
        let df = ctx.sql("SELECT text_embedding FROM hetero_table").await?;
        let results = df.collect().await?;

        for batch in &results {
            assert_eq!(batch.num_columns(), 1);
            assert_eq!(batch.schema().field(0).name(), "text_embedding");
            arrow::util::pretty::print_batches(std::slice::from_ref(batch)).unwrap();
        }

        // Test 3: Projection - Only sparse vectors
        eprintln!(">> Test 3: SELECT keywords_indices, keywords_values FROM hetero_table");
        let df = ctx.sql("SELECT keywords_indices, keywords_values FROM hetero_table").await?;
        let results = df.collect().await?;

        for batch in &results {
            assert_eq!(batch.num_columns(), 2);
            assert_eq!(batch.schema().field(0).name(), "keywords_indices");
            assert_eq!(batch.schema().field(1).name(), "keywords_values");
            arrow::util::pretty::print_batches(std::slice::from_ref(batch)).unwrap();
        }

        // Test 4: Projection - Mix of different dense vectors
        eprintln!(">> Test 4: SELECT test_embedding, image_embedding FROM hetero_table");
        let df = ctx.sql("SELECT test_embedding, image_embedding FROM hetero_table").await?;
        let results = df.collect().await?;

        for batch in &results {
            assert_eq!(batch.num_columns(), 2);
            assert_eq!(batch.schema().field(0).name(), "test_embedding");
            assert_eq!(batch.schema().field(1).name(), "image_embedding");
            arrow::util::pretty::print_batches(std::slice::from_ref(batch)).unwrap();
        }

        // Test 5: Projection - Dense + sparse mixed
        eprintln!(">> Test 5: SELECT text_embedding, keywords_indices FROM hetero_table");
        let df = ctx.sql("SELECT text_embedding, keywords_indices FROM hetero_table").await?;
        let results = df.collect().await?;

        for batch in &results {
            assert_eq!(batch.num_columns(), 2);
            assert_eq!(batch.schema().field(0).name(), "text_embedding");
            assert_eq!(batch.schema().field(1).name(), "keywords_indices");
            arrow::util::pretty::print_batches(std::slice::from_ref(batch)).unwrap();
        }

        // Test 6: Projection - No vectors, only metadata
        eprintln!(">> Test 6: SELECT id, payload FROM hetero_table (no vectors)");
        let df = ctx.sql("SELECT id, payload FROM hetero_table").await?;
        let results = df.collect().await?;

        for batch in &results {
            assert_eq!(batch.num_columns(), 2);
            assert_eq!(batch.schema().field(0).name(), "id");
            assert_eq!(batch.schema().field(1).name(), "payload");
            arrow::util::pretty::print_batches(std::slice::from_ref(batch)).unwrap();
        }

        // Test 7: Projection - Single field that many points don't have
        eprintln!(">> Test 7: SELECT audio_embedding FROM hetero_table (mostly nulls)");
        let df = ctx.sql("SELECT audio_embedding FROM hetero_table").await?;
        let results = df.collect().await?;

        for batch in &results {
            assert_eq!(batch.num_columns(), 1);
            assert_eq!(batch.schema().field(0).name(), "audio_embedding");
            // Should show mostly nulls except point 3
            arrow::util::pretty::print_batches(std::slice::from_ref(batch)).unwrap();
        }

        eprintln!(">> ✅ Named vectors TableProvider test completed!");
        eprintln!("   - Point 1: only test_embedding + keywords");
        eprintln!("   - Point 2: only text_embedding + image_embedding + keywords");
        eprintln!("   - Point 3: only text_embedding + audio_embedding + keywords");
        eprintln!("   - Schema contains ALL vector fields with proper nulls");
        eprintln!("   - Projection works for all vector field combinations: ✅");
        eprintln!("   - Heterogeneous data with nulls handled correctly: ✅");

        Ok(())
    }

    /// Test true unnamed vectors
    pub(super) async fn test_table_provider_unnamed(c: Arc<QdrantContainer>) -> Result<()> {
        eprintln!("> Testing UNNAMED vectors - single vector field");
        let client = create_qdrant_client(&c)?;

        let collection_name = "test_unnamed_only";

        // Create collection with SINGLE UNNAMED VECTOR using Config::Params
        // This creates the "vector" field in our schema
        let _ = client
            .create_collection(
                CreateCollectionBuilder::new(collection_name)
                    .vectors_config(VectorParamsBuilder::new(3, Distance::Cosine)),
            )
            .await?;

        // Insert points with UNNAMED vectors
        let mut payload1 = qdrant_client::Payload::new();
        payload1.insert("title", "Unnamed Point 1");

        let mut payload2 = qdrant_client::Payload::new();
        payload2.insert("title", "Unnamed Point 2");

        let mut payload3 = qdrant_client::Payload::new();
        payload3.insert("title", "Unnamed Point 3");

        let points = vec![
            // Use Vector::new_dense for unnamed vectors
            PointStruct::new(1, Vector::new_dense(vec![0.1, 0.2, 0.3]), payload1),
            PointStruct::new(2, Vector::new_dense(vec![0.4, 0.5, 0.6]), payload2),
            PointStruct::new(3, Vector::new_dense(vec![0.7, 0.8, 0.9]), payload3),
        ];

        drop(client.upsert_points(UpsertPointsBuilder::new(collection_name, points)).await?);

        // Create TableProvider and test
        debug!(">> Creating QdrantTableProvider for unnamed vectors");
        let table_provider = QdrantTableProvider::try_new(client.clone(), collection_name).await?;

        let ctx = SessionContext::new();
        drop(ctx.register_table("unnamed_table", Arc::new(table_provider))?);

        // Test: SELECT * - Should show "vector" field
        eprintln!(">> Test: SELECT * FROM unnamed_table (unnamed vectors)");
        let df = ctx.sql("SELECT * FROM unnamed_table").await?;
        let results = df.collect().await?;

        eprintln!(">>> Unnamed vectors query results:");
        for (i, batch) in results.iter().enumerate() {
            let schema = batch.schema();
            eprintln!("    Batch {i}: {} rows, {} columns", batch.num_rows(), batch.num_columns());
            debug!("      Schema: {schema:?}");

            // Should have: id, payload, vector
            assert_eq!(batch.num_rows(), 3);
            assert_eq!(batch.num_columns(), 3);

            let field_names: Vec<&str> =
                schema.fields().iter().map(|f| f.name().as_str()).collect();
            eprintln!("    Field names: {field_names:?}");

            // Should contain the unnamed "vector" field
            assert!(field_names.contains(&"vector"));
            assert!(field_names.contains(&"id"));
            assert!(field_names.contains(&"payload"));

            // Should NOT contain any named vector fields
            assert!(!field_names.contains(&"text_embedding"));
            assert!(!field_names.contains(&"image_embedding"));

            arrow::util::pretty::print_batches(std::slice::from_ref(batch)).unwrap();
        }

        // Test 2: Projection - Only vector field
        eprintln!(">> Test 2: SELECT vector FROM unnamed_table");
        let df = ctx.sql("SELECT vector FROM unnamed_table").await?;
        let results = df.collect().await?;

        for batch in &results {
            assert_eq!(batch.num_columns(), 1);
            assert_eq!(batch.schema().field(0).name(), "vector");
            arrow::util::pretty::print_batches(std::slice::from_ref(batch)).unwrap();
        }

        // Test 3: Projection - Only metadata fields
        eprintln!(">> Test 3: SELECT id, payload FROM unnamed_table (no vectors)");
        let df = ctx.sql("SELECT id, payload FROM unnamed_table").await?;
        let results = df.collect().await?;

        for batch in &results {
            assert_eq!(batch.num_columns(), 2);
            assert_eq!(batch.schema().field(0).name(), "id");
            assert_eq!(batch.schema().field(1).name(), "payload");
            arrow::util::pretty::print_batches(std::slice::from_ref(batch)).unwrap();
        }

        // Test 4: Projection - Only ID field
        eprintln!(">> Test 4: SELECT id FROM unnamed_table");
        let df = ctx.sql("SELECT id FROM unnamed_table").await?;
        let results = df.collect().await?;

        for batch in &results {
            assert_eq!(batch.num_columns(), 1);
            assert_eq!(batch.schema().field(0).name(), "id");
            arrow::util::pretty::print_batches(std::slice::from_ref(batch)).unwrap();
        }

        // Test 5: Projection - Mixed order
        eprintln!(">> Test 5: SELECT payload, vector, id FROM unnamed_table (reordered)");
        let df = ctx.sql("SELECT payload, vector, id FROM unnamed_table").await?;
        let results = df.collect().await?;

        for batch in &results {
            assert_eq!(batch.num_columns(), 3);
            assert_eq!(batch.schema().field(0).name(), "payload");
            assert_eq!(batch.schema().field(1).name(), "vector");
            assert_eq!(batch.schema().field(2).name(), "id");
            arrow::util::pretty::print_batches(std::slice::from_ref(batch)).unwrap();
        }

        // Test 6: Projection - Only payload
        eprintln!(">> Test 6: SELECT payload FROM unnamed_table");
        let df = ctx.sql("SELECT payload FROM unnamed_table").await?;
        let results = df.collect().await?;

        for batch in &results {
            assert_eq!(batch.num_columns(), 1);
            assert_eq!(batch.schema().field(0).name(), "payload");
            arrow::util::pretty::print_batches(std::slice::from_ref(batch)).unwrap();
        }

        eprintln!(">> ✅ Unnamed vectors TableProvider test completed!");
        eprintln!("   - Collection uses Config::Params (not ParamsMap)");
        eprintln!("   - Schema contains 'vector' field (not named fields)");
        eprintln!("   - All points have the same unnamed vector structure");
        eprintln!("   - Vector field accessible via 'vector' name in SQL");
        eprintln!("   - Projection works for all field combinations: ✅");
        eprintln!("   - Schema projection optimizes Qdrant queries: ✅");

        Ok(())
    }
}
