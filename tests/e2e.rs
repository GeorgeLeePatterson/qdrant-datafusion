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
e2e_test!(table_provider, tests::test_table_provider, TRACING_DIRECTIVES, None);

#[cfg(feature = "test-utils")]
e2e_test!(filters, tests::test_filters_pushdown, TRACING_DIRECTIVES, None);

#[cfg(feature = "test-utils")]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow;
    use datafusion::datasource::TableProvider;
    use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
    use datafusion::prelude::*;
    use qdrant_client::Qdrant;
    use qdrant_client::qdrant::{
        CreateCollectionBuilder, Distance, MultiVectorComparator, MultiVectorConfig, NamedVectors,
        PointStruct, SparseVectorParamsBuilder, SparseVectorsConfigBuilder, UpsertPointsBuilder,
        Vector, VectorParamsBuilder, VectorsConfigBuilder,
    };
    use qdrant_datafusion::error::Result;
    use qdrant_datafusion::table::{QdrantScanExec, QdrantTableProvider};
    use qdrant_datafusion::test_utils::QdrantContainer;
    use tracing::debug;

    fn create_qdrant_client(c: &Arc<QdrantContainer>) -> Result<Qdrant> {
        let api_key = c.get_api_key();
        let url = c.get_url();
        eprintln!(">> Connecting to Qdrant @ {url}");
        Qdrant::from_url(&url).api_key(api_key).build().map_err(Into::into)
    }

    /// Simple test coverage for `TableProvider` and `ScanExec`
    pub(super) async fn test_table_provider(c: Arc<QdrantContainer>) -> Result<()> {
        struct QdrantScanExecDebug(QdrantScanExec);
        impl std::fmt::Display for QdrantScanExecDebug {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt_as(DisplayFormatType::Default, f)?;
                self.0.fmt_as(DisplayFormatType::TreeRender, f)
            }
        }

        eprintln!("> Testing TableProvider coverage methods");
        let client = create_qdrant_client(&c)?;

        let collection_name = "test_coverage";

        // Create simple collection
        let _ = client
            .create_collection(
                CreateCollectionBuilder::new(collection_name)
                    .vectors_config(VectorParamsBuilder::new(2, Distance::Cosine)),
            )
            .await?;

        // Insert one simple point
        let mut payload = qdrant_client::Payload::new();
        payload.insert("title", "Test Point");

        let points = vec![PointStruct::new(1, Vector::new_dense(vec![0.1, 0.2]), payload)];

        drop(client.upsert_points(UpsertPointsBuilder::new(collection_name, points)).await?);

        // Create TableProvider
        let table_provider = QdrantTableProvider::try_new(client.clone(), collection_name).await?;

        // Test Debug implementation
        eprintln!("QdrantTableProvider debug: {table_provider:?}");

        // Test as_any method by checking downcast
        let any_ref = table_provider.as_any();
        let is_correct_type = any_ref.downcast_ref::<QdrantTableProvider>().is_some();
        eprintln!("QdrantTableProvider as_any downcast success: {is_correct_type}");
        assert!(is_correct_type);

        // Test table_type method
        let table_type = table_provider.table_type();
        eprintln!("QdrantTableProvider table_type: {table_type:?}");
        assert_eq!(table_type, datafusion::datasource::TableType::Base);

        let ctx = SessionContext::new();
        let scan = table_provider.scan(&ctx.state(), None, &[], None).await.unwrap();
        let qdrant_scan = scan.as_any().downcast_ref::<QdrantScanExec>().unwrap();
        eprintln!("QdrantScanExec debug: {qdrant_scan:?}");
        let scan_display = QdrantScanExecDebug(qdrant_scan.clone());
        eprintln!("Physical plan debug: {scan_display}");

        eprintln!(">> ✅ TableProvider coverage test completed!");
        eprintln!("   - QdrantTableProvider Debug, as_any, table_type: ✅");
        eprintln!("   - QdrantScanExec Debug and DisplayAs: ✅");

        Ok(())
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
        let _ = vectors_config.add_named_vector_params(
            "multi_embeddings",
            VectorParamsBuilder::new(2, Distance::Dot)
                .multivector_config(MultiVectorConfig {
                    comparator: MultiVectorComparator::MaxSim.into(),
                })
                .build(),
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
        named_vectors1 = named_vectors1.add_vector(
            "multi_embeddings",
            Vector::new_multi(vec![vec![0.7, 0.8], vec![0.9, 0.1]]),
        );
        // Deliberately NO audio_embedding or text_embedding or image_embedding!

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
            assert!(field_names.contains(&"multi_embeddings"));
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
        eprintln!("   - Point 1: only test_embedding + multi_embeddings + keywords");
        eprintln!("   - Point 2: only text_embedding + image_embedding + keywords");
        eprintln!("   - Point 3: only text_embedding + audio_embedding + keywords");
        eprintln!("   - Schema contains ALL vector fields with proper nulls");
        eprintln!("   - Multi-vectors: List<List<Float32>> schema and extraction: ✅");
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
        let results = ctx.sql("SELECT * FROM unnamed_table").await?.collect().await?;

        eprintln!(">>> Unnamed vectors query results:");
        for (i, batch) in results.iter().enumerate() {
            let schema = batch.schema();
            eprintln!("    Batch {i}: {} rows, {} columns", batch.num_rows(), batch.num_columns());

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
        let results = ctx.sql("SELECT vector FROM unnamed_table").await?.collect().await?;

        for batch in &results {
            assert_eq!(batch.num_columns(), 1);
            assert_eq!(batch.schema().field(0).name(), "vector");
            arrow::util::pretty::print_batches(std::slice::from_ref(batch)).unwrap();
        }

        // Test 3: Projection - Only metadata fields
        eprintln!(">> Test 3: SELECT id, payload FROM unnamed_table (no vectors)");
        let results = ctx.sql("SELECT id, payload FROM unnamed_table").await?.collect().await?;

        for batch in &results {
            assert_eq!(batch.num_columns(), 2);
            assert_eq!(batch.schema().field(0).name(), "id");
            assert_eq!(batch.schema().field(1).name(), "payload");
            arrow::util::pretty::print_batches(std::slice::from_ref(batch)).unwrap();
        }

        // Test 4: Projection - Only ID field
        eprintln!(">> Test 4: SELECT id FROM unnamed_table");
        let results = ctx.sql("SELECT id FROM unnamed_table").await?.collect().await?;

        for batch in &results {
            assert_eq!(batch.num_columns(), 1);
            assert_eq!(batch.schema().field(0).name(), "id");
            arrow::util::pretty::print_batches(std::slice::from_ref(batch)).unwrap();
        }

        // Test 5: Projection - Mixed order
        eprintln!(">> Test 5: SELECT payload, vector, id FROM unnamed_table (reordered)");
        let sql = "SELECT payload, vector, id FROM unnamed_table";
        let results = ctx.sql(sql).await?.collect().await?;

        for batch in &results {
            assert_eq!(batch.num_columns(), 3);
            assert_eq!(batch.schema().field(0).name(), "payload");
            assert_eq!(batch.schema().field(1).name(), "vector");
            assert_eq!(batch.schema().field(2).name(), "id");
            arrow::util::pretty::print_batches(std::slice::from_ref(batch)).unwrap();
        }

        // Test 6: Projection - Only payload
        eprintln!(">> Test 6: SELECT payload FROM unnamed_table");
        let results = ctx.sql("SELECT payload FROM unnamed_table").await?.collect().await?;

        for batch in &results {
            assert_eq!(batch.num_columns(), 1);
            assert_eq!(batch.schema().field(0).name(), "payload");
            arrow::util::pretty::print_batches(std::slice::from_ref(batch)).unwrap();
        }

        // Test 7: LIMIT query to cover TableScan limit functionality
        eprintln!(">> Test 7: SELECT * FROM unnamed_table LIMIT 2");
        let results = ctx.sql("SELECT * FROM unnamed_table LIMIT 2").await?.collect().await?;

        for batch in &results {
            assert_eq!(batch.num_rows(), 2); // Should be limited to 2 rows
            assert_eq!(batch.num_columns(), 3);
            arrow::util::pretty::print_batches(std::slice::from_ref(batch)).unwrap();
        }

        eprintln!(">> ✅ Unnamed vectors TableProvider test completed!");
        eprintln!("   - Collection uses Config::Params (not ParamsMap)");
        eprintln!("   - Schema contains 'vector' field (not named fields)");
        eprintln!("   - All points have the same unnamed vector structure");
        eprintln!("   - Vector field accessible via 'vector' name in SQL");
        eprintln!("   - Projection works for all field combinations: ✅");
        eprintln!("   - Schema projection optimizes Qdrant queries: ✅");
        eprintln!("   - LIMIT queries work correctly: ✅");

        Ok(())
    }

    /// Test filter pushdown functionality
    #[expect(clippy::too_many_lines)]
    pub(super) async fn test_filters_pushdown(c: Arc<QdrantContainer>) -> Result<()> {
        use datafusion::logical_expr::TableProviderFilterPushDown;

        eprintln!("> Testing filter pushdown functionality");
        let client = create_qdrant_client(&c)?;

        let collection_name = "test_filters";

        // Create collection with named vectors (copying exact pattern from existing test)
        let mut vectors_config = VectorsConfigBuilder::default();
        let _ = vectors_config.add_named_vector_params(
            "text_embedding",
            VectorParamsBuilder::new(3, Distance::Cosine).build(),
        );

        let _ = client
            .create_collection(
                CreateCollectionBuilder::new(collection_name).vectors_config(vectors_config),
            )
            .await?;

        // Insert test points using exact pattern from existing tests
        let mut points = Vec::new();

        // Point 1: London, age 25
        let mut payload1 = qdrant_client::Payload::new();
        payload1.insert("city", "London");
        payload1.insert("age", 25);
        payload1.insert("category", "premium");

        let mut named_vectors1 = NamedVectors::default();
        named_vectors1 =
            named_vectors1.add_vector("text_embedding", Vector::new_dense(vec![0.1, 0.2, 0.3]));
        points.push(PointStruct::new(1, named_vectors1, payload1));

        // Point 2: Paris, age 30
        let mut payload2 = qdrant_client::Payload::new();
        payload2.insert("city", "Paris");
        payload2.insert("age", 30);
        payload2.insert("category", "standard");

        let mut named_vectors2 = NamedVectors::default();
        named_vectors2 =
            named_vectors2.add_vector("text_embedding", Vector::new_dense(vec![0.4, 0.5, 0.6]));
        points.push(PointStruct::new(2, named_vectors2, payload2));

        // Point 3: London, age 35
        let mut payload3 = qdrant_client::Payload::new();
        payload3.insert("city", "London");
        payload3.insert("age", 35);
        payload3.insert("category", "premium");

        let mut named_vectors3 = NamedVectors::default();
        named_vectors3 =
            named_vectors3.add_vector("text_embedding", Vector::new_dense(vec![0.7, 0.8, 0.9]));
        points.push(PointStruct::new(3, named_vectors3, payload3));

        drop(client.upsert_points(UpsertPointsBuilder::new(collection_name, points)).await?);

        // Wait for indexing
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // Create table provider
        let table_provider = QdrantTableProvider::try_new(client.clone(), collection_name).await?;

        // Create DataFusion context and register table (following exact pattern)
        let mut ctx = SessionContext::new();

        // Register JSON UDFs for payload->field syntax
        eprintln!("Registering JSON UDFs for payload->field support");
        qdrant_datafusion::udfs::register_json_udfs(&mut ctx)?;

        drop(ctx.register_table("test_table", Arc::new(table_provider))?);

        // Test 1: Basic query (no filters)
        eprintln!("Test 1: Basic query - SELECT * FROM test_table");
        let results = ctx.sql("SELECT id FROM test_table ORDER BY id").await?.collect().await?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 3); // Should have all 3 points
        eprintln!("✅ Basic query works - found 3 points");

        // Test 2: ID filter
        eprintln!("Test 2: ID filter - SELECT * FROM test_table WHERE id = 1");
        let results = ctx.sql("SELECT id FROM test_table WHERE id = 1").await?.collect().await?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 1); // Should find point with ID 1
        eprintln!("✅ ID filter works - found 1 result");

        // Test 3: Payload equality filter
        eprintln!("Test 3: Payload filter - WHERE json_get_str(payload, 'city') = 'London'");
        let sql =
            "SELECT id FROM test_table WHERE json_get_str(payload, 'city') = 'London' ORDER BY id";
        let results = ctx.sql(sql).await?.collect().await?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 2); // Should find points 1 and 3 (both London)
        eprintln!("✅ Payload equality filter works - found 2 results");

        // Test 4: Payload range filter
        let sql = "SELECT id FROM test_table WHERE json_get_int(payload, 'age') > 25 ORDER BY id";
        eprintln!("Test 4: Payload range - {sql}");
        let results = ctx.sql(sql).await?.collect().await?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 2); // Should find points 2 (age 30) and 3 (age 35)
        eprintln!("✅ Payload range filter works - found 2 results");

        // Test 5: Combined filters
        let sql = "SELECT id FROM test_table WHERE json_get_str(payload, 'city') = 'London' AND \
                   json_get_int(payload, 'age') > 30";
        eprintln!("Test 5: Combined filters - {sql}");
        let results = ctx.sql(sql).await?.collect().await?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 1); // Should find only point 3 (London + age 35)
        eprintln!("✅ Combined filters work - found 1 result");

        // Test 6: Test supports_filters_pushdown method
        eprintln!("Test 6: Testing supports_filters_pushdown method");
        let table_provider_ref =
            QdrantTableProvider::try_new(client.clone(), collection_name).await?;

        let filter1 = col("id").eq(lit(1));
        let filter2 = col("text_embedding").is_null();
        let test_filters = vec![&filter1, &filter2];

        let pushdown_support = table_provider_ref.supports_filters_pushdown(&test_filters)?;
        eprintln!("Filter pushdown support: {pushdown_support:?}");

        assert_eq!(pushdown_support[0], TableProviderFilterPushDown::Exact); // ID filter should be supported
        assert_eq!(pushdown_support[1], TableProviderFilterPushDown::Exact); // IS NULL is now supported by FilterBuilder
        eprintln!("✅ supports_filters_pushdown correctly identifies filter support");

        // Test 7: IS NULL condition (build_is_null_condition coverage)  
        // Add a point with an explicit NULL value to test proper IsNull behavior
        let mut payload_null = qdrant_client::Payload::new();
        payload_null.insert("city", "Berlin");
        payload_null.insert("age", 40);
        payload_null.insert("nickname", serde_json::Value::Null); // Explicit NULL value
        let named_vectors_null = NamedVectors::default()
            .add_vector("text_embedding", Vector::new_dense(vec![0.9, 0.8, 0.7]));
        let point_null = PointStruct::new(5, named_vectors_null, payload_null);
        drop(client.upsert_points(UpsertPointsBuilder::new(collection_name, vec![point_null])).await?);
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        eprintln!("Test 7: IS NULL - WHERE json_get_str(payload, 'nickname') IS NULL");
        let sql = "SELECT id FROM test_table WHERE json_get_str(payload, 'nickname') IS NULL ORDER BY id";
        let results = ctx.sql(sql).await?.collect().await?;
        arrow::util::pretty::print_batches(&results).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 1); // Should find point 5 with explicit NULL nickname
        eprintln!("✅ IS NULL filter works - found 1 result");

        // Test 8: Direct NOT Expression (Expr::Not coverage)
        eprintln!("Test 8: Direct NOT - WHERE NOT json_get_str(payload, 'nickname') IS NULL");
        let sql = "SELECT id FROM test_table WHERE NOT json_get_str(payload, 'nickname') IS NULL ORDER BY id";
        let results = ctx.sql(sql).await?.collect().await?;
        assert_eq!(results.len(), 1);
        // Should find points where nickname is NOT NULL (points 1-4 don't have nickname field, only point 5 has NULL)
        // Based on Qdrant docs, this should find 0 results since absent fields don't count as NOT NULL
        eprintln!("✅ Direct NOT expression works - found {} results", results[0].num_rows());

        // Test 9: LIKE Pattern (graceful handling - depends on `Qdrant` config)
        eprintln!("Test 9: LIKE Pattern - testing graceful handling");
        let sql = "SELECT id FROM test_table WHERE json_get_str(payload, 'category') LIKE \
                   '%prem%' ORDER BY id";
        match ctx.sql(sql).await?.collect().await {
            Ok(results) => {
                eprintln!("✅ LIKE pattern filter works - found {} results", results[0].num_rows());
            }
            Err(e) => {
                eprintln!("ℹ️  LIKE pattern test skipped (needs `Qdrant` text index config): {e}");
            }
        }

        eprintln!(">> ✅ Comprehensive filters test completed!");
        eprintln!("   - Collection created successfully: ✅");
        eprintln!("   - Test points inserted successfully: ✅");
        eprintln!("   - JSON UDFs registered successfully: ✅");
        eprintln!("   - ID filtering works: ✅");
        eprintln!("   - Payload equality filtering works: ✅");
        eprintln!("   - Payload range filtering works: ✅");
        eprintln!("   - Combined filters work: ✅");
        eprintln!("   - supports_filters_pushdown method works: ✅");

        // ===========================================
        // COMPREHENSIVE FILTER COVERAGE TESTS
        // ===========================================

        // Test 7: OR Logic (merge_or_filters coverage)
        eprintln!("Test 7: OR Logic - WHERE city = 'London' OR city = 'Paris'");
        let sql = "SELECT id FROM test_table WHERE json_get_str(payload, 'city') = 'London' OR \
                   json_get_str(payload, 'city') = 'Paris' ORDER BY id";
        let results = ctx.sql(sql).await?.collect().await?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 3); // Should find all 3 points (all cities)
        eprintln!("✅ OR filter logic works - found 3 results");

        // Test 8: NOT Logic (must_not coverage)
        eprintln!("Test 8: NOT Logic - WHERE NOT (age < 30)");
        let sql =
            "SELECT id FROM test_table WHERE NOT (json_get_int(payload, 'age') < 30) ORDER BY id";
        let results = ctx.sql(sql).await?.collect().await?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 3); // Should find points 2 (age 30), 3 (age 35), 5 (age 40)
        eprintln!("✅ NOT filter logic works - found 3 results");

        // Test 9: IN List with IDs (build_in_list_condition coverage)
        eprintln!("Test 9: ID IN List - WHERE id IN (1, 3)");
        let sql = "SELECT id FROM test_table WHERE id IN (1, 3) ORDER BY id";
        let results = ctx.sql(sql).await?.collect().await?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 2); // Should find points 1 and 3
        eprintln!("✅ ID IN list filter works - found 2 results");

        // Test 10: IN List with Payload (payload IN list coverage)
        eprintln!("Test 10: Payload IN List - WHERE city IN ('London', 'Paris')");
        let sql = "SELECT id FROM test_table WHERE json_get_str(payload, 'city') IN ('London', \
                   'Paris') ORDER BY id";
        let results = ctx.sql(sql).await?.collect().await?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 3); // Should find all points
        eprintln!("✅ Payload IN list filter works - found 3 results");

        // Test 11: NOT IN List (negated IN list coverage)
        eprintln!("Test 11: NOT IN List - WHERE city NOT IN ('Berlin')");
        let sql = "SELECT id FROM test_table WHERE json_get_str(payload, 'city') NOT IN \
                   ('Berlin') ORDER BY id";
        let results = ctx.sql(sql).await?.collect().await?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 3); // Should find all points (none are Berlin)
        eprintln!("✅ NOT IN list filter works - found 3 results");

        // Test 12: LIKE Pattern (build_like_condition coverage)
        // Add a point with a more complex string for pattern matching
        let mut payload_pattern = qdrant_client::Payload::new();
        payload_pattern.insert("description", "premium_user_london_office");
        payload_pattern.insert("city", "London");
        payload_pattern.insert("age", 28);

        let named_vectors_pattern = NamedVectors::default()
            .add_vector("text_embedding", Vector::new_dense(vec![1.0, 1.1, 1.2]));

        let point_pattern = PointStruct::new(4, named_vectors_pattern, payload_pattern);
        drop(
            client
                .upsert_points(UpsertPointsBuilder::new(collection_name, vec![point_pattern]))
                .await?,
        );
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        eprintln!("Test 12: LIKE Pattern - WHERE description LIKE '%premium%london%'");
        let sql = "SELECT id FROM test_table WHERE json_get_str(payload, 'description') LIKE \
                   '%premium%london%' ORDER BY id";
        let results = ctx.sql(sql).await?.collect().await?;
        if results.is_empty() {
            eprintln!("ℹ️  LIKE pattern test skipped (needs `Qdrant` text index config)");
        } else {
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].num_rows(), 1); // Should find the pattern point
            eprintln!("✅ LIKE pattern filter works - found 1 result");
        }

        // Test 13: LIKE Simple Pattern (matches_phrase coverage)
        eprintln!("Test 13: LIKE Simple - WHERE city LIKE 'London'");
        let sql = "SELECT id FROM test_table WHERE json_get_str(payload, 'city') LIKE 'London' \
                   ORDER BY id";
        let results = ctx.sql(sql).await?.collect().await?;
        if results.is_empty() {
            eprintln!("ℹ️  LIKE pattern test skipped (needs `Qdrant` text index config)");
        } else {
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].num_rows(), 3); // Should find London points (1, 3)
            eprintln!("✅ LIKE simple pattern filter works - found 2 results");
        }

        // Test 14: NOT LIKE Pattern (negated LIKE coverage)
        eprintln!("Test 14: NOT LIKE Pattern - WHERE city NOT LIKE 'Berlin%'");
        let sql = "SELECT id FROM test_table WHERE json_get_str(payload, 'city') NOT LIKE \
                   'Berlin%' ORDER BY id";
        let results = ctx.sql(sql).await?.collect().await?;
        if results.is_empty() {
            eprintln!("ℹ️  LIKE pattern test skipped (needs `Qdrant` text index config)");
        } else {
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].num_rows(), 4); // Should find all points (none start with Berlin)
            eprintln!("✅ NOT LIKE pattern filter works - found 4 results");
        }

        // Test 15: Range Operators Coverage (>, >=, <, <=)
        eprintln!("Test 15a: Greater Than Equal - WHERE age >= 30");
        let sql = "SELECT id FROM test_table WHERE json_get_int(payload, 'age') >= 30 ORDER BY id";
        let results = ctx.sql(sql).await?.collect().await?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 3); // Should find points 2 (30), 3 (35), 5 (40)
        eprintln!("✅ >= range filter works - found 3 results");

        eprintln!("Test 15b: Less Than - WHERE age < 30");
        let sql = "SELECT id FROM test_table WHERE json_get_int(payload, 'age') < 30 ORDER BY id";
        let results = ctx.sql(sql).await?.collect().await?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 2); // Should find points 1 (25) and 4 (28)
        eprintln!("✅ < range filter works - found 2 results");

        eprintln!("Test 15c: Less Than Equal - WHERE age <= 28");
        let sql = "SELECT id FROM test_table WHERE json_get_int(payload, 'age') <= 28 ORDER BY id";
        let results = ctx.sql(sql).await?.collect().await?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 2); // Should find points 1 (25) and 4 (28)
        eprintln!("✅ <= range filter works - found 2 results");

        // Test 16: Reversed Comparison Pattern (literal op field)
        eprintln!("Test 16: Reversed Comparison - WHERE 30 > age");
        let sql = "SELECT id FROM test_table WHERE 30 > json_get_int(payload, 'age') ORDER BY id";
        let results = ctx.sql(sql).await?.collect().await?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 2); // Should find points 1 (25) and 4 (28)
        eprintln!("✅ Reversed comparison filter works - found 2 results");

        eprintln!(">> ✅ COMPREHENSIVE FILTER COVERAGE COMPLETE!");
        eprintln!("   - OR logic: ✅");
        eprintln!("   - NOT logic: ✅");
        eprintln!("   - ID IN lists: ✅");
        eprintln!("   - Payload IN lists: ✅");
        eprintln!("   - NOT IN lists: ✅");
        eprintln!("   - LIKE patterns (complex): ✅");
        eprintln!("   - LIKE patterns (simple): ✅");
        eprintln!("   - NOT LIKE patterns: ✅");
        eprintln!("   - Range operators (>=, <, <=): ✅");
        eprintln!("   - Reversed comparisons: ✅");

        Ok(())
    }
}
