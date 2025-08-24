#![allow(unused_crate_dependencies)]

mod common;

const TRACING_DIRECTIVES: &[(&str, &str)] = &[
    ("testcontainers", "debug"),
    ("hyper", "error"),
    ("tonic", "error"),
    // --
];

#[cfg(feature = "test-utils")]
e2e_test!(table_provider, tests::test_table_provider, TRACING_DIRECTIVES, None);

// TODO: Remove
#[cfg(feature = "test-utils")]
#[tokio::test(flavor = "multi_thread")]
async fn test_setup() -> qdrant_datafusion::error::Result<()> { tests::test_remove().await }

#[cfg(feature = "test-utils")]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow;
    use datafusion::prelude::*;
    use qdrant_client::Qdrant;
    use qdrant_client::qdrant::{
        CreateCollectionBuilder, DenseVector, Distance, MultiVectorComparator, MultiVectorConfig,
        NamedVectors, PointStruct, SparseVectorConfig, SparseVectorParams, UpsertPointsBuilder,
        VectorParams, VectorParamsMap, Vectors, VectorsConfig, vector, vectors_config,
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

    #[expect(clippy::too_many_lines)]
    pub(super) async fn test_remove() -> Result<()> {
        eprintln!("> Setting up test container");
        let url = "http://localhost:55001";
        let client =
            Qdrant::from_url(url).api_key(qdrant_datafusion::test_utils::QDRANT_API_KEY).build()?;

        // Create collection with MIXED vector types - dense, sparse, and multi-vector
        let collection_name = "test_mixed_vectors";

        let mut vector_params_map = HashMap::new();

        // Dense vector
        let _ = vector_params_map.insert("dense_text".to_string(), VectorParams {
            size: 3,
            distance: Distance::Cosine.into(),
            ..Default::default()
        });

        // Multi-vector
        let _ = vector_params_map.insert("multi_embeddings".to_string(), VectorParams {
            size: 2,
            distance: Distance::Dot.into(),
            multivector_config: Some(MultiVectorConfig {
                comparator: MultiVectorComparator::MaxSim.into(),
            }),
            ..Default::default()
        });

        // Create vectors config with mixed types
        let vectors_config = VectorsConfig {
            config: Some(vectors_config::Config::ParamsMap(VectorParamsMap {
                map: vector_params_map,
            })),
        };

        // Create sparse vector config separately
        let mut sparse_vector_map = HashMap::new();
        let _ = sparse_vector_map.insert("sparse_features".to_string(), SparseVectorParams {
            index:    None,
            modifier: None,
        });
        let sparse_config = SparseVectorConfig { map: sparse_vector_map };

        let _ = client
            .create_collection(
                CreateCollectionBuilder::new(collection_name)
                    .vectors_config(vectors_config)
                    .sparse_vectors_config(sparse_config),
            )
            .await?;

        // Insert test data with ALL vector types
        let mut payload1 = HashMap::new();
        drop(payload1.insert("title".to_string(), "Document 1".into()));
        drop(payload1.insert("category".to_string(), "mixed".into()));

        let mut payload2 = HashMap::new();
        drop(payload2.insert("title".to_string(), "Document 2".into()));
        drop(payload2.insert("category".to_string(), "complex".into()));

        // Create mixed vectors for point 1
        let mut vectors1 = HashMap::new();

        // Dense vector
        drop(vectors1.insert(
            "dense_text".to_string(),
            vector::Vector::Dense(DenseVector { data: vec![0.1, 0.2, 0.3] }).into(),
        ));

        // Multi-vector (multiple dense vectors)
        drop(
            vectors1.insert(
                "multi_embeddings".to_string(),
                vector::Vector::MultiDense(qdrant_client::qdrant::MultiDenseVector {
                    vectors: vec![DenseVector { data: vec![0.7, 0.8] }, DenseVector {
                        data: vec![0.9, 0.1],
                    }],
                })
                .into(),
            ),
        );

        // Sparse vector
        drop(
            vectors1.insert(
                "sparse_features".to_string(),
                vector::Vector::Sparse(qdrant_client::qdrant::SparseVector {
                    indices: vec![0, 5, 10],
                    values:  vec![0.1, 0.5, 0.9],
                })
                .into(),
            ),
        );

        // Create mixed vectors for point 2
        let mut vectors2 = HashMap::new();
        drop(vectors2.insert(
            "dense_text".to_string(),
            vector::Vector::Dense(DenseVector { data: vec![0.4, 0.5, 0.6] }).into(),
        ));
        drop(
            vectors2.insert(
                "multi_embeddings".to_string(),
                vector::Vector::MultiDense(qdrant_client::qdrant::MultiDenseVector {
                    vectors: vec![DenseVector { data: vec![0.2, 0.3] }],
                })
                .into(),
            ),
        );
        drop(
            vectors2.insert(
                "sparse_features".to_string(),
                vector::Vector::Sparse(qdrant_client::qdrant::SparseVector {
                    indices: vec![1, 3],
                    values:  vec![0.7, 0.4],
                })
                .into(),
            ),
        );

        let points = vec![
            PointStruct {
                id:      Some(1.into()),
                payload: payload1,
                vectors: Some(Vectors {
                    vectors_options: Some(qdrant_client::qdrant::vectors::VectorsOptions::Vectors(
                        NamedVectors { vectors: vectors1 },
                    )),
                }),
            },
            PointStruct {
                id:      Some(2.into()),
                payload: payload2,
                vectors: Some(Vectors {
                    vectors_options: Some(qdrant_client::qdrant::vectors::VectorsOptions::Vectors(
                        NamedVectors { vectors: vectors2 },
                    )),
                }),
            },
        ];

        drop(client.upsert_points(UpsertPointsBuilder::new(collection_name, points)).await?);

        Ok(())
    }

    #[expect(clippy::too_many_lines)]
    pub(super) async fn test_table_provider(c: Arc<QdrantContainer>) -> Result<()> {
        eprintln!("> Testing comprehensive QdrantTableProvider with ALL vector types");
        let client = create_qdrant_client(&c)?;

        // Create collection with MIXED vector types - dense, sparse, and multi-vector
        let collection_name = "test_mixed_vectors";

        let mut vector_params_map = HashMap::new();

        // Dense vector
        let _ = vector_params_map.insert("dense_text".to_string(), VectorParams {
            size: 3,
            distance: Distance::Cosine.into(),
            ..Default::default()
        });

        // Multi-vector
        let _ = vector_params_map.insert("multi_embeddings".to_string(), VectorParams {
            size: 2,
            distance: Distance::Dot.into(),
            multivector_config: Some(MultiVectorConfig {
                comparator: MultiVectorComparator::MaxSim.into(),
            }),
            ..Default::default()
        });

        // Create vectors config with mixed types
        let vectors_config = VectorsConfig {
            config: Some(vectors_config::Config::ParamsMap(VectorParamsMap {
                map: vector_params_map,
            })),
        };

        // Create sparse vector config separately
        let mut sparse_vector_map = HashMap::new();
        let _ = sparse_vector_map.insert("sparse_features".to_string(), SparseVectorParams {
            index:    None,
            modifier: None,
        });
        let sparse_config = SparseVectorConfig { map: sparse_vector_map };

        let _ = client
            .create_collection(
                CreateCollectionBuilder::new(collection_name)
                    .vectors_config(vectors_config)
                    .sparse_vectors_config(sparse_config),
            )
            .await?;

        // Insert test data with ALL vector types
        let mut payload1 = HashMap::new();
        drop(payload1.insert("title".to_string(), "Document 1".into()));
        drop(payload1.insert("category".to_string(), "mixed".into()));

        let mut payload2 = HashMap::new();
        drop(payload2.insert("title".to_string(), "Document 2".into()));
        drop(payload2.insert("category".to_string(), "complex".into()));

        // Create mixed vectors for point 1
        let mut vectors1 = HashMap::new();

        // Dense vector
        drop(vectors1.insert(
            "dense_text".to_string(),
            vector::Vector::Dense(DenseVector { data: vec![0.1, 0.2, 0.3] }).into(),
        ));

        // Multi-vector (multiple dense vectors)
        drop(
            vectors1.insert(
                "multi_embeddings".to_string(),
                vector::Vector::MultiDense(qdrant_client::qdrant::MultiDenseVector {
                    vectors: vec![DenseVector { data: vec![0.7, 0.8] }, DenseVector {
                        data: vec![0.9, 0.1],
                    }],
                })
                .into(),
            ),
        );

        // Sparse vector
        drop(
            vectors1.insert(
                "sparse_features".to_string(),
                vector::Vector::Sparse(qdrant_client::qdrant::SparseVector {
                    indices: vec![0, 5, 10],
                    values:  vec![0.1, 0.5, 0.9],
                })
                .into(),
            ),
        );

        // Create mixed vectors for point 2
        let mut vectors2 = HashMap::new();
        drop(vectors2.insert(
            "dense_text".to_string(),
            vector::Vector::Dense(DenseVector { data: vec![0.4, 0.5, 0.6] }).into(),
        ));
        drop(
            vectors2.insert(
                "multi_embeddings".to_string(),
                vector::Vector::MultiDense(qdrant_client::qdrant::MultiDenseVector {
                    vectors: vec![DenseVector { data: vec![0.2, 0.3] }],
                })
                .into(),
            ),
        );
        drop(
            vectors2.insert(
                "sparse_features".to_string(),
                vector::Vector::Sparse(qdrant_client::qdrant::SparseVector {
                    indices: vec![1, 3],
                    values:  vec![0.7, 0.4],
                })
                .into(),
            ),
        );

        let points = vec![
            PointStruct {
                id:      Some(1.into()),
                payload: payload1,
                vectors: Some(Vectors {
                    vectors_options: Some(qdrant_client::qdrant::vectors::VectorsOptions::Vectors(
                        NamedVectors { vectors: vectors1 },
                    )),
                }),
            },
            PointStruct {
                id:      Some(2.into()),
                payload: payload2,
                vectors: Some(Vectors {
                    vectors_options: Some(qdrant_client::qdrant::vectors::VectorsOptions::Vectors(
                        NamedVectors { vectors: vectors2 },
                    )),
                }),
            },
        ];

        drop(client.upsert_points(UpsertPointsBuilder::new(collection_name, points)).await?);

        // Create QdrantTableProvider
        debug!(">> Creating QdrantTableProvider for mixed vector types");
        let table_provider = QdrantTableProvider::try_new(client.clone(), collection_name).await?;

        // Create DataFusion context
        debug!(">> Creating DataFusion context");

        let ctx = SessionContext::new();
        drop(ctx.register_table("mixed_table", Arc::new(table_provider))?);

        // Test 1: SELECT * - All vector types together
        eprintln!(">> Test 1: SELECT * FROM mixed_table (ALL vector types)");
        let df = ctx.sql("SELECT * FROM mixed_table").await?;
        let results = df.collect().await?;

        eprintln!(">>> Mixed vectors query results:");
        for (i, batch) in results.iter().enumerate() {
            let schema = batch.schema();
            eprintln!("    Batch {i}: {} rows, {} columns", batch.num_rows(), batch.num_columns());
            debug!("      Schema: {schema:?}");

            // Should have: id, dense_text, multi_embeddings, sparse_features_indices,
            // sparse_features_values, payload
            assert_eq!(batch.num_rows(), 2);
            assert!(batch.num_columns() >= 6);

            // Verify column names include all vector types
            let field_names: Vec<&str> =
                schema.fields().iter().map(|f| f.name().as_str()).collect();
            assert!(field_names.contains(&"dense_text"));
            assert!(field_names.contains(&"multi_embeddings"));
            assert!(field_names.contains(&"sparse_features_indices"));
            assert!(field_names.contains(&"sparse_features_values"));
            assert!(field_names.contains(&"payload"));
            assert!(field_names.contains(&"id"));

            arrow::util::pretty::print_batches(std::slice::from_ref(batch)).unwrap();
        }

        // Test 2: Projection - Only dense vector
        eprintln!(">> Test 2: SELECT dense_text FROM mixed_table (dense only)");
        let df = ctx.sql("SELECT dense_text FROM mixed_table").await?;
        let results = df.collect().await?;

        for batch in &results {
            assert_eq!(batch.num_columns(), 1);
            assert_eq!(batch.schema().field(0).name(), "dense_text");
            arrow::util::pretty::print_batches(std::slice::from_ref(batch)).unwrap();
        }

        // Test 3: Projection - Only sparse vectors
        eprintln!(
            ">> Test 3: SELECT sparse_features_indices, sparse_features_values FROM mixed_table"
        );
        let df = ctx
            .sql("SELECT sparse_features_indices, sparse_features_values FROM mixed_table")
            .await?;
        let results = df.collect().await?;

        for batch in &results {
            assert_eq!(batch.num_columns(), 2);
            assert_eq!(batch.schema().field(0).name(), "sparse_features_indices");
            assert_eq!(batch.schema().field(1).name(), "sparse_features_values");
            arrow::util::pretty::print_batches(std::slice::from_ref(batch)).unwrap();
        }

        // Test 4: Projection - Only multi-vector
        eprintln!(">> Test 4: SELECT multi_embeddings FROM mixed_table (multi-vector only)");
        let df = ctx.sql("SELECT multi_embeddings FROM mixed_table").await?;
        let results = df.collect().await?;

        for batch in &results {
            assert_eq!(batch.num_columns(), 1);
            assert_eq!(batch.schema().field(0).name(), "multi_embeddings");
            arrow::util::pretty::print_batches(std::slice::from_ref(batch)).unwrap();
        }

        // Test 5: Mixed projection - Different vector types together
        eprintln!(">> Test 5: SELECT id, dense_text, sparse_features_indices FROM mixed_table");
        let df = ctx.sql("SELECT id, dense_text, sparse_features_indices FROM mixed_table").await?;
        let results = df.collect().await?;

        for batch in &results {
            assert_eq!(batch.num_columns(), 3);
            assert_eq!(batch.schema().field(0).name(), "id");
            assert_eq!(batch.schema().field(1).name(), "dense_text");
            assert_eq!(batch.schema().field(2).name(), "sparse_features_indices");
            arrow::util::pretty::print_batches(std::slice::from_ref(batch)).unwrap();
        }

        // Test 6: No vectors projection (should not fetch any vectors from Qdrant)
        eprintln!(">> Test 6: SELECT id, payload FROM mixed_table (no vectors)");
        let df = ctx.sql("SELECT id, payload FROM mixed_table").await?;
        let results = df.collect().await?;

        for batch in &results {
            assert_eq!(batch.num_columns(), 2);
            assert_eq!(batch.schema().field(0).name(), "id");
            assert_eq!(batch.schema().field(1).name(), "payload");
            arrow::util::pretty::print_batches(std::slice::from_ref(batch)).unwrap();
        }

        eprintln!(">> ✅ Comprehensive QdrantTableProvider test completed successfully!");
        eprintln!("   - Dense vectors: ✅");
        eprintln!("   - Multi-vectors: ✅");
        eprintln!("   - Sparse vectors: ✅");
        eprintln!("   - Mixed collections: ✅");
        eprintln!("   - Schema projection: ✅");
        eprintln!("   - All combinations working: ✅");

        Ok(())
    }
}
