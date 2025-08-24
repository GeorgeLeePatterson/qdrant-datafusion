# 🛸 `Qdrant` `DataFusion` Integration

A high-performance Rust library that provides seamless integration between [Qdrant](https://qdrant.tech) vector database and [Apache DataFusion](https://datafusion.apache.org), enabling SQL queries over vector data with full support for heterogeneous collections, complex projections, and mixed vector types.

[![Crates.io](https://img.shields.io/crates/v/qdrant-datafusion.svg)](https://crates.io/crates/qdrant-datafusion)
[![Documentation](https://docs.rs/qdrant-datafusion/badge.svg)](https://docs.rs/qdrant-datafusion)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Build Status](https://img.shields.io/github/actions/workflow/status/GeorgeLeePatterson/qdrant-datafusion/ci.yml?branch=main)](https://github.com/GeorgeLeePatterson/qdrant-datafusion/actions)
[![Coverage](https://codecov.io/gh/GeorgeLeePatterson/qdrant-datafusion/branch/main/graph/badge.svg)](https://codecov.io/gh/GeorgeLeePatterson/qdrant-datafusion)

## 🎯 Features

### **Complete Vector Support**
- **Dense Vectors**: Single embeddings as `List<Float32>`
- **Multi-Vectors**: Multiple embeddings per point as `List<List<Float32>>`
- **Sparse Vectors**: Efficient sparse representations with separate indices and values
- **Mixed Collections**: Supports collections with different vector types

### **Advanced Query Capabilities**
- **SQL Interface**: Query Qdrant collections using standard SQL syntax
- **Schema Projection**: Optimized queries that only fetch requested fields
- **Heterogeneous Data**: Handle points with different vector field subsets
- **Nullable Fields**: Proper null handling for missing vector data
- **LIMIT Support**: Efficient query limiting pushed to Qdrant

### **High Performance Architecture**
- **Schema-Driven**: Clean, efficient deserialization with O(F) performance
- **Single-Pass Processing**: Minimized memory allocations and data copying
- **Async Streaming**: Non-blocking query execution with proper backpressure
- **Connection Pooling**: Reusable client connections for optimal throughput

### **Production Ready**
- **> 90% Test Coverage**: Comprehensive testing with real Qdrant instances
- **Memory Safe**: Full Rust safety guarantees with zero unsafe code
- **Error Handling**: Detailed error types with context for debugging
- **Extensible**: Ready for custom UDFs and advanced query planning

## 🚀 Quick Start

Add this to your `Cargo.toml`:

```toml
[dependencies]
qdrant-datafusion = "0.1"
```

### Basic Usage

```rust,ignore
use qdrant_datafusion::prelude::*;
use qdrant_client::Qdrant;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    // Connect to Qdrant
    let client = Qdrant::from_url("http://localhost:6334").build()?;

    // Create DataFusion table provider
    let table_provider = QdrantTableProvider::try_new(client, "my_collection").await?;

    // Register with DataFusion context
    let ctx = SessionContext::new();
    ctx.register_table("vectors", Arc::new(table_provider))?;

    // Query with SQL!
    let df = ctx.sql("
        SELECT id, payload, embedding
        FROM vectors
        WHERE id IN ('doc1', 'doc2')
        LIMIT 10
    ").await?;

    let results = df.collect().await?;
    println!("{:?}", results);

    Ok(())
}
```

### Advanced Queries

```rust,ignore
// Complex projections with mixed vector types
let df = ctx.sql("
    SELECT
        id,
        text_embedding,
        image_embedding,
        multi_embeddings,
        keywords_indices,
        keywords_values
    FROM mixed_vectors
    WHERE payload IS NOT NULL
").await?;

// Efficient schema projection - only fetches requested vector fields
let df = ctx.sql("SELECT text_embedding FROM vectors").await?;
```

## 📊 Vector Type Support

| Vector Type | Schema | Description | Example Query |
|------------|--------|-------------|---------------|
| **Dense** | `List<Float32>` | Single embedding per field | `SELECT text_embedding FROM docs` |
| **Multi** | `List<List<Float32>>` | Multiple embeddings per field | `SELECT multi_embeddings FROM docs` |
| **Sparse** | `List<UInt32>` + `List<Float32>` | Efficient sparse vectors | `SELECT keywords_indices, keywords_values FROM docs` |

## 🔧 Collection Types

### Named Collections (Heterogeneous)
Collections with multiple named vector fields where different points can have different subsets:

```sql
-- Schema automatically includes all possible vector fields
SELECT
    id,
    text_embedding,      -- Some points have this
    image_embedding,     -- Some points have this
    audio_embedding      -- Some points have this
FROM heterogeneous_collection;
```

### Unnamed Collections (Homogeneous)
Collections with a single unnamed vector field:

```sql
-- Schema contains single 'vector' field
SELECT id, payload, vector
FROM homogeneous_collection;
```

## 🎯 Current Capabilities

✅ **Complete `TableProvider` Implementation**
- Full SQL querying via `DataFusion`
- All Qdrant vector types supported
- Schema projection optimization
- Proper null handling for missing fields

✅ **Production Ready**
- 90% test coverage with real Qdrant instances
- Comprehensive error handling
- Memory-safe Rust implementation
- Async streaming execution

## 🔮 Future Roadmap

🔄 **In Development**
- **Custom UDFs**: Distance functions, similarity search, recommendations, and more
- **Query Planning**: Qdrant-specific optimizations and filter pushdown
- **Advanced Filters**: Native Qdrant filter integration with SQL WHERE clauses

🎯 **Planned**
- **Multi-Database Joins**: Join Qdrant data with other `DataFusion` sources
- **Vector Search UDFs**: `similarity()`, `recommend()`, `discover()` like functions
- **Extension Nodes**: Custom physical plan nodes for complex vector operations

## 🧪 Testing

Run the test suite with a real Qdrant instance:

```bash
# Start Qdrant
docker run -p 6333:6333 -p 6334:6334 qdrant/qdrant

# Run tests
cargo test --features test-utils

# Check coverage
just coverage
```


## 🏗️ Architecture

### Schema-Driven Design
Built around a schema-driven architecture that reduces complex matching logic and leaves room for future expansion:

```rust
// Schema defines extractors upfront
enum FieldExtractor {
    Id(StringBuilder),
    Payload(StringBuilder),
    DenseVector { name: String, builder: ListBuilder<Float32Builder> },
    MultiVector { name: String, builder: ListBuilder<ListBuilder<Float32Builder>> },
    SparseIndices { name: String, builder: ListBuilder<UInt32Builder> },
    SparseValues { name: String, builder: ListBuilder<Float32Builder> },
}

// Single pass processing with owned iteration
pub fn append_point(&mut self, point: ScoredPoint) {
    let ScoredPoint { id, payload, vectors, .. } = point;
    let vector_lookup = build_vector_lookup(vectors);

    for extractor in &mut self.field_extractors {
        // All logic inline - no hidden abstractions
    }
}
```

## 🤝 Contributing

We welcome contributions! Please see [CONTRIBUTING.md](https://github.com/GeorgeLeePatterson/qdrant-datafusion/blob/main/CONTRIBUTING.md) for guidelines.

## 📝 License

Licensed under the Apache License, Version 2.0. See [LICENSE](https://github.com/GeorgeLeePatterson/qdrant-datafusion/blob/main/LICENSE) for details.
