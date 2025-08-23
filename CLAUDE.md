# Claude Development Notes

## Overview
This document captures key learnings and insights from implementing the Qdrant-DataFusion bridge, specifically the TableProvider and QdrantScanExec ExecutionPlan.

## Key Architectural Decisions

### 1. Query vs Scroll for TableProvider
**Decision**: Use Qdrant's `query()` instead of `scroll()` for TableProvider scans.

**Rationale**: 
- `query()` without query vector = `SELECT * ORDER BY id LIMIT N` - perfect for basic table scans
- `scroll()` = cursor-based pagination for processing ALL points - better for ETL/batch processing
- TableProvider semantics align better with query's "retrieve N points" vs scroll's "iterate through all"

**Evidence**: Integration test `compare_query_vs_scroll` showed both respect limits, but query is semantically correct for SQL table operations.

### 2. Vector Selector Architecture Issue
**Problem**: Current `build_selectors()` method creates named vector selectors even for unnamed vector collections.

**Root Cause**: 
- Unnamed vector collections store vectors without names
- Our schema creation assumes "vector" as default name for unnamed vectors
- But Qdrant query API rejects vector name "vector" for unnamed collections
- Should use simple `with_vectors(true)` for unnamed, specific names for named collections

**Fix Needed**: Distinguish between collection types in selector building.

### 3. ScoredPoint vs RetrievedPoint
**Discovery**: `query()` returns `ScoredPoint[]` while `scroll()` returns `RetrievedPoint[]`.

**Impact**: Extraction logic needed separate functions:
- `extract_ids_from_scored()` vs `extract_ids()`
- Both have same field structure (id, payload, vectors) but different types

## Implementation Patterns That Work

### 1. Schema-Driven Extraction
```rust
for field in schema.fields() {
    let array = match field.name().as_str() {
        "id" => extract_ids_from_scored(points)?,
        "payload" => extract_payload_from_scored(points)?,
        name => extract_dense_vector_from_scored(points, name)?,
    };
    arrays.push(array);
}
```
This pattern ensures RecordBatch column ordering matches schema exactly.

### 2. Clean Stream Architecture
```rust
struct QdrantQueryStream {
    schema: SchemaRef,
    future: Option<Pin<Box<dyn Future<Output = Result<RecordBatch>> + Send>>>,
}
```
- Single future that executes once and completes
- No complex state management or object recreation
- Yields one RecordBatch per Qdrant query result

### 3. Vector Data Extraction
```rust
match &vectors.vectors_options {
    Some(vectors_output::VectorsOptions::Vector(vector_output)) if name == "vector" => {
        Some(&vector_output.data)
    }
    Some(vectors_output::VectorsOptions::Vectors(named_vectors)) => {
        named_vectors.vectors.get(name).map(|vo| &vo.data)
    }
    _ => None,
}
```
Handles both unnamed (`Vector`) and named (`Vectors`) cases correctly.

## Anti-Patterns Avoided

### 1. Object Recreation in Streams
**Bad**: Creating new QdrantScanExec in poll_next()
**Good**: Store future in stream state, poll once

### 2. Ignoring Builder APIs
**Bad**: Manual struct construction
**Good**: Use QueryPointsBuilder, ScrollPointsBuilder from qdrant-rust-client

### 3. Guessing Data Formats
**Bad**: Assuming vector names or response structures
**Good**: Integration tests to examine actual Qdrant responses

## Testing Strategy

### Integration Test Pattern
```rust
e2e_test!(qdrant_test, tests::test_qdrant_functionality, TRACING_DIRECTIVES, None);
```
- Single reusable test function for iteration
- Test raw Qdrant operations before TableProvider
- Validate assumptions about data formats

## Current State (Updated)

### Working
- ✅ Schema generation from CollectionInfo
- ✅ Query-based ExecutionPlan architecture  
- ✅ Vector data extraction (dense, sparse, multi-vector)
- ✅ Payload and ID extraction
- ✅ Clean streaming implementation
- ✅ Named vector collections support
- ✅ Schema projection optimization
- ✅ VectorSelectorSpec enum (None/All/Named)

### In Progress
- 🔄 Mixed vector type collections (dense+sparse+multi in one table)
- 🔄 Vector format detection (newer vs deprecated fields)

## CRITICAL DISCOVERY: Qdrant Vector Format Issue

### Problem Found
When testing mixed vector types, discovered schema/data mismatch:
- **Expected**: `List<List<Float32>>` (multi-vector schema)  
- **Found**: `List<Float32>` (regular dense vector extraction)
- **Error**: Column types must match schema types at column index

### Root Cause Analysis
Debug output reveals Qdrant is using **deprecated format** for ALL vectors:

**Dense Vector Example**:
```
DEBUG: VectorOutput for dense_text: data.len=3, vectors_count=None, vector=None
```

**Multi-Vector Example**: 
```
DEBUG: VectorOutput for multi_embeddings: data.len=4, vectors_count=Some(2), vector=None
```

### Key Insights
1. **Newer Format Not Used**: `vector_output.vector = None` for all cases
2. **Deprecated Format Active**: Data stored in `data`, `indices`, `vectors_count` fields
3. **Multi-Vector Detection**: `vectors_count.is_some()` indicates multi-vector in deprecated format
4. **Multi-Vector Data**: Flattened in `data` field (e.g., `[0.7, 0.8, 0.9, 0.1]` = 2 vectors × 2 dims)

### Protobuf Analysis
**VectorOutput Structure** (what Qdrant returns):
```rust
pub struct VectorOutput {
    /// Vector data (flatten for multi vectors), deprecated  
    pub data: Vec<f32>,
    /// Number of vectors per multi vector, deprecated
    pub vectors_count: Option<u32>,
    /// Newer format (unused by current Qdrant)
    pub vector: Option<vector_output::Vector>,
}
```

### Root Cause Discovered
**Why Qdrant Uses Deprecated Format:**
1. **Backwards Compatibility**: Qdrant server maintains compatibility by using deprecated fields as primary format
2. **Protobuf Design**: Deprecated fields have lower tags (1,2,3), newer format has higher tags (101,102,103)  
3. **Server Strategy**: Qdrant populates deprecated fields while newer `vector` field remains `None`
4. **Future-Proofing**: Newer format exists in protobuf but not actively used by current server versions

**Evidence from qdrant-rust-client source:**
```rust
// VectorOutput struct (qdrant.rs:3168-3178)
pub struct VectorOutput {
    /// Vector data (flatten for multi vectors), deprecated
    pub data: Vec<f32>,                           // tag = "1" 
    /// Number of vectors per multi vector, deprecated  
    pub vectors_count: Option<u32>,               // tag = "3"
    /// Newer format (unused by current server)
    pub vector: Option<vector_output::Vector>,    // tags = "101, 102, 103"
}
```

**Qdrant-rust-client has conversion logic already:**
```rust  
// try_into_multi method shows exactly how to handle deprecated format
pub fn try_into_multi(self) -> Result<Vec<Vec<f32>>, QdrantError> {
    if self.vectors_count.is_none() { /* single vector */ }
    // Split flattened data using vectors_count
    Ok(self.data.chunks(self.data.len() / self.vectors_count.unwrap() as usize)
        .map(|v| v.to_vec()).collect())
}
```

### Required Fix
Implement same conversion logic as qdrant-rust-client:
1. **Multi-Vector Detection**: `vectors_count.is_some()` (not `vector` field) 
2. **Multi-Vector Extraction**: `data.chunks(data.len() / vectors_count as usize)`
3. **Error Handling**: Validate `data.len() % vectors_count == 0`

### Status: ✅ COMPLETE
- Schema creation: ✅ Correctly creates `List<List<Float32>>` for multi-vectors  
- Vector insertion: ✅ Uses newer format (client converts to deprecated for server)
- Vector retrieval: ✅ **Understood**: Qdrant intentionally uses deprecated format
- Detection logic: ✅ Checks both `vector.is_some()` AND `vectors_count.is_some()`
- Extraction: ✅ Implements same chunking logic as qdrant-rust-client's `try_into_multi()`

### Complete Features: 🎯 ALL WORKING
- Dense vectors: Single unnamed and multiple named ✅
- Sparse vectors: Indices (`UInt32`) and values (`Float32`) extraction ✅  
- Multi-vectors: `List<List<Float32>>` schema and deprecated format extraction ✅
- Schema projection: Only fetch needed vectors from Qdrant ✅
- Mixed collections: All vector types working together in one table ✅
- Comprehensive testing: All scenarios covered in single test ✅

### Test Results Summary
```
>> ✅ Comprehensive QdrantTableProvider test completed successfully!
   - Dense vectors: ✅
   - Multi-vectors: ✅  
   - Sparse vectors: ✅
   - Mixed collections: ✅
   - Schema projection: ✅
   - All combinations working: ✅
```

## Lessons Learned

1. **Start with Qdrant behavior, not assumptions**: Query/scroll comparison revealed semantic differences
2. **Use the client builders**: QueryPointsBuilder handles edge cases better than manual construction
3. **Test data formats early**: ScoredPoint vs RetrievedPoint would have been caught sooner
4. **Keep stream state minimal**: Complex polling logic leads to lifetime issues
5. **Schema drives everything**: Field iteration pattern works well for RecordBatch construction

## Performance Notes

- Single query call per scan (not pagination) - appropriate for TableProvider
- Qdrant handles limit at query level (not DataFusion filtering)
- Vector data extracted via slices, not cloning
- Schema projection respected in Qdrant query (only fetch needed vectors)

## Debug Commands

```bash
# Test current implementation
just test-integration qdrant_test

# Check compilation
cargo check

# Run with specific test output
cargo test -F test-utils --test "e2e" "qdrant_test" -- --nocapture --show-output
```