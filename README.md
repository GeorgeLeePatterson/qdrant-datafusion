# `qdrant-datafusion`

`qdrant-datafusion` exposes `Qdrant` collections as `DataFusion` tables.

The current crate scope is intentionally narrow: correct, paginated collection scans over the
canonical Arrow carriers used by `ndarrow` and `nabled::arrow`. It is not yet the broad SQL
surface for `Qdrant` search, recommend, discover, fusion, or planner rewrites.

## Current Scan Contract

Collection scans currently expose:

- `id`: `Utf8`
- `payload`: JSON encoded as `Utf8`
- dense vector fields: nullable `FixedSizeList<Float32>(D)`
- multivector fields: nullable `arrow.variable_shape_tensor<Float32>` with rank 2
- sparse vector fields: nullable `ndarrow.csr_matrix_batch<Float32>`

For named-vector collections, a vector configured at the collection level may be missing on an
individual point. In that case the column value is `NULL` for that row. Present values stay in the
canonical carrier; missing values are not imputed during scan.

## Current Capabilities

- collection config introspection into the scan schema
- true table scans via paginated `Qdrant::scroll`
- schema/projection-driven vector selection
- SQL `LIMIT` pushdown to the scan stream
- exact physical sort pushdown for `ORDER BY id ASC`
- validated internal payload-key ordered-scroll continuation groundwork
- heterogeneous named-vector scans with top-level nullable vector columns

## Not Yet Admitted

- write support or `INSERT INTO`
- filter pushdown
- payload-key SQL `ORDER BY` pushdown beyond `ORDER BY id ASC`
- `Qdrant`-specific UDFs, UDAFs, or UDTFs
- SQL-native search / recommend / discover / fusion semantics
- custom planning or rewrite passes

## Basic Usage

```rust,ignore
use std::sync::Arc;

use datafusion::prelude::*;
use qdrant_client::Qdrant;
use qdrant_datafusion::prelude::*;

# async fn example() -> Result<()> {
let client = Qdrant::from_url("http://localhost:6334").build()?;
let table_provider = QdrantTableProvider::try_new(client, "my_collection").await?;

let ctx = SessionContext::new();
ctx.register_table("vectors", Arc::new(table_provider))?;

let batches = ctx
    .sql("SELECT id, payload, vector FROM vectors ORDER BY id LIMIT 10")
    .await?
    .collect()
    .await?;
# Ok(())
# }
```

## Example SQL

```sql
SELECT id, payload
FROM docs
LIMIT 10;

SELECT text_embedding
FROM docs
WHERE text_embedding IS NOT NULL
ORDER BY id;

SELECT multi_embedding, keywords
FROM docs
WHERE multi_embedding IS NOT NULL OR keywords IS NOT NULL;
```

## Verification

```bash
just checks
```
