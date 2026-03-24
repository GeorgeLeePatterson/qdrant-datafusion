# `qdrant-datafusion`

`qdrant-datafusion` exposes `Qdrant` collections as `DataFusion` tables.

The current crate scope is intentionally narrow: correct, paginated collection scans over the
canonical Arrow carriers used by `ndarrow` and `nabled::arrow`, the exact pushdown-first SQL
bridge for ordering and filtering, and the first narrow planner slices for exact `COUNT(*)` and
top-facet grouped-count pushdown. It is not yet the broad SQL surface for `Qdrant` search,
recommend, discover, fusion, or broader planner rewrites.

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
- exact payload-key sort pushdown for the admitted `ORDER BY payload:<path>` subset on indexed integer, float, and datetime payload fields
- exact boolean filter pushdown over the admitted leaf subset:
  - `AND`, `OR`, and `NOT`
  - `id =`, `id !=`, `id IN (...)`, `id NOT IN (...)`
  - vector-column `IS NULL` / `IS NOT NULL`
- indexed scalar `payload:<path>` comparisons, `IN`, `NOT IN`, `BETWEEN`, and `NOT BETWEEN`
- exact `COUNT(*)` pushdown over a single `Qdrant` source through the crate's session/planner helper
- exact top-facet grouped-count pushdown over one keyword `payload:<path>` field through the crate's session/planner helper
- a unified relation-pushdown analyzer scaffold now owns the admitted planner-layer subtree
  replacements instead of relying on separate analyzer-rule ownership by convention
  - the scaffold now classifies subtree source, topology, and composition explicitly as the
    basis for later island expansion
  - it now also distinguishes exact-self kernels from local shells around extracted child kernels
- heterogeneous named-vector scans with top-level nullable vector columns
- exact SQL null semantics for `payload:<path>`:
  - `IS NULL` means missing or explicit null
  - `IS NOT NULL` means present and non-null

## Not Yet Admitted

- write support or `INSERT INTO`
- payload empty semantics, text, geo, nested, and count-oriented payload predicates
- broader payload-key SQL `ORDER BY` pushdown beyond the admitted `payload:<path>` subset
- broader aggregate/grouped SQL beyond the admitted keyword-facet subset
- projection-time `payload:<path>` execution outside an admitted `Qdrant` kernel in the prepared
  session/planner path
- `Qdrant`-specific UDFs, UDAFs, or UDTFs
- SQL-native search / recommend / discover / fusion semantics
- broader planner rewrites beyond the narrow exact `COUNT(*)` / facet slices

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

Exact aggregate-like pushdown currently requires the crate's prepared session context:

```rust,ignore
use std::sync::Arc;

use datafusion::prelude::*;
use qdrant_client::Qdrant;
use qdrant_datafusion::prelude::*;

# async fn example() -> Result<()> {
let client = Qdrant::from_url("http://localhost:6334").build()?;
let table_provider = QdrantTableProvider::try_new(client, "my_collection").await?;

let ctx = QdrantSessionContext::from(SessionContext::new());
ctx.session_context()
    .register_table("vectors", Arc::new(table_provider))?;

let batches = ctx
    .sql("SELECT COUNT(*) AS total FROM vectors WHERE payload:rank >= 10")
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

SELECT id
FROM docs
WHERE id IN ('1', '2', '3');

SELECT id
FROM docs
WHERE payload:rank >= 10
ORDER BY payload:rank;

SELECT id
FROM docs
WHERE (payload:tag = 'red' OR id = '2')
  AND NOT payload:rank > 20
ORDER BY id;

SELECT multi_embedding, keywords
FROM docs
WHERE multi_embedding IS NOT NULL
  AND payload:rank BETWEEN 10 AND 20;

SELECT payload:tag AS tag, COUNT(*) AS total
FROM docs
WHERE payload:rank >= 10
GROUP BY payload:tag
ORDER BY total DESC
LIMIT 10;
```

## Verification

```bash
just checks
```
