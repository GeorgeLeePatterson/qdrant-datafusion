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
- exact payload-key sort pushdown for the admitted single-key payload-path subset, including direct `payload:<path>` and equivalent `payload(payload:<path>, 'Type')` forms, on indexed integer, float, and datetime payload fields
- exact boolean filter pushdown over the admitted leaf subset:
  - `AND`, `OR`, and `NOT`
  - `id =`, `id !=`, `id IN (...)`, `id NOT IN (...)`
  - vector-column `IS NULL` / `IS NOT NULL`
- indexed scalar payload-field comparisons, `IN`, `NOT IN`, `BETWEEN`, and `NOT BETWEEN` over the admitted `payload:<path>` and equivalent public `payload(...)` forms
  - integer match predicates require lookup-capable integer indexes
  - integer range predicates require range-capable integer indexes
- exact `COUNT(*)` pushdown over a single `Qdrant` source through the crate's session/planner helper
- exact top-facet grouped-count pushdown over one scalar `payload:<path>` field through the crate's session/planner helper
  - currently admitted facet fields are keyword, bool, and lookup-capable integer payload indexes
  - facet keys currently surface as `Utf8`, matching the current textual `payload:<path>` SQL bridge
- a generic public `QdrantOpNode` / `QdrantOp` layer now exists above the shared kernel family
  for the current nearest-retrieval prototype
- current exact `COUNT(*)`, scalar facet, and nearest retrieval now lower through one shared
  internal `QdrantKernelNode` / `QdrantKernelSpec` family rather than isolated logical node types
- the first retrieval prototype is a DataFusion-native marker UDF:
  - `qdrant_nearest_score(vector_column, ...)`
  - exact lowering currently admits dense query vectors, descending score sort, `LIMIT`,
    optional exact base filters, and optional score-threshold predicates
  - the score column is only added when projected; aliases follow normal `DataFusion` naming
- a unified relation-pushdown analyzer scaffold now owns the admitted planner-layer subtree
  replacements instead of relying on separate analyzer-rule ownership by convention
  - the scaffold now classifies subtree source, topology, and composition explicitly as the
    basis for later island expansion
  - it now also distinguishes exact-self kernels from local shells around extracted child kernels
  - the first concrete `mergeable` multi-branch states are now executable:
    - same-collection raw `UNION ALL` branches only when exact filters imply pairwise-disjoint
      finite point-ID bounds
    - same-collection raw `UNION DISTINCT` branches over exact filters
    - same-collection raw `INTERSECT DISTINCT` branches over exact filters
    - same-collection raw `EXCEPT DISTINCT` branches over exact filters
    - all currently rewrite to a single filtered scan
  - those extracted child kernels now also compose upward in the same analyzer pass:
    - exact `COUNT(*)` and exact scalar-facet grouped counts can still replace the larger parent
      subtree after a mergeable child region collapses to one scan-local kernel
  - redundant `DISTINCT` over a raw full-row `Qdrant` scan is now dropped because row identity
    already includes unique `id`
- heterogeneous named-vector scans with top-level nullable vector columns
- exact SQL null semantics for `payload:<path>`:
  - `IS NULL` means missing or explicit null
  - `IS NOT NULL` means present and non-null
  - empty scalar values remain ordinary non-null SQL values, for example `payload:<path> = ''`
- direct scan-path projection of known payload fields now becomes typed logical output
- public typed payload helper `payload(accessor, 'Type')` is available when SQL planning needs an explicit payload scalar type

## Not Yet Admitted

- write support or `INSERT INTO`
- payload empty-container/cardinality semantics, text, geo, nested, and count-oriented payload predicates
- broader payload-key SQL `ORDER BY` pushdown beyond the admitted `payload:<path>` subset
- broader aggregate/grouped SQL beyond the admitted scalar-facet subset
- fully implicit arithmetic and similar typed SQL over raw `payload:<path>` when `DataFusion` must infer the payload scalar type during SQL planning; use `payload(payload:<path>, 'Type')` or an explicit `CAST(...)` today
- broader `Qdrant`-specific UDF, UDAF, or UDTF surface beyond the current nearest marker UDF and typed `payload(...)` helper
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

Exact aggregate-like pushdown and the first retrieval relation currently require the crate's
prepared session context:

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

The first retrieval prototype is nearest-neighbor query through a DataFusion-native marker UDF on
the same prepared session surface:

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
    .sql(
        "SELECT id, payload, qdrant_nearest_score(embedding, 1.0, 0.0, 0.0) AS score \
         FROM vectors \
         WHERE id <> '3' AND qdrant_nearest_score(embedding, 1.0, 0.0, 0.0) >= 0.25 \
         ORDER BY score DESC \
         LIMIT 10",
    )
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

SELECT payload:rank AS rank
FROM docs
ORDER BY payload(payload:rank, 'Integer');

SELECT id, payload(payload:rank, 'Integer') + 1 AS next_rank
FROM docs
WHERE payload(payload:rank, 'Integer') >= 10
ORDER BY payload(payload:rank, 'Integer');

SELECT id, qdrant_nearest_score(embedding, 1.0, 0.0) AS score
FROM docs
WHERE qdrant_nearest_score(embedding, 1.0, 0.0) >= 0.3
ORDER BY score DESC
LIMIT 10;

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

SELECT payload:active AS active, COUNT(*) AS total
FROM docs
GROUP BY payload:active
ORDER BY total DESC
LIMIT 10;
```

## Verification

```bash
just checks
```
