# `qdrant-datafusion`

`qdrant-datafusion` exposes `Qdrant` collections as `DataFusion` tables.

The current crate scope is intentionally narrow: correct, paginated collection scans over the
canonical Arrow carriers used by `ndarrow` and `nabled::arrow`, the exact pushdown-first SQL
bridge for ordering and filtering, canonical-schema `INSERT INTO` / `REPLACE INTO` /
`INSERT OVERWRITE`, exact `DELETE`, and canonical row-rewrite `UPDATE` mutation semantics,
including `DataFusion` target-column reshaping into the write schema for inserts and
exact-plus-residual filter handling for updates, and the first narrow planner slices for exact
`COUNT(*)` and top-facet grouped-count pushdown. It is not yet the broad SQL surface for
`Qdrant` fusion, broader grouped retrieval, or broader planner rewrites.

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
- canonical-schema `INSERT INTO`, `REPLACE INTO`, and `INSERT OVERWRITE` through `DataSinkExec` when `DataFusion` produces the canonical qdrant provider schema
  - current admitted write row contract is the provider schema: `id`, optional `payload` JSON text, and the declared qdrant vector columns
  - explicit target-column inserts are admitted when `DataFusion` normalizes them into that schema, including reordered target columns and omission of nullable `payload`
  - `INSERT INTO` now follows the current qdrant `InsertOnly` contract on the validated runtime line: existing ids are preserved while new ids insert
  - `REPLACE INTO` now follows the current qdrant `Upsert` contract on the validated runtime line: colliding ids are replaced while new ids insert
  - `INSERT OVERWRITE` now clears the collection first, then writes canonical rows through the same sink path
- exact `DELETE FROM vectors [WHERE ...]` on the provider table through `QdrantDeleteExec`
  - admitted delete predicates currently reuse the existing exact qdrant filter algebra
  - empty `DELETE FROM vectors` deletes all rows and returns the standard `count` result row
  - broader residual/local delete semantics are not implied; unsupported delete predicates stay explicit in the mirrored SQL inventory
- canonical row-rewrite `UPDATE ... SET ... [WHERE ...]` on the provider table through `QdrantUpdateExec`
  - updates execute as stable-id row rewrites over the canonical provider schema rather than backend-specific partial mutation APIs
  - current row identity stays stable: `id` assignment remains deferred
  - exact admitted qdrant filters lower remotely; non-pushdownable predicates stay as local residual filters over the materialized candidate rows before rewrite
  - current mirrored SQL inventory covers payload rewrites, `CASE` assignment, `NULL`, and whole-table update; `UPDATE ... FROM` remains upstream unsupported
- schema/projection-driven vector selection
- SQL `LIMIT` pushdown to the scan stream
- exact physical sort pushdown for `ORDER BY id ASC`
- exact payload-key sort pushdown for the admitted single-key payload-path subset, including direct `payload:<path>`, equivalent `payload(payload:<path>, 'Type')` forms, and order-preserving casts over the authoritative payload scalar type on indexed integer, float, and datetime payload fields when `collection_cluster_info` proves a stable single-peer collection
  - distributed, transferring, or resharding collection states fall back to local `DataFusion` sorting instead of claiming exact remote order
- exact boolean filter pushdown over the admitted leaf subset:
  - `AND`, `OR`, and `NOT`
  - `id =`, `id !=`, `id IN (...)`, `id NOT IN (...)`
  - vector-column `IS NULL` / `IS NOT NULL`
- indexed scalar payload-field comparisons, `IN`, `NOT IN`, `BETWEEN`, and `NOT BETWEEN` over the admitted `payload:<path>`, equivalent public `payload(...)` forms, and exact casts whose target type matches the authoritative payload scalar type
  - integer match predicates require lookup-capable integer indexes
  - integer range predicates require range-capable integer indexes
- exact `COUNT(*)` pushdown over a single `Qdrant` source through the crate's session/planner helper
- exact top-facet grouped-count pushdown over one scalar `payload:<path>` field through the crate's session/planner helper
  - currently admitted facet fields are keyword, bool, and lookup-capable integer payload indexes
  - facet keys currently surface as `Utf8`, matching the current textual `payload:<path>` SQL bridge
- a generic public `QdrantOpNode` / `QdrantOp` layer now exists above the shared kernel family
  for the current query-family retrieval prototypes
- current exact `COUNT(*)`, scalar facet, and current query-family retrieval slices now lower
  through one shared internal `QdrantKernelNode` / `QdrantKernelSpec` family rather than
  isolated logical node types
- current retrieval prototypes are DataFusion-native marker UDFs on the prepared session surface:
  - `qdrant_nearest_score(vector_column, ...)`
    - exact lowering currently admits dense query vectors, an optional `LIMIT`,
      optional exact base filters, and optional score-threshold predicates; when SQL omits `LIMIT`, the remote request uses Qdrant's default result count and omitted projected score ordering uses Qdrant's native score-desc result order
  - `qdrant_sample_score([method])`
    - exact lowering currently admits random sampling and an optional `LIMIT`; when SQL omits `LIMIT`, the remote request uses Qdrant's default result count and omitted projected score ordering uses Qdrant's native result order
    - the method currently defaults to `'random'`, and the public Rust helper is nullary because that is the only admitted method today
  - `qdrant_order_by_score(payload:<path>[, direction])`
    - exact lowering currently admits canonical indexed integer / float / datetime payload paths
      plus order-preserving casts on the prepared session surface; direction currently admits
      boolean or `'asc'` / `'desc'` literals
    - the public Rust helper surface exposes `qdrant_order_by_score(path,
      QdrantOrderByDirection::{Asc, Desc})` so direction is typed rather than boolean or stringly
    - when SQL omits `LIMIT`, the remote request uses Qdrant's default result count; the current
      mirrored SQL inventory closes descending remote order through `ORDER BY score DESC`, and
      non-canonical scalar expressions remain unsupported by design
  - `qdrant_recommend_score(...)`
    - exact lowering currently admits positive and negative example lists and an optional `LIMIT`; when SQL omits `LIMIT`, the remote request uses Qdrant's default result count and omitted projected score ordering uses Qdrant's native score-desc result order
    - the default or explicit recommend strategy is admitted, and the public Rust helper uses typed `QdrantRecommendStrategy`
  - `qdrant_discover_score(...)` and `qdrant_context_score(...)`
    - exact lowering currently admits dense vector targets/context pairs and an optional `LIMIT`; when SQL omits `LIMIT`, the remote request uses Qdrant's default result count and omitted projected score ordering uses Qdrant's native score-desc result order
  - `qdrant_nearest_with_mmr_score(...)`
    - exact lowering currently admits dense query vectors, diversity, candidates limit, and an optional `LIMIT`; when SQL omits `LIMIT`, the remote request uses Qdrant's default result count and omitted projected score ordering uses Qdrant's native score-desc result order
  - `qdrant_relevance_feedback_score(...)`
    - exact lowering currently admits dense vector targets, feedback-item arrays using
      `struct(example, score)` entries, an optional `LIMIT`, and required naive
      strategy coefficients; when SQL omits `LIMIT`, the remote request uses Qdrant's default result count and omitted projected score ordering uses Qdrant's native score-desc result order
  - grouped query-family top-1 via `DISTINCT ON (payload:<path>)`, with outer `LIMIT` kept local after exact grouped retrieval
    - exact lowering currently admits one scalar keyword or lookup-capable integer payload field and one grouped query-family source among `qdrant_nearest_score(...)`, `qdrant_sample_score(...)`, `qdrant_recommend_score(...)`, `qdrant_discover_score(...)`, `qdrant_context_score(...)`, `qdrant_nearest_with_mmr_score(...)`, or `qdrant_relevance_feedback_score(...)`,
      `ORDER BY payload:<path>[ DESC]` with optional trailing `, score DESC` as an explicit in-group tie-break,
      validates returned group ids against scalar payload values on hits, and keeps any outer `LIMIT` local
    - broader grouped `DISTINCT ON` SQL that cannot truthfully map to `query_groups`, such as multi-key `DISTINCT ON`, now stays local above a normal `QdrantQueryExec` instead of failing
  - coordinated score composition through `qdrant_formula_score(...)` and `qdrant_fusion_score(...)`
    - exact lowering currently admits the narrow coordinated subset over retrieval relations: an id-preserving `FULL OUTER JOIN USING (id)` over admitted query-family score branches with effective `ORDER BY score DESC`
    - outer `LIMIT` is optional; when SQL omits it, the remote coordinated request omits `limit` and uses Qdrant's default result count
    - `qdrant_formula_score(...)` admits the current coordinated score arithmetic subset plus the current branch-local qdrant-leaf subset when the formula binds to one independently-closable branch and the outer join remains local
    - `qdrant_fusion_score(...)` now additionally admits a local aligned-join fallback over `INNER` / `LEFT` / `RIGHT` joins on `id` with explicit score-column inputs for both `RRF` and `DBSF`; `CROSS JOIN` stays unsupported by design because it does not align the same candidate across branches, and the public Rust helper now uses typed `QdrantFusionMethod`
  - the public Rust helper surface now also closes the current admitted formula/payload helper gaps:
    - `qdrant_payload(payload:<path>, <DataType>)` is typed on the Rust side instead of stringly
    - `qdrant_payload_num(...)` / `qdrant_payload_datetime(...)` now expose current default-value forms without raw UDF calls
    - `qdrant_exp_decay(...)` / `qdrant_gauss_decay(...)` / `qdrant_lin_decay(...)` now expose the admitted target and midpoint forms without raw UDF calls
  - projected `ORDER BY score DESC` is redundant and optimizes away, while projected `ORDER BY score ASC` remains a local `DataFusion` sort
  - explicit payload presence/null/missing/empty/non-empty/count semantics through `payload_exists(payload:<path>)`, `payload_is_missing(payload:<path>)`, `payload_is_null(payload:<path>)`, `payload_is_empty(payload:<path>)`, `payload_has_values(payload:<path>)`, and `payload_values_count(payload:<path>)`, all with exact scan filter pushdown and local execution
  - explicit geo distance semantics through `payload_geo_distance(payload:<path>, lon, lat)`, with local numeric execution and exact scan filter pushdown for the `<= radius` subset on geo payload fields
  - explicit nested-array predicates through `payload_nested_match(payload:<path>, <predicate>)`, with exact scan filter pushdown for nested payload-array/object predicates expressed in the existing payload filter algebra
  - explicit text and phrase semantics through `payload_text_match(payload:<path>, 'query')` and `payload_phrase_match(payload:<path>, 'phrase')`, both as exact scan filter pushdown on text-indexed payload fields, with phrase matching requiring a text index that enables phrase support
  - retrieval kernels can now leave benign local projection shells, local aggregate shells, local window shells, and local residual filter shells above the closed qdrant query kernel instead of requiring fully remote-only projection/filter shapes, including later score projection above those local filter shells
  - projected score columns follow normal `DataFusion` naming and aliasing rules
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
- exact `CAST(payload:<path> AS <canonical type>)` forms now preserve the same payload-path semantics for scan filter pushdown and qdrant query-surface payload projections when authoritative payload metadata exists, while payload-key sort pushdown also admits broader order-preserving casts such as numeric-to-numeric or temporal-to-temporal forms on the same guarded stable single-peer contract

## Not Yet Admitted

- broader write semantics beyond the current canonical `INSERT INTO` / `REPLACE INTO` / `INSERT OVERWRITE` / exact `DELETE` / row-rewrite `UPDATE` contract, including stable-id mutation, merge semantics, backend-specific partial payload/vector mutation APIs, richer reshaping than the current target-column normalization path, and delete shapes that need residual/local predicate execution
- broader payload/container distinctions beyond the current explicit `payload_exists(...)`, `payload_is_missing(...)`, `payload_is_null(...)`, `payload_is_empty(...)`, `payload_has_values(...)`, `payload_values_count(...)`, `payload_geo_distance(...) <= radius`, `payload_geo_within_bbox(...)`, `payload_geo_within_polygon(...)`, `payload_nested_match(...)`, `payload_text_match(...)`, `payload_text_any(...)`, and `payload_phrase_match(...)` subset
- broader payload-key SQL `ORDER BY` pushdown beyond the admitted `payload:<path>` subset
- broader aggregate/grouped SQL beyond the admitted scalar-facet subset
- fully implicit arithmetic and similar typed SQL over raw `payload:<path>` when `DataFusion` must infer the payload scalar type during SQL planning; use `payload(payload:<path>, 'Type')` or an explicit `CAST(...)` today
- broader `Qdrant`-specific UDF, UDAF, or UDTF surface beyond the current retrieval marker UDFs and typed `payload(...)` helper
- broader SQL-native coordination / fusion / grouped-query semantics beyond the current admitted coordinated full-outer-join subset and `DISTINCT ON` grouped query-family subset
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
INSERT INTO docs
SELECT id, payload, vector
FROM staging_docs;

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
LIMIT 10;

SELECT id, qdrant_sample_score() AS score
FROM docs
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
