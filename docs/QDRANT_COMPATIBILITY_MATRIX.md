# Qdrant Compatibility Matrix

Last updated: 2026-04-06

## Purpose

This document is the detailed capability inventory for `qdrant-datafusion`.

Unlike `docs/CAPABILITY_MATRIX.md`, which tracks the repository’s current admitted scope, this
document inventories the broader current `Qdrant` feature surface and classifies it by semantic
family, implementation shape, and release fit.

The organizing rule is semantic, not SDK-shaped:

1. row restriction
2. row ordering
3. row production
4. ranking / re-scoring
5. aggregation / grouping
6. mutation
7. administration

That classification is what the next release plan should compose over.

## Sources

This matrix is derived from:

1. official `Qdrant` concept docs on:
   - collections
   - points
   - payload
   - filtering
   - indexing
   - explore
   - hybrid queries
   - snapshots
2. the pinned local `qdrant-client 1.17.0` API surface

## Legend

- `Current`: already admitted in `qdrant-datafusion`
- `Next`: reasonable for the next feature release
- `Later`: likely useful, but should follow a more foundational layer
- `Out`: not a good fit for the crate’s SQL/data-source responsibility

## Matrix

| Semantic family | Qdrant capability | Current qdrant-client surface | SQL / DataFusion shape | Release fit | Notes |
|---|---|---|---|---|---|
| Row restriction | `must` / `should` / `must_not` / nested filter composition | `Filter`, `Condition` | predicate algebra | `Current` | The core bridge is now provider-owned predicate IR rather than ad hoc inline lowering, including nested-scope reuse through `payload_nested_match(...)`. |
| Row restriction | `has_id` | `Condition::has_id` | `id =`, `id !=`, `id IN`, `id NOT IN` | `Current` | Already admitted. |
| Row restriction | `has_vector` | `Condition::has_vector` | vector-column `IS NULL` / `IS NOT NULL` | `Current` | Already admitted through nullable vector scan contract. |
| Row restriction | payload equality | `Condition::matches` | `payload:<path> = ...` | `Current` | Already admitted for indexed scalar payload fields. Integer equality currently requires lookup-capable integer indexes. |
| Row restriction | payload inequality / range | `Condition::range`, `Condition::datetime_range` | `<`, `<=`, `>`, `>=`, non-negated `BETWEEN` | `Current` | Already admitted for integer / float / datetime. |
| Row restriction | `Match Any` | `Condition::matches` over collections | `IN (...)` | `Current` | Admitted for the current exact predicate algebra. |
| Row restriction | `Match Except` | `Condition::matches(!MatchValue::...)` | `NOT IN (...)` | `Current` | Admitted for the current exact predicate algebra. |
| Row restriction | same-field equality disjunction | normalized to `IN` | `a = 1 OR a = 2` | `Current` | Admitted as an optimization inside the broader exact boolean predicate algebra. |
| Row restriction | general `OR` | `should` | boolean predicate normalization | `Current` | Admitted exactly over the current leaf subset. Unsupported branches still reject cleanly. |
| Row restriction | general `NOT` | `must_not` | boolean predicate normalization | `Current` | Admitted exactly over the current leaf subset. Payload-empty semantics are still deferred. |
| Row restriction | `is_null` | field condition | payload null semantics | `Current` | SQL `payload:<path> IS NULL` is now admitted exactly as missing or explicit null. Backend lowering composes `is_null` with missing-only detection. |
| Row restriction | field existence | `values_count >= 0` | `payload_exists(payload:<path>)` | `Current` | The SQL bridge now exposes explicit field presence semantics separately from SQL null semantics. Present explicit `null` and empty arrays still count as existing fields. |
| Row restriction | field missing | `NOT(values_count >= 0)` | `payload_is_missing(payload:<path>)` | `Current` | The SQL bridge now exposes missing-field semantics explicitly instead of forcing callers to derive them from SQL null rules. |
| Row restriction | explicit null | `is_null` | `payload_is_null(payload:<path>)` | `Current` | This is the explicit-null-only counterpart to SQL `payload:<path> IS NULL`, which still means missing-or-null on the SQL bridge. |
| Row restriction | `is_empty` | field condition | payload empty / missing semantics | `Current` | Runtime contract is now validated more precisely: `is_empty` matches missing, explicit null, and `[]`, but not empty strings or empty objects on the current runtime line. The SQL bridge now exposes that explicit subset through `payload_is_empty(payload:<path>)` while still keeping empty strings on ordinary equality semantics. |
| Row restriction | positive value count | `values_count > 0` | `payload_has_values(payload:<path>)` | `Current` | This is the explicit non-empty counterpart to the presence/empty family. On the current runtime line, scalars and objects count as `1`, so this means positive cardinality rather than array-only non-emptiness. |
| Row restriction | `values_count` | field condition | cardinality predicates | `Current` | The SQL bridge now exposes explicit cardinality predicates through `payload_values_count(payload:<path>)`. Current runtime tests on the active line show missing fields map to `NULL`, explicit `null` and `[]` map to `0`, and present non-array values map to `1`. Broader typed/container semantics are still deferred. |
| Row restriction | nested object filter | nested condition | `payload_nested_match(payload:<path>, <predicate>)` | `Current` | The SQL bridge now exposes explicit nested-array/object predicates by reusing the existing payload filter algebra inside a nested scope instead of introducing a string mini-language. |
| Row restriction | geo radius via explicit payload distance | `Condition::geo_radius` | `payload_geo_distance(payload:<path>, lon, lat) <= radius` | `Current` | The public SQL bridge is numeric and locally executable; only the `<= radius` subset is claimed as exact remote pushdown. |
| Row restriction | geo bbox / polygon | geo conditions | `payload_geo_within_bbox(payload:<path>, lon1, lat1, lon2, lat2)` / `payload_geo_within_polygon(payload:<path>, [[lon, lat], ...])` | `Current` | The SQL bridge now exposes explicit bbox and polygon predicates over geo payload fields. Bbox accepts two opposing corners, polygon currently models one exterior ring and auto-closes an open ring instead of requiring the first point to be repeated. |
| Row restriction | text match | text condition | `payload_text_match(payload:<path>, 'query')` | `Current` | This stays an explicit remote predicate because exact semantics depend on the configured Qdrant text index rather than SQL `LIKE` or local string functions. |
| Row restriction | phrase match | phrase condition | `payload_phrase_match(payload:<path>, 'phrase')` | `Current` | Exact pushdown now exists when the payload field is backed by a text index that enables phrase support. |
| Row restriction | text match any | text condition | `payload_text_any(payload:<path>, ['term', ...])` | `Current` | The SQL bridge now exposes explicit text-any predicates on text-indexed payload fields through the same remote-only exact predicate model as text/phrase. |
| Row ordering | ID-ordered scan | `scroll` | `ORDER BY id ASC` | `Current` | Already exact. |
| Row ordering | payload-key ordered scroll | `order_by` on `scroll` | `ORDER BY payload:<path>` | `Current` | Admitted exact subset for indexed integer / float / datetime fields. Exact remote pushdown is now guarded by `collection_cluster_info`: stable single-peer collections stay exact, while distributed or in-flight cluster states fall back to local sorting. |
| Row ordering | broader payload ordering | `order_by` | richer payload path ordering | `Later` | Only after payload access contract stabilizes further. |
| Row production | ID-ordered full scan | `scroll` | base table relation | `Current` | This is the stable table-scan baseline. |
| Row production | nearest-neighbor search | `query(Query::Nearest)` / `search` | relation-producing retrieval | `Current` | The first retrieval prototype now uses `qdrant_nearest_score(...)` as a marker UDF over the prepared session context. Exact lowering currently admits dense query vectors, an optional `LIMIT`, optional exact base filters, and optional score-threshold predicates; when SQL omits `LIMIT`, the remote request uses Qdrant's default result count, and when SQL omits projected score ordering it uses Qdrant's native score-desc result order. Projected `ORDER BY score DESC` is redundant and optimizes away, while projected `ORDER BY score ASC` remains a local DataFusion sort. Benign local projection shells, local aggregate shells, local window shells, and local residual filter shells can now remain above the closed qdrant query kernel instead of forcing fully remote-only projection/filter shapes, including aggregate subquery / CTE cases, direct query-family window cases, direct nearest `HAVING`, and later score projection above those local filter shells. The score only enters the output when projected, and aliases follow normal `DataFusion` naming. |
| Row production | nearest with MMR | `query(Query::NearestWithMmr)` | retrieval + ranking modifier | `Current` | Now exposed through `qdrant_nearest_with_mmr_score(...)` on the prepared session surface. Exact lowering currently admits dense query vectors, diversity/candidates-limit literals, and an optional `LIMIT`; when SQL omits `LIMIT`, the remote request uses Qdrant's default result count, and when SQL omits projected score ordering it uses Qdrant's native score-desc result order. Projected `ORDER BY score DESC` is redundant and optimizes away, while projected `ORDER BY score ASC` remains a local DataFusion sort. |
| Row production | recommendation | `query(Query::Recommend)`, `recommend` | relation-producing retrieval | `Current` | Now exposed through `qdrant_recommend_score(...)` on the prepared session surface. Exact lowering currently admits positive / negative example lists and an optional `LIMIT`; when SQL omits `LIMIT`, the remote request uses Qdrant's default result count, with default or explicit strategy selection, and when SQL omits projected score ordering it uses Qdrant's native score-desc result order. Projected `ORDER BY score DESC` is redundant and optimizes away, while projected `ORDER BY score ASC` remains a local DataFusion sort. |
| Row production | discovery | `query(Query::Discover)`, `discover` | relation-producing retrieval | `Current` | Now exposed through `qdrant_discover_score(...)` on the prepared session surface. Exact lowering currently admits a dense vector target, context pairs, and an optional `LIMIT`; when SQL omits `LIMIT`, the remote request uses Qdrant's default result count, and when SQL omits projected score ordering it uses Qdrant's native score-desc result order. Projected `ORDER BY score DESC` is redundant and optimizes away, while projected `ORDER BY score ASC` remains a local DataFusion sort. |
| Row production | context query | `query(Query::Context)` | relation-producing retrieval | `Current` | Now exposed through `qdrant_context_score(...)` on the prepared session surface. Exact lowering currently admits context pairs and an optional `LIMIT`; when SQL omits `LIMIT`, the remote request uses Qdrant's default result count, and when SQL omits projected score ordering it uses Qdrant's native score-desc result order. Projected `ORDER BY score DESC` is redundant and optimizes away, while projected `ORDER BY score ASC` remains a local DataFusion sort. |
| Row production | sample | `query(Query::Sample)` | relation-producing retrieval | `Current` | Now exposed through `qdrant_sample_score([method])` on the prepared session surface. Exact lowering currently admits random sampling with an optional `LIMIT`; when SQL omits `LIMIT`, the remote request uses Qdrant's default result count, the method still defaults to `'random'` when omitted, and omitted projected score ordering uses Qdrant's native result order. Projected `ORDER BY score DESC` is redundant and optimizes away, while projected `ORDER BY score ASC` remains a local DataFusion sort. |
| Row production | prefetch subqueries | `QueryPointsBuilder::prefetch` | retrieval pipeline / subquery composition | `Later` | Important for hybrid query plans, but should follow core retrieval IR. |
| Row production | `using` named vector | query/search/recommend builders | retrieval relation parameter | `Current` | The current nearest prototype already admits named-vector selection through the vector column argument to `qdrant_nearest_score(...)`. |
| Row production | `lookup_from` | query/search/recommend/group builders | cross-collection lookup parameter | `Later` | Useful, but not first-wave. |
| Row production | score threshold | query/search builders | retrieval relation modifier | `Current` | The current nearest prototype already admits optional score-threshold predicates over `qdrant_nearest_score(...)`. |
| Row production | search params (`ef`, exact, quantization knobs) | `SearchParams` | retrieval relation modifier / hint | `Later` | Important, but probably better as explicit parameters after the relation surface exists. |
| Row production | read consistency / timeout / shard selector | builders | execution modifiers | `Later` | Real features, but not part of the core SQL denotation. |
| Ranking / re-scoring | fusion (`RRF`, `DBSF`) | `Query::Fusion`, `Query::Rrf` | ranking composition over retrieval relations | `Current` | The current coordinated fusion slice is admitted through `qdrant_fusion_score(...)` over the narrow exact relation shape: id-preserving `FULL OUTER JOIN USING (id)` over admitted query-family score branches with effective `ORDER BY score DESC`. Outer `LIMIT` is optional; when SQL omits it, the coordinated remote request omits `limit` and uses Qdrant's default result count. `RRF` and `DBSF` now additionally have a local aligned-join fallback over `INNER` / `LEFT` / `RIGHT` joins on `id` with explicit score-column inputs. `CROSS JOIN` remains by-design unsupported because it does not align the same candidate across branches. Broader coordination remains later. |
| Ranking / re-scoring | formula query | `Query::Formula` | score-expression modifier | `Current` | The current coordinated formula slice is now admitted through `qdrant_formula_score(...)` over the same narrow exact coordination contract: id-preserving `FULL OUTER JOIN USING (id)` over admitted query-family score branches with effective `ORDER BY score DESC`, plus the current coordinated score-arithmetic subset. `qdrant_formula_score(...)` now also admits the current branch-local qdrant-leaf subset when the formula binds to one independently-closable join branch and the outer SQL join remains local. Outer `LIMIT` is optional; when SQL omits it, the coordinated remote request omits `limit` and uses Qdrant's default result count. Broader score-expression semantics remain later. |
| Ranking / re-scoring | relevance feedback | `Query::RelevanceFeedback` | feedback-driven ranking | `Current` | Now exposed through `qdrant_relevance_feedback_score(...)` on the prepared session surface. Exact lowering currently admits dense vector targets, feedback-item arrays using `struct(example, score)` entries, an optional `LIMIT`, and required naive strategy coefficients; when SQL omits `LIMIT`, the remote request uses Qdrant's default result count, and when SQL omits projected score ordering it uses Qdrant's native score-desc result order. Projected `ORDER BY score DESC` is redundant and optimizes away, while projected `ORDER BY score ASC` remains a local DataFusion sort. |
| Aggregation / grouping | point count | `count` | exact `COUNT(*)`-like pushdown | `Current` | The first aggregate-like slice is now admitted through a narrow analyzer / extension-planner path over a single `Qdrant` source. It composes directly over the existing predicate algebra. |
| Aggregation / grouping | top-facet grouped counts over one scalar payload field | `facet` | `GROUP BY payload:<path> ... LIMIT N` | `Current` | The current facet slice now admits keyword, bool, and lookup-capable integer payload indexes. Projected `ORDER BY count DESC` is optional and redundant because it matches qdrant facet's native top-count order. Facet keys still surface as `Utf8`, matching the current textual `payload:<path>` SQL bridge, so this remains intentionally narrower than general SQL grouping. Live collection introspection on the current runtime line now preserves integer lookup/range metadata well enough to keep integer facets exact on the same admission contract. |
| Aggregation / grouping | grouped search results | `query_groups`, `search_groups`, `recommend_groups` | grouped retrieval relation | `Current` | The current admitted grouped retrieval slice is narrow but now query-family rather than nearest-only: top-1 grouped retrieval through `SELECT DISTINCT ON (payload:<path>) ... <grouped-score-surface> ... ORDER BY payload:<path>[ DESC]`, with optional trailing `, score DESC` as an explicit in-group tie-break, lowering to `query_groups` with group size 1 over one scalar keyword or lookup-capable integer payload field. Current grouped score surfaces are `qdrant_nearest_score(...)`, `qdrant_recommend_score(...)`, `qdrant_discover_score(...)`, and `qdrant_context_score(...)`. Grouped execution validates that returned group ids match scalar payload values on the hits, and because SQL group ordering is finalized locally, outer `LIMIT` remains local instead of being pushed into `query_groups`. Broader grouped `DISTINCT ON` SQL now localizes above a normal query kernel instead of failing, but broader exact grouped retrieval variants remain later. |
| Aggregation / grouping | `with_lookup` on groups | group builders | grouped retrieval enrichment | `Later` | Depends on grouped retrieval surface. |
| Aggregation / grouping | search matrix pairs | `search_matrix_pairs` | similarity-graph / pair relation | `Later` | Interesting, but specialized. |
| Aggregation / grouping | search matrix offsets | `search_matrix_offsets` | sparse similarity-matrix relation | `Later` | Same as above. |
| Mutation | point upsert | `upsert_points`, `upsert_points_chunked` | append-only `INSERT INTO` on the canonical provider schema | `Current` | Current write support is narrow and deliberate: `QdrantTableProvider::insert_into` lowers append-only writes through `DataSinkExec` when the upstream schema is logically equivalent to the qdrant table schema. Broader `MERGE`-like or reshaping writes remain later. |
| Mutation | point delete | `delete_points` | `DELETE` | `Later` | Same as above. |
| Mutation | vector update / delete | `update_vectors`, `delete_vectors` | `UPDATE` | `Later` | Requires a row identity and mutation contract first. |
| Mutation | payload set / overwrite / delete / clear | payload mutation APIs | `UPDATE` | `Later` | Needs payload-structure policy and write semantics. |
| Mutation | batched update | `UpdateBatchPointsBuilder` | DML batching | `Later` | Only after base write semantics exist. |
| Administration | collection create / update / delete | collection APIs | DDL / admin | `Out` | Better handled by admin tooling or a separate crate/layer. |
| Administration | field-index management | `create_field_index`, delete-field-index builder | DDL / admin | `Out` | Important operationally, but not a good first responsibility for this SQL scan crate. |
| Administration | aliases | alias APIs | DDL / catalog indirection | `Out` | Better treated as catalog / admin layer. |
| Administration | snapshots | snapshot APIs | admin / backup | `Out` | Not a SQL query concern. |
| Administration | shard / cluster setup | cluster APIs | admin / topology | `Out` | Operational concern, not part of SQL data-source semantics. |

## Deconstruction

### 1. Predicate algebra

This is still the highest-value foundation because it composes everywhere:

1. base scans
2. count
3. facet
4. nearest / sample / recommend / discover / context
5. grouped retrieval

The remaining predicate work should be treated as a single algebra problem:

1. boolean normalization
2. payload path reference normalization
3. leaf predicate typing and coercion
4. exact-subset validation
5. lowering to `Qdrant` filter conditions

### 2. Retrieval algebra

`Qdrant`’s major differentiator is not just filtering. It is row production:

1. nearest
2. recommend
3. discover
4. context
5. relevance feedback

These should not be forced into the current table-scan contract. They are relation-producing
operators and should be modeled as retrieval relations or retrieval specs that compose with the
predicate algebra.

### 3. Ranking algebra

The following are not independent top-level features. They are modifiers over retrieval:

1. fusion
2. formula
3. MMR
4. relevance feedback

So they should be designed only after the first retrieval relation exists. The current fusion /
formula subset now follows that rule by composing over retrieval relations rather than table scans.

### 4. Aggregation / grouping algebra

`count` and `facet` are the strongest next SQL fits after predicate completion because they already
compose over filters and do not require the larger retrieval SQL design to be settled first.

Grouped retrieval should follow only after the retrieval relation surface is stable.

## Next Release Priority

### P0: continue aggregate-like exploration over the predicate algebra

This remains the strongest next implementation focus.

1. explicit output contracts for aggregate-like `Qdrant` exploration surfaces beyond exact `COUNT(*)` and the first scalar-facet slice
2. determine the next grouped/exploration surface without overstating `Qdrant` facet as general SQL grouping
4. preserve the exact-subset-first boundary already established by the predicate algebra

### P0.5: broaden predicate families beyond the current explicit empty/cardinality/geo/text subset

The explicit `payload_exists(...)` / `payload_is_missing(...)` / `payload_is_null(...)` / `payload_is_empty(...)` / `payload_has_values(...)` / `payload_values_count(...)` slice is now in place, the first geo bridge now exists through `payload_geo_distance(...) <= radius`, the first text bridge now exists through `payload_text_match(...)` / `payload_phrase_match(...)`, and nested payload-array/object predicates now exist through `payload_nested_match(...)`. The remaining work is to widen the predicate family without guessing semantics.

1. keep missing-vs-null-vs-empty semantics explicit instead of guessing
2. extend broader array/object-only container predicates only where the SQL contract is explicit
3. avoid conflating SQL null with backend-specific container predicates

### P1: add aggregate-like exploration that composes over filters

These are strong next-release candidates because they are SQL-natural and reuse the predicate work:

1. broader aggregate-like exploration beyond exact single-source counts and the first scalar-facet slice

### P2: broaden retrieval relations beyond nearest

The first retrieval relation now exists through the prepared session surface. The next expansion is
to keep the retrieval family compositional without freezing SQL syntax too early:

1. search params and execution hints where they do not distort denotation
2. recommendation / discovery / context
3. keep the score/output contract stable while widening relation kinds

### P3: follow with retrieval modifiers

Now that the first retrieval relation exists:

1. broader fusion
2. broader grouped retrieval
3. broader formula
4. MMR

### Deferred from the next release

1. broader grouped retrieval variants
2. search-matrix APIs
3. write semantics
4. collection / index / alias / snapshot / cluster administration

## Recommended Implementation Order

1. settle whether the next grouped/exploration step is broader facet semantics or a separate aggregate-like relation
2. settle explicit payload empty semantics
3. preserve the nearest score/output contract while widening retrieval
4. implement the next retrieval relations
5. layer retrieval modifiers and secondary retrieval operators on top

That order is the most compositional one currently available.

## Planner Note

The current planner structure now has an explicit subtree-replacement scaffold for admitted
`Qdrant` relations:

1. source class
2. topology class
3. composition class
4. relation recognizer

The scaffold now classifies a broader internal space:

1. source class:
   - `none`
   - `single-source Qdrant`
   - `multi-source Qdrant`
   - `mixed`
2. topology class:
   - `leaf`
   - `unary chain`
   - `unary relation change`
   - `multi-branch`
3. composition class:
   - `atomic`
   - `mergeable`
   - `batchable`
   - `coordinated`
   - `local-compose`
   - `invalid`
4. kernel placement:
   - `none`
   - `exact-self`
   - `exact-child`
   - `exact-children`

The currently admitted replacement subset is still intentionally narrower:

1. exact single-source atomic relations plus the current exact-child / exact-children extraction subsets
2. relation kinds: exact `COUNT(*)` and the first scalar-facet grouped-count subset
3. the first explicit invalid planner surface is projection-time `payload:<path>` access in the prepared session/planner path when no admitted exact kernel owns that expression
4. the first explicit `mergeable` multi-branch state is same-collection raw `UNION ALL`
   branches only when exact filters imply pairwise-disjoint finite point-ID bounds
5. that first `mergeable` case is now executable and rewrites to a single filtered scan rather
   than remaining classifier-only
6. raw same-collection `UNION DISTINCT` over exact filters is now the second executable
   `mergeable` case because duplicate elimination is already part of the SQL semantics
7. raw same-collection `INTERSECT DISTINCT` and `EXCEPT DISTINCT` over exact filters are now
   executable `mergeable` cases too; for raw full-row branches they lower to conjunction and
   left-minus-right filter algebra respectively
8. redundant `DISTINCT` over raw full-row `Qdrant` scans is now dropped when the row identity
   still includes unique `id`
9. those mergeable child kernels are now explicitly validated as compositional:
   exact `COUNT(*)` and the first scalar-facet grouped-count relation can still claim the larger
   parent subtree after the child region collapses in the same analyzer pass

Future expansion should widen those axes explicitly rather than adding planner-layer endpoint
features one by one.
