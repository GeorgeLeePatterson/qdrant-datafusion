# Status Snapshot

Last updated: 2026-04-05

## Summary

`qdrant-datafusion` now has a stable collection-scan baseline on the current dependency line.

Current branch reality:

1. The source baseline is a clean rewrite from `main`, not the earlier payload-filter spike branch.
2. `Cargo.toml` is aligned to:
   - the same `DataFusion` git revision used by `ndatafusion`
   - the current `qdrant-client` line
   - `ndarrow 0.0.4`
3. Collection scans now use paginated `Qdrant::scroll`, not `query`.
4. Vector columns use canonical carriers with top-level nullable scan fields.
5. Missing per-row named vectors become `NULL`, not execution errors.
6. Deprecated `qdrant-client` response fields are not part of the implementation surface.
7. Append-only `INSERT INTO` is now supported on the canonical qdrant table schema through `DataSinkExec`.
8. A shared `Qdrant` semantics layer plus provider-owned scan-pushdown model now exists for projection, payload access, filters, ordering, limit, and continuation.
9. Write-side Arrow/Qdrant serialization now exists for the canonical provider schema.
   - append-only `INSERT INTO` lowers through a `QdrantInsertSink` / `DataSinkExec` path
   - the current admitted write contract is explicit: upstream input must be logically equivalent to the qdrant table schema
10. `ORDER BY id ASC` is admitted as an exact physical sort pushdown case.
11. The single-node payload-key ordered-scroll runtime contract is now validated for integer, float, and datetime payload indexes.
12. Ordered continuation lowering is implemented internally through `order_by`, `start_from`, and boundary-ID exclusion.
13. The first payload-key SQL sort subset is now admitted as `ORDER BY payload:<path>` for indexed integer, float, and datetime payload fields, including exact casts whose target type matches the authoritative payload scalar type.
14. Payload-key sort pushdown is currently admitted as `Exact` on the validated runtime contract because `DataFusion` cannot execute a fallback physical sort for the `:` operator.
15. Predicate algebra over the admitted leaf subset is now exact:
    - `AND`, `OR`, and `NOT`
    - `id =`, `id !=`, `id IN (...)`, `id NOT IN (...)`
    - vector-column `IS NULL` / `IS NOT NULL`
    - indexed scalar `payload:<path>` comparisons, `IN`, `NOT IN`, `BETWEEN`, and `NOT BETWEEN`, including exact casts whose target type matches the authoritative payload scalar type
      - integer match predicates require lookup-capable integer indexes
      - integer range predicates require range-capable integer indexes
16. Physical filter pushdown now absorbs the admitted predicate algebra so `FilterExec` does not remain above `QdrantScanExec`.
17. Payload filter literals are coerced by indexed payload field type because `DataFusion`’s physical `payload:<path>` expressions surface generic scalar literals such as `Utf8("10")`.
18. Exact `COUNT(*)` pushdown is now admitted as the first aggregate-like planner slice.
    - it lowers into `Qdrant`’s native `count` API
    - it currently requires the `Qdrant` session/planner helper
    - it reuses the existing provider-owned predicate algebra for admitted exact filters
19. Exact top-facet grouped counts are now admitted as the second aggregate-like planner slice.
    - it lowers into `Qdrant`’s native `facet` API
    - it currently requires the `Qdrant` session/planner helper
    - it is currently limited to one admitted scalar `payload:<path>` field with `ORDER BY count DESC LIMIT N`
    - it reuses the existing provider-owned predicate algebra for admitted exact filters
20. The root `README.md`, repo notes, and tracker docs describe only the admitted baseline.
21. Detailed capability-expansion planning now has an explicit semantic inventory in `docs/QDRANT_COMPATIBILITY_MATRIX.md`.
22. Payload-null runtime semantics are now validated, and explicit empty/cardinality SQL semantics are now admitted through dedicated functions.
23. On March 25, 2026, live `Qdrant 1.17.0` tests through `qdrant-client 1.17.0` validated the runtime contract:
    - explicit payload `NULL` written via point upsert is preserved
    - explicit payload `NULL` written via `set_payload` is preserved
    - `is_null` matches explicit null only
    - `is_empty` matches missing, explicit null, and `[]`
    - `is_empty` does not match empty strings or empty objects on the current runtime line
    - `values_count >= 0` matches present fields, including explicit null and empty arrays
24. SQL null semantics for `payload:<path>` are now admitted exactly:
    - `IS NULL` means missing or explicit null
    - `IS NOT NULL` means present and non-null
    - the current lowering excludes empty arrays from SQL null by composing `is_null`, `is_empty`, and `values_count`
25. Payload-empty and cardinality semantics are now explicit rather than overloaded onto SQL nulls.
    - empty strings remain ordinary non-null values and are expressed through normal equality, for example `payload:<path> = ''`
    - explicit empty/container predicates now use `payload_is_empty(payload:<path>)`
    - explicit cardinality predicates now use `payload_values_count(payload:<path>)`
    - current runtime tests prove `payload_values_count` matches missing as `NULL`, explicit `null` and `[]` as `0`, and present non-array values as `1`
26. Planner-layer subtree replacement now uses a unified relation-pushdown analyzer scaffold for the admitted `Qdrant` relation replacements instead of separate analyzer-rule ownership by convention.
27. The planner scaffold now derives broader subtree classes explicitly before relation recognition.
    - source class: `none`, `single-source Qdrant`, `multi-source Qdrant`, `mixed`
    - topology class: `leaf`, `unary chain`, `unary relation change`, `multi-branch`
    - composition class: `atomic`, `mergeable`, `batchable`, `coordinated`, `local-compose`, `invalid`
    - kernel placement: `none`, `exact-self`, `exact-child`, `exact-children`
    - current admitted replacements still remain exact single-source atomic `Qdrant` relations only
28. The planner scaffold now distinguishes exact-self kernels from local shells around extracted child kernels.
    - direct scan-path `payload:<path>` projections are no longer treated as an invalid surface; they now rewrite to typed local payload accessors when the source payload schema is authoritative
    - raw unhinted arithmetic over `payload:<path>` still fails earlier in SQL planning and currently requires `payload(...)` or an explicit `CAST(...)`
29. The planner scaffold now has a first concrete `mergeable` multi-branch state.
    - same-collection raw `UNION ALL` branches are only classified as `mergeable` when exact filters imply pairwise-disjoint finite point-ID bounds
    - overlapping same-collection branches remain `local-compose`
30. That first `mergeable` case is now executable.
    - a provably disjoint same-collection raw `UNION ALL` rewrites to a single filtered scan
    - this is the first multi-branch `Qdrant` kernel extraction beyond classifier-only planner state
31. Raw same-collection `UNION DISTINCT` over exact filters is now the second executable `mergeable` case.
    - overlap between branches is admitted because duplicate elimination is already part of the SQL semantics
    - the analyzer rewrites that subtree to a single filtered scan too
32. Raw same-collection `INTERSECT DISTINCT` and `EXCEPT DISTINCT` over exact filters are now executable `mergeable` cases too.
    - DataFusion lowers these through `LeftSemi` / `LeftAnti` joins over raw full-row branches
    - the analyzer sees through only the planner-generated alias and redundant left-side `DISTINCT` wrappers for that exact set-operator shape
    - `INTERSECT DISTINCT` lowers to conjunction over the admitted exact branch filters
    - `EXCEPT DISTINCT` lowers to left-minus-right filter algebra over the admitted exact branch filters
33. Redundant `DISTINCT` over raw full-row `Qdrant` scans is now dropped.
    - this is admitted only for raw scan/filter chains where the full row identity still includes unique `id`
    - projected `DISTINCT` remains a separate semantic case
34. Mergeable child-kernel extraction is now explicitly validated as compositional.
    - a nested same-collection set-algebra region can collapse to one scan-local kernel first
    - exact `COUNT(*)` and exact scalar-facet grouped counts can still replace the larger parent
      subtree after that child rewrite in the same bottom-up analyzer pass
35. The admitted facet slice is now broader without overstating typed payload SQL semantics.
    - top-facet grouped counts now admit keyword, bool, and lookup-capable integer payload indexes
    - facet keys still surface as `Utf8`, matching the current textual `payload:<path>` SQL bridge
    - integer payload metadata now distinguishes `lookup` from `range`, so integer `=` / `IN` pushdown no longer overstates range-only integer indexes
    - live collection introspection on the current runtime line now preserves integer lookup/range metadata well enough to admit integer facet pushdown on the same exact contract
36. The current public retrieval prototypes are now DataFusion-native marker surfaces over the
    prepared session context.
    - current public markers are:
      - `qdrant_nearest_score(...)`
      - `qdrant_sample_score(...)`
      - `qdrant_recommend_score(...)`
      - `qdrant_discover_score(...)`
      - `qdrant_context_score(...)`
      - `qdrant_nearest_with_mmr_score(...)`
      - `qdrant_relevance_feedback_score(...)`
    - current nearest admitted scope is:
      - dense nearest-neighbor query over `Qdrant::query`
      - named-vector selection by the vector column argument
      - exact admitted base filters from the existing predicate algebra
      - explicit `LIMIT` pushdown when present, otherwise Qdrant's default result count
      - omitted projected score ordering uses Qdrant's native result order
      - optional score-threshold predicates
    - current sample admitted scope is:
      - `Query::Sample` with random sampling
      - explicit `LIMIT` pushdown when present, otherwise Qdrant's default result count
      - omitted projected score ordering uses Qdrant's native result order
      - default method `'random'` when omitted
    - current recommend admitted scope is:
      - positive / negative example lists
      - explicit `LIMIT` pushdown when present, otherwise Qdrant's default result count
      - omitted projected score ordering uses Qdrant's native result order
      - default or explicit strategy literal
    - current discover / context admitted scope is:
      - dense vector target/context pair inputs
      - explicit `LIMIT` pushdown when present, otherwise Qdrant's default result count
      - omitted projected score ordering uses Qdrant's native result order
    - current nearest-with-MMR admitted scope is:
      - dense query vectors
      - diversity and candidates-limit literals
      - explicit `LIMIT` pushdown when present, otherwise Qdrant's default result count
      - omitted projected score ordering uses Qdrant's native result order
    - current relevance-feedback admitted scope is:
      - dense vector targets
      - feedback-item arrays using `struct(example, score)` entries
      - explicit `LIMIT` pushdown when present, otherwise Qdrant's default result count
      - omitted projected score ordering uses Qdrant's native result order
      - required naive strategy coefficients
    - current grouped-nearest admitted scope is:
      - `SELECT DISTINCT ON (payload:<path>)` over one scalar keyword or lookup-capable integer payload field
      - one grouped retrieval source among `qdrant_nearest_score(...)`, `qdrant_recommend_score(...)`, `qdrant_discover_score(...)`, or `qdrant_context_score(...)`
      - `ORDER BY payload:<path>[ DESC]`, with optional trailing `, score DESC` as an explicit in-group tie-break
      - group size `1`
      - grouped execution validates that returned group ids match scalar payload values on hits
      - any outer `LIMIT` remains local above the grouped exec
    - score output is only present when projected
    - retrieval kernels can now leave benign local projection shells and local residual filter shells above the closed qdrant query kernel instead of requiring fully remote-only projection/filter shapes, including later score projection above those local filter shells
    - when projected, aliases win; otherwise naming follows normal `DataFusion` expression naming
37. Current exact `Qdrant` leaf relations now converge on one generic extracted kernel family.
    - exact count, scalar facet, and nearest retrieval all share one `QdrantKernelNode` /
      `QdrantKernelSpec` structure
38. Current public marker semantics now also converge on one generic operator family.
    - `QdrantOpNode` / `QdrantOp` now own the current public nearest-retrieval marker semantics
    - `QdrantSessionContext` now remains only as the prepared-session wrapper that installs the
      analyzer, planner, and marker-UDF hooks
39. Shared `Qdrant` semantics now live in `src/qdrant.rs` and are reused across analyzer, filter pushdown, and sort pushdown instead of duplicating payload-path recognition.
    - canonical payload access recognition now admits raw `payload:<path>`, public `payload(payload:<path>, 'Type')`, exact casts to the authoritative payload scalar type for filter semantics, DataFusion-style order-preserving casts for ordering semantics, and the internal executable payload-access UDFs
40. Scan-path payload projection is now schema-aware and executable.
    - direct `payload:<path>` projections over known payload fields rewrite to typed local payload accessors early enough for honest logical schema propagation
    - plain scan queries can now project typed payload scalars while preserving remote filter/sort pushdown
41. A public typed payload helper now exists for SQL planning gaps.
    - `payload(accessor, 'Type')` gives `DataFusion` a planning-time payload scalar type
    - it currently unlocks arithmetic and similar contexts where raw `payload:<path>` would otherwise still be typed as `Utf8`
    - raw unhinted arithmetic like `payload:rank + 1` is still intentionally deferred until an earlier SQL-planning normalization seam exists
42. Qdrant query-surface payload projections now reuse the same canonical payload resolver as scan filter/sort pushdown.
    - raw `payload:<path>`, public `payload(...)`, and exact casts to the authoritative payload scalar type now all resolve to the same payload-output path on qdrant query projections
    - exact cast query projections now preserve remote payload fetch and materialize typed output columns when authoritative payload metadata exists
43. Ordered payload-key scroll exactness is now explicitly bounded by cluster metadata.
    - `QdrantTableProvider::try_new` now consults `collection_cluster_info`
    - exact payload-key sort pushdown remains enabled only for stable single-peer collections
    - distributed, transferring, or resharding collection states now fall back to local `DataFusion` sorting instead of claiming exact remote order

## Current Code Ownership

1. `src/table.rs`, `src/table/provider.rs`, `src/table/exec.rs`, `src/table/scroll.rs`
   - `TableProvider`, scan execution plan, physical pushdown hooks, and paginated `scroll`
     orchestration
2. `src/table/scan_spec.rs`
   - scan-local selectors, scan spec, ordering, and continuation contract
3. `src/qdrant.rs`, `src/qdrant/filter/*`
   - shared `Qdrant` payload schema, payload access/path recognition, payload index metadata
     normalization, and filter semantics
4. `src/expr_fn.rs`, `src/expr_fn/payload.rs`, `src/expr_fn/payload_access.rs`, `src/expr_fn/*`
   - public marker/helper UDF registration, the public typed `payload(...)` helper, internal
     executable payload accessors, and query-family UDF surfaces
5. `src/analyzer.rs`, `src/analyzer/*`
   - unified relation-pushdown analyzer scaffold, operator-marker detection, subtree
     classification, kernel extraction, and optimizer-side coordinated rewrites
6. `src/context.rs`, `src/context/planner.rs`, `src/context/exec.rs`
   - prepared-session wrapper, extension-planner support, and runtime request execution helpers
7. `src/arrow/schema.rs`, `src/arrow/deserialize.rs`, `src/arrow/serialize.rs`
   - collection-config to Arrow schema translation plus `Qdrant` point to Arrow record-batch
     materialization and record-batch to `Qdrant` point serialization
8. `src/table/insert.rs`
   - append-only `INSERT INTO` sink implementation over the canonical provider schema
9. `tests/e2e.rs`
   - integration coverage for the admitted scan baseline, typed payload access, aggregate-like
     slices, current query-family surfaces, and append-only inserts

## Operational Notes

1. Prefer clean reimplementation over porting code from the old spike branch.
2. Remove deprecated `qdrant-client` paths instead of preserving fallback behavior.
3. Preserve truthful nullability at the scan boundary; do not impute missing vectors during scan.
4. The next step is no longer ad hoc feature growth. It is broader retrieval and aggregate-like
   expansion on the now-landed generic `Qdrant` operator / kernel architecture tracked as
   `M-003`.
5. That next phase is explicitly anchored on `DataFusion`’s own idioms:
   - `TreeNode` traversal / rewriting
   - `LogicalPlan` expression and subquery helpers
   - source capability checks and physical sort pushdown hooks
6. Physical sort pushdown has started with the exact `ORDER BY id ASC` case because Qdrant already guarantees ID-ordered scroll output.
7. Ordered payload-key `scroll` support is no longer speculative on the single-node runtime path:
   - `next_page_offset` is absent
   - duplicate-boundary pagination requires accumulated boundary-ID exclusion
   - datetime `order_value` currently returns integer microseconds
8. The admitted payload SQL bridge now includes raw `payload:<path>` plus the public `payload(payload:<path>, 'Type')` helper where SQL planning needs an explicit scalar type.
   - exact scan filter and payload-key sort pushdown both reuse the same canonical payload-access recognition
   - raw unhinted arithmetic such as `payload:rank + 1` is still intentionally deferred until an earlier SQL-planning normalization seam exists
9. The admitted exact filter bridge is now a real predicate algebra over the current admitted leaves, not just conjunctive leaf pushdown.
10. Payload-key sort exactness is now guarded by live cluster state instead of being assumed.
    - exact payload-key sort pushdown is admitted only for stable single-peer collections proven via `collection_cluster_info`
    - distributed or in-flight cluster states now keep the sort local instead of overstating remote exactness
11. The next capability round is now planned semantically rather than endpoint-by-endpoint:
    - broader aggregate-like exploration beyond the first scalar-facet slice
    - retrieval relations beyond nearest after that
12. Planner expansion should now build on the explicit subtree classifier rather than adding recognizers in isolation:
    - broader source-set ownership
    - richer composition classes beyond the first sound `mergeable` proof case
    - maximal exact kernel extraction inside larger `Qdrant` regions beyond the first raw-union, union-distinct, and raw-distinct collapses
