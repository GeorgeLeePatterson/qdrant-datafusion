# Status Snapshot

Last updated: 2026-03-25

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
7. `INSERT INTO` is explicitly unsupported.
8. A provider-owned pushdown model now exists for projection, payload access, filters, ordering, limit, and continuation.
9. `ORDER BY id ASC` is admitted as an exact physical sort pushdown case.
10. The single-node payload-key ordered-scroll runtime contract is now validated for integer, float, and datetime payload indexes.
11. Ordered continuation lowering is implemented internally through `order_by`, `start_from`, and boundary-ID exclusion.
12. The first payload-key SQL sort subset is now admitted as `ORDER BY payload:<path>` for indexed integer, float, and datetime payload fields.
13. Payload-key sort pushdown is currently admitted as `Exact` on the validated runtime contract because `DataFusion` cannot execute a fallback physical sort for the `:` operator.
14. Predicate algebra over the admitted leaf subset is now exact:
    - `AND`, `OR`, and `NOT`
    - `id =`, `id !=`, `id IN (...)`, `id NOT IN (...)`
    - vector-column `IS NULL` / `IS NOT NULL`
    - indexed scalar `payload:<path>` comparisons, `IN`, `NOT IN`, `BETWEEN`, and `NOT BETWEEN`
      - integer match predicates require lookup-capable integer indexes
      - integer range predicates require range-capable integer indexes
15. Physical filter pushdown now absorbs the admitted predicate algebra so `FilterExec` does not remain above `QdrantScanExec`.
16. Payload filter literals are coerced by indexed payload field type because `DataFusion`’s physical `payload:<path>` expressions surface generic scalar literals such as `Utf8("10")`.
17. Exact `COUNT(*)` pushdown is now admitted as the first aggregate-like planner slice.
    - it lowers into `Qdrant`’s native `count` API
    - it currently requires the `Qdrant` session/planner helper
    - it reuses the existing provider-owned predicate algebra for admitted exact filters
18. Exact top-facet grouped counts are now admitted as the second aggregate-like planner slice.
    - it lowers into `Qdrant`’s native `facet` API
    - it currently requires the `Qdrant` session/planner helper
    - it is currently limited to one admitted scalar `payload:<path>` field with `ORDER BY count DESC LIMIT N`
    - it reuses the existing provider-owned predicate algebra for admitted exact filters
19. The root `README.md`, repo notes, and tracker docs describe only the admitted baseline.
20. Detailed capability-expansion planning now has an explicit semantic inventory in `docs/QDRANT_COMPATIBILITY_MATRIX.md`.
21. Payload-null runtime semantics are now validated; payload-empty SQL semantics are still intentionally deferred.
22. On March 25, 2026, live `Qdrant 1.17.0` tests through `qdrant-client 1.17.0` validated the runtime contract:
    - explicit payload `NULL` written via point upsert is preserved
    - explicit payload `NULL` written via `set_payload` is preserved
    - `is_null` matches explicit null only
    - `is_empty` matches missing, explicit null, and `[]`
    - `is_empty` does not match empty strings or empty objects on the current runtime line
    - `values_count >= 0` matches present fields, including explicit null and empty arrays
23. SQL null semantics for `payload:<path>` are now admitted exactly:
    - `IS NULL` means missing or explicit null
    - `IS NOT NULL` means present and non-null
    - the current lowering excludes empty arrays from SQL null by composing `is_null`, `is_empty`, and `values_count`
24. Payload-empty SQL semantics are now narrowed to the standard SQL subset that the current bridge can state honestly.
    - empty strings remain ordinary non-null values and are expressed through normal equality, for example `payload:<path> = ''`
    - current tests now prove that empty strings stay distinct from `payload:<path> IS NULL`
    - empty-container/cardinality semantics are still intentionally deferred
25. Planner-layer subtree replacement now uses a unified relation-pushdown analyzer scaffold for the admitted `Qdrant` relation replacements instead of separate analyzer-rule ownership by convention.
26. The planner scaffold now derives broader subtree classes explicitly before relation recognition.
    - source class: `none`, `single-source Qdrant`, `multi-source Qdrant`, `mixed`
    - topology class: `leaf`, `unary chain`, `unary relation change`, `multi-branch`
    - composition class: `atomic`, `mergeable`, `batchable`, `coordinated`, `local-compose`, `invalid`
    - kernel placement: `none`, `exact-self`, `exact-child`, `exact-children`
    - current admitted replacements still remain exact single-source atomic `Qdrant` relations only
27. The planner scaffold now distinguishes exact-self kernels from local shells around extracted child kernels.
    - the first explicit invalid planner surface is projection-time `payload:<path>` access in the prepared session/planner path when no admitted exact kernel owns that expression
28. The planner scaffold now has a first concrete `mergeable` multi-branch state.
    - same-collection raw `UNION ALL` branches are only classified as `mergeable` when exact filters imply pairwise-disjoint finite point-ID bounds
    - overlapping same-collection branches remain `local-compose`
29. That first `mergeable` case is now executable.
    - a provably disjoint same-collection raw `UNION ALL` rewrites to a single filtered scan
    - this is the first multi-branch `Qdrant` kernel extraction beyond classifier-only planner state
30. Raw same-collection `UNION DISTINCT` over exact filters is now the second executable `mergeable` case.
    - overlap between branches is admitted because duplicate elimination is already part of the SQL semantics
    - the analyzer rewrites that subtree to a single filtered scan too
31. Raw same-collection `INTERSECT DISTINCT` and `EXCEPT DISTINCT` over exact filters are now executable `mergeable` cases too.
    - DataFusion lowers these through `LeftSemi` / `LeftAnti` joins over raw full-row branches
    - the analyzer sees through only the planner-generated alias and redundant left-side `DISTINCT` wrappers for that exact set-operator shape
    - `INTERSECT DISTINCT` lowers to conjunction over the admitted exact branch filters
    - `EXCEPT DISTINCT` lowers to left-minus-right filter algebra over the admitted exact branch filters
32. Redundant `DISTINCT` over raw full-row `Qdrant` scans is now dropped.
    - this is admitted only for raw scan/filter chains where the full row identity still includes unique `id`
    - projected `DISTINCT` remains a separate semantic case
33. Mergeable child-kernel extraction is now explicitly validated as compositional.
    - a nested same-collection set-algebra region can collapse to one scan-local kernel first
    - exact `COUNT(*)` and exact scalar-facet grouped counts can still replace the larger parent
      subtree after that child rewrite in the same bottom-up analyzer pass
34. The admitted facet slice is now broader without overstating typed payload SQL semantics.
    - top-facet grouped counts now admit keyword, bool, and lookup-capable integer payload indexes
    - facet keys still surface as `Utf8`, matching the current textual `payload:<path>` SQL bridge
    - integer payload metadata now distinguishes `lookup` from `range`, so integer `=` / `IN` pushdown no longer overstates range-only integer indexes
    - live collection introspection on the current runtime line now preserves integer lookup/range metadata well enough to admit integer facet pushdown on the same exact contract

## Current Code Ownership

1. `src/table.rs`
   - `TableProvider`
   - scan execution plan
   - provider-owned scan spec lowering
   - paginated `scroll` orchestration
   - exact `ORDER BY id ASC` pushdown
   - exact `ORDER BY payload:<path>` pushdown
2. `src/pushdown.rs`
   - shared payload schema, payload path, and filter semantics
   - payload index metadata normalization for admitted sort and filter pushdown
3. `src/table/pushdown.rs`
   - scan-local selectors, scan spec, ordering, and continuation contract
4. `src/arrow/schema.rs`
   - collection-config to Arrow schema translation
5. `src/arrow/deserialize.rs`
   - `Qdrant` point to Arrow record-batch materialization
6. `tests/e2e.rs`
   - integration coverage for the admitted scan baseline and the first exact aggregate-like slices
7. `src/context.rs`, `src/context/planner.rs`, `src/context/plan_node.rs`
   - narrow session / analyzer / extension-planner support for exact `COUNT(*)` and scalar-facet pushdown
8. `src/analyzer.rs`, `src/analyzer/common.rs`, `src/analyzer/relation_pushdown.rs`, `src/analyzer/count_pushdown.rs`, `src/analyzer/facet_pushdown.rs`
   - unified relation-pushdown analyzer scaffold with explicit subtree source / topology / composition / kernel-placement classification, modular recognizers for exact single-source `Qdrant` counts and the first scalar-facet grouped-count subset, and the first narrow invalid-surface rejection

## Operational Notes

1. Prefer clean reimplementation over porting code from the old spike branch.
2. Remove deprecated `qdrant-client` paths instead of preserving fallback behavior.
3. Preserve truthful nullability at the scan boundary; do not impute missing vectors during scan.
4. The next step is still not ad hoc implementation. It is the remaining pushdown-first SQL-bridge work tracked as `Q-017`, `M-002`, and the narrowed empty-container/cardinality part of `Q-020`.
5. That next phase is explicitly anchored on `DataFusion`’s own idioms:
   - `TreeNode` traversal / rewriting
   - `LogicalPlan` expression and subquery helpers
   - source capability checks and physical sort pushdown hooks
6. Physical sort pushdown has started with the exact `ORDER BY id ASC` case because Qdrant already guarantees ID-ordered scroll output.
7. Ordered payload-key `scroll` support is no longer speculative on the single-node runtime path:
   - `next_page_offset` is absent
   - duplicate-boundary pagination requires accumulated boundary-ID exclusion
   - datetime `order_value` currently returns integer microseconds
8. The admitted SQL bridge for that runtime path is currently `payload:<path>` only, and it is treated as exact on the validated runtime contract because fallback physical execution of `:` is not available.
9. The admitted exact filter bridge is now a real predicate algebra over the current admitted leaves, not just conjunctive leaf pushdown.
10. Distributed-ordering behavior is still intentionally deferred before claiming broader payload-key sort exactness.
11. The next capability round is now planned semantically rather than endpoint-by-endpoint:
    - broader aggregate-like exploration beyond the first scalar-facet slice
    - retrieval relations after that
12. Planner expansion should now build on the explicit subtree classifier rather than adding recognizers in isolation:
    - broader source-set ownership
    - richer composition classes beyond the first sound `mergeable` proof case
    - maximal exact kernel extraction inside larger `Qdrant` regions beyond the first raw-union, union-distinct, and raw-distinct collapses
