# Status Snapshot

Last updated: 2026-03-23

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
15. Physical filter pushdown now absorbs the admitted predicate algebra so `FilterExec` does not remain above `QdrantScanExec`.
16. Payload filter literals are coerced by indexed payload field type because `DataFusion`’s physical `payload:<path>` expressions surface generic scalar literals such as `Utf8("10")`.
17. Exact `COUNT(*)` pushdown is now admitted as the first aggregate-like planner slice.
    - it lowers into `Qdrant`’s native `count` API
    - it currently requires the `Qdrant` session/planner helper
    - it reuses the existing provider-owned predicate algebra for admitted exact filters
18. Exact top-facet grouped counts are now admitted as the second aggregate-like planner slice.
    - it lowers into `Qdrant`’s native `facet` API
    - it currently requires the `Qdrant` session/planner helper
    - it is currently limited to one keyword-indexed `payload:<path>` field with `ORDER BY count DESC LIMIT N`
    - it reuses the existing provider-owned predicate algebra for admitted exact filters
19. The root `README.md`, repo notes, and tracker docs describe only the admitted baseline.
20. Detailed capability-expansion planning now has an explicit semantic inventory in `docs/QDRANT_COMPATIBILITY_MATRIX.md`.

## Current Code Ownership

1. `src/table.rs`
   - `TableProvider`
   - scan execution plan
   - provider-owned scan spec lowering
   - paginated `scroll` orchestration
   - exact `ORDER BY id ASC` pushdown
   - exact `ORDER BY payload:<path>` pushdown
2. `src/pushdown.rs`
   - provider-owned pushdown model
   - scan projection / payload / filters / ordering / continuation contract
   - payload index metadata normalization for admitted sort and filter pushdown
3. `src/arrow/schema.rs`
   - collection-config to Arrow schema translation
4. `src/arrow/deserialize.rs`
   - `Qdrant` point to Arrow record-batch materialization
5. `tests/e2e.rs`
   - integration coverage for the admitted scan baseline and the first exact aggregate-like slices
6. `src/context.rs`, `src/context/planner.rs`, `src/context/plan_node.rs`
   - narrow session / analyzer / extension-planner support for exact `COUNT(*)` and keyword-facet pushdown
7. `src/analyzer.rs`, `src/analyzer/common.rs`, `src/analyzer/count_pushdown.rs`, `src/analyzer/facet_pushdown.rs`
   - exact aggregate-like plan admission for single-source `Qdrant` counts and the first keyword-facet grouped-count subset

## Operational Notes

1. Prefer clean reimplementation over porting code from the old spike branch.
2. Remove deprecated `qdrant-client` paths instead of preserving fallback behavior.
3. Preserve truthful nullability at the scan boundary; do not impute missing vectors during scan.
4. The next step is still not ad hoc implementation. It is the remaining pushdown-first SQL-bridge work tracked as `Q-017`, `M-002`, and `Q-020`.
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
    - broader aggregate-like exploration beyond the first keyword-facet slice
    - retrieval relations after that
