# Status Snapshot

Last updated: 2026-03-21

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
11. Ordered continuation lowering is implemented internally through `order_by`, `start_from`, and boundary-ID exclusion, but payload-key SQL sort admission is still deferred.
12. The root `README.md`, repo notes, and tracker docs describe only the admitted baseline.

## Current Code Ownership

1. `src/table.rs`
   - `TableProvider`
   - scan execution plan
   - provider-owned scan spec lowering
   - paginated `scroll` orchestration
   - exact `ORDER BY id ASC` pushdown
2. `src/pushdown.rs`
   - provider-owned pushdown model
   - scan projection / payload / ordering / continuation contract
3. `src/arrow/schema.rs`
   - collection-config to Arrow schema translation
4. `src/arrow/deserialize.rs`
   - `Qdrant` point to Arrow record-batch materialization
5. `tests/e2e.rs`
   - integration coverage for the admitted scan baseline only

## Operational Notes

1. Prefer clean reimplementation over porting code from the old spike branch.
2. Remove deprecated `qdrant-client` paths instead of preserving fallback behavior.
3. Preserve truthful nullability at the scan boundary; do not impute missing vectors during scan.
4. The next step is still not ad hoc implementation. It is the remaining pushdown-first SQL-bridge work tracked as `Q-015` through `Q-018`.
5. That next phase is explicitly anchored on `DataFusion`’s own idioms:
   - `TreeNode` traversal / rewriting
   - `LogicalPlan` expression and subquery helpers
   - source capability checks and physical sort pushdown hooks
6. Physical sort pushdown has started with the exact `ORDER BY id ASC` case because Qdrant already guarantees ID-ordered scroll output.
7. Ordered payload-key `scroll` support is no longer speculative on the single-node runtime path:
   - `next_page_offset` is absent
   - duplicate-boundary pagination requires accumulated boundary-ID exclusion
   - datetime `order_value` currently returns integer microseconds
8. Distributed-ordering behavior is still intentionally deferred before claiming broader payload-key sort exactness.
