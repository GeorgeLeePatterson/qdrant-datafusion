# qdrant-datafusion Docs

This folder is the compaction-safe planning and execution source of truth for `qdrant-datafusion`.

## Current Direction

1. `qdrant-datafusion` is the `DataFusion` and SQL layer over `Qdrant`.
2. Canonical numerical Arrow contracts come from `ndarrow` and `nabled::arrow`, not repo-local ad hoc schemas.
3. The current round is a pushdown-first capability-expansion round on top of the stable scan baseline:
   - keep the dependency line aligned with `ndatafusion`
   - preserve truthful collection scans over canonical carriers
   - use current `qdrant-client` APIs only
   - widen the SQL-native capability surface only through explicit semantic milestones
4. The current milestone is the first full public/operator checkpoint for broader `Qdrant`
   relation work.
   - current exact `COUNT(*)`, scalar-facet grouped counts, and nearest-neighbor retrieval now
     converge on one `QdrantKernelNode` / `QdrantKernelSpec` family instead of separate node types
   - the generic public operator layer now exists above that kernel layer:
     `QdrantOpNode` / `QdrantOp`
   - the first public prototype on that layer is a DataFusion-native nearest marker surface via
     `qdrant_nearest_score(...)`
   - the next checkpoint is broader query-family and aggregate-like growth on those shared enums,
     not more one-off node families

## Documents

1. `docs/DECISIONS.md`: locked constraints and surface-shaping rules.
2. `docs/CAPABILITY_MATRIX.md`: scope, current capability inventory, and sufficiency verdict.
3. `docs/ADMISSION_MATRIX.md`: explicit inventory of where behavior is exact-only, residual-capable, local-fallback, or remote-only.
4. `tests/catalog/mod.rs`: mirrored supported / unsupported SQL inventories consumed by the integration suites, including explicit subquery coverage per namespace, broader SQL syntax-family inventory, and explicit unsupported classifications (`Deferred`, `ByDesign`, `Upstream`, `InvalidInput`). The catalog should be expanded from the SQL space outward, not only from already-known code gaps, and queries should move from `unsupported` to `supported` as soon as behavior widens rather than being left behind as stale inventory.
5. `docs/QDRANT_COMPATIBILITY_MATRIX.md`: detailed inventory of the broader `Qdrant` feature surface, organized by semantic family and release fit.
6. `docs/EXECUTION_TRACKER.md`: canonical `Done / Next / Needed` tracker for compaction-safe continuation.
7. `docs/STATUS.md`: current repository snapshot and active branch reality.

## Context Resume Protocol

When resuming from compacted context, read in this order:

1. `docs/README.md`
2. `docs/DECISIONS.md`
3. `docs/CAPABILITY_MATRIX.md`
4. `docs/ADMISSION_MATRIX.md`
5. `docs/QDRANT_COMPATIBILITY_MATRIX.md`
6. `docs/EXECUTION_TRACKER.md`
7. `docs/STATUS.md`

Then verify repository state quickly:

1. `git status --short`
2. `just checks`

## Scope Boundary

This round is not the broad `Qdrant` capability expansion round yet.

The collection-scan baseline is now correct, current, and contract-aligned. Widen the SQL surface
only through semantic milestones that compose cleanly over the provider-owned pushdown model and
the now-explicit `Qdrant` operator / kernel architecture.
