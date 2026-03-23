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
4. The current milestone is predicate algebra completion. The next milestone is aggregate-like exploration over that algebra.

## Documents

1. `docs/DECISIONS.md`: locked constraints and surface-shaping rules.
2. `docs/CAPABILITY_MATRIX.md`: scope, current capability inventory, and sufficiency verdict.
3. `docs/QDRANT_COMPATIBILITY_MATRIX.md`: detailed inventory of the broader `Qdrant` feature surface, organized by semantic family and release fit.
4. `docs/EXECUTION_TRACKER.md`: canonical `Done / Next / Needed` tracker for compaction-safe continuation.
5. `docs/STATUS.md`: current repository snapshot and active branch reality.

## Context Resume Protocol

When resuming from compacted context, read in this order:

1. `docs/README.md`
2. `docs/DECISIONS.md`
3. `docs/CAPABILITY_MATRIX.md`
4. `docs/QDRANT_COMPATIBILITY_MATRIX.md`
5. `docs/EXECUTION_TRACKER.md`
6. `docs/STATUS.md`

Then verify repository state quickly:

1. `git status --short`
2. `just checks`

## Scope Boundary

This round is not the broad `Qdrant` capability expansion round yet.

The collection-scan baseline is now correct, current, and contract-aligned. Widen the SQL surface
only through semantic milestones that compose cleanly over the provider-owned pushdown model.
