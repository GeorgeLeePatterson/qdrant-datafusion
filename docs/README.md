# qdrant-datafusion Docs

This folder is the compaction-safe planning and execution source of truth for `qdrant-datafusion`.

## Current Direction

1. `qdrant-datafusion` is the `DataFusion` and SQL layer over `Qdrant`.
2. Canonical numerical Arrow contracts come from `ndarrow` and `nabled::arrow`, not repo-local ad hoc schemas.
3. The current round is a baseline-hardening round before feature expansion:
   - keep the dependency line aligned with `ndatafusion`
   - expose truthful collection scans over canonical carriers
   - use current `qdrant-client` APIs only
   - keep the SQL-native capability surface intentionally small until planning is complete
4. After the scan baseline is correct and stable, pause for explicit SQL-bridge planning before widening capabilities.

## Documents

1. `docs/DECISIONS.md`: locked constraints and surface-shaping rules.
2. `docs/CAPABILITY_MATRIX.md`: scope, current capability inventory, and sufficiency verdict.
3. `docs/EXECUTION_TRACKER.md`: canonical `Done / Next / Needed` tracker for compaction-safe continuation.
4. `docs/STATUS.md`: current repository snapshot and active branch reality.

## Context Resume Protocol

When resuming from compacted context, read in this order:

1. `docs/README.md`
2. `docs/DECISIONS.md`
3. `docs/CAPABILITY_MATRIX.md`
4. `docs/EXECUTION_TRACKER.md`
5. `docs/STATUS.md`

Then verify repository state quickly:

1. `git status --short`
2. `just checks`

## Scope Boundary

This round is not the broad `Qdrant` capability expansion round yet.

First make the collection-scan baseline correct, current, and contract-aligned. Only then widen the
SQL surface for search, recommendation, discover, fusion, planner hooks, and related features.
