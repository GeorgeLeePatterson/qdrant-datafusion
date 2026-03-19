# qdrant-datafusion Docs

This folder is the compaction-safe planning and execution source of truth for `qdrant-datafusion`.

## Current Direction

1. `qdrant-datafusion` is the `DataFusion` and SQL layer over `Qdrant`.
2. Canonical numerical Arrow contracts come from `ndarrow` and `nabled::arrow`, not repo-local ad hoc list schemas.
3. The current round is a stabilization round before feature expansion:
   - restore a clean compile baseline on the current `DataFusion` and `qdrant-client` lines
   - realign scan output types with the now-stable `nabled::arrow` expectations
   - retire deprecated `qdrant-client` response handling
4. After the scan baseline is correct and stable, widen the SQL-native capability surface over `Qdrant` operations.

## Documents

1. `docs/DECISIONS.md`: locked constraints and surface-shaping rules.
2. `docs/CAPABILITY_MATRIX.md`: scope, current capability inventory, and sufficiency verdict.
3. `docs/EXECUTION_TRACKER.md`: canonical `Done / Next / Needed` tracker for compaction-safe continuation.
4. `docs/STATUS.md`: current repository snapshot and baseline breakage summary.

## Context Resume Protocol

When resuming from compacted context, read in this order:

1. `docs/README.md`
2. `docs/DECISIONS.md`
3. `docs/CAPABILITY_MATRIX.md`
4. `docs/EXECUTION_TRACKER.md`
5. `docs/STATUS.md`

Then verify repository state quickly:

1. `git status --short`
2. `cargo check`
3. `cargo test --lib`
4. `cargo test --tests --features test-utils`

Use `docs/EXECUTION_TRACKER.md` as the first resume source. Only re-audit the full codebase if the tracker or status snapshot is stale or contradicted by the tree.

## Context Sufficiency Check

After reading the docs above, a contributor should be able to answer:

1. What is the current milestone? Re-baseline and stabilize the collection-scan surface.
2. What is the first non-negotiable technical goal? Compile cleanly on the current dependency line with one compatible `DataFusion` graph.
3. What output contracts should collection scans target? The canonical `ndarrow` / `nabled::arrow` carriers, not ad hoc nested lists.
4. What is done vs next vs needed? See `docs/EXECUTION_TRACKER.md`.
5. What should not be inferred from memory? Current compile state, admitted capability surface, and exact next tracker item.

## Scope Boundary

This round is not the broad `Qdrant` capability expansion round yet.

First make the existing table-provider baseline correct, current, and contract-aligned. Only then widen the SQL surface for search, recommendation, discover, fusion, planner hooks, and related features.
