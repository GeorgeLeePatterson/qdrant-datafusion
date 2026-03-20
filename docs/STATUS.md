# Status Snapshot

Last updated: 2026-03-19

## Summary

`qdrant-datafusion` is now advancing from a clean rebaseline branch off `main` with the dependency line aligned to `ndatafusion`.

Current reality on this branch:

1. The source baseline is `main`, not the prior payload-filter spike branch.
2. `Cargo.toml` is aligned to:
   - the same `DataFusion` git revision used by `ndatafusion`
   - the current `qdrant-client` line
3. Baseline compile health has been restored on that dependency posture.
   - `cargo check` is green
   - `cargo check --features test-utils` is green
4. This branch is now a rewrite from first principles, not a compatibility migration.
5. Legacy field naming, legacy tests, deprecated client paths, and null-preserving scan behavior are not considered part of the target design.
6. The active implementation target is the canonical `ndarrow` / `nabled::arrow` scan contract only.
7. The root `README.md` still documents old output contracts and needs to be rebaselined after the scan path is correct.

## Branch Posture

The old spike branch remains reference material only.

Its useful value is limited to:

1. reminders of areas that were explored previously
2. examples of APIs and tests that should not be treated as source of truth

Its implementation is not the baseline for this branch.

## Current Code Ownership

1. `src/table.rs`
   - `TableProvider`
   - scan execution plan
   - scan-to-record-batch glue
2. `src/arrow/schema.rs`
   - collection-config to Arrow schema translation
3. `src/arrow/deserialize.rs`
   - `Qdrant` point to Arrow record-batch materialization
4. `src/udfs.rs`
   - minimal crate-local UDF registration hook only
5. `tests/e2e.rs`
   - integration coverage for the admitted scan baseline only

## Operational Notes

1. Prefer clean reimplementation over porting code from the old spike branch.
2. Remove deprecated `qdrant-client` paths instead of preserving fallback behavior.
3. Prefer canonical non-null scan carriers unless a later design decision explicitly admits null semantics.
4. The compile-baseline milestone is complete.
5. The next implementation item is `Q-007` from `docs/EXECUTION_TRACKER.md`.
6. Before designing the SQL-to-vector-store semantic bridge, stop for explicit planning.

## Next Required Milestone

Lock the collection-scan output contracts to the canonical `ndarrow` / `nabled::arrow` carriers, then rewrite deserialization around current `qdrant-client` vector outputs only.
