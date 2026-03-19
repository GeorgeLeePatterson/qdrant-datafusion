# Status Snapshot

Last updated: 2026-03-19

## Summary

`qdrant-datafusion` is now being advanced from a clean rebaseline branch off `main`.

Current reality on this branch:

1. The source baseline is `main`, not the prior payload-filter spike branch.
2. `Cargo.toml` has been immediately upgraded to:
   - the same `DataFusion` git revision used by `ndatafusion`
   - the current `qdrant-client` major line
3. No payload-filter translation or query-builder spike code was ported forward by default.
4. Collection-scan schema generation and deserialization still use old ad hoc Arrow list encodings rather than the canonical `ndarrow` / `nabled::arrow` contracts.
5. The existing scan implementation on `main` still needs adaptation to the upgraded dependency line and current `qdrant-client` APIs.
6. The root `README.md` on `main` is clean, but still documents old output contracts such as `List<Float32>` and `List<List<Float32>>`.

## Branch Posture

The old spike branch remains as reference material only.

Its useful value is limited to:

1. ideas for a thin query-builder boundary
2. scalar conversion helpers for future filter work
3. e2e filter scenarios that can be rewritten later as behavioral tests

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
   - JSON UDF registration glue only
5. `tests/e2e.rs`
   - current end-to-end table-scan coverage from `main`

## Operational Notes

1. The rebaseline branch should prefer clean reimplementation over porting code from the old spike branch.
2. The old spike branch may still be consulted for ideas, but not treated as a source of truth.
3. The next implementation item is `Q-006` from `docs/EXECUTION_TRACKER.md`.

## Next Required Milestone

Re-baseline the repository so the `main` code shape compiles cleanly on the upgraded dependency line, then immediately lock the collection-scan output contracts to the canonical `ndarrow` / `nabled::arrow` carriers.
