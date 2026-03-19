# Capability Matrix

Last updated: 2026-03-19

## Purpose

This file is the canonical scope and sufficiency map for `qdrant-datafusion`.

## Status Legend

- `Implemented`: present and validated on the current dependency line.
- `Partial`: present in some form, but not yet correct or stable on the current baseline.
- `Missing`: not yet admitted.

## Current Inventory

| Area | Capability | Status | Notes |
|---|---|---|---|
| Governance | compaction-safe docs and tracker | Implemented | `docs/README.md`, `docs/DECISIONS.md`, `docs/CAPABILITY_MATRIX.md`, `docs/EXECUTION_TRACKER.md`, and `docs/STATUS.md` are now the internal planning baseline. |
| Dependency surface | single compatible `DataFusion` graph | Partial | `Cargo.toml` now points to the same `DataFusion` git revision as `ndatafusion`, but the current tree is not yet compile-clean on that baseline. |
| Dependency surface | current `qdrant-client` line | Partial | `Cargo.toml` now tracks the current `qdrant-client` major line, but scan deserialization still references deprecated `VectorOutput` fields on the old implementation surface. |
| Build baseline | `cargo check` on current tree | Missing | Current tree still needs re-baselining on the upgraded dependency set. |
| Collection introspection | collection config to scan schema | Partial | Present in `src/arrow/schema.rs`, but it still emits old ad hoc Arrow list layouts. |
| TableProvider | base collection scan path | Partial | Provider, scan exec, and batch builder exist on `main`, but the surface must be revalidated and reshaped on the current dependency line. |
| Projection pushdown | requested vectors and payload only | Partial | Projected-schema-driven selection exists conceptually, but is not yet revalidated on the current baseline. |
| Limit pushdown | SQL `LIMIT` to `Qdrant` | Partial | Present on the main-line scan path and needs revalidation after the dependency reset. |
| Filter pushdown | ID and payload filter translation | Missing | Deliberately not ported from the old spike branch; this will be rebuilt from scratch if admitted. |
| Dense vector output contract | canonical fixed-dimension vector carrier | Missing | Current scan schema still uses variable `List<Float32>`. |
| Multivector output contract | canonical ragged tensor carrier | Missing | Current scan schema still uses `List<List<Float32>>`. |
| Sparse vector output contract | canonical sparse carrier | Missing | Current scan schema still exposes `*_indices` / `*_values` pairs. |
| Payload contract | baseline payload exposure | Implemented | Baseline surface is JSON/text payload projection. |
| SQL-native `Qdrant` capability surface | search / recommend / discover / fusion / grouped query forms | Missing | Not yet admitted in a stable SQL form. |
| UDF/UDAF/UDTF surface | `Qdrant`-specific SQL helpers | Missing | Only JSON UDF registration glue exists today on the main branch. |
| Planner integration | query rewriting / tree visitors / custom planning | Missing | Deferred until the core scan and capability surface are stable. |
| Validation | end-to-end tests on current baseline | Partial | Test scaffolding exists on `main`, but it will need re-baselining after the dependency and Arrow-contract updates. |
| Documentation | public README aligned with current tree | Partial | `main` README is clean but still describes old output contracts. |

## Sufficiency Verdict

`qdrant-datafusion` is not yet sufficient for the next stable release round.

The first blocking milestone is not new functionality. It is restoring dependency coherence, compile health, and canonical Arrow output contracts for the existing collection-scan surface.

## Remaining Strategic Work

1. finish the stabilization round for the table-provider and scan baseline
2. admit the stable SQL-native expansion surface for `Qdrant` query capabilities
3. only after core stability, add planner hooks, query rewriting, and optional `ndatafusion`-leveraged helpers where they materially improve the SQL experience
