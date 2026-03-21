# Capability Matrix

Last updated: 2026-03-21

## Purpose

This file is the canonical scope and sufficiency map for `qdrant-datafusion`.

## Status Legend

- `Implemented`: present and validated on the current dependency line.
- `Partial`: present in some form, but not yet fully stabilized or intentionally incomplete.
- `Missing`: not yet admitted.

## Current Inventory

| Area | Capability | Status | Notes |
|---|---|---|---|
| Governance | compaction-safe docs and tracker | Implemented | `docs/README.md`, `docs/DECISIONS.md`, `docs/CAPABILITY_MATRIX.md`, `docs/EXECUTION_TRACKER.md`, and `docs/STATUS.md` are the internal planning baseline. |
| Dependency surface | single compatible `DataFusion` graph | Implemented | `Cargo.toml` stays aligned to the same `DataFusion` revision as `ndatafusion`. |
| Dependency surface | current `qdrant-client` line | Implemented | Baseline scan code uses the current `qdrant-client` APIs only. |
| Dependency surface | current `ndarrow` line | Implemented | The crate is aligned to `ndarrow 0.0.4`. |
| Build baseline | repository verification gate | Implemented | `just checks` is green on the rewritten scan baseline. |
| Collection introspection | collection config to scan schema | Implemented | `src/arrow/schema.rs` emits canonical dense, multivector, and sparse carriers with top-level nullable vector fields. |
| TableProvider | base collection scan path | Implemented | Provider and scan exec now perform true paginated collection scans via `scroll`. |
| Projection pushdown | requested vectors and payload only | Implemented | Projected-schema-driven selectors now classify vector fields from Arrow contract metadata, not field names alone. |
| Limit pushdown | SQL `LIMIT` to `Qdrant` | Implemented | Limit is enforced at the paginated scroll boundary. |
| Pushdown architecture | provider-owned pushdown model | Implemented | `src/pushdown.rs` now owns the scan contract for projection, payload access, filters, ordering, limit, and continuation. |
| DataFusion integration | DataFusion-native traversal / rewrite strategy | Partial | Primary-source pattern study is complete; `supports_filters_pushdown` and `ExecutionPlan::try_pushdown_sort` are now explicit, but broader traversal / rewrite work is still pending. |
| Ordering pushdown | exact `ORDER BY id ASC` pushdown | Implemented | Qdrant’s existing ID-ordered scroll path is now admitted as an exact physical sort pushdown case. |
| Ordering pushdown | payload-key ordered-scroll lowering | Partial | The provider runtime now supports validated payload-key ordered scroll continuation, but no admitted SQL `ORDER BY` subset maps to it yet. |
| Ordered continuation | duplicate-boundary ordered pagination contract | Partial | Single-node ordered continuation is validated and implemented through `start_from` plus accumulated boundary-ID exclusion; distributed exactness is still deferred. |
| Filter pushdown | ID and payload filter translation | Missing | Deliberately deferred until the SQL bridge is designed. |
| Dense vector output contract | canonical fixed-dimension vector carrier | Implemented | Dense scans use nullable `FixedSizeList<Float32>(D)`. |
| Multivector output contract | canonical ragged tensor carrier | Implemented | Multivectors use nullable `arrow.variable_shape_tensor<Float32>`. |
| Sparse vector output contract | canonical sparse carrier | Implemented | Sparse scans use nullable `ndarrow.csr_matrix_batch<Float32>`. |
| Null semantics | heterogeneous named vectors | Implemented | Missing per-row vectors become top-level `NULL` without changing the inner carrier. |
| Payload contract | baseline payload exposure | Implemented | Baseline surface is JSON/text payload projection. |
| Writes | `INSERT INTO` | Partial | The provider now fails explicitly instead of panicking, but write support is not admitted. |
| SQL-native `Qdrant` capability surface | search / recommend / discover / fusion / grouped query forms | Missing | Not yet admitted in a stable SQL form. |
| UDF/UDAF/UDTF surface | `Qdrant`-specific SQL helpers | Missing | No crate-local SQL helpers are intentionally exposed yet. |
| Planner integration | query rewriting / tree visitors / custom planning | Missing | Deferred until the core scan and capability surface are stable. |
| Validation | end-to-end scan tests on current baseline | Implemented | Integration tests cover canonical carriers, nullable heterogeneous scans, non-truncated full scans, and raw ordered-scroll runtime contracts. |
| Documentation | public docs aligned with current tree | Implemented | Root README, tracker docs, and repository notes describe the admitted baseline only. |

## Sufficiency Verdict

`qdrant-datafusion` is now sufficient for the next planning round, but not yet for the broader SQL-native capability expansion.

The next blocking milestone is not more implementation against ad hoc assumptions. It is an explicit
planning pass for the stable SQL-to-`Qdrant` semantic bridge.
