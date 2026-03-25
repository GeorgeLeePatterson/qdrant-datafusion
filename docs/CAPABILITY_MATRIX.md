# Capability Matrix

Last updated: 2026-03-25

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
| Pushdown architecture | provider-owned pushdown model | Implemented | Shared payload schema / path / filter semantics now live in `src/pushdown.rs`, while scan-local selectors, scan specs, ordering, and continuation now live in `src/table/pushdown.rs`. |
| DataFusion integration | DataFusion-native traversal / rewrite strategy | Partial | Physical sort/filter pushdown is explicit, and a unified relation-pushdown analyzer scaffold now derives subtree source / topology / composition plus exact-kernel placement before trying the current exact `COUNT(*)` and exact scalar-facet replacements. Broader traversal / rewrite work is still pending. |
| Ordering pushdown | exact `ORDER BY id ASC` pushdown | Implemented | Qdrant’s existing ID-ordered scroll path is now admitted as an exact physical sort pushdown case. |
| Ordering pushdown | payload-key `ORDER BY payload:<path>` subset | Implemented | The provider now admits a single-key `payload:<path>` sort subset for indexed integer / float / datetime payload fields and lowers it into ordered `scroll` as an exact physical sort pushdown on the currently validated runtime contract. |
| Ordered continuation | duplicate-boundary ordered pagination contract | Partial | Single-node ordered continuation is validated and implemented through `start_from` plus accumulated boundary-ID exclusion; distributed exactness is still deferred. |
| Filter pushdown | predicate algebra over admitted leaves | Implemented | Exact pushdown now admits `AND`, `OR`, and `NOT` over `id` equality / membership, vector-column `IS NULL` / `IS NOT NULL`, payload-field `IS NULL` / `IS NOT NULL` with SQL semantics, and indexed scalar `payload:<path>` comparisons / `IN` / `NOT IN` / `BETWEEN` / `NOT BETWEEN`. Integer match predicates now require lookup-capable integer indexes, while integer range predicates require range-capable integer indexes. Scalar empty values stay on ordinary SQL semantics, so empty-string cases use normal equality such as `payload:<path> = ''` rather than a dedicated backend-shaped empty predicate. Physical pushdown absorbs the same subset so `FilterExec` does not remain above the scan. |
| Filter pushdown | payload empty-container/cardinality, text, geo, nested, and count-oriented predicates | Missing | Empty strings now fall under ordinary scalar equality, but explicit empty-container/cardinality semantics and the other listed predicate families are still intentionally deferred until their SQL contracts are explicit. |
| Aggregation / grouping | exact `COUNT(*)` over a single `Qdrant` source | Implemented | The first aggregate-like slice now lands through a narrow analyzer / extension-planner path and lowers into `Qdrant`’s native `count` API while reusing the provider-owned predicate algebra. |
| Aggregation / grouping | top-facet grouped counts over one scalar payload field | Implemented | The second aggregate-like slice now lands through the analyzer / extension-planner path and lowers into `Qdrant`’s native `facet` API for the admitted `GROUP BY payload:<path> ORDER BY count DESC LIMIT N` subset. Current admitted facet fields are keyword, bool, and lookup-capable integer indexes, and facet keys still surface as `Utf8` because the current `payload:<path>` SQL bridge remains textual. |
| Aggregation / grouping | broader aggregate-like exploration | Partial | Exact `COUNT(*)` and the first scalar-facet grouped-count slice now exist, but richer grouped/exploration semantics are still pending. |
| Dense vector output contract | canonical fixed-dimension vector carrier | Implemented | Dense scans use nullable `FixedSizeList<Float32>(D)`. |
| Multivector output contract | canonical ragged tensor carrier | Implemented | Multivectors use nullable `arrow.variable_shape_tensor<Float32>`. |
| Sparse vector output contract | canonical sparse carrier | Implemented | Sparse scans use nullable `ndarrow.csr_matrix_batch<Float32>`. |
| Null semantics | heterogeneous named vectors | Implemented | Missing per-row vectors become top-level `NULL` without changing the inner carrier. |
| Payload contract | baseline payload exposure | Implemented | Baseline surface is JSON/text payload projection. |
| Writes | `INSERT INTO` | Partial | The provider now fails explicitly instead of panicking, but write support is not admitted. |
| SQL-native `Qdrant` capability surface | search / recommend / discover / fusion / grouped query forms | Missing | Not yet admitted in a stable SQL form. |
| UDF/UDAF/UDTF surface | `Qdrant`-specific SQL helpers | Missing | No crate-local SQL helpers are intentionally exposed yet. |
| Planner integration | query rewriting / tree visitors / custom planning | Partial | A unified relation-pushdown analyzer / extension-planner scaffold now owns the current exact single-source `COUNT(*)` and exact scalar-facet grouped-count replacement path, classifies subtree source / topology / composition plus exact-kernel placement explicitly for later island expansion, rejects projection-time `payload:<path>` surfaces in the prepared session/planner path when no admitted exact kernel can own them, rewrites four concrete `mergeable` multi-branch cases (`UNION ALL`, `UNION DISTINCT`, `INTERSECT DISTINCT`, `EXCEPT DISTINCT` over admitted raw same-collection branches) to one filtered scan, composes those extracted child kernels upward into exact `COUNT(*)` and exact scalar-facet replacements in the same analyzer pass, and now drops redundant `DISTINCT` over raw full-row `Qdrant` scans because row identity already includes unique `id`. Broader planner-layer capability expansion is still deferred. |
| Validation | end-to-end scan tests on current baseline | Implemented | Integration tests cover canonical carriers, nullable heterogeneous scans, non-truncated full scans, and raw ordered-scroll runtime contracts. |
| Documentation | public docs aligned with current tree | Implemented | Root README, tracker docs, and repository notes describe the admitted baseline only. |

## Sufficiency Verdict

`qdrant-datafusion` is now sufficient for the next planning round, but not yet for the broader SQL-native capability expansion.

The next blocking milestone is the remainder of aggregate-like exploration beyond exact count and
the first scalar-facet slice, without breaking the provider-owned composition boundary already in
place.
