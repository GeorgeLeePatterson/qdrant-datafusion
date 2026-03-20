# Execution Tracker

Last updated: 2026-03-19

## Purpose

This is the canonical `Done / Next / Needed` tracker for `qdrant-datafusion`.

Use it to resume work without replaying the full repository history.

## Current State

1. `qdrant-datafusion` is now being treated as a clean rewrite from first principles, not as a migration of the earlier spike implementation.
2. The active baseline is:
   - `main` source tree
   - dependency line immediately aligned to `ndatafusion`'s `DataFusion` git revision
   - `qdrant-client` moved to the current line
   - no obligation to preserve any earlier schema choices, tests, naming, or edge-case behavior
3. Baseline compile health has been restored on that upgraded dependency line.
   - `cargo check` is green on the default feature set
   - `cargo check --features test-utils` is green
4. The collection-scan Arrow contracts are being rewritten to the canonical `nabled::arrow` / `ndarrow` carriers only.
5. Deprecated `qdrant-client` vector-output fields are considered dead code and should be deleted rather than preserved as fallback paths.
6. Fast-path correctness and performance are the priority; speculative null-tolerant or heterogeneous slow-path handling is out of scope unless the canonical design truly requires it.

## Done

1. `Q-001`: Crate identity established and published with a minimal `TableProvider` baseline.
2. `Q-002`: A prior spike branch explored payload-filter translation, query-builder shaping, and filter-focused e2e coverage.
3. `Q-003`: A clean rebaseline branch was created from `main` to avoid carrying forward broken spike implementation state.
4. `Q-004`: Dependency posture on the rebaseline branch was immediately upgraded:
   - `DataFusion` moved to the same git revision currently used by `ndatafusion`
   - `qdrant-client` moved to the current major line
5. `Q-005`: Internal planning governance is now explicit through `docs/README.md`, `docs/DECISIONS.md`, `docs/CAPABILITY_MATRIX.md`, `docs/EXECUTION_TRACKER.md`, and `docs/STATUS.md`.
6. `Q-006`: Baseline compile health was restored on the upgraded dependency line.
   - removed the stray `datafusion-functions-json` dependency from the baseline compile path
   - updated the scan execution plan to the current `DataFusion` `ExecutionPlan` trait shape
   - kept the UDF surface minimal rather than preserving speculative JSON integration
   - verified `cargo check` on both the default feature set and `test-utils`

## Next

1. `Q-007`: Lock the collection-scan Arrow output contracts to the canonical numerical carriers.
   - dense vector: `FixedSizeList<Float32>(D)`
   - multivector: `arrow.variable_shape_tensor<Float32>` with rank 2 and fixed inner width
   - sparse vector: `ndarrow.csr_matrix_batch<Float32>`
   - no backward-compatibility aliases or legacy field splitting
2. `Q-008`: Rewrite scan deserialization around current `qdrant-client` vector outputs only.
   - use only typed `VectorOutput.vector` variants
   - delete deprecated `data`, `indices`, and `vectors_count` fallback paths
   - treat missing requested vectors as execution errors on the admitted fast path
3. `Q-009`: Rebaseline validation and public docs.
   - delete stale tests that encode wrong Arrow contracts or null-heavy legacy behavior
   - replace them with tests that assert canonical carrier compatibility
   - update the root `README.md` to describe the admitted current contracts
4. `Q-010`: Keep the runtime surface intentionally small until scan correctness is complete.
   - no speculative query-builder or SQL-translation surface by default
   - only add abstractions that are justified by the admitted SQL contract
5. `Q-011`: Design the stable SQL-native expansion map for `Qdrant` capabilities after the scan baseline is green.
   - pause for detailed planning before settling the semantic bridge
   - keep the future bridge general enough to support other vector stores where practical

## Needed

When the next implementation round starts:

1. treat `Q-007` as the highest-priority open item unless explicitly redirected
2. do not infer behavior from the old spike branch; it is reference material only
3. prefer deletion over adaptation when legacy code conflicts with the admitted fast path
4. keep the stabilization order intact:
   - compile baseline
   - canonical output contracts
   - deserializer/API cleanup
   - test and docs rebaseline
   - capability expansion
5. pause for explicit planning before designing SQL-to-vector-store semantic translation
6. update this tracker in the same change set as any non-trivial landing
7. avoid null-tolerant or compatibility-driven scan behavior unless it is explicitly re-admitted later
