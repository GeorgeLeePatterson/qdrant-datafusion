# Execution Tracker

Last updated: 2026-03-19

## Purpose

This is the canonical `Done / Next / Needed` tracker for `qdrant-datafusion`.

Use it to resume work without replaying the full repository history.

## Current State

1. The published crate exists, but the next round starts from a clean branch off `main` rather than the prior payload-filter spike branch.
2. The current chosen baseline is:
   - `main` source tree
   - upgraded dependency posture immediately applied in `Cargo.toml`
   - no payload-filter or query-shaping spike code ported forward yet
3. The collection-scan Arrow contracts are not yet aligned with the now-stable `nabled::arrow` / `ndarrow` expectations.
4. The next round is therefore a stabilization round, not a feature-expansion round.

## Done

1. `Q-001`: Crate identity established and published with a minimal `TableProvider` baseline.
2. `Q-002`: A prior spike branch explored payload-filter translation, query-builder shaping, and filter-focused e2e coverage.
3. `Q-003`: A clean rebaseline branch was created from `main` to avoid carrying forward broken spike implementation state.
4. `Q-004`: Dependency posture on the rebaseline branch was immediately upgraded:
   - `DataFusion` moved to the same git revision currently used by `ndatafusion`
   - `qdrant-client` moved to the current major line
5. `Q-005`: Internal planning governance is now explicit through `docs/README.md`, `docs/DECISIONS.md`, `docs/CAPABILITY_MATRIX.md`, `docs/EXECUTION_TRACKER.md`, and `docs/STATUS.md`.

## Next

1. `Q-006`: Restore baseline compile health on the upgraded dependency line.
   - update the `ExecutionPlan` implementation to the current `DataFusion` trait shape
   - resolve any dependency-family drift introduced by the upgraded `DataFusion` line
   - get `cargo check` green before widening any feature surface
2. `Q-007`: Lock the collection-scan Arrow output contracts to the canonical numerical carriers.
   - dense vector: fixed-dimension dense-vector carrier
   - multivector: canonical ragged tensor carrier
   - sparse vector: canonical sparse carrier
3. `Q-008`: Rewrite scan deserialization around non-deprecated `qdrant-client` vector outputs.
   - remove steady-state reliance on deprecated `VectorOutput` fields
   - preserve heterogeneous-collection null behavior
4. `Q-009`: Rebaseline validation and public docs.
   - update e2e tests to the corrected output contracts
   - update the root `README.md` to describe the admitted current contracts
5. `Q-010`: Decide whether to admit any query-builder abstraction at all on the new branch.
   - do not port the old spike’s semantics by default
   - only keep an abstraction if it stays thin and demonstrably matches the admitted SQL contract
6. `Q-011`: Design the stable SQL-native expansion map for `Qdrant` capabilities after the scan baseline is green.
   - search
   - recommend
   - discover
   - hybrid / fusion / grouped forms where justified

## Needed

When the next implementation round starts:

1. treat `Q-006` as the highest-priority open item unless explicitly redirected
2. do not infer behavior from the old spike branch; it is reference material only
3. keep the stabilization order intact:
   - compile baseline
   - output contract parity
   - deserializer/API cleanup
   - test and docs rebaseline
   - capability expansion
4. update this tracker in the same change set as any non-trivial landing
5. avoid reintroducing ad hoc Arrow layouts once the canonical carriers are admitted
