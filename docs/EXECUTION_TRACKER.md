# Execution Tracker

Last updated: 2026-03-22

## Purpose

This is the canonical `Done / Next / Needed` tracker for `qdrant-datafusion`.

Use it to resume work without replaying the full repository history.

## Current State

1. `qdrant-datafusion` is being treated as a clean rewrite from first principles, not as a migration of the earlier spike implementation.
2. The dependency line is aligned to the current `ndatafusion` baseline and `ndarrow 0.0.4`.
3. The baseline collection-scan path is now contract-correct:
   - `scroll`-based paginated retrieval
   - canonical dense / multivector / sparse carriers
   - top-level nullable vector columns for heterogeneous named collections
   - current typed `qdrant-client` vector outputs only
4. `INSERT INTO` is explicitly unsupported instead of panicking.
5. The broad SQL-native `Qdrant` capability surface is still intentionally undefined.

## Done

1. `Q-001`: Crate identity established and published with a minimal `TableProvider` baseline.
2. `Q-002`: A prior spike branch explored payload-filter translation, query-builder shaping, and filter-focused e2e coverage.
3. `Q-003`: A clean rebaseline branch was created from `main` to avoid carrying forward broken spike implementation state.
4. `Q-004`: Dependency posture on the rebaseline branch was immediately upgraded:
   - `DataFusion` moved to the same git revision currently used by `ndatafusion`
   - `qdrant-client` moved to the current major line
5. `Q-005`: Internal planning governance is explicit through `docs/README.md`, `docs/DECISIONS.md`, `docs/CAPABILITY_MATRIX.md`, `docs/EXECUTION_TRACKER.md`, and `docs/STATUS.md`.
6. `Q-006`: Baseline compile health was restored on the upgraded dependency line.
7. `Q-007`: Collection-scan Arrow output contracts are locked to the canonical numerical carriers.
8. `Q-008`: Scan deserialization now targets current typed `qdrant-client` vector outputs only.
9. `Q-009`: The scan baseline now admits heterogeneous named-vector collections through top-level nullable vector columns.
10. `Q-010`: Table scans now use paginated `scroll`, selector classification is contract-based, and stale public docs/speculative SQL examples were scrubbed.
11. `Q-011`: A provider-owned pushdown model now exists for:
    - projection
    - payload access
    - filters
    - ordering
    - limit
    - continuation
12. `Q-012`: Initial DataFusion-native pushdown wiring has landed:
    - `supports_filters_pushdown` is explicit and now anchors exact admission of the current filter subset
    - `ExecutionPlan::try_pushdown_sort` now admits the exact `ORDER BY id ASC` case
13. `Q-013`: The single-node ordered-scroll contract has now been validated directly against `Qdrant`:
    - `order_by` is payload-key ordering only
    - integer ordering requires a range-capable integer index
    - `next_page_offset` is absent when ordered scroll is active
    - duplicate-boundary pagination requires `start_from` plus accumulated boundary-ID exclusion
    - returned datetime `order_value` currently surfaces as integer microseconds
14. `Q-014`: Ordered-scroll lowering is now implemented in the provider runtime for the validated contract.
    - `QdrantContinuation::Ordered` now lowers to `order_by`
    - ordered pagination now uses `start_from` and `must_not has_id`
    - ordered scroll rejects unexpected ID-offset pagination
15. `Q-015`: The pushdown model is now wired further into `DataFusion`’s physical sort idioms.
    - payload-key sort admission is now recognized from physical `payload:<path>` expressions
    - provider-side payload index metadata is now retained explicitly for pushdown validation
    - unit plan-inspection coverage now checks both logical and physical sort expression shapes
16. `Q-016`: The first explicit payload-key SQL `ORDER BY` subset is now admitted.
    - single sort key only
    - direct `payload:<path>` only
    - indexed integer / float / datetime payload fields only
    - current pushdown is `Exact` because `DataFusion` cannot execute fallback `payload:<path>` physical sorts
    - end-to-end SQL coverage now exercises the admitted path against live `Qdrant`
17. `Q-018`: The first exact filter subset is now admitted through the provider-owned pushdown model.
    - conjunctions of exact leaves
    - `id =`, `id !=`, `id IN (...)`, `id NOT IN (...)`
    - same-field equality `OR` chains normalized to `IN`
    - vector-column `IS NULL` / `IS NOT NULL`
    - indexed scalar `payload:<path>` comparisons, `IN`, `NOT IN`, and non-negated `BETWEEN`
    - physical filter pushdown now absorbs the admitted subset so `FilterExec` does not remain above `QdrantScanExec`
    - payload filter literals are coerced by indexed payload field type because `DataFusion`’s physical `payload:<path>` expressions do not carry a typed scalar contract

## Next

1. The detailed planning inventory for the next expansion round now lives in `docs/QDRANT_COMPATIBILITY_MATRIX.md`.
2. `Q-017`: Validate distributed-ordering behavior on the target `Qdrant` deployment modes before claiming broader exact payload-key sort pushdown.
3. `Q-019`: Continue the payload-aware SQL bridge beyond the first exact filter subset.
   - broader boolean filter semantics beyond same-field equality disjunctions
   - payload `is_null` / `is_empty` semantics
   - count / facet support as the first aggregate-like exploration surface
   - the first retrieval relation only after the predicate algebra remains explicit
4. Continue mapping the pushdown model onto `DataFusion`’s own idioms where broader traversal is required.
   - `TreeNode` visitors / rewriters instead of ad hoc recursion
   - `LogicalPlan` expression and subquery helpers before project-local traversal
   - exact admission of broader filter families instead of ad hoc expression splitting

## Needed

When the next implementation round starts:

1. do not infer SQL semantics from the abandoned spike branch or stale SDK surface ideas
2. preserve the current scan truth:
   - canonical carriers
   - top-level nullable vector columns
   - paginated `scroll`
   - current typed `qdrant-client` APIs only
3. keep writes unsupported until a deliberate write contract exists
4. use `DataFusion` primary-source idioms before inventing project-local traversal or rewrite patterns
5. admit only explicit pushdown subsets; reject unsupported cases cleanly instead of approximating them
6. track unresolved distributed `Qdrant` ordering edge cases explicitly; the single-node ordered continuation contract is now known
7. update this tracker in the same change set as any non-trivial landing
8. stop for planning again before widening the SQL surface in a way that could affect other vector-store integrations
