# Execution Tracker

Last updated: 2026-03-23

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
5. The broad SQL-native `Qdrant` capability surface is still intentionally incomplete, but the predicate algebra foundation and the first aggregate-like planner slices are now in place for the next expansion round.

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
17. `M-001`: Predicate algebra foundation is now implemented through the provider-owned pushdown model.
    - exact boolean composition over admitted leaves: `AND`, `OR`, `NOT`
    - admitted exact leaves:
      - `id =`, `id !=`, `id IN (...)`, `id NOT IN (...)`
      - vector-column `IS NULL` / `IS NOT NULL`
      - indexed scalar `payload:<path>` comparisons, `IN`, `NOT IN`, `BETWEEN`, and `NOT BETWEEN`
    - physical filter pushdown now absorbs the admitted subset so `FilterExec` does not remain above `QdrantScanExec`
    - payload filter literals are coerced by indexed payload field type because `DataFusion`’s physical `payload:<path>` expressions do not carry a typed scalar contract
18. `Q-021`: Exact `COUNT(*)` pushdown is now admitted as the first aggregate-like planner slice.
    - the current `DataFusion` revision does not expose aggregate pushdown on `TableProvider`
    - a narrow analyzer rule and extension planner now recognize exact single-source `COUNT(*)`
    - the execution path lowers into `Qdrant`’s native `count` API
    - admitted exact filters still reuse the existing provider-owned predicate algebra
    - unsupported aggregate shapes fall back cleanly instead of pretending to be exact
19. `Q-022`: Exact top-facet grouped counts are now admitted as the second aggregate-like planner slice.
    - the admitted SQL subset is `GROUP BY payload:<path> ORDER BY count DESC LIMIT N`
    - the grouped field is currently limited to keyword-indexed payload fields
    - the execution path lowers into `Qdrant`’s native `facet` API
    - admitted exact filters still reuse the existing provider-owned predicate algebra
    - broader grouped SQL remains deferred because `Qdrant` facet denotes top-N grouped counts, not unconstrained SQL grouping

## Next

1. The detailed planning inventory for the next expansion round now lives in `docs/QDRANT_COMPATIBILITY_MATRIX.md`.
2. `Q-017`: Validate distributed-ordering behavior on the target `Qdrant` deployment modes before claiming broader exact payload-key sort pushdown.
3. `M-002`: Continue aggregate-like exploration over the predicate algebra.
   - explicit output contracts for aggregate-like `Qdrant` exploration surfaces beyond exact `COUNT(*)` and top-facet grouped counts
   - determine whether the next grouped slice is broader facet semantics or a separate aggregate-like relation
4. `Q-020`: Extend the predicate algebra only where the SQL semantics are explicit.
   - payload `is_null` / `is_empty`
   - text, geo, nested, and count-oriented predicates
5. Continue mapping the pushdown model onto `DataFusion`’s own idioms where broader traversal is required.
   - `TreeNode` visitors / rewriters instead of ad hoc recursion
   - `LogicalPlan` expression and subquery helpers before project-local traversal
   - exact admission of broader filter families and aggregate-like shapes instead of ad hoc expression splitting

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
9. keep aggregate-like planner work compositional:
   - provider-owned predicate algebra remains the lowering target
   - relation-producing retrieval work is still a separate higher layer
