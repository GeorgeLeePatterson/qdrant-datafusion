# Execution Tracker

Last updated: 2026-03-24

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
20. `Q-023`: Payload null / empty runtime behavior is now validated directly against the target dependency line.
    - on March 24, 2026, live `Qdrant 1.17.0` tests through `qdrant-client 1.17.0` confirmed:
      - explicit payload `NULL` written via point upsert is preserved
      - explicit payload `NULL` written via `set_payload` is preserved
      - `is_null` matches explicit null only
      - `is_empty` matches explicit null plus missing
    - payload empty SQL semantics remain deferred, but they are no longer blocked on runtime uncertainty
21. `Q-024`: SQL null semantics for `payload:<path>` are now admitted exactly.
    - `payload:<path> IS NULL` means missing or explicit null
    - `payload:<path> IS NOT NULL` means present and non-null
    - the current lowering is exact on the validated runtime contract:
      - explicit null uses `is_null`
      - missing-only uses `is_empty AND NOT values_count >= 0`
      - empty arrays are therefore not conflated with SQL null
22. `Q-025`: Planner-layer subtree replacement now flows through a unified `Qdrant` relation-pushdown analyzer scaffold.
    - current recognizers remain modular
    - current admitted replacement kinds remain:
      - exact single-source `COUNT(*)`
      - exact single-source keyword facet grouped counts
23. `Q-026`: The unified relation-pushdown scaffold now derives broader subtree classification explicitly before relation recognition.
    - source class:
      - `none`
      - `single-source Qdrant`
      - `multi-source Qdrant`
      - `mixed`
    - topology class:
      - `leaf`
      - `unary chain`
      - `unary relation change`
      - `multi-branch`
    - composition class:
      - `atomic`
      - `batchable`
      - `coordinated`
      - `local-compose`
    - current admitted replacements still only fire for exact single-source atomic `Qdrant` relations
24. `Q-027`: The planner scaffold is now exact-kernel aware inside broader `Qdrant` regions.
    - subtree status now tracks kernel placement explicitly:
      - `none`
      - `exact-self`
      - `exact-child`
      - `exact-children`
    - local shells around extracted child kernels are now classified separately from atomic exact kernels
    - the first explicit invalid planner surface is now rejected early:
      - projection-time `payload:<path>` access in the prepared session/planner path when no admitted exact `Qdrant` kernel owns that expression
25. `Q-028`: The planner scaffold now has a first concrete `mergeable` multi-branch state.
    - the admitted case is intentionally narrow:
      - same raw `Qdrant` collection on every branch
      - exact filters only
      - every branch filter implies a finite point-ID upper bound
      - those branch point-ID bounds are pairwise disjoint
    - same-collection raw `UNION ALL` alone is not treated as mergeable because overlapping branches would collapse duplicate rows
26. `Q-029`: The first `mergeable` multi-branch case is now executable.
    - a provably disjoint same-collection raw `UNION ALL` now rewrites to a single filtered scan
    - this is the first planner extraction step that turns a multi-branch `Qdrant` region into one scan-local kernel instead of only classifying it
27. `Q-030`: Raw same-collection `UNION DISTINCT` over exact filters is now admitted as the second executable `mergeable` case.
    - overlap between branches is allowed because duplicate elimination is already part of the SQL semantics
    - the analyzer now rewrites that subtree to a single filtered scan as well
28. `Q-031`: Raw same-collection `INTERSECT DISTINCT` and `EXCEPT DISTINCT` over exact filters are now admitted as executable `mergeable` cases.
    - DataFusion lowers these through `LeftSemi` / `LeftAnti` joins over raw full-row branches
    - the analyzer now sees through the planner-generated alias and redundant left-side `DISTINCT` wrappers for that exact shape only
    - `INTERSECT DISTINCT` lowers to conjunction over the admitted exact branch filters
    - `EXCEPT DISTINCT` lowers to left-minus-right filter algebra over the admitted exact branch filters
29. `Q-032`: Redundant `DISTINCT` over raw full-row `Qdrant` scans is now dropped.
    - this is admitted only for raw scan/filter chains where the full row identity still includes unique `id`
    - projected `DISTINCT` remains a separate semantic case
30. `Q-033`: Mergeable child-kernel extraction is now explicitly validated as compositional.
    - a nested same-collection set-algebra region can collapse to one scan-local kernel first
    - exact `COUNT(*)` and exact keyword-facet grouped counts can still replace the larger parent
      subtree after that child rewrite in the same bottom-up analyzer pass
## Next

1. The detailed planning inventory for the next expansion round now lives in `docs/QDRANT_COMPATIBILITY_MATRIX.md`.
2. `Q-017`: Validate distributed-ordering behavior on the target `Qdrant` deployment modes before claiming broader exact payload-key sort pushdown.
3. `M-002`: Continue aggregate-like exploration over the predicate algebra.
   - explicit output contracts for aggregate-like `Qdrant` exploration surfaces beyond exact `COUNT(*)` and top-facet grouped counts
   - determine whether the next grouped slice is broader facet semantics or a separate aggregate-like relation
4. `Q-020`: Extend the predicate algebra only where the SQL semantics are explicit.
   - payload empty semantics
   - text, geo, nested, and count-oriented predicates
5. Continue mapping the pushdown model onto `DataFusion`’s own idioms where broader traversal is required.
   - `TreeNode` visitors / rewriters instead of ad hoc recursion
   - `LogicalPlan` expression and subquery helpers before project-local traversal
   - exact admission of broader filter families and aggregate-like shapes instead of ad hoc expression splitting
6. Extend the planner scaffold beyond the current explicit classifier set toward richer island composition and kernel extraction.
   - source-set ownership over larger plan regions
   - widen `mergeable` only with explicit algebraic proofs such as disjointness or duplicate-elimination semantics, not collection identity alone
   - broaden `invalid` detection carefully as more remote-only surfaces are introduced
   - maximal exact kernel extraction inside broader `Qdrant`-sourced regions beyond the first raw-union, union-distinct, and raw-distinct collapses

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
