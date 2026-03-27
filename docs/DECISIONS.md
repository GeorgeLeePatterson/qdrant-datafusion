# Locked Decisions

Last updated: 2026-03-26

## Core Constraints

1. `qdrant-datafusion` must align `DataFusion`, Arrow, `ndarrow`, `nabled`, and `qdrant-client` on one compatible dependency line.
2. `ndarrow` owns the Arrow/ndarray boundary contract. `nabled::arrow` owns the numerical Arrow contract. `qdrant-datafusion` adapts `Qdrant` data into those contracts.
3. Baseline collection scans stay thin:
   - collection introspection
   - paginated scan execution
   - result materialization into canonical Arrow carriers
4. Baseline collection scans use `Qdrant::scroll`, not `query`.
5. Dense scan outputs use fixed-dimension dense-vector carriers, not variable `List<Float32>` columns.
6. Multivector scan outputs use the canonical ragged tensor carrier, not ad hoc `List<List<Float32>>` columns.
7. Sparse scan outputs use canonical sparse carriers, not public `*_indices` / `*_values` column pairs.
8. Vector scan columns are top-level nullable. Missing per-row vector values become `NULL`, not execution errors and not implicit fills.
9. Deprecated `qdrant-client` response fields are not an admitted steady-state dependency.
10. Push down only deterministic SQL-to-`Qdrant` mappings. Unsupported filters must fall back cleanly instead of pretending to be exact.
11. `Qdrant` capability expansion must be SQL-native. Do not mirror the SDK one-to-one without first defining the SQL contract.
12. Baseline collection scans expose:
    - `id` as `Utf8`
    - `payload` as JSON/text
    until a more structured payload contract is explicitly admitted.
13. Any non-trivial behavior or surface-area change must update `docs/CAPABILITY_MATRIX.md`, `docs/EXECUTION_TRACKER.md`, and `docs/STATUS.md` in the same change set.
14. Pushdown work must follow `DataFusion`’s own traversal and rewrite idioms first:
    - `TreeNode` visitors / rewriters
    - `LogicalPlan` expression / subquery traversal helpers
    - source capability checks such as `supports_filters_pushdown`
    - physical sort pushdown hooks such as `ExecutionPlan::try_pushdown_sort`
15. Do not implement SQL-native `Qdrant` features as isolated one-offs. Projection, payload access, filters, ordering, limit, and continuation should be represented in a provider-owned pushdown model first and only then lowered into `Qdrant` requests.
16. Ordered `scroll` pushdown must be admitted only for explicitly supported cases.
    - the validated single-node runtime contract is payload-key ordering over indexed scalar payload fields
    - integer ordered scroll requires a range-capable integer index
    - ordered pagination does not use `next_page_offset`
    - continuation uses `start_from` plus accumulated boundary-ID exclusion
    - returned datetime order values currently surface as integer microseconds
17. Distributed ordered-scroll exactness is still deferred. Do not claim broader payload-key sort exactness until the target `Qdrant` deployment mode is explicitly validated.
18. Payload-aware SQL features are semantic pushdown concerns first, not generic JSON-function concerns first. Reintroduce generic JSON helpers only when they materially improve the SQL surface over the provider-owned payload contract.
19. The first admitted payload-key SQL sort subset is `ORDER BY payload:<path>`.
    - single sort key only
    - direct `payload:<path>` expression only
    - indexed integer / float / datetime payload fields only
    - current pushdown result is `Exact` because `DataFusion` cannot execute the `:` operator in a fallback physical `SortExec`
    - distributed exactness is still a tracked validation item beyond the current admitted runtime contract
20. The predicate algebra composes over the provider-owned pushdown model rather than lowering `DataFusion` expressions inline at the scan callsite.
    - admitted exact boolean operators are `AND`, `OR`, and `NOT`
    - admitted exact leaves are:
      - `id =`, `id !=`, `id IN (...)`, `id NOT IN (...)`
      - vector-column `IS NULL` / `IS NOT NULL`
      - `payload:<path> IS NULL` / `IS NOT NULL` with SQL semantics:
        - `IS NULL` means missing or explicit null
        - `IS NOT NULL` means present and non-null
      - indexed scalar `payload:<path>` comparisons, `IN`, `NOT IN`, `BETWEEN`, and `NOT BETWEEN`
        - integer match predicates require lookup-capable integer indexes
        - integer range predicates require range-capable integer indexes
    - scalar empty values use ordinary SQL semantics rather than a dedicated backend-shaped empty predicate
      - for the current admitted bridge, `payload:<path> = ''` is the canonical empty-string case
      - empty strings remain distinct from `IS NULL`
    - payload empty-container/cardinality semantics, text, geo, nested, and count-oriented predicates remain deferred until those SQL contracts are explicit
21. Physical filter pushdown must absorb the admitted exact subset, not just logical filter pushdown declarations.
    - `supports_filters_pushdown` alone is not sufficient on the current `DataFusion` revision
    - `QdrantScanExec` must absorb supported physical predicates so `FilterExec` disappears from the final plan
22. Payload filter coercion is governed by indexed payload field type, not by raw `DataFusion` physical literal type.
    - current physical `payload:<path>` predicates may surface comparison literals as `Utf8`, for example `Utf8("10")`
    - integer / float / bool / datetime payload filters therefore coerce from string literals when needed
23. Capability expansion planning must be organized by semantic family, not by mirroring `Qdrant` SDK endpoints one-for-one.
    - row restriction
    - row ordering
    - row production
    - ranking / re-scoring
    - aggregation / grouping
    - mutation
    - administration
    - the detailed inventory for this planning round lives in `docs/QDRANT_COMPATIBILITY_MATRIX.md`
24. Aggregate-like exploration is the first admitted `Qdrant` feature family that crosses beyond plain `TableProvider::scan`.
    - the current `DataFusion` revision does not expose an aggregate pushdown hook on `TableProvider`
    - exact `COUNT(*)` pushdown therefore uses a narrow analyzer / extension-planner path instead of overloading scan semantics
    - the existing provider-owned predicate algebra remains the lowering target for that higher layer; it is not duplicated
25. The first admitted aggregate-like SQL subset is exact `COUNT(*)` over a single `Qdrant` source.
    - no `GROUP BY`
    - no grouped aggregates
    - no `COUNT(column)`
    - exact admitted filters may still participate through the existing predicate algebra
    - this path currently requires the `Qdrant` session/planner helper rather than plain `SessionContext`
26. The second admitted aggregate-like SQL subset is exact top-facet grouped counts over one scalar payload field with an admitted facet contract.
    - the admitted SQL shape is `SELECT payload:<path>, COUNT(*) ... GROUP BY payload:<path> ORDER BY count DESC LIMIT N`
    - the current implementation admits one grouped field only
    - the grouped field must currently be a keyword-, bool-, or lookup-capable integer-indexed payload field
    - exact admitted filters may still participate through the existing predicate algebra
    - this path also requires the `Qdrant` session/planner helper rather than plain `SessionContext`
    - facet keys still surface as `Utf8`, matching the current textual `payload:<path>` SQL bridge
    - broader grouped SQL remains deferred because `Qdrant` facet denotes top-N grouped counts, not unconstrained SQL grouping
27. Planner-layer subtree replacement should be owned by one `Qdrant` relation-pushdown analyzer scaffold rather than by independent analyzer rules alone.
    - separate recognizers may remain modular
    - the scaffold now derives broader internal subtree classifications:
      - source class: `none`, `single-source Qdrant`, `multi-source Qdrant`, `mixed`
      - topology class: `leaf`, `unary chain`, `unary relation change`, `multi-branch`
      - composition class: `atomic`, `mergeable`, `batchable`, `coordinated`, `local-compose`, `invalid`
      - kernel placement: `none`, `exact-self`, `exact-child`, `exact-children`
    - current admitted replacement ownership is still intentionally narrower than the full classifier space
    - current admitted replacement kinds are:
      - exact single-source `COUNT(*)`
      - exact single-source scalar facet grouped counts
    - the first explicit invalid planner surface is projection-time `payload:<path>` access in the prepared session/planner path when no admitted exact `Qdrant` kernel owns that expression
    - the first explicit `mergeable` multi-branch state is same-collection raw `UNION ALL`
      branches only when exact filters imply pairwise-disjoint finite point-ID bounds; same
      collection alone is not sufficient because duplicate preservation is part of `UNION ALL`
      semantics
    - that first `mergeable` case is now executable: it rewrites to a single filtered scan rather
      than remaining a classifier-only state
    - same-collection raw `UNION DISTINCT` over exact filters is also now an admitted executable
      `mergeable` case because duplicate elimination removes the overlap hazard present in
      `UNION ALL`
    - same-collection raw `INTERSECT DISTINCT` and `EXCEPT DISTINCT` over exact filters are now
      also admitted executable `mergeable` cases; for raw full-row scan/filter branches they lower
      to conjunction and left-minus-right filter algebra respectively
    - mergeable child-kernel extraction is now treated as compositional rather than terminal:
      exact `COUNT(*)` and exact scalar-facet grouped counts may still claim the larger parent
      subtree after a mergeable child region rewrites to one scan-local kernel in the same
      bottom-up analyzer pass
    - redundant `DISTINCT` over a raw full-row `Qdrant` scan is now dropped because row identity
      already includes unique `id`
28. Structural cleanup should preserve the semantic layering explicitly.
    - shared semantic vocabulary such as payload schema, payload paths, and filter IR belongs in
      shared modules
    - scan-specific selectors, scan specs, and continuation state belong with the table / scan
      runtime rather than in the shared semantic layer
29. When behavior clearly defines or classifies a type, prefer type-owned methods over detached
    helper functions.
    - constructors / recognizers such as `Type::from_plan(...)` and `Type::of(...)` are the
      preferred shape
    - detached helpers should remain only where there is no natural semantic owner
30. The first admitted retrieval prototype is nearest-neighbor query through a DataFusion-native
    marker surface over the prepared session context, not through a context-owned relation helper.
    - current public marker is `qdrant_nearest_score(...)`
    - it lowers through the generic public operator layer over `QdrantOpNode` / `QdrantOp`, then
      into the generic extracted kernel layer over `QdrantKernelNode` / `QdrantKernelSpec::Query`
    - current admitted scope is:
      - dense query vector
      - named-vector selection by the vector column argument
      - exact admitted base filters from the existing predicate algebra
      - descending score sort
      - `LIMIT`
      - optional score-threshold predicates
    - projecting the score column is optional
    - when the score is projected, aliases win; otherwise naming follows normal `DataFusion`
      expression naming
    - `QdrantSessionContext` remains only as the prepared-session wrapper that installs the
      analyzer, planner, and marker-UDF hooks
31. `Qdrant` planner integration now has an explicit two-stage architecture.
    - public or marker semantics belong on a generic unary `QdrantOpNode` over `QdrantOp`
    - extracted exact remote execution belongs on a generic leaf `QdrantKernelNode` over
      `QdrantKernelSpec`
    - feature growth should add enum variants to those families before adding new top-level node
      families
32. Kernel planning should be organized around actual admitted `Qdrant` request families rather
    than one logical node type per feature.
    - current admitted kernel families are:
      - `count`
      - `facet`
      - `query`
    - retrieval-specific shapes such as nearest-neighbor search should live inside the `query`
      family rather than as separate top-level kernel nodes
    - later admitted grouped / batch retrieval should extend that same request-family structure

## Execution Ordering

1. Restore compile and dependency coherence.
2. Lock and implement the correct Arrow output contracts for dense, multivector, and sparse vector data.
3. Replace deprecated `qdrant-client` response handling.
4. Rebaseline tests and public docs.
5. Only then design the stable SQL-native `Qdrant` capability surface.
