# Locked Decisions

Last updated: 2026-03-21

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

## Execution Ordering

1. Restore compile and dependency coherence.
2. Lock and implement the correct Arrow output contracts for dense, multivector, and sparse vector data.
3. Replace deprecated `qdrant-client` response handling.
4. Rebaseline tests and public docs.
5. Only then design the stable SQL-native `Qdrant` capability surface.
