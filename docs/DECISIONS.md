# Locked Decisions

Last updated: 2026-03-19

## Core Constraints

1. `qdrant-datafusion` must align `DataFusion`, Arrow, `ndarrow`, `nabled`, and `qdrant-client` on one compatible dependency line. Mixed `DataFusion` graphs are not an admitted steady state.
2. `ndarrow` owns the Arrow/ndarray boundary contract. `nabled::arrow` owns the numerical Arrow contract. `qdrant-datafusion` adapts `Qdrant` data into those contracts.
3. The collection-scan baseline stays thin:
   - query translation to `qdrant-client`
   - result materialization into canonical Arrow carriers
   - clean fallback to `DataFusion` when pushdown is not exact
4. Dense vector outputs from collection scans must use fixed-dimension dense-vector carriers, not variable `List<Float32>` columns.
5. Multivector outputs must use the canonical ragged tensor carrier, not ad hoc `List<List<Float32>>` columns.
6. Sparse vector outputs must use canonical sparse carriers compatible with `nabled::arrow`, not long-term public `*_indices` / `*_values` column pairs.
7. Deprecated `qdrant-client` response fields are not an admitted steady-state dependency.
8. Push down only deterministic SQL-to-`Qdrant` mappings. Unsupported filters must fall back cleanly instead of pretending to be exact.
9. `Qdrant` capability expansion must be SQL-native. Do not mirror the SDK one-to-one without first defining the SQL contract.
10. Baseline collection scans expose:
    - `id` as `Utf8`
    - `payload` as JSON/text
    until a more structured payload contract is explicitly admitted.
11. Any non-trivial behavior or surface-area change must update `docs/CAPABILITY_MATRIX.md`, `docs/EXECUTION_TRACKER.md`, and `docs/STATUS.md` in the same change set.

## Execution Ordering

1. Restore compile and dependency coherence.
2. Lock and implement the correct Arrow output contracts for dense, multivector, and sparse vector data.
3. Replace deprecated `qdrant-client` response handling.
4. Rebaseline tests and public docs.
5. Only then widen the SQL-native `Qdrant` capability surface.
