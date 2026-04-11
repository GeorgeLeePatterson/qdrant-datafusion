# Admission Matrix

Last updated: 2026-04-11

## Purpose

This file is the canonical inventory of how `qdrant-datafusion` currently admits, rewrites, or
rejects SQL shapes.

It exists separately from `docs/CAPABILITY_MATRIX.md` because feature presence alone is not enough.
For each semantic family, we need to know whether the current behavior is:

- remote exact only
- remote exact plus local residual
- local fallback
- remote-only by design
- strict for now

That is the boundary that tells us whether a missing query shape is a deliberate contract or an
implementation gap that must be revisited.

The reviewable SQL inventory for those boundaries lives in `tests/catalog/mod.rs`, consumed by
`tests/e2e.rs` for admitted queries and `tests/unsupported_e2e.rs` for deferred ones.

The supported catalog is explicitly classified as well:

- `Full`: admitted on the library's current intended contract, even when local `DataFusion`
  operators are part of the honest execution shape
- `LocalFallback`: admitted, but still exposing a meaningful capability gap relative to the
  stronger qdrant-owned contract; these entries must carry explicit plan-shape assertions in
  `tests/e2e.rs`

The unsupported catalog is also explicitly classified so review does not depend on reading code or
tracker prose alone:

- `Deferred`: intended future capability gap
- `ByDesign`: explicit boundary that is not expected to grow unless the product contract changes
- `Upstream`: blocked by current `DataFusion` or upstream SQL-planning behavior rather than local
  implementation policy
- `InvalidInput`: invalid against the current public contract and not expected to become supported
  without changing the contract itself

That inventory should be expanded from the SQL space outward, not primarily by mining known code
gaps. Code and analyzer audits remain useful, but only as a secondary explanation for why a SQL
shape lands on the unsupported side. The primary inventory should stretch syntax families,
expression forms, nesting, and composition broadly enough that the catalogs themselves expose the
real supported and unsupported surface.

That inventory should not stop at the canonical happy path. Major namespaces should carry:

- at least one supported subquery-shaped case
- at least one unsupported subquery-shaped case when the namespace still has known gaps
- explicit broader SQL syntax families such as `CTE`, `UNION ALL`, `UNNEST`, `WINDOW`, and
  non-`FULL OUTER JOIN` composition wherever those forms materially interact with qdrant admission
  behavior
- explicit join matrices where join semantics materially affect admission or fallback behavior,
  including `INNER`, `LEFT`, `RIGHT`, `FULL`, `CROSS`, `SEMI`, and `ANTI` forms plus representative
  `ON` and `USING` variants where `DataFusion` admits them
- explicit `ByDesign`, `Upstream`, or `InvalidInput` unsupported cases wherever those boundaries
  materially shape the public capability story

## Mode Legend

- `Exact only`: the current path only admits a shape when it can be lowered exactly to `Qdrant`.
- `Exact + residual`: the current path pushes the exact subset remotely and leaves the remainder to
  local `DataFusion`.
- `Local fallback`: the current path can rewrite the operation entirely into local executable SQL
  semantics when remote lowering does not apply.
- `Remote-only`: the current path intentionally has no local equivalent because exact semantics are
  owned by `Qdrant`.
- `Strict for now`: the current path is narrower than the eventual intended semantics.

## Current Inventory

| Semantic family | Primary code path | Current mode | Current boundary | Follow-up |
|---|---|---|---|---|
| Scan filter pushdown | `src/table/provider.rs`, `src/qdrant/filter/normalize.rs`, `src/qdrant/filter.rs` | `Exact only` | Provider-level filter pushdown only reports `Exact` or `Unsupported`; partial remote/local decomposition does not exist yet on the scan path. | Broaden toward `Exact + residual` where `DataFusion` can safely keep a local `FilterExec` above the scan. |
| Scan physical filter absorption | `src/table/exec.rs`, `src/qdrant/filter.rs` | `Exact only` | Physical pushdown absorbs the admitted exact subset and leaves everything else fully local. | Keep aligned with any future logical residual decomposition. |
| Scan sort pushdown | `src/table/exec.rs`, `src/qdrant.rs` | `Exact only` | Exact for `ORDER BY id ASC` and the admitted payload-key ordered-scroll subset; other orderings remain local. | Widen only with an explicit remote contract or an honest `DataFusion` fallback path. |
| Scan payload projection typing | `src/context/expr_planner.rs`, `src/analyzer/state/source.rs`, `src/expr_fn/payload.rs`, `src/expr_fn/payload_access.rs` | `Local fallback` | Known `payload:<path>` projections and raw arithmetic / scalar-function compositions now type during SQL planning from authoritative payload-schema metadata, so direct, subquery, CTE, union, ordering, window, and aggregate shells localize cleanly when they leave exact qdrant ownership. The current exact count/facet kernels now also survive benign payload-alias projection shells, exact row-count kernels additionally survive benign non-null `id` alias shells, and exact row-count/facet kernels now also survive benign literal alias shells, when later aggregate semantics still match the admitted exact contracts. Remaining unsupported cases in this family are broader SQL-shape limits such as non-lateral `UNNEST`, not missing payload typing. | Keep payload typing anchored to schema metadata when new SQL-planning seams are added, and widen exact shell preservation only when the aggregate contract remains provably unchanged. |
| Canonical insert / replace / overwrite sink semantics | `src/table/provider.rs`, `src/table/insert.rs`, `src/arrow/serialize.rs` | `Strict for now` | The admitted sink contract is canonical-schema `INSERT INTO`, `REPLACE INTO`, and `INSERT OVERWRITE` over the provider table. Explicit target-column inserts are part of that contract when `DataFusion` normalizes reordered columns or omitted nullable `payload` back into the canonical schema. On the current qdrant runtime line, `INSERT INTO` uses verified `InsertOnly` behavior, preserving existing ids while inserting new ids; `REPLACE INTO` uses verified `Upsert` behavior, replacing colliding ids while inserting new ids; and `INSERT OVERWRITE` clears the collection first, then writes canonical rows through the same sink path. Remaining noncanonical write shapes stay in the unsupported inventory as `InvalidInput` or `Upstream`, not hidden sink coercions. | Broaden only through explicit write-semantics work, not by silently coercing arbitrary insert schemas at the provider boundary. |
| Canonical delete semantics | `src/table/provider.rs`, `src/table/delete.rs` | `Exact + residual` | Provider-table `DELETE FROM ...` now partitions exact admitted qdrant filters from local residual predicates. Exact filters still lower through the existing qdrant filter algebra, residual predicates evaluate locally over materialized candidate rows before deletion, and empty `DELETE FROM vectors` still deletes all rows through the same provider-owned boundary. | Broaden deliberately: joined delete forms still depend on upstream support before provider planning. |
| Canonical row-rewrite update semantics | `src/table/provider.rs`, `src/table/update.rs`, `src/arrow/serialize.rs` | `Exact + residual` | Provider-table `UPDATE` now executes as a canonical row rewrite over the provider schema. Exact admitted qdrant filters lower remotely, non-pushdownable predicates stay local over the materialized candidate rows, and matched rows are rewritten through the same canonical row serializer before `upsert`. `id` assignment is now admitted on the same path when the rewritten final ids remain unique within the update set and do not collide with untouched existing rows; `UPDATE ... FROM` remains upstream unsupported. | Broaden deliberately: backend-specific partial payload/vector mutation APIs and broader joined update forms still need explicit contract work beyond the current row-rewrite checkpoint. |
| Query-kernel filter handling | `src/analyzer/op.rs` | `Exact + residual` | Exact payload/id filters and score-threshold predicates lower remotely; residual score predicates stay local above the closed query kernel. | Extend the same pattern to more admitted local shells as new retrieval surfaces land. |
| Query-kernel projection / aggregation handling | `src/analyzer/op.rs`, `src/analyzer/state/local.rs`, `src/analyzer/state/processing.rs` | `Exact + residual` | Remote retrieval kernels can now leave benign local projection shells, local aggregate shells, local window shells, and local join shells, including aggregate subquery / CTE cases, direct query-family window cases, direct nearest `HAVING`, and independent query-family branches joined locally above remote kernels, while later score projection still works above residual local filters. | Keep broadening only through explicit local-shell rewrites rather than shape-specific fatal paths. |
| Formula lowering | `src/analyzer/query/formula.rs`, `src/analyzer/optimize.rs` | `Exact + local fallback` | Coordinated remote formula lowering is exact when admitted; local fallback currently covers plain SQL arithmetic over resolved columns, and branch-local qdrant-only formula leaves now also admit the current single-prefetch rewrite when the formula binds to one independently-closable join branch. | Broaden local fallback by materializing/supporting more qdrant-specific leaves where honest local execution exists. |
| Coordinated combiners | `src/analyzer/optimize.rs` | `Exact + local fallback` | Coordinated `formula` / `fusion` rewrites still admit the current id-preserving `FULL OUTER JOIN USING (id)` subset with effective score-desc ordering and optional outer `LIMIT`. `formula` additionally admits the current branch-local qdrant-leaf subset when the formula binds to one independently-closable branch and the outer join stays local. `fusion` now additionally admits the current local aligned-join fallback over `INNER` / `LEFT` / `RIGHT` joins on `id` with explicit score-column inputs for both `RRF` and `DBSF`. `CROSS JOIN` remains by-design unsupported because it does not align the same candidate across branches. | Keep remote rewrite narrow; broaden via explicit local fallback or residual shells, not hidden exact-shape expansion. |
| Explicit text / phrase predicates | `src/expr_fn/payload_text.rs`, `src/qdrant/filter/normalize.rs` | `Remote-only` | Exact semantics depend on qdrant text-index configuration; local execution is intentionally not approximated. | Extend only with more explicit remote text predicates, not local imitation of qdrant text behavior. |
| Grouped query-family retrieval | `src/analyzer/op.rs`, `src/analyzer/kernel/query.rs`, `src/context/exec.rs` | `Exact + local fallback` | The current single-key grouped `DISTINCT ON` subset lowers exactly to grouped retrieval with local outer ordering/limit preservation; broader grouped `DISTINCT ON` SQL now stays local above a normal query kernel instead of failing, while only the truthful `query_groups` subset claims exact remote grouped semantics. | Broaden the exact remote contract only through explicit grouped SQL semantics on the shared query/kernel structure. |

## Review Rule

When a new SQL capability is added or widened, update this file in the same change set and answer:

1. Is the new path `Exact only`, `Exact + residual`, `Local fallback`, `Remote-only`, or `Strict for now`?
2. If it is `Strict for now`, where is the follow-up tracked?
3. If the strictness comes from a shared recognizer or shared helper, which other subsystems inherit that strictness?
4. Is there a representative SQL case in `tests/catalog/mod.rs` showing the admitted or deferred behavior?
5. Does the mirrored SQL inventory still expose the namespace in a subquery context and in any broader SQL syntax families that materially affect admission?
6. If a deferred SQL case now passes, was it moved from `unsupported` to `supported` instead of being left behind as stale inventory?
