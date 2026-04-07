# Execution Tracker

Last updated: 2026-04-07

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
4. Append-only `INSERT INTO` is now supported through `DataSinkExec` and a write-side Arrow/Qdrant serializer.
5. The broad SQL-native `Qdrant` capability surface is still intentionally incomplete, but the predicate algebra foundation, the first aggregate-like planner slices, the first public nearest-retrieval prototype, and the typed payload-access bridge are now in place.
6. The next expansion round is now explicitly staged around the full `Qdrant` relation
   architecture rather than feature-by-feature node growth:
   - first checkpoint: unify current exact count / facet / nearest kernels behind one generic
     extracted kernel family
   - second checkpoint: land the generic public operator layer above that kernel family
   - both checkpoints are now in place, so the next work should extend those enums rather than
     introduce new one-off node families

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
11. `Q-011`: A shared `Qdrant` semantics layer plus provider-owned scan pushdown model now exists for:
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
    - canonical payload-path forms only: direct `payload:<path>`, equivalent public `payload(payload:<path>, 'Type')` shapes, and exact casts to the authoritative payload scalar type
    - indexed integer / float / datetime payload fields only
    - current pushdown is `Exact` because `DataFusion` cannot execute fallback `payload:<path>` physical sorts
    - end-to-end SQL coverage now exercises the admitted path against live `Qdrant`
17. `M-001`: Predicate algebra foundation is now implemented through the provider-owned pushdown model.
    - exact boolean composition over admitted leaves: `AND`, `OR`, `NOT`
    - admitted exact leaves:
      - `id =`, `id !=`, `id IN (...)`, `id NOT IN (...)`
      - vector-column `IS NULL` / `IS NOT NULL`
      - indexed scalar `payload:<path>` comparisons, `IN`, `NOT IN`, `BETWEEN`, and `NOT BETWEEN`
        - integer match predicates require lookup-capable integer indexes
        - integer range predicates require range-capable integer indexes
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
    - the grouped field is currently limited to scalar payload fields with an admitted facet contract
    - the execution path lowers into `Qdrant`’s native `facet` API
    - admitted exact filters still reuse the existing provider-owned predicate algebra
    - broader grouped SQL remains deferred because `Qdrant` facet denotes top-N grouped counts, not unconstrained SQL grouping
20. `Q-023`: Payload null / empty runtime behavior is now validated directly against the target dependency line.
    - on March 25, 2026, live `Qdrant 1.17.0` tests through `qdrant-client 1.17.0` confirmed:
      - explicit payload `NULL` written via point upsert is preserved
      - explicit payload `NULL` written via `set_payload` is preserved
      - `is_null` matches explicit null only
      - `is_empty` matches missing, explicit null, and `[]`
      - `is_empty` does not match empty strings or empty objects on the current runtime line
      - `values_count >= 0` matches present fields, including explicit null and empty arrays
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
      - exact single-source scalar facet grouped counts
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
    - direct scan-path `payload:<path>` projections are no longer treated as an invalid surface; they now rewrite to typed local payload accessors when the source payload schema is authoritative
    - raw unhinted arithmetic over `payload:<path>` still fails earlier in SQL planning and currently requires `payload(...)` or an explicit `CAST(...)`
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
    - exact `COUNT(*)` and exact scalar-facet grouped counts can still replace the larger parent
      subtree after that child rewrite in the same bottom-up analyzer pass
31. `Q-034`: The admitted facet slice is now broader without overstating typed payload SQL semantics.
    - top-facet grouped counts now admit keyword, bool, and lookup-capable integer payload indexes
    - facet keys still surface as `Utf8`, matching the current textual `payload:<path>` SQL bridge
    - integer payload metadata now distinguishes `lookup` from `range`, so integer `=` / `IN` pushdown no longer overstates range-only integer indexes
    - live collection introspection on the current runtime line now preserves integer lookup/range metadata well enough to admit integer facet pushdown on the same exact contract
    - this keeps the broadened facet slice exact on the current planner/runtime contract without pretending typed payload projection is already admitted
32. `Q-035`: Payload-empty SQL semantics are now narrowed to the standard SQL subset that the current bridge can state honestly.
    - scalar empty values do not get a dedicated backend-shaped predicate
    - empty strings remain ordinary non-null values and are expressed through normal equality, for example `payload:<path> = ''`
    - current tests now prove that empty strings stay distinct from `payload:<path> IS NULL`
    - explicit empty/container/cardinality semantics landed later through dedicated `payload_is_empty(...)` / `payload_values_count(...)` functions while keeping scalar empty-string handling on ordinary SQL equality
33. `Q-036`: The first retrieval relation reached runtime exactness through the `query` request
    family before the public/operator checkpoint landed.
    - the admitted semantics were already:
      - dense nearest-neighbor query over `Qdrant::query`
      - named-vector selection
      - exact admitted filters from the existing predicate algebra
      - explicit `LIMIT` pushdown when present, otherwise Qdrant's default result count
      - optional score threshold
    - that exact scope is now carried forward by the generic operator/kernel architecture instead
      of the removed context-owned helper surface
34. `Q-037`: The current exact `Qdrant` leaf relations are now represented by one generic kernel
    family instead of separate logical node types.
    - `QdrantKernelNode` is now the extracted leaf node for the current exact remote kernels
    - `QdrantKernelSpec` now carries the admitted current kernel families:
      - `count`
      - `facet`
      - `query`
    - the current nearest-neighbor retrieval slice now lives under the `query` kernel family
      instead of its own top-level kernel node type
35. `Q-038`: The first generic public `Qdrant` operator checkpoint is now in place.
    - `QdrantOpNode` / `QdrantOp` now own the current public marker semantics above the shared
      kernel family
    - the first public marker is `qdrant_nearest_score(...)`
    - exact lowering currently admits:
      - dense query vectors
      - named-vector selection by the vector column argument
      - explicit `LIMIT` pushdown when present, otherwise Qdrant's default result count
      - omitted projected score ordering uses Qdrant's native score-desc result order
      - projected `ORDER BY score DESC` is redundant and optimizes away, while projected `ORDER BY score ASC` remains local
      - optional exact base filters
      - optional score-threshold predicates
      - benign local projection shells and local residual filter shells can remain above the closed qdrant query kernel, including later score projection above those local filter shells
    - score output is only present when projected
    - when projected, aliases win; otherwise naming follows normal `DataFusion` expression naming
    - `QdrantSessionContext` now remains only as the prepared-session wrapper that installs the
      analyzer, planner, and marker-UDF hooks
36. `Q-039`: Shared payload/path semantics now live in one canonical `Qdrant` layer instead of
    drifting across scan pushdown, analyzer, and execution code.
    - `src/qdrant.rs` now owns payload schema, canonical payload access/path recognition, and
      payload ordering capability
    - `src/table/scan_spec.rs` now owns only scan-local selectors, ordering, and continuation
      state
    - exact scan filter pushdown and payload-key sort pushdown now reuse the same canonical payload-access
      recognition, with operation-specific cast handling: exact casts for filter semantics and
      order-preserving casts for ordering semantics
37. `Q-040`: Typed payload access is now a first-class admitted bridge instead of a planner-layer
    dead end.
    - direct scan-path `payload:<path>` projections over known payload fields now rewrite to typed
      local payload accessors early enough for honest logical schema propagation
    - the public `payload(accessor, 'Type')` helper now supplies a planning-time payload scalar
      type when SQL would otherwise still see raw `payload:<path>` as `Utf8`
    - exact scan filter pushdown admits equivalent public `payload(...)` forms and exact casts
      when they lower to the same canonical payload path
    - payload-key sort pushdown now also admits order-preserving casts that preserve the same
      effective ordering over the authoritative payload scalar type
38. `Q-042`: Query-surface payload projections now reuse the same canonical payload resolver as
    scan filter/sort pushdown.
    - query projection admission now resolves payload outputs through authoritative payload schema
      metadata when available instead of only recognizing raw path syntax
    - exact `CAST(payload:<path> AS <canonical type>)` query projections now preserve remote
      payload fetch and typed output materialization on qdrant query kernels
    - planner and e2e coverage now prove the exact-cast query projection path
39. `Q-041`: Append-only `INSERT INTO` now lands on a deliberate write contract.
    - `QdrantTableProvider::insert_into` now follows the standard `DataFusion` `DataSinkExec` pattern
    - write-side Arrow/Qdrant serialization now converts canonical provider rows into `PointStruct` values
    - the current admitted contract is explicit: append-only only, with upstream schemas logically equivalent to the qdrant table schema
    - planner coverage now proves the physical plan lowers to `QdrantInsertSink` and unit coverage proves dense / named / sparse write serialization
40. `Q-043`: Ordered payload-key scroll exactness is now explicitly guarded by cluster metadata, closing the `Q-017` validation gap.
    - `QdrantTableProvider::try_new` now consults `collection_cluster_info`
    - exact payload-key sort pushdown remains enabled only for stable single-peer collections
    - distributed, transferring, or resharding collection states now fall back to local `DataFusion` sorting instead of claiming exact remote order
    - e2e coverage now validates the single-peer multi-shard ordered-scroll contract on the target runtime
41. `Q-044`: `sample` is now a validated current retrieval relation on the shared operator / kernel architecture.
    - `qdrant_sample_score([method])` now has live end-to-end coverage on the prepared session surface
    - the current admitted method is `random`, and omitting the method now falls back to the same exact contract
    - docs and tracker state now promote `sample` from `Next` to `Current`
42. `Q-045`: recommend / discover / context are now validated current retrieval relations on the shared operator / kernel architecture.
    - live end-to-end coverage now proves the prepared-session recommend / discover / context surfaces against `Qdrant`
    - recommend now has explicit strategy coverage in addition to the default strategy contract
    - the public Rust helper surface now includes `qdrant_recommend_score_with_strategy(...)` for the explicit-strategy DataFrame path
    - docs and tracker state now promote recommend / discover / context from deferred planning inventory to current retrieval prototypes
43. `Q-046`: nearest-with-MMR and relevance-feedback are now validated current retrieval modifiers on the shared operator / kernel architecture.
    - live end-to-end coverage now proves the prepared-session `qdrant_nearest_with_mmr_score(...)` surface against `Qdrant`
    - relevance feedback now accepts DataFusion-native feedback-item arrays using `struct(example, score)` entries on the prepared-session SQL surface
    - relevance feedback live coverage now proves the explicit naive-strategy coefficient path
    - the public Rust helper surface now takes explicit naive strategy coefficients directly on `qdrant_relevance_feedback_score(...)`
    - docs and tracker state now promote nearest-with-MMR and relevance feedback from deferred planning inventory to current retrieval modifiers
44. `Q-047`: grouped nearest top-1 retrieval is now validated as a narrow current grouped-query slice on the shared query/kernel architecture, with exact remote grouped retrieval and local outer `LIMIT` preservation.
    - live end-to-end coverage now proves the prepared-session `DISTINCT ON (payload:<path>) ... qdrant_nearest_score(...)` surface against `Qdrant`
    - exact lowering currently admits one scalar keyword or lookup-capable integer payload field, `ORDER BY payload:<path>[ DESC]` with optional trailing `, score DESC` as an explicit in-group tie-break, and group size 1 while any outer `LIMIT` remains local
    - docs and tracker state now promote that grouped-nearest `DISTINCT ON` subset from an implementation detail to an admitted current capability while keeping broader grouped retrieval deferred
    - grouped execution validates that returned group ids match scalar payload values on hits so multi-valued grouped fields fail clearly instead of producing SQL-incompatible results
    - outer SQL `LIMIT` is preserved locally because final group ordering is imposed after grouped retrieval, while the grouped request itself uses an exact point-count upper bound instead of Qdrant's default group limit
45. `Q-048`: retrieval kernels now allow benign local projection shells and local residual filter shells above the closed qdrant query kernel instead of requiring fully remote-only projection/filter shapes.
    - live end-to-end coverage now proves local projection over `qdrant_nearest_score(...) + ...` above a closed `QdrantQueryExec`
    - mixed query-kernel predicates now split into exact remote payload/id filters plus local residual `FilterExec` shells instead of fataling when a non-pushdownable score predicate remains
    - those local residual filter shells can now still feed a later projected score column instead of forcing the score marker back into an unsupported local surface
    - grouped nearest top-1 no longer requires an explicit trailing `score DESC` when SQL only orders by the grouped payload key, while an explicit score tie-break still lowers when present
46. `Q-049`: grouped retrieval is now widened from nearest-only to the current grouped query-family subset on the existing `DISTINCT ON` contract.
    - planner and live end-to-end coverage now prove grouped `qdrant_recommend_score(...)`, `qdrant_discover_score(...)`, and `qdrant_context_score(...)` on the same `QdrantQueryGroupsExec` path
    - the admitted grouped SQL surface is still narrow: one scalar keyword or lookup-capable integer payload field, group size `1`, and `ORDER BY payload:<path>[ DESC]` with optional trailing `, score DESC` as an explicit in-group tie-break
    - docs now promote grouped nearest / recommend / discover / context as the current grouped retrieval subset while leaving broader grouped retrieval and other query-family variants deferred
47. `Q-050`: explicit payload empty/cardinality semantics are now admitted without overloading SQL null semantics.
    - `payload_is_empty(payload:<path>)` now executes locally and lowers exactly to Qdrant `is_empty` predicates on scan filters
    - `payload_values_count(payload:<path>)` now executes locally and lowers exactly to Qdrant `values_count` predicates on scan filters
    - live runtime coverage now locks the current contract down: missing maps to `NULL`, explicit `null` and `[]` map to `0`, and present non-array values map to `1` for `payload_values_count`
48. `Q-051`: explicit geo distance semantics are now admitted without pretending all geo predicates are SQL-native.
    - `payload_geo_distance(payload:<path>, lon, lat)` now executes locally as a numeric payload function over geo payload objects
    - the exact `payload_geo_distance(...) <= radius` subset now lowers to Qdrant `geo_radius` filters on geo payload fields
    - broader numeric comparisons stay local `FilterExec` shells instead of being rejected or over-pushed
49. `Q-052`: explicit text and phrase semantics are now admitted without guessing local full-text behavior.
    - `payload_text_match(payload:<path>, 'query')` now lowers exactly to Qdrant text-match filters on text-indexed payload fields
    - `payload_phrase_match(payload:<path>, 'phrase')` now lowers exactly to Qdrant phrase-match filters only when the text index enables phrase support
    - both predicates stay explicit remote-only SQL surfaces because exact local execution would have to duplicate Qdrant tokenizer, stopword, and stemming semantics
50. `Q-053`: admission-mode tracking and SQL inventory tracking are now explicit and separate from feature tracking.
    - `docs/ADMISSION_MATRIX.md` is now the canonical inventory of exact-only, exact-plus-residual, local-fallback, remote-only, and strict-for-now behavior
    - shared strictness inherited through common recognizers or helpers is now expected to be visible there instead of remaining implicit in code
    - `tests/catalog/mod.rs` now mirrors supported / unsupported SQL catalogs consumed by `tests/e2e.rs` and `tests/unsupported_e2e.rs`, so capability movement can be reviewed directly from query inventory
51. `Q-054`: the supported / unsupported SQL catalogs now act as the namespace inventory rather than a thin sample set.
    - major supported and unsupported namespaces now carry explicit subquery-shaped cases
    - broader SQL syntax families such as `CTE`, `UNION ALL`, `UNNEST`, `WINDOW`, and non-`FULL OUTER JOIN` composition are now represented where they materially interact with qdrant admission behavior
    - `tests/e2e.rs` and `tests/unsupported_e2e.rs` now exercise those catalog additions directly instead of leaving them as unconsumed inventory
    - catalog expansion is now explicitly SQL-space-first: the inventories should grow by stretching the admitted and deferred SQL surface broadly, with code-gap audit used only to explain failures after the catalog exposes them
    - unsupported inventory is now explicitly classified per query as `Deferred`, `ByDesign`, `Upstream`, or `InvalidInput`, so the reference surface distinguishes “not yet”, “not intended”, and “not our limitation”
52. `Q-055`: facet/count-localization gaps are closed.
    - unfinished scalar-facet regions now localize cleanly instead of failing when later SQL leaves the exact top-facet contract
    - recursive typed payload rewriting now keeps `payload:<path>` executable through broader local aggregate / `HAVING` / window shells
    - `tests/catalog/mod.rs::supported::scan::aggregates::HAVING_FACET` and `tests/catalog/mod.rs::supported::scan::aggregates::WINDOW_OVER_FACET_SUBQUERY` now track those admitted shapes directly
53. `Q-056`: explicit nested payload predicates are now admitted without introducing a string mini-language.
    - `payload_nested_match(payload:<path>, <predicate>)` now lowers exactly to Qdrant nested conditions on scan filters
    - the nested predicate reuses the existing payload filter algebra in a nested scope instead of inventing a separate parser surface
    - catalog coverage now tracks both direct and subquery nested-filter SQL in the supported inventory

## Next

1. The detailed planning inventory for the next expansion round now lives in `docs/QDRANT_COMPATIBILITY_MATRIX.md`.
3. `M-003`: Extend broader aggregate-like and retrieval growth on
   the shared operator / kernel structure.
   - aggregate-like: explicit output contracts beyond exact `COUNT(*)` and the current scalar
     facet slice
   - retrieval: broader `query`-family relations and modifiers such as grouped retrieval beyond the current grouped query-family `DISTINCT ON` subset now that nearest / sample / recommend / discover / context / nearest-with-MMR / relevance feedback are all fully absorbed
   - current retrieval kernels now allow omitted SQL `LIMIT`, deferring to Qdrant's native default result count unless SQL specifies one
   - current retrieval kernels now also allow omitted projected `ORDER BY score DESC`, deferring to Qdrant's native result order unless SQL specifies a local re-sort
4. `Q-020`: Extend the predicate algebra only where the SQL semantics are explicit.
   - broader count-oriented predicates beyond the current explicit `payload_is_empty(...)`, `payload_values_count(...)`, `payload_geo_distance(...) <= radius`, `payload_geo_within_bbox(...)`, `payload_geo_within_polygon(...)`, `payload_nested_match(...)`, `payload_text_match(...)`, `payload_text_any(...)`, and `payload_phrase_match(...)` subset
5. Keep the new admission/fallback catalog current as behavior widens.
   - move shared strictness out of scattered implicit notes and into `docs/ADMISSION_MATRIX.md`
   - when a path is strict-for-now, track the affected consumers and the intended widening there
6. Keep the shared supported / unsupported SQL catalogs current as behavior widens.
   - new admitted SQL should land in `tests/catalog/mod.rs::supported` and be consumed by `tests/e2e.rs`
   - new deferred SQL should land in `tests/catalog/mod.rs::unsupported` and be exercised by `tests/unsupported_e2e.rs` until it migrates to the supported side
   - if an unsupported catalog query starts passing, move it to the supported side instead of weakening the unsupported assertion or leaving stale deferred inventory behind
   - treat the catalogs as the namespace inventory, not a sample appendix: major namespaces should expose subquery-shaped SQL and any broader syntax families that materially affect admission behavior
   - expand the catalogs from the SQL space outward first, not from the current code gaps inward
7. Continue mapping the pushdown model onto `DataFusion`’s own idioms where broader traversal is required.
   - `TreeNode` visitors / rewriters instead of ad hoc recursion
   - `LogicalPlan` expression and subquery helpers before project-local traversal
   - exact admission of broader filter families and aggregate-like shapes instead of ad hoc expression splitting
8. Extend the planner scaffold beyond the current explicit classifier set toward richer island composition and kernel extraction.
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
3. keep the write contract explicit: append-only only unless broader semantics are deliberately specified
4. use `DataFusion` primary-source idioms before inventing project-local traversal or rewrite patterns
5. admit only explicit pushdown subsets; reject unsupported cases cleanly instead of approximating them
   - when widening behavior, prefer one shared invariant-based recognizer over duplicated local shape checks
6. track unresolved distributed `Qdrant` ordering edge cases explicitly; the single-node ordered continuation contract is now known
7. update this tracker in the same change set as any non-trivial landing
8. stop for planning again before widening the SQL surface in a way that could affect other vector-store integrations
9. keep aggregate-like planner work compositional:
   - provider-owned predicate algebra remains the lowering target
   - relation-producing retrieval work is still a separate higher layer
10. keep the new architectural checkpoints explicit:
   - do not add more feature-specific `Qdrant` logical node families under the new plan
   - extend the generic operator / kernel enums instead
