# Qdrant Compatibility Matrix

Last updated: 2026-03-22

## Purpose

This document is the detailed capability inventory for `qdrant-datafusion`.

Unlike `docs/CAPABILITY_MATRIX.md`, which tracks the repository’s current admitted scope, this
document inventories the broader current `Qdrant` feature surface and classifies it by semantic
family, implementation shape, and release fit.

The organizing rule is semantic, not SDK-shaped:

1. row restriction
2. row ordering
3. row production
4. ranking / re-scoring
5. aggregation / grouping
6. mutation
7. administration

That classification is what the next release plan should compose over.

## Sources

This matrix is derived from:

1. official `Qdrant` concept docs on:
   - collections
   - points
   - payload
   - filtering
   - indexing
   - explore
   - hybrid queries
   - snapshots
2. the pinned local `qdrant-client 1.17.0` API surface

## Legend

- `Current`: already admitted in `qdrant-datafusion`
- `Next`: reasonable for the next feature release
- `Later`: likely useful, but should follow a more foundational layer
- `Out`: not a good fit for the crate’s SQL/data-source responsibility

## Matrix

| Semantic family | Qdrant capability | Current qdrant-client surface | SQL / DataFusion shape | Release fit | Notes |
|---|---|---|---|---|---|
| Row restriction | `must` / `should` / `must_not` / nested filter composition | `Filter`, `Condition` | predicate algebra | `Next` | This is the core bridge. General boolean normalization should be implemented over provider-owned predicate IR, not inline expression lowering. |
| Row restriction | `has_id` | `Condition::has_id` | `id =`, `id !=`, `id IN`, `id NOT IN` | `Current` | Already admitted. |
| Row restriction | `has_vector` | `Condition::has_vector` | vector-column `IS NULL` / `IS NOT NULL` | `Current` | Already admitted through nullable vector scan contract. |
| Row restriction | payload equality | `Condition::matches` | `payload:<path> = ...` | `Current` | Already admitted for indexed scalar payload fields. |
| Row restriction | payload inequality / range | `Condition::range`, `Condition::datetime_range` | `<`, `<=`, `>`, `>=`, non-negated `BETWEEN` | `Current` | Already admitted for integer / float / datetime. |
| Row restriction | `Match Any` | `Condition::matches` over collections | `IN (...)` | `Next` | Already effectively present for exact subset; should be generalized as part of the broader predicate algebra. |
| Row restriction | `Match Except` | `Condition::matches(!MatchValue::...)` | `NOT IN (...)` | `Next` | Already effectively present for admitted scalar fields; broaden with boolean normalization. |
| Row restriction | same-field equality disjunction | normalized to `IN` | `a = 1 OR a = 2` | `Current` | Admitted exact subset. This is not general `OR`. |
| Row restriction | general `OR` | `should` | boolean predicate normalization | `Next` | Requires full exact-subset analysis. Partial pushdown is not acceptable. |
| Row restriction | general `NOT` | `must_not` | boolean predicate normalization | `Next` | Requires leaf-level inversion rules and explicit null / empty semantics. |
| Row restriction | `is_null` | field condition | payload null semantics | `Next` | Must not be conflated with SQL nulls blindly. |
| Row restriction | `is_empty` | field condition | payload empty / missing semantics | `Next` | Distinct from `is_null`; important to model explicitly. |
| Row restriction | `values_count` | field condition | cardinality predicates | `Later` | Good fit semantically, but depends on payload shape policy. |
| Row restriction | nested object filter | nested condition | correlated payload-array predicates | `Later` | Important, but it is not equivalent to dotted-path conjunctions. Needs explicit SQL semantics. |
| Row restriction | geo radius / bbox / polygon | geo conditions | geo predicates / functions | `Later` | Natural fit for SQL functions or typed expressions, but not first-wave. |
| Row restriction | text match | text condition | explicit text-search predicate | `Later` | Not the same as SQL `LIKE`. |
| Row restriction | phrase match | phrase condition | explicit text-search predicate | `Later` | Same reasoning as text match. |
| Row ordering | ID-ordered scan | `scroll` | `ORDER BY id ASC` | `Current` | Already exact. |
| Row ordering | payload-key ordered scroll | `order_by` on `scroll` | `ORDER BY payload:<path>` | `Current` | Admitted exact subset for indexed integer / float / datetime fields. Distributed exactness still deferred. |
| Row ordering | broader payload ordering | `order_by` | richer payload path ordering | `Later` | Only after payload access contract stabilizes further. |
| Row production | ID-ordered full scan | `scroll` | base table relation | `Current` | This is the stable table-scan baseline. |
| Row production | nearest-neighbor search | `query(Query::Nearest)` / `search` | relation-producing retrieval | `Next` | First retrieval surface to add. Should not be bolted into plain table scan semantics. |
| Row production | nearest with MMR | `query(Query::NearestWithMmr)` | retrieval + ranking modifier | `Later` | Best treated as retrieval modifier after nearest is admitted. |
| Row production | recommendation | `query(Query::Recommend)`, `recommend` | relation-producing retrieval | `Later` | Depends on retrieval IR and SQL surface decision. |
| Row production | discovery | `query(Query::Discover)`, `discover` | relation-producing retrieval | `Later` | Same dependency as recommendation. |
| Row production | context query | `query(Query::Context)` | relation-producing retrieval | `Later` | Same dependency as recommendation / discovery. |
| Row production | sample | `query(Query::Sample)` | relation-producing retrieval | `Next` | Conceptually simple and useful as a retrieval relation once query IR exists. |
| Row production | prefetch subqueries | `QueryPointsBuilder::prefetch` | retrieval pipeline / subquery composition | `Later` | Important for hybrid query plans, but should follow core retrieval IR. |
| Row production | `using` named vector | query/search/recommend builders | retrieval relation parameter | `Next` | Core to multi-vector collections. |
| Row production | `lookup_from` | query/search/recommend/group builders | cross-collection lookup parameter | `Later` | Useful, but not first-wave. |
| Row production | score threshold | query/search builders | retrieval relation modifier | `Next` | Natural once score-bearing retrieval relations exist. |
| Row production | search params (`ef`, exact, quantization knobs) | `SearchParams` | retrieval relation modifier / hint | `Later` | Important, but probably better as explicit parameters after the relation surface exists. |
| Row production | read consistency / timeout / shard selector | builders | execution modifiers | `Later` | Real features, but not part of the core SQL denotation. |
| Ranking / re-scoring | fusion (`RRF`, `DBSF`) | `Query::Fusion`, `Query::Rrf` | ranking composition over retrieval relations | `Later` | Should compose over retrieval relations, not over table scans. |
| Ranking / re-scoring | formula query | `Query::Formula` | score-expression modifier | `Later` | This is a scoring algebra problem, not a scan problem. |
| Ranking / re-scoring | relevance feedback | `Query::RelevanceFeedback` | feedback-driven ranking | `Later` | Likely after retrieval IR and ranking algebra exist. |
| Aggregation / grouping | point count | `count` | `COUNT(*)`-like pushdown | `Next` | Strong fit for SQL and composes directly over predicate algebra. |
| Aggregation / grouping | facet counts | `facet` | grouped count / facet relation | `Next` | Strong fit. Should likely be expressed as aggregate-like relation, not as a scalar function. |
| Aggregation / grouping | grouped search results | `query_groups`, `search_groups`, `recommend_groups` | grouped retrieval relation | `Later` | Likely after core retrieval relation exists. |
| Aggregation / grouping | `with_lookup` on groups | group builders | grouped retrieval enrichment | `Later` | Depends on grouped retrieval surface. |
| Aggregation / grouping | search matrix pairs | `search_matrix_pairs` | similarity-graph / pair relation | `Later` | Interesting, but specialized. |
| Aggregation / grouping | search matrix offsets | `search_matrix_offsets` | sparse similarity-matrix relation | `Later` | Same as above. |
| Mutation | point upsert | `upsert_points`, `upsert_points_chunked` | `INSERT` / `MERGE`-like | `Later` | Needs an explicit write contract; currently unsupported on purpose. |
| Mutation | point delete | `delete_points` | `DELETE` | `Later` | Same as above. |
| Mutation | vector update / delete | `update_vectors`, `delete_vectors` | `UPDATE` | `Later` | Requires a row identity and mutation contract first. |
| Mutation | payload set / overwrite / delete / clear | payload mutation APIs | `UPDATE` | `Later` | Needs payload-structure policy and write semantics. |
| Mutation | batched update | `UpdateBatchPointsBuilder` | DML batching | `Later` | Only after base write semantics exist. |
| Administration | collection create / update / delete | collection APIs | DDL / admin | `Out` | Better handled by admin tooling or a separate crate/layer. |
| Administration | field-index management | `create_field_index`, delete-field-index builder | DDL / admin | `Out` | Important operationally, but not a good first responsibility for this SQL scan crate. |
| Administration | aliases | alias APIs | DDL / catalog indirection | `Out` | Better treated as catalog / admin layer. |
| Administration | snapshots | snapshot APIs | admin / backup | `Out` | Not a SQL query concern. |
| Administration | shard / cluster setup | cluster APIs | admin / topology | `Out` | Operational concern, not part of SQL data-source semantics. |

## Deconstruction

### 1. Predicate algebra

This is still the highest-value foundation because it composes everywhere:

1. base scans
2. count
3. facet
4. nearest / recommend / discover / context
5. grouped retrieval

The remaining predicate work should be treated as a single algebra problem:

1. boolean normalization
2. payload path reference normalization
3. leaf predicate typing and coercion
4. exact-subset validation
5. lowering to `Qdrant` filter conditions

### 2. Retrieval algebra

`Qdrant`’s major differentiator is not just filtering. It is row production:

1. nearest
2. recommend
3. discover
4. context
5. sample

These should not be forced into the current table-scan contract. They are relation-producing
operators and should be modeled as retrieval relations or retrieval specs that compose with the
predicate algebra.

### 3. Ranking algebra

The following are not independent top-level features. They are modifiers over retrieval:

1. fusion
2. formula
3. MMR
4. relevance feedback

So they should be designed only after the first retrieval relation exists.

### 4. Aggregation / grouping algebra

`count` and `facet` are the strongest next SQL fits after predicate completion because they already
compose over filters and do not require the larger retrieval SQL design to be settled first.

Grouped retrieval should follow only after the retrieval relation surface is stable.

## Next Release Priority

### P0: finish the predicate algebra

This should be the next implementation focus.

1. broaden exact boolean semantics beyond the currently admitted same-field equality `OR` subset
2. model payload `is_null` and `is_empty` explicitly
3. extend scalar payload matching cleanly across keyword / integer / bool / datetime cases
4. keep the contract exact-subset-first and reject unsupported combinations cleanly

### P1: add aggregate-like exploration that composes over filters

These are strong next-release candidates because they are SQL-natural and reuse the predicate work:

1. count pushdown
2. facet counts

### P2: introduce the first retrieval relation

This is the first major SQL-surface expansion that is still reasonable for the next release:

1. nearest-neighbor retrieval via unified `query`
2. `using` named-vector selection
3. `limit`, filters, and score threshold
4. explicit score-bearing output contract

### P3: follow with retrieval modifiers

Only after the first retrieval relation exists:

1. sample
2. recommend
3. discover
4. context
5. fusion
6. formula
7. MMR

### Deferred from the next release

1. grouped retrieval variants
2. search-matrix APIs
3. write semantics
4. collection / index / alias / snapshot / cluster administration

## Recommended Implementation Order

1. complete predicate algebra
2. implement count and facet on top of that algebra
3. design the first retrieval relation and its score contract
4. implement nearest retrieval
5. layer retrieval modifiers and secondary retrieval operators on top

That order is the most compositional one currently available.
