#![allow(dead_code)]

// Shared SQL inventories for integration coverage.
//
// `supported` contains the current admitted SQL surface used by `tests/e2e.rs`.
// `unsupported` contains the current deferred / failing SQL surface used by
// `tests/unsupported_e2e.rs`.
//
// The module structure is intentionally mirrored between `supported` and
// `unsupported` so capability movement is visible as queries migrate from one
// side to the other.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SqlCase {
    pub(crate) id:  &'static str,
    pub(crate) sql: &'static str,
}

impl SqlCase {
    pub(crate) const fn new(id: &'static str, sql: &'static str) -> Self { Self { id, sql } }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UnsupportedKind {
    Deferred,
    ByDesign,
    Upstream,
    InvalidInput,
}

impl UnsupportedKind {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Deferred => "deferred",
            Self::ByDesign => "by_design",
            Self::Upstream => "upstream",
            Self::InvalidInput => "invalid_input",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UnsupportedSqlCase {
    pub(crate) case:           SqlCase,
    pub(crate) kind:           UnsupportedKind,
    pub(crate) tracker:        Option<&'static str>,
    pub(crate) boundary:       &'static str,
    pub(crate) error_contains: &'static str,
}

impl UnsupportedSqlCase {
    pub(crate) const fn new(
        id: &'static str,
        sql: &'static str,
        tracker: &'static str,
        boundary: &'static str,
        error_contains: &'static str,
    ) -> Self {
        Self::deferred(id, sql, tracker, boundary, error_contains)
    }

    pub(crate) const fn deferred(
        id: &'static str,
        sql: &'static str,
        tracker: &'static str,
        boundary: &'static str,
        error_contains: &'static str,
    ) -> Self {
        Self {
            case: SqlCase::new(id, sql),
            kind: UnsupportedKind::Deferred,
            tracker: Some(tracker),
            boundary,
            error_contains,
        }
    }

    pub(crate) const fn by_design(
        id: &'static str,
        sql: &'static str,
        boundary: &'static str,
        error_contains: &'static str,
    ) -> Self {
        Self {
            case: SqlCase::new(id, sql),
            kind: UnsupportedKind::ByDesign,
            tracker: None,
            boundary,
            error_contains,
        }
    }

    pub(crate) const fn upstream(
        id: &'static str,
        sql: &'static str,
        boundary: &'static str,
        error_contains: &'static str,
    ) -> Self {
        Self {
            case: SqlCase::new(id, sql),
            kind: UnsupportedKind::Upstream,
            tracker: None,
            boundary,
            error_contains,
        }
    }

    pub(crate) const fn invalid(
        id: &'static str,
        sql: &'static str,
        boundary: &'static str,
        error_contains: &'static str,
    ) -> Self {
        Self {
            case: SqlCase::new(id, sql),
            kind: UnsupportedKind::InvalidInput,
            tracker: None,
            boundary,
            error_contains,
        }
    }
}

pub(crate) mod supported {
    use super::SqlCase;

    pub(crate) mod scan {
        use super::SqlCase;

        pub(crate) mod projection {
            use super::SqlCase;

            pub(crate) const DOCS_FULL: SqlCase = SqlCase::new(
                "scan.projection.docs_full",
                "SELECT id, payload, text_embedding, multi_embedding, keywords FROM docs ORDER BY \
                 id",
            );
            pub(crate) const DOCS_TEXT_NONNULL: SqlCase = SqlCase::new(
                "scan.projection.docs_text_nonnull",
                "SELECT text_embedding FROM docs WHERE text_embedding IS NOT NULL ORDER BY id",
            );
            pub(crate) const DOCS_MULTI_NONNULL: SqlCase = SqlCase::new(
                "scan.projection.docs_multi_nonnull",
                "SELECT multi_embedding FROM docs WHERE multi_embedding IS NOT NULL ORDER BY id",
            );
            pub(crate) const DOCS_KEYWORDS_NONNULL: SqlCase = SqlCase::new(
                "scan.projection.docs_keywords_nonnull",
                "SELECT keywords FROM docs WHERE keywords IS NOT NULL ORDER BY id",
            );
            pub(crate) const VECTORS_ALL_IDS: SqlCase =
                SqlCase::new("scan.projection.vectors_all_ids", "SELECT id FROM vectors");
            pub(crate) const UNNAMED_FULL: SqlCase = SqlCase::new(
                "scan.projection.unnamed_full",
                "SELECT id, payload, vector FROM vectors ORDER BY id",
            );
            pub(crate) const TYPED_PAYLOAD_FIELDS: SqlCase = SqlCase::new(
                "scan.projection.typed_payload_fields",
                concat!(
                    "SELECT id, payload:rank AS rank, payload:active AS active, payload:tag AS \
                     tag, ",
                    "CAST(payload:rank AS BIGINT) + 1 AS next_rank, payload(payload:rank, \
                     'Int64') AS hinted_rank, ",
                    "payload(payload:rank, 'Integer') + 1 AS hinted_next_rank FROM vectors ORDER \
                     BY id"
                ),
            );
            pub(crate) const HINTED_PAYLOAD_ARITHMETIC: SqlCase = SqlCase::new(
                "scan.projection.hinted_payload_arithmetic",
                "SELECT id, payload(payload:rank, 'Integer') + 1 AS next_rank FROM vectors ORDER \
                 BY next_rank, id",
            );
            pub(crate) const RAW_PAYLOAD_ARITHMETIC: SqlCase = SqlCase::new(
                "scan.projection.raw_payload_arithmetic",
                "SELECT payload:rank + 1 AS next_rank FROM vectors ORDER BY id",
            );
            pub(crate) const SUBQUERY_RAW_PAYLOAD_ARITHMETIC: SqlCase = SqlCase::new(
                "scan.projection.subquery_raw_payload_arithmetic",
                "SELECT next_rank FROM (SELECT payload:rank + 1 AS next_rank FROM vectors) \
                 projected ORDER BY next_rank",
            );
            pub(crate) const CTE_RAW_PAYLOAD_ARITHMETIC: SqlCase = SqlCase::new(
                "scan.projection.cte_raw_payload_arithmetic",
                "WITH projected AS (SELECT payload:rank + 1 AS next_rank FROM vectors) SELECT \
                 next_rank FROM projected ORDER BY next_rank",
            );
            pub(crate) const UNION_ALL_RAW_PAYLOAD_ARITHMETIC: SqlCase = SqlCase::new(
                "scan.projection.union_all_raw_payload_arithmetic",
                "SELECT next_rank FROM (SELECT payload:rank + 1 AS next_rank FROM vectors) a \
                 UNION ALL SELECT next_rank FROM (SELECT payload:rank + 1 AS next_rank FROM \
                 vectors) b",
            );
            pub(crate) const HINTED_PAYLOAD_FUNCTION: SqlCase = SqlCase::new(
                "scan.projection.hinted_payload_function",
                "SELECT id, ABS(payload(payload:rank, 'Integer') - 15) AS rank_distance FROM \
                 vectors ORDER BY rank_distance, id",
            );
            pub(crate) const RAW_PAYLOAD_FUNCTION: SqlCase = SqlCase::new(
                "scan.projection.raw_payload_function",
                "SELECT ABS(payload:rank) AS abs_rank FROM vectors ORDER BY id",
            );
            pub(crate) const RAW_PAYLOAD_DIVISION: SqlCase = SqlCase::new(
                "scan.projection.raw_payload_division",
                "SELECT payload:rank / 2 AS half_rank FROM vectors ORDER BY id",
            );
            pub(crate) const CASE_HINTED_PAYLOAD: SqlCase = SqlCase::new(
                "scan.projection.case_hinted_payload",
                "SELECT id, CASE WHEN payload(payload:rank, 'Integer') >= 20 THEN \
                 payload(payload:rank, 'Integer') ELSE 0 END AS rank_bucket FROM vectors ORDER BY \
                 id",
            );
            pub(crate) const COALESCE_HINTED_PAYLOAD: SqlCase = SqlCase::new(
                "scan.projection.coalesce_hinted_payload",
                "SELECT id, COALESCE(payload(payload:rank, 'Integer'), 0) AS rank_value FROM \
                 vectors ORDER BY id",
            );
            pub(crate) const RAW_PAYLOAD_CASE: SqlCase = SqlCase::new(
                "scan.projection.raw_payload_case",
                "SELECT CASE WHEN payload:rank >= 20 THEN payload:rank ELSE 0 END AS rank_bucket \
                 FROM vectors ORDER BY id",
            );
            pub(crate) const UNION_ALL_HINTED_PAYLOAD: SqlCase = SqlCase::new(
                "scan.projection.union_all_hinted_payload",
                "SELECT next_rank FROM (SELECT payload(payload:rank, 'Integer') + 1 AS next_rank \
                 FROM vectors UNION ALL SELECT payload(payload:rank, 'Integer') + 1 AS next_rank \
                 FROM vectors) ranked ORDER BY next_rank LIMIT 4",
            );
            pub(crate) const UNION_DISTINCT_HINTED_PAYLOAD: SqlCase = SqlCase::new(
                "scan.projection.union_distinct_hinted_payload",
                "SELECT rank FROM (SELECT payload(payload:rank, 'Integer') AS rank FROM vectors \
                 UNION SELECT payload(payload:rank, 'Integer') AS rank FROM vectors) ranked ORDER \
                 BY rank",
            );
            pub(crate) const INTERSECT_HINTED_PAYLOAD: SqlCase = SqlCase::new(
                "scan.projection.intersect_hinted_payload",
                "SELECT rank FROM (SELECT payload(payload:rank, 'Integer') AS rank FROM vectors \
                 INTERSECT SELECT payload(payload:rank, 'Integer') AS rank FROM vectors WHERE id \
                 <> '2') ranked ORDER BY rank",
            );
            pub(crate) const EXCEPT_HINTED_PAYLOAD: SqlCase = SqlCase::new(
                "scan.projection.except_hinted_payload",
                "SELECT rank FROM (SELECT payload(payload:rank, 'Integer') AS rank FROM vectors \
                 EXCEPT SELECT payload(payload:rank, 'Integer') AS rank FROM vectors WHERE id = \
                 '2') ranked ORDER BY rank",
            );
            pub(crate) const WINDOW_OVER_TYPED_SUBQUERY: SqlCase = SqlCase::new(
                "scan.projection.window_over_typed_subquery",
                "SELECT id, AVG(rank) OVER () AS avg_rank FROM (SELECT id, payload(payload:rank, \
                 'Integer') AS rank FROM vectors) ranked ORDER BY id",
            );
            pub(crate) const SCALAR_SUBQUERY_TYPED: SqlCase = SqlCase::new(
                "scan.projection.scalar_subquery_typed",
                "SELECT id, (SELECT MAX(payload(payload:rank, 'Integer')) FROM vectors) AS \
                 max_rank FROM vectors ORDER BY id",
            );
            pub(crate) const SELF_JOIN_TYPED: SqlCase = SqlCase::new(
                "scan.projection.self_join_typed",
                "SELECT lhs.id, rhs.id AS rhs_id FROM (SELECT id, payload(payload:rank, \
                 'Integer') AS rank FROM vectors) lhs JOIN (SELECT id, payload(payload:rank, \
                 'Integer') AS rank FROM vectors) rhs ON lhs.rank = rhs.rank ORDER BY lhs.id, \
                 rhs_id",
            );
            pub(crate) const LEFT_JOIN_TYPED: SqlCase = SqlCase::new(
                "scan.projection.left_join_typed",
                "SELECT lhs.id, COALESCE(rhs.rank, -1) AS rhs_rank FROM (SELECT id, \
                 payload(payload:rank, 'Integer') AS rank FROM vectors) lhs LEFT JOIN (SELECT id, \
                 payload(payload:rank, 'Integer') AS rank FROM vectors) rhs ON lhs.id = rhs.id \
                 ORDER BY lhs.id",
            );
            pub(crate) const INNER_JOIN_USING: SqlCase = SqlCase::new(
                "scan.projection.inner_join_using",
                "SELECT id FROM (SELECT id, payload(payload:rank, 'Integer') AS rank FROM \
                 vectors) lhs JOIN (SELECT id, payload(payload:rank, 'Integer') AS rank FROM \
                 vectors) rhs USING (id) ORDER BY id",
            );
            pub(crate) const RIGHT_JOIN_TYPED: SqlCase = SqlCase::new(
                "scan.projection.right_join_typed",
                "SELECT rhs.id, COALESCE(lhs.rank, -1) AS lhs_rank FROM (SELECT id, \
                 payload(payload:rank, 'Integer') AS rank FROM vectors WHERE id <> '3') lhs RIGHT \
                 JOIN (SELECT id, payload(payload:rank, 'Integer') AS rank FROM vectors) rhs ON \
                 lhs.id = rhs.id ORDER BY rhs.id",
            );
            pub(crate) const FULL_JOIN_TYPED: SqlCase = SqlCase::new(
                "scan.projection.full_join_typed",
                "SELECT COALESCE(lhs.id, rhs.id) AS id FROM (SELECT id, payload(payload:rank, \
                 'Integer') AS rank FROM vectors WHERE id <> '3') lhs FULL OUTER JOIN (SELECT id, \
                 payload(payload:rank, 'Integer') AS rank FROM vectors WHERE id <> '1') rhs ON \
                 lhs.id = rhs.id ORDER BY id",
            );
            pub(crate) const CROSS_JOIN_TYPED: SqlCase = SqlCase::new(
                "scan.projection.cross_join_typed",
                "SELECT lhs.id, rhs.rank AS rhs_rank FROM (SELECT id FROM vectors WHERE id IN \
                 ('1', '2')) lhs CROSS JOIN (SELECT payload(payload:rank, 'Integer') AS rank FROM \
                 vectors WHERE id IN ('1', '2')) rhs ORDER BY lhs.id, rhs_rank",
            );
            pub(crate) const LEFT_SEMI_JOIN_TYPED: SqlCase = SqlCase::new(
                "scan.projection.left_semi_join_typed",
                "SELECT lhs.id FROM (SELECT id, payload(payload:rank, 'Integer') AS rank FROM \
                 vectors) lhs LEFT SEMI JOIN (SELECT id, payload(payload:rank, 'Integer') AS rank \
                 FROM vectors WHERE id <> '3') rhs ON lhs.id = rhs.id ORDER BY lhs.id",
            );
            pub(crate) const LEFT_ANTI_JOIN_TYPED: SqlCase = SqlCase::new(
                "scan.projection.left_anti_join_typed",
                "SELECT lhs.id FROM (SELECT id, payload(payload:rank, 'Integer') AS rank FROM \
                 vectors) lhs LEFT ANTI JOIN (SELECT id FROM vectors WHERE id = '2') rhs ON \
                 lhs.id = rhs.id ORDER BY lhs.id",
            );
            pub(crate) const RIGHT_SEMI_JOIN_TYPED: SqlCase = SqlCase::new(
                "scan.projection.right_semi_join_typed",
                "SELECT rhs.id FROM (SELECT id FROM vectors WHERE id <> '3') lhs RIGHT SEMI JOIN \
                 (SELECT id, payload(payload:rank, 'Integer') AS rank FROM vectors) rhs ON lhs.id \
                 = rhs.id ORDER BY rhs.id",
            );
            pub(crate) const RIGHT_ANTI_JOIN_TYPED: SqlCase = SqlCase::new(
                "scan.projection.right_anti_join_typed",
                "SELECT rhs.id FROM (SELECT id FROM vectors WHERE id = '9') lhs RIGHT ANTI JOIN \
                 (SELECT id, payload(payload:rank, 'Integer') AS rank FROM vectors) rhs ON lhs.id \
                 = rhs.id ORDER BY rhs.id",
            );
            pub(crate) const EMPTY_AND_COUNT_VALUES: SqlCase = SqlCase::new(
                "scan.projection.empty_and_count_values",
                concat!(
                    "SELECT id, payload_exists(payload:list) AS list_exists, \
                     payload_is_missing(payload:list) AS list_missing, ",
                    "payload_is_null(payload:list) AS list_null, payload_is_empty(payload:list) \
                     AS list_empty, ",
                    "payload_has_values(payload:list) AS list_has_values, ",
                    "payload_values_count(payload:list) AS list_count FROM vectors ORDER BY id"
                ),
            );
            pub(crate) const GEO_DISTANCE_VALUES: SqlCase = SqlCase::new(
                "scan.projection.geo_distance_values",
                concat!(
                    "SELECT id, CAST(payload_geo_distance(payload:location, 0.0, 0.0) AS BIGINT) \
                     AS distance ",
                    "FROM vectors ORDER BY id"
                ),
            );
            pub(crate) const INSERT_VERIFY: SqlCase = SqlCase::new(
                "scan.projection.insert_verify",
                "SELECT id, payload:rank AS rank FROM vectors ORDER BY id",
            );
            pub(crate) const NEAREST_PAYLOAD_PATH: SqlCase = SqlCase::new(
                "scan.projection.nearest_payload_path",
                concat!(
                    "SELECT id, payload:rank AS rank, qdrant_nearest_score(vector, 1.0, 0.0) AS \
                     score ",
                    "FROM vectors ORDER BY score DESC LIMIT 2"
                ),
            );
            pub(crate) const NEAREST_PAYLOAD_PATH_CAST: SqlCase = SqlCase::new(
                "scan.projection.nearest_payload_path_cast",
                concat!(
                    "SELECT id, CAST(payload:rank AS BIGINT) AS rank, \
                     qdrant_nearest_score(vector, 1.0, 0.0) AS score ",
                    "FROM vectors ORDER BY score DESC LIMIT 3"
                ),
            );
            pub(crate) const SUBQUERY: SqlCase = SqlCase::new(
                "scan.projection.subquery",
                "SELECT id, rank FROM (SELECT id, payload:rank AS rank FROM vectors) projected \
                 ORDER BY rank",
            );
            pub(crate) const CTE: SqlCase = SqlCase::new(
                "scan.projection.cte",
                "WITH projected AS (SELECT id, payload:rank AS rank FROM vectors) SELECT id, rank \
                 FROM projected ORDER BY rank",
            );
            pub(crate) const UNNEST: SqlCase = SqlCase::new(
                "scan.projection.unnest",
                "SELECT id, item FROM vectors CROSS JOIN UNNEST([1, 2]) AS t(item) ORDER BY id, \
                 item",
            );
            pub(crate) const WINDOW: SqlCase = SqlCase::new(
                "scan.projection.window",
                "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS row_num FROM vectors ORDER BY id",
            );

            pub(crate) const ALL: &[SqlCase] = &[
                DOCS_FULL,
                DOCS_TEXT_NONNULL,
                DOCS_MULTI_NONNULL,
                DOCS_KEYWORDS_NONNULL,
                VECTORS_ALL_IDS,
                UNNAMED_FULL,
                TYPED_PAYLOAD_FIELDS,
                HINTED_PAYLOAD_ARITHMETIC,
                RAW_PAYLOAD_ARITHMETIC,
                SUBQUERY_RAW_PAYLOAD_ARITHMETIC,
                CTE_RAW_PAYLOAD_ARITHMETIC,
                UNION_ALL_RAW_PAYLOAD_ARITHMETIC,
                HINTED_PAYLOAD_FUNCTION,
                RAW_PAYLOAD_FUNCTION,
                RAW_PAYLOAD_DIVISION,
                CASE_HINTED_PAYLOAD,
                COALESCE_HINTED_PAYLOAD,
                RAW_PAYLOAD_CASE,
                UNION_ALL_HINTED_PAYLOAD,
                UNION_DISTINCT_HINTED_PAYLOAD,
                INTERSECT_HINTED_PAYLOAD,
                EXCEPT_HINTED_PAYLOAD,
                WINDOW_OVER_TYPED_SUBQUERY,
                SCALAR_SUBQUERY_TYPED,
                SELF_JOIN_TYPED,
                LEFT_JOIN_TYPED,
                INNER_JOIN_USING,
                RIGHT_JOIN_TYPED,
                FULL_JOIN_TYPED,
                CROSS_JOIN_TYPED,
                LEFT_SEMI_JOIN_TYPED,
                LEFT_ANTI_JOIN_TYPED,
                RIGHT_SEMI_JOIN_TYPED,
                RIGHT_ANTI_JOIN_TYPED,
                EMPTY_AND_COUNT_VALUES,
                GEO_DISTANCE_VALUES,
                INSERT_VERIFY,
                NEAREST_PAYLOAD_PATH,
                NEAREST_PAYLOAD_PATH_CAST,
                SUBQUERY,
                CTE,
                UNNEST,
                WINDOW,
            ];
        }

        pub(crate) mod ordering {
            use super::SqlCase;

            pub(crate) const PAYLOAD: SqlCase = SqlCase::new(
                "scan.ordering.payload",
                "SELECT id FROM vectors ORDER BY payload:rank",
            );
            pub(crate) const ALIASED_PAYLOAD: SqlCase = SqlCase::new(
                "scan.ordering.aliased_payload",
                "SELECT id, payload:rank AS rank FROM vectors ORDER BY rank",
            );
            pub(crate) const CAST_PAYLOAD_DESC: SqlCase = SqlCase::new(
                "scan.ordering.cast_payload_desc",
                "SELECT id FROM vectors ORDER BY CAST(payload:rank AS DOUBLE) DESC",
            );
            pub(crate) const ORDER_BY_SCORE_CAST: SqlCase = SqlCase::new(
                "scan.ordering.order_by_score_cast",
                concat!(
                    "SELECT id, qdrant_order_by_score(CAST(payload:rank AS DOUBLE), true) AS \
                     score ",
                    "FROM vectors ORDER BY score DESC"
                ),
            );
            pub(crate) const HINTED_PAYLOAD_ARITHMETIC_DESC: SqlCase = SqlCase::new(
                "scan.ordering.hinted_payload_arithmetic_desc",
                "SELECT id FROM vectors ORDER BY payload(payload:rank, 'Integer') + 1 DESC, id \
                 DESC",
            );
            pub(crate) const RAW_PAYLOAD_ARITHMETIC: SqlCase = SqlCase::new(
                "scan.ordering.raw_payload_arithmetic",
                "SELECT id FROM vectors ORDER BY payload:rank + 1",
            );
            pub(crate) const RAW_PAYLOAD_DIVISION: SqlCase = SqlCase::new(
                "scan.ordering.raw_payload_division",
                "SELECT id FROM vectors ORDER BY payload:rank / 2, id",
            );
            pub(crate) const HINTED_PAYLOAD_FUNCTION: SqlCase = SqlCase::new(
                "scan.ordering.hinted_payload_function",
                "SELECT id FROM vectors ORDER BY ABS(payload(payload:rank, 'Integer') - 15), id",
            );
            pub(crate) const CASE_HINTED_PAYLOAD: SqlCase = SqlCase::new(
                "scan.ordering.case_hinted_payload",
                "SELECT id FROM vectors ORDER BY CASE WHEN payload(payload:rank, 'Integer') >= 20 \
                 THEN 0 ELSE 1 END, id",
            );
            pub(crate) const ORDINAL_TYPED: SqlCase = SqlCase::new(
                "scan.ordering.ordinal_typed",
                "SELECT id, payload(payload:rank, 'Integer') AS rank FROM vectors ORDER BY 2 \
                 DESC, 1",
            );
            pub(crate) const NULLS_LAST_TYPED: SqlCase = SqlCase::new(
                "scan.ordering.nulls_last_typed",
                "SELECT id, payload(payload:rank, 'Integer') AS rank FROM vectors ORDER BY rank \
                 DESC NULLS LAST, id",
            );
            pub(crate) const NULLS_FIRST_TYPED: SqlCase = SqlCase::new(
                "scan.ordering.nulls_first_typed",
                "SELECT id, payload(payload:rank, 'Integer') AS rank FROM vectors ORDER BY rank \
                 ASC NULLS FIRST, id",
            );
            pub(crate) const MULTI_KEY_LOCAL: SqlCase = SqlCase::new(
                "scan.ordering.multi_key_local",
                "SELECT id FROM vectors ORDER BY ABS(payload(payload:rank, 'Integer') - 20), \
                 payload(payload:tag, 'Utf8'), id",
            );
            pub(crate) const MIXED_LOCAL_RAW_PAYLOAD_SECONDARY: SqlCase = SqlCase::new(
                "scan.ordering.mixed_local_raw_payload_secondary",
                "SELECT id FROM vectors ORDER BY ABS(payload(payload:rank, 'Integer') - 20), \
                 payload:tag, id",
            );
            pub(crate) const SUBQUERY: SqlCase = SqlCase::new(
                "scan.ordering.subquery",
                "SELECT id FROM (SELECT id, payload:rank AS rank FROM vectors) ordered ORDER BY \
                 rank",
            );
            pub(crate) const SUBQUERY_RAW_PAYLOAD_ARITHMETIC: SqlCase = SqlCase::new(
                "scan.ordering.subquery_raw_payload_arithmetic",
                "SELECT id FROM (SELECT id, payload:rank + 1 AS sort_key FROM vectors) ranked \
                 ORDER BY sort_key",
            );
            pub(crate) const CTE_DESC: SqlCase = SqlCase::new(
                "scan.ordering.cte_desc",
                "WITH ordered AS (SELECT id, payload:rank AS rank FROM vectors) SELECT id FROM \
                 ordered ORDER BY rank DESC",
            );
            pub(crate) const CTE_RAW_PAYLOAD_ARITHMETIC: SqlCase = SqlCase::new(
                "scan.ordering.cte_raw_payload_arithmetic",
                "WITH ranked AS (SELECT id, payload:rank + 1 AS sort_key FROM vectors) SELECT id \
                 FROM ranked ORDER BY sort_key",
            );
            pub(crate) const WINDOW_RAW_PAYLOAD_ARITHMETIC: SqlCase = SqlCase::new(
                "scan.ordering.window_raw_payload_arithmetic",
                "SELECT id, ROW_NUMBER() OVER (ORDER BY payload:rank + 1) AS row_num FROM vectors \
                 ORDER BY id",
            );

            pub(crate) const ALL: &[SqlCase] = &[
                PAYLOAD,
                ALIASED_PAYLOAD,
                CAST_PAYLOAD_DESC,
                ORDER_BY_SCORE_CAST,
                HINTED_PAYLOAD_ARITHMETIC_DESC,
                RAW_PAYLOAD_ARITHMETIC,
                RAW_PAYLOAD_DIVISION,
                HINTED_PAYLOAD_FUNCTION,
                CASE_HINTED_PAYLOAD,
                ORDINAL_TYPED,
                NULLS_LAST_TYPED,
                NULLS_FIRST_TYPED,
                MULTI_KEY_LOCAL,
                MIXED_LOCAL_RAW_PAYLOAD_SECONDARY,
                SUBQUERY,
                SUBQUERY_RAW_PAYLOAD_ARITHMETIC,
                CTE_DESC,
                CTE_RAW_PAYLOAD_ARITHMETIC,
                WINDOW_RAW_PAYLOAD_ARITHMETIC,
            ];
        }

        pub(crate) mod filters {
            use super::SqlCase;

            pub(crate) const RAW_PAYLOAD_ARITHMETIC: SqlCase = SqlCase::new(
                "scan.filters.raw_payload_arithmetic",
                "SELECT id FROM vectors WHERE payload:rank + 5 > 20 ORDER BY id",
            );
            pub(crate) const SUBQUERY_RAW_PAYLOAD_ARITHMETIC: SqlCase = SqlCase::new(
                "scan.filters.subquery_raw_payload_arithmetic",
                "SELECT id FROM (SELECT id FROM vectors WHERE payload:rank + 5 > 20) filtered \
                 ORDER BY id",
            );
            pub(crate) const CTE_RAW_PAYLOAD_ARITHMETIC: SqlCase = SqlCase::new(
                "scan.filters.cte_raw_payload_arithmetic",
                "WITH filtered AS (SELECT id FROM vectors WHERE payload:rank + 5 > 20) SELECT id \
                 FROM filtered ORDER BY id",
            );
            pub(crate) const UNION_ALL_RAW_PAYLOAD_ARITHMETIC: SqlCase = SqlCase::new(
                "scan.filters.union_all_raw_payload_arithmetic",
                "SELECT id FROM (SELECT id FROM vectors WHERE payload:rank + 5 > 20 UNION ALL \
                 SELECT id FROM vectors WHERE payload:rank + 5 > 20) filtered ORDER BY id",
            );
            pub(crate) const RAW_PAYLOAD_FUNCTION: SqlCase = SqlCase::new(
                "scan.filters.raw_payload_function",
                "SELECT id FROM vectors WHERE ABS(payload:rank) > 10 ORDER BY id",
            );
            pub(crate) const RAW_PAYLOAD_DIVISION: SqlCase = SqlCase::new(
                "scan.filters.raw_payload_division",
                "SELECT id FROM vectors WHERE payload:rank / 2 > 10 ORDER BY id",
            );
            pub(crate) const PAYLOAD_HINTED_GTE: SqlCase = SqlCase::new(
                "scan.filters.payload_hinted_gte",
                "SELECT id FROM vectors WHERE payload(payload:rank, 'Integer') >= 15 ORDER BY id",
            );
            pub(crate) const PAYLOAD_CAST_GTE: SqlCase = SqlCase::new(
                "scan.filters.payload_cast_gte",
                "SELECT id FROM vectors WHERE CAST(payload:rank AS BIGINT) >= 15 ORDER BY \
                 CAST(payload:rank AS BIGINT)",
            );
            pub(crate) const HINTED_PAYLOAD_ARITHMETIC: SqlCase = SqlCase::new(
                "scan.filters.hinted_payload_arithmetic",
                "SELECT id FROM vectors WHERE payload(payload:rank, 'Integer') + 5 > 20 ORDER BY \
                 id",
            );
            pub(crate) const HINTED_PAYLOAD_FUNCTION: SqlCase = SqlCase::new(
                "scan.filters.hinted_payload_function",
                "SELECT id FROM vectors WHERE ABS(payload(payload:rank, 'Integer') - 20) < 10 \
                 ORDER BY id",
            );
            pub(crate) const CASE_HINTED_PAYLOAD: SqlCase = SqlCase::new(
                "scan.filters.case_hinted_payload",
                "SELECT id FROM vectors WHERE CASE WHEN payload(payload:rank, 'Integer') >= 20 \
                 THEN true ELSE false END ORDER BY id",
            );
            pub(crate) const IN_SUBQUERY: SqlCase = SqlCase::new(
                "scan.filters.in_subquery",
                "SELECT id FROM vectors WHERE id IN (SELECT id FROM vectors WHERE payload:rank >= \
                 20) ORDER BY id",
            );
            pub(crate) const EXISTS_CORRELATED: SqlCase = SqlCase::new(
                "scan.filters.exists_correlated",
                "SELECT outer_v.id FROM vectors outer_v WHERE EXISTS (SELECT 1 FROM vectors \
                 inner_v WHERE inner_v.id = outer_v.id AND inner_v.payload:rank >= 20) ORDER BY \
                 outer_v.id",
            );
            pub(crate) const SCALAR_SUBQUERY: SqlCase = SqlCase::new(
                "scan.filters.scalar_subquery",
                "SELECT id FROM vectors WHERE payload(payload:rank, 'Integer') >= (SELECT \
                 AVG(payload(payload:rank, 'Integer')) FROM vectors) ORDER BY id",
            );
            pub(crate) const PAYLOAD_CAST_BETWEEN: SqlCase = SqlCase::new(
                "scan.filters.payload_cast_between",
                "SELECT id FROM vectors WHERE CAST(payload:rank AS BIGINT) BETWEEN 10 AND 20 \
                 ORDER BY id",
            );
            pub(crate) const TEXT_MATCH_WRAPPED: SqlCase = SqlCase::new(
                "scan.filters.text_match_wrapped",
                "SELECT id FROM vectors WHERE COALESCE(payload_text_match(payload:description, \
                 'good cheap'), false) ORDER BY id",
            );
            pub(crate) const TEXT_MATCH_CASE: SqlCase = SqlCase::new(
                "scan.filters.text_match_case",
                "SELECT id FROM vectors WHERE CASE WHEN payload_text_match(payload:description, \
                 'good cheap') THEN true ELSE false END ORDER BY id",
            );
            pub(crate) const PHRASE_MATCH_WRAPPED: SqlCase = SqlCase::new(
                "scan.filters.phrase_match_wrapped",
                "SELECT id FROM vectors WHERE NOT payload_phrase_match(payload:description, 'time \
                 is a flat circle') ORDER BY id",
            );
            pub(crate) const EMPTY: SqlCase = SqlCase::new(
                "scan.filters.empty",
                "SELECT id FROM vectors WHERE payload_is_empty(payload:list) ORDER BY id",
            );
            pub(crate) const EXISTS: SqlCase = SqlCase::new(
                "scan.filters.exists",
                "SELECT id FROM vectors WHERE payload_exists(payload:list) ORDER BY id",
            );
            pub(crate) const NOT_EXISTS: SqlCase = SqlCase::new(
                "scan.filters.not_exists",
                "SELECT id FROM vectors WHERE NOT payload_exists(payload:list) ORDER BY id",
            );
            pub(crate) const IS_MISSING: SqlCase = SqlCase::new(
                "scan.filters.is_missing",
                "SELECT id FROM vectors WHERE payload_is_missing(payload:list) ORDER BY id",
            );
            pub(crate) const IS_NULL_EXPLICIT: SqlCase = SqlCase::new(
                "scan.filters.is_null_explicit",
                "SELECT id FROM vectors WHERE payload_is_null(payload:list) ORDER BY id",
            );
            pub(crate) const VALUES_COUNT_ZERO: SqlCase = SqlCase::new(
                "scan.filters.values_count_zero",
                "SELECT id FROM vectors WHERE payload_values_count(payload:list) = 0 ORDER BY id",
            );
            pub(crate) const VALUES_COUNT_GE_ONE: SqlCase = SqlCase::new(
                "scan.filters.values_count_ge_one",
                "SELECT id FROM vectors WHERE payload_values_count(payload:list) >= 1 ORDER BY id",
            );
            pub(crate) const HAS_VALUES: SqlCase = SqlCase::new(
                "scan.filters.has_values",
                "SELECT id FROM vectors WHERE payload_has_values(payload:list) ORDER BY id",
            );
            pub(crate) const VALUES_COUNT_NOT_ZERO: SqlCase = SqlCase::new(
                "scan.filters.values_count_not_zero",
                "SELECT id FROM vectors WHERE payload_values_count(payload:list) != 0 ORDER BY id",
            );
            pub(crate) const VALUES_COUNT_LT_ONE: SqlCase = SqlCase::new(
                "scan.filters.values_count_lt_one",
                "SELECT id FROM vectors WHERE payload_values_count(payload:list) < 1 ORDER BY id",
            );
            pub(crate) const VALUES_COUNT_GT_ONE: SqlCase = SqlCase::new(
                "scan.filters.values_count_gt_one",
                "SELECT id FROM vectors WHERE payload_values_count(payload:list) > 1 ORDER BY id",
            );
            pub(crate) const VALUES_COUNT_BETWEEN: SqlCase = SqlCase::new(
                "scan.filters.values_count_between",
                "SELECT id FROM vectors WHERE payload_values_count(payload:list) BETWEEN 0 AND 1 \
                 ORDER BY id",
            );
            pub(crate) const VALUES_COUNT_NOT_BETWEEN: SqlCase = SqlCase::new(
                "scan.filters.values_count_not_between",
                "SELECT id FROM vectors WHERE payload_values_count(payload:list) NOT BETWEEN 0 \
                 AND 1 ORDER BY id",
            );
            pub(crate) const GEO_RADIUS: SqlCase = SqlCase::new(
                "scan.filters.geo_radius",
                "SELECT id FROM vectors WHERE payload_geo_distance(payload:location, 0.0, 0.0) <= \
                 12000.0 ORDER BY id",
            );
            pub(crate) const GEO_RESIDUAL: SqlCase = SqlCase::new(
                "scan.filters.geo_residual",
                "SELECT id FROM vectors WHERE payload_geo_distance(payload:location, 0.0, 0.0) > \
                 12000.0 ORDER BY id",
            );
            pub(crate) const GEO_BBOX: SqlCase = SqlCase::new(
                "scan.filters.geo_bbox",
                "SELECT id FROM vectors WHERE payload_geo_within_bbox(payload:location, -1.0, \
                 -1.0, 1.0, 1.5) ORDER BY id",
            );
            pub(crate) const GEO_POLYGON: SqlCase = SqlCase::new(
                "scan.filters.geo_polygon",
                "SELECT id FROM vectors WHERE payload_geo_within_polygon(payload:location, \
                 [[-1.0, -1.0], [1.0, -1.0], [1.0, 1.5], [-1.0, 1.5]]) ORDER BY id",
            );
            pub(crate) const TEXT_MATCH: SqlCase = SqlCase::new(
                "scan.filters.text_match",
                "SELECT id FROM vectors WHERE payload_text_match(payload:description, 'good \
                 cheap') ORDER BY id",
            );
            pub(crate) const PHRASE_MATCH: SqlCase = SqlCase::new(
                "scan.filters.phrase_match",
                "SELECT id FROM vectors WHERE payload_phrase_match(payload:description, 'time is \
                 a flat circle') ORDER BY id",
            );
            pub(crate) const TEXT_ANY: SqlCase = SqlCase::new(
                "scan.filters.text_any",
                "SELECT id FROM vectors WHERE payload_text_any(payload:description, ['good', \
                 'cheap']) ORDER BY id",
            );
            pub(crate) const NESTED_MATCH: SqlCase = SqlCase::new(
                "scan.filters.nested_match",
                "SELECT id FROM vectors WHERE payload_nested_match(payload:metadata, payload:rank \
                 >= 20 AND payload:tag = 'red') ORDER BY id",
            );
            pub(crate) const IDS_IN: SqlCase = SqlCase::new(
                "scan.filters.ids_in",
                "SELECT id FROM docs WHERE id IN ('1', '3') ORDER BY id",
            );
            pub(crate) const VECTOR_IS_NULL: SqlCase = SqlCase::new(
                "scan.filters.vector_is_null",
                "SELECT id FROM docs WHERE text_embedding IS NULL ORDER BY id",
            );
            pub(crate) const VECTOR_NONNULL_AND_MULTI_NULL: SqlCase = SqlCase::new(
                "scan.filters.vector_nonnull_and_multi_null",
                "SELECT id FROM docs WHERE text_embedding IS NOT NULL AND multi_embedding IS NULL \
                 ORDER BY id",
            );
            pub(crate) const RANK_GTE: SqlCase = SqlCase::new(
                "scan.filters.rank_gte",
                "SELECT id FROM vectors WHERE payload:rank >= 20 ORDER BY id",
            );
            pub(crate) const SCORE_IN: SqlCase = SqlCase::new(
                "scan.filters.score_in",
                "SELECT id FROM vectors WHERE payload:score IN (1.5, 3.5) ORDER BY id",
            );
            pub(crate) const TAG_NOT_IN: SqlCase = SqlCase::new(
                "scan.filters.tag_not_in",
                "SELECT id FROM vectors WHERE payload:tag NOT IN ('red') ORDER BY id",
            );
            pub(crate) const TAG_OR: SqlCase = SqlCase::new(
                "scan.filters.tag_or",
                "SELECT id FROM vectors WHERE payload:tag = 'red' OR payload:tag = 'blue' ORDER \
                 BY id",
            );
            pub(crate) const TAG_OR_ID_AND_NOT_RANK: SqlCase = SqlCase::new(
                "scan.filters.tag_or_id_and_not_rank",
                concat!(
                    "SELECT id FROM vectors WHERE (payload:tag = 'red' OR id = '2') ",
                    "AND NOT payload:rank > 20 ORDER BY id"
                ),
            );
            pub(crate) const REMARK_IS_NULL: SqlCase = SqlCase::new(
                "scan.filters.remark_is_null",
                "SELECT id FROM vectors WHERE payload:remark IS NULL ORDER BY id",
            );
            pub(crate) const REMARK_IS_NOT_NULL: SqlCase = SqlCase::new(
                "scan.filters.remark_is_not_null",
                "SELECT id FROM vectors WHERE payload:remark IS NOT NULL ORDER BY id",
            );
            pub(crate) const EMPTY_STRING_EQ: SqlCase = SqlCase::new(
                "scan.filters.empty_string_eq",
                "SELECT id FROM vectors WHERE payload:tag = '' ORDER BY id",
            );
            pub(crate) const EMPTY_STRING_IS_NULL: SqlCase = SqlCase::new(
                "scan.filters.empty_string_is_null",
                "SELECT id FROM vectors WHERE payload:tag IS NULL ORDER BY id",
            );
            pub(crate) const EMPTY_STRING_IS_NOT_NULL: SqlCase = SqlCase::new(
                "scan.filters.empty_string_is_not_null",
                "SELECT id FROM vectors WHERE payload:tag IS NOT NULL ORDER BY id",
            );
            pub(crate) const SUBQUERY: SqlCase = SqlCase::new(
                "scan.filters.subquery",
                "SELECT id FROM (SELECT id FROM vectors WHERE payload:rank >= 20) filtered ORDER \
                 BY id",
            );
            pub(crate) const CTE: SqlCase = SqlCase::new(
                "scan.filters.cte",
                "WITH filtered AS (SELECT id FROM vectors WHERE payload:rank >= 20) SELECT id \
                 FROM filtered ORDER BY id",
            );
            pub(crate) const TEXT_ANY_SUBQUERY: SqlCase = SqlCase::new(
                "scan.filters.text_any_subquery",
                "SELECT id FROM (SELECT id FROM vectors WHERE \
                 payload_text_any(payload:description, ['good', 'cheap'])) filtered ORDER BY id",
            );
            pub(crate) const EXISTS_SUBQUERY: SqlCase = SqlCase::new(
                "scan.filters.exists_subquery",
                "SELECT id FROM (SELECT id FROM vectors WHERE payload_exists(payload:list)) \
                 filtered ORDER BY id",
            );
            pub(crate) const IS_MISSING_SUBQUERY: SqlCase = SqlCase::new(
                "scan.filters.is_missing_subquery",
                "SELECT id FROM (SELECT id FROM vectors WHERE payload_is_missing(payload:list)) \
                 filtered ORDER BY id",
            );
            pub(crate) const VALUES_COUNT_BETWEEN_SUBQUERY: SqlCase = SqlCase::new(
                "scan.filters.values_count_between_subquery",
                "SELECT id FROM (SELECT id FROM vectors WHERE payload_values_count(payload:list) \
                 BETWEEN 0 AND 1) filtered ORDER BY id",
            );
            pub(crate) const GEO_BBOX_SUBQUERY: SqlCase = SqlCase::new(
                "scan.filters.geo_bbox_subquery",
                "SELECT id FROM (SELECT id FROM vectors WHERE \
                 payload_geo_within_bbox(payload:location, -1.0, -1.0, 1.0, 1.5)) filtered ORDER \
                 BY id",
            );
            pub(crate) const GEO_POLYGON_SUBQUERY: SqlCase = SqlCase::new(
                "scan.filters.geo_polygon_subquery",
                "SELECT id FROM (SELECT id FROM vectors WHERE \
                 payload_geo_within_polygon(payload:location, [[-1.0, -1.0], [1.0, -1.0], [1.0, \
                 1.5], [-1.0, 1.5]])) filtered ORDER BY id",
            );
            pub(crate) const NESTED_MATCH_SUBQUERY: SqlCase = SqlCase::new(
                "scan.filters.nested_match_subquery",
                "SELECT id FROM (SELECT id FROM vectors WHERE \
                 payload_nested_match(payload:metadata, payload:rank >= 20 AND payload:tag = \
                 'red')) filtered ORDER BY id",
            );

            pub(crate) const ALL: &[SqlCase] = &[
                RAW_PAYLOAD_ARITHMETIC,
                SUBQUERY_RAW_PAYLOAD_ARITHMETIC,
                CTE_RAW_PAYLOAD_ARITHMETIC,
                UNION_ALL_RAW_PAYLOAD_ARITHMETIC,
                RAW_PAYLOAD_FUNCTION,
                RAW_PAYLOAD_DIVISION,
                PAYLOAD_HINTED_GTE,
                PAYLOAD_CAST_GTE,
                HINTED_PAYLOAD_ARITHMETIC,
                HINTED_PAYLOAD_FUNCTION,
                CASE_HINTED_PAYLOAD,
                IN_SUBQUERY,
                EXISTS_CORRELATED,
                SCALAR_SUBQUERY,
                PAYLOAD_CAST_BETWEEN,
                TEXT_MATCH_WRAPPED,
                TEXT_MATCH_CASE,
                PHRASE_MATCH_WRAPPED,
                EMPTY,
                EXISTS,
                NOT_EXISTS,
                IS_MISSING,
                IS_NULL_EXPLICIT,
                VALUES_COUNT_ZERO,
                VALUES_COUNT_GE_ONE,
                HAS_VALUES,
                VALUES_COUNT_NOT_ZERO,
                VALUES_COUNT_LT_ONE,
                VALUES_COUNT_GT_ONE,
                VALUES_COUNT_BETWEEN,
                VALUES_COUNT_NOT_BETWEEN,
                GEO_RADIUS,
                GEO_RESIDUAL,
                GEO_BBOX,
                GEO_POLYGON,
                TEXT_MATCH,
                PHRASE_MATCH,
                TEXT_ANY,
                NESTED_MATCH,
                IDS_IN,
                VECTOR_IS_NULL,
                VECTOR_NONNULL_AND_MULTI_NULL,
                RANK_GTE,
                SCORE_IN,
                TAG_NOT_IN,
                TAG_OR,
                TAG_OR_ID_AND_NOT_RANK,
                REMARK_IS_NULL,
                REMARK_IS_NOT_NULL,
                EMPTY_STRING_EQ,
                EMPTY_STRING_IS_NULL,
                EMPTY_STRING_IS_NOT_NULL,
                SUBQUERY,
                CTE,
                TEXT_ANY_SUBQUERY,
                EXISTS_SUBQUERY,
                IS_MISSING_SUBQUERY,
                VALUES_COUNT_BETWEEN_SUBQUERY,
                GEO_BBOX_SUBQUERY,
                GEO_POLYGON_SUBQUERY,
                NESTED_MATCH_SUBQUERY,
            ];
        }

        pub(crate) mod aggregates {
            use super::SqlCase;

            pub(crate) const BOOL_FACET: SqlCase = SqlCase::new(
                "scan.aggregates.bool_facet",
                "SELECT payload:active AS active, COUNT(*) AS total FROM vectors GROUP BY \
                 payload:active ORDER BY total DESC LIMIT 2",
            );
            pub(crate) const INT_FACET: SqlCase = SqlCase::new(
                "scan.aggregates.int_facet",
                "SELECT payload:rank AS rank, COUNT(*) AS total FROM vectors GROUP BY \
                 payload:rank ORDER BY total DESC LIMIT 2",
            );
            pub(crate) const COUNT_RANK_GTE: SqlCase = SqlCase::new(
                "scan.aggregates.count_rank_gte",
                "SELECT COUNT(*) AS total FROM vectors WHERE payload:rank >= 20",
            );
            pub(crate) const AVG_HINTED_PAYLOAD: SqlCase = SqlCase::new(
                "scan.aggregates.avg_hinted_payload",
                "SELECT AVG(payload(payload:rank, 'Integer')) AS avg_rank FROM vectors",
            );
            pub(crate) const RAW_PAYLOAD_ARITHMETIC: SqlCase = SqlCase::new(
                "scan.aggregates.raw_payload_arithmetic",
                "SELECT SUM(payload:rank + 1) AS total FROM vectors",
            );
            pub(crate) const SUBQUERY_RAW_PAYLOAD_ARITHMETIC: SqlCase = SqlCase::new(
                "scan.aggregates.subquery_raw_payload_arithmetic",
                "SELECT AVG(next_rank) AS total FROM (SELECT payload:rank + 1 AS next_rank FROM \
                 vectors) ranked",
            );
            pub(crate) const CTE_RAW_PAYLOAD_ARITHMETIC: SqlCase = SqlCase::new(
                "scan.aggregates.cte_raw_payload_arithmetic",
                "WITH ranked AS (SELECT payload:rank + 1 AS next_rank FROM vectors) SELECT \
                 AVG(next_rank) AS total FROM ranked",
            );
            pub(crate) const SUM_HINTED_PAYLOAD_ARITHMETIC: SqlCase = SqlCase::new(
                "scan.aggregates.sum_hinted_payload_arithmetic",
                "SELECT SUM(payload(payload:rank, 'Integer') + 1) AS total FROM vectors",
            );
            pub(crate) const RAW_PAYLOAD_FUNCTION: SqlCase = SqlCase::new(
                "scan.aggregates.raw_payload_function",
                "SELECT MAX(ABS(payload:rank)) AS max_rank FROM vectors",
            );
            pub(crate) const RAW_PAYLOAD_DIVISION: SqlCase = SqlCase::new(
                "scan.aggregates.raw_payload_division",
                "SELECT AVG(payload:rank / 2) AS avg_rank FROM vectors",
            );
            pub(crate) const CASE_HINTED_PAYLOAD: SqlCase = SqlCase::new(
                "scan.aggregates.case_hinted_payload",
                "SELECT SUM(CASE WHEN payload(payload:rank, 'Integer') >= 20 THEN 1 ELSE 0 END) \
                 AS total FROM vectors",
            );
            pub(crate) const AVG_VALUES_COUNT: SqlCase = SqlCase::new(
                "scan.aggregates.avg_values_count",
                "SELECT AVG(list_count) AS avg_list_count FROM (SELECT \
                 payload_values_count(payload:list) AS list_count FROM vectors) counted",
            );
            pub(crate) const SUM_NULL_AND_MISSING_FLAGS: SqlCase = SqlCase::new(
                "scan.aggregates.sum_null_and_missing_flags",
                "SELECT SUM(CASE WHEN payload_is_null(payload:list) THEN 1 ELSE 0 END) AS \
                 null_total, SUM(CASE WHEN payload_is_missing(payload:list) THEN 1 ELSE 0 END) AS \
                 missing_total FROM vectors",
            );
            pub(crate) const HAVING_LOCAL_TYPED: SqlCase = SqlCase::new(
                "scan.aggregates.having_local_typed",
                "SELECT bucket, COUNT(*) AS total FROM (SELECT CASE WHEN payload(payload:rank, \
                 'Integer') >= 20 THEN 'high' ELSE 'low' END AS bucket FROM vectors) ranked GROUP \
                 BY bucket HAVING COUNT(*) >= 1 ORDER BY bucket",
            );
            pub(crate) const TAG_FACET_WITH_FILTER: SqlCase = SqlCase::new(
                "scan.aggregates.tag_facet_with_filter",
                "SELECT payload:tag AS tag, COUNT(*) AS total FROM vectors WHERE payload:rank >= \
                 10 GROUP BY payload:tag ORDER BY total DESC LIMIT 2",
            );
            pub(crate) const TAG_FACET_LIMIT_ONLY: SqlCase = SqlCase::new(
                "scan.aggregates.tag_facet_limit_only",
                "SELECT payload:tag AS tag, COUNT(*) AS total FROM vectors GROUP BY payload:tag \
                 LIMIT 2",
            );
            pub(crate) const TAG_GROUP_LOCAL: SqlCase = SqlCase::new(
                "scan.aggregates.tag_group_local",
                "SELECT payload:tag AS tag, COUNT(*) AS total FROM vectors GROUP BY payload:tag \
                 ORDER BY total DESC, tag",
            );
            pub(crate) const HAVING_FACET: SqlCase = SqlCase::new(
                "scan.aggregates.having_facet",
                "SELECT payload:tag AS tag, COUNT(*) AS total FROM vectors GROUP BY payload:tag \
                 HAVING COUNT(*) >= 1 ORDER BY total DESC, tag",
            );
            pub(crate) const WINDOW_OVER_FACET_SUBQUERY: SqlCase = SqlCase::new(
                "scan.aggregates.window_over_facet_subquery",
                "SELECT tag, total, ROW_NUMBER() OVER (ORDER BY total DESC, tag) AS row_num FROM \
                 (SELECT payload:tag AS tag, COUNT(*) AS total FROM vectors GROUP BY payload:tag) \
                 facet ORDER BY row_num",
            );
            pub(crate) const SUBQUERY: SqlCase = SqlCase::new(
                "scan.aggregates.subquery",
                "SELECT COUNT(*) AS total FROM (SELECT id FROM vectors WHERE payload:rank >= 20) \
                 filtered",
            );
            pub(crate) const CTE: SqlCase = SqlCase::new(
                "scan.aggregates.cte",
                "WITH base AS (SELECT payload:tag AS tag FROM vectors) SELECT tag, COUNT(*) AS \
                 total FROM base GROUP BY tag ORDER BY total DESC, tag LIMIT 2",
            );

            pub(crate) const ALL: &[SqlCase] = &[
                BOOL_FACET,
                INT_FACET,
                COUNT_RANK_GTE,
                AVG_HINTED_PAYLOAD,
                RAW_PAYLOAD_ARITHMETIC,
                SUBQUERY_RAW_PAYLOAD_ARITHMETIC,
                CTE_RAW_PAYLOAD_ARITHMETIC,
                SUM_HINTED_PAYLOAD_ARITHMETIC,
                RAW_PAYLOAD_FUNCTION,
                RAW_PAYLOAD_DIVISION,
                CASE_HINTED_PAYLOAD,
                AVG_VALUES_COUNT,
                SUM_NULL_AND_MISSING_FLAGS,
                HAVING_LOCAL_TYPED,
                TAG_FACET_WITH_FILTER,
                TAG_FACET_LIMIT_ONLY,
                TAG_GROUP_LOCAL,
                HAVING_FACET,
                WINDOW_OVER_FACET_SUBQUERY,
                SUBQUERY,
                CTE,
            ];
        }
    }

    pub(crate) mod writes {
        use super::SqlCase;

        pub(crate) mod append {
            use super::SqlCase;

            pub(crate) const INSERT_SELECT: SqlCase = SqlCase::new(
                "writes.append.insert_select",
                "INSERT INTO vectors SELECT id, payload, vector FROM staging",
            );
            pub(crate) const SUBQUERY: SqlCase = SqlCase::new(
                "writes.append.subquery",
                "INSERT INTO vectors SELECT * FROM (SELECT id, payload, vector FROM staging) \
                 staged",
            );
            pub(crate) const UNION_ALL: SqlCase = SqlCase::new(
                "writes.append.union_all",
                "INSERT INTO vectors SELECT * FROM staging UNION ALL SELECT * FROM staging",
            );
            pub(crate) const WINDOW_SUBQUERY: SqlCase = SqlCase::new(
                "writes.append.window_subquery",
                "INSERT INTO vectors SELECT id, payload, vector FROM (SELECT id, payload, vector, \
                 ROW_NUMBER() OVER (ORDER BY id) AS row_num FROM staging) staged",
            );
            pub(crate) const ORDERED_SUBQUERY: SqlCase = SqlCase::new(
                "writes.append.ordered_subquery",
                "INSERT INTO vectors SELECT id, payload, vector FROM (SELECT id, payload, vector \
                 FROM staging ORDER BY id) staged",
            );
            pub(crate) const JOIN: SqlCase = SqlCase::new(
                "writes.append.join",
                "INSERT INTO vectors SELECT s.id, s.payload, s.vector FROM staging s JOIN staging \
                 t ON s.id = t.id",
            );
            pub(crate) const JOIN_USING: SqlCase = SqlCase::new(
                "writes.append.join_using",
                "INSERT INTO vectors SELECT id, s.payload, s.vector FROM staging s JOIN staging t \
                 USING (id)",
            );
            pub(crate) const LEFT_JOIN: SqlCase = SqlCase::new(
                "writes.append.left_join",
                "INSERT INTO vectors SELECT s.id, s.payload, s.vector FROM staging s LEFT JOIN \
                 staging t ON s.id = t.id",
            );
            pub(crate) const RIGHT_JOIN: SqlCase = SqlCase::new(
                "writes.append.right_join",
                "INSERT INTO vectors SELECT t.id, t.payload, t.vector FROM staging s RIGHT JOIN \
                 staging t ON s.id = t.id",
            );
            pub(crate) const FULL_JOIN: SqlCase = SqlCase::new(
                "writes.append.full_join",
                "INSERT INTO vectors SELECT s.id, s.payload, s.vector FROM staging s FULL OUTER \
                 JOIN staging t ON s.id = t.id WHERE s.id IS NOT NULL",
            );
            pub(crate) const CROSS_JOIN: SqlCase = SqlCase::new(
                "writes.append.cross_join",
                "INSERT INTO vectors SELECT s.id, s.payload, s.vector FROM staging s CROSS JOIN \
                 staging t WHERE s.id = t.id",
            );
            pub(crate) const LEFT_SEMI_JOIN: SqlCase = SqlCase::new(
                "writes.append.left_semi_join",
                "INSERT INTO vectors SELECT s.id, s.payload, s.vector FROM staging s LEFT SEMI \
                 JOIN staging t ON s.id = t.id",
            );
            pub(crate) const LEFT_ANTI_JOIN: SqlCase = SqlCase::new(
                "writes.append.left_anti_join",
                "INSERT INTO vectors SELECT s.id, s.payload, s.vector FROM staging s LEFT ANTI \
                 JOIN (SELECT id FROM staging WHERE id = '9') t ON s.id = t.id",
            );
            pub(crate) const RIGHT_SEMI_JOIN: SqlCase = SqlCase::new(
                "writes.append.right_semi_join",
                "INSERT INTO vectors SELECT t.id, t.payload, t.vector FROM (SELECT id FROM \
                 staging WHERE id = '1') s RIGHT SEMI JOIN staging t ON s.id = t.id",
            );
            pub(crate) const RIGHT_ANTI_JOIN: SqlCase = SqlCase::new(
                "writes.append.right_anti_join",
                "INSERT INTO vectors SELECT t.id, t.payload, t.vector FROM (SELECT id FROM \
                 staging WHERE id = '9') s RIGHT ANTI JOIN staging t ON s.id = t.id",
            );
            pub(crate) const UNION_DISTINCT: SqlCase = SqlCase::new(
                "writes.append.union_distinct",
                "INSERT INTO vectors SELECT * FROM staging UNION SELECT * FROM staging",
            );
            pub(crate) const ALL: &[SqlCase] = &[
                INSERT_SELECT,
                SUBQUERY,
                UNION_ALL,
                WINDOW_SUBQUERY,
                ORDERED_SUBQUERY,
                JOIN,
                JOIN_USING,
                LEFT_JOIN,
                RIGHT_JOIN,
                FULL_JOIN,
                CROSS_JOIN,
                LEFT_SEMI_JOIN,
                LEFT_ANTI_JOIN,
                RIGHT_SEMI_JOIN,
                RIGHT_ANTI_JOIN,
                UNION_DISTINCT,
            ];
        }
    }

    pub(crate) mod query {
        use super::SqlCase;

        pub(crate) mod nearest {
            use super::SqlCase;

            pub(crate) const WITHOUT_LIMIT: SqlCase = SqlCase::new(
                "query.nearest.without_limit",
                "SELECT id, qdrant_nearest_score(vector, 1.0, 0.0) AS score FROM vectors",
            );
            pub(crate) const CANONICAL: SqlCase = SqlCase::new(
                "query.nearest.canonical",
                "SELECT id, qdrant_nearest_score(vector, 1.0, 0.0) AS score FROM vectors",
            );
            pub(crate) const LOCAL_PROJECTION: SqlCase = SqlCase::new(
                "query.nearest.local_projection",
                "SELECT id, qdrant_nearest_score(vector, 1.0, 0.0) + CAST(1.0 AS FLOAT) AS score \
                 FROM vectors",
            );
            pub(crate) const LOCAL_FILTER: SqlCase = SqlCase::new(
                "query.nearest.local_filter",
                "SELECT id FROM vectors WHERE qdrant_nearest_score(vector, 1.0, 0.0) <= 0.5",
            );
            pub(crate) const LOCAL_FILTER_AND_PROJECTION: SqlCase = SqlCase::new(
                "query.nearest.local_filter_and_projection",
                "SELECT id, qdrant_nearest_score(vector, 1.0, 0.0) AS score FROM vectors WHERE \
                 qdrant_nearest_score(vector, 1.0, 0.0) <= 0.5",
            );
            pub(crate) const ASC_LOCAL_SORT: SqlCase = SqlCase::new(
                "query.nearest.asc_local_sort",
                "SELECT id, qdrant_nearest_score(vector, 1.0, 0.0) AS score FROM vectors ORDER BY \
                 score ASC LIMIT 2",
            );
            pub(crate) const ABS_LOCAL_PROJECTION: SqlCase = SqlCase::new(
                "query.nearest.abs_local_projection",
                "SELECT id, ABS(qdrant_nearest_score(vector, 1.0, 0.0)) AS score FROM vectors",
            );
            pub(crate) const CASE_LOCAL_PROJECTION: SqlCase = SqlCase::new(
                "query.nearest.case_local_projection",
                "SELECT id, CASE WHEN qdrant_nearest_score(vector, 1.0, 0.0) >= 0.5 THEN 1 ELSE 0 \
                 END AS bucket FROM vectors ORDER BY id",
            );
            pub(crate) const WINDOW_DIRECT: SqlCase = SqlCase::new(
                "query.nearest.window_direct",
                "SELECT id, AVG(qdrant_nearest_score(vector, 1.0, 0.0)) OVER () AS avg_score FROM \
                 vectors",
            );
            pub(crate) const WINDOW_OVER_SUBQUERY: SqlCase = SqlCase::new(
                "query.nearest.window_over_subquery",
                "SELECT id, AVG(score) OVER () AS avg_score FROM (SELECT id, \
                 qdrant_nearest_score(vector, 1.0, 0.0) AS score FROM vectors) ranked ORDER BY id",
            );
            pub(crate) const AGGREGATE_SUBQUERY: SqlCase = SqlCase::new(
                "query.nearest.aggregate_subquery",
                "SELECT * FROM (SELECT AVG(qdrant_nearest_score(vector, 1.0, 0.0)) AS avg_score \
                 FROM vectors) ranked",
            );
            pub(crate) const AGGREGATE_CTE: SqlCase = SqlCase::new(
                "query.nearest.aggregate_cte",
                "WITH ranked AS (SELECT AVG(qdrant_nearest_score(vector, 1.0, 0.0)) AS avg_score \
                 FROM vectors) SELECT * FROM ranked",
            );
            pub(crate) const FILTERED_SUBQUERY_FUNCTION: SqlCase = SqlCase::new(
                "query.nearest.filtered_subquery_function",
                "SELECT id FROM (SELECT id, qdrant_nearest_score(vector, 1.0, 0.0) AS score FROM \
                 vectors) ranked WHERE ABS(score) <= 0.5 ORDER BY id",
            );
            pub(crate) const PAYLOAD_PROJECTION_SIMPLE: SqlCase = SqlCase::new(
                "query.nearest.payload_projection_simple",
                concat!(
                    "SELECT id, payload, vector, qdrant_nearest_score(vector, 1.0, 0.0) AS score ",
                    "FROM vectors WHERE id <> '3' AND qdrant_nearest_score(vector, 1.0, 0.0) >= \
                     0.3 ",
                    "ORDER BY score DESC LIMIT 3"
                ),
            );
            pub(crate) const PAYLOAD_PROJECTION: SqlCase = SqlCase::new(
                "query.nearest.payload_projection",
                concat!(
                    "SELECT id, payload, embedding, aux, qdrant_nearest_score(embedding, 1.0, \
                     0.0) AS score ",
                    "FROM vectors WHERE id <> '3' AND qdrant_nearest_score(embedding, 1.0, 0.0) \
                     >= 0.3 ",
                    "ORDER BY score DESC LIMIT 3"
                ),
            );
            pub(crate) const SUBQUERY: SqlCase = SqlCase::new(
                "query.nearest.subquery",
                "SELECT id, score FROM (SELECT id, qdrant_nearest_score(vector, 1.0, 0.0) AS \
                 score FROM vectors) ranked ORDER BY score DESC LIMIT 2",
            );
            pub(crate) const CTE: SqlCase = SqlCase::new(
                "query.nearest.cte",
                "WITH ranked AS (SELECT id, qdrant_nearest_score(vector, 1.0, 0.0) AS score FROM \
                 vectors) SELECT id, score FROM ranked LIMIT 2",
            );
            pub(crate) const WINDOW: SqlCase = SqlCase::new(
                "query.nearest.window",
                "SELECT id, score, ROW_NUMBER() OVER (ORDER BY score DESC) AS row_num FROM \
                 (SELECT id, qdrant_nearest_score(vector, 1.0, 0.0) AS score FROM vectors) ranked",
            );
            pub(crate) const UNION_ALL: SqlCase = SqlCase::new(
                "query.nearest.union_all",
                concat!(
                    "SELECT id, score FROM (SELECT id, qdrant_nearest_score(vector, 1.0, 0.0) AS \
                     score FROM vectors ORDER BY score DESC LIMIT 2) a ",
                    "UNION ALL SELECT id, score FROM (SELECT id, qdrant_nearest_score(vector, \
                     0.0, 1.0) AS score FROM vectors ORDER BY score DESC LIMIT 2) b"
                ),
            );
            pub(crate) const INNER_JOIN: SqlCase = SqlCase::new(
                "query.nearest.inner_join",
                concat!(
                    "SELECT lhs.id FROM (SELECT id, qdrant_nearest_score(vector, 1.0, 0.0) AS \
                     score FROM vectors ORDER BY score DESC LIMIT 2) lhs ",
                    "JOIN (SELECT id, qdrant_nearest_score(vector, 0.0, 1.0) AS score FROM \
                     vectors ORDER BY score DESC LIMIT 2) rhs ON lhs.id = rhs.id ORDER BY lhs.id"
                ),
            );
            pub(crate) const UNION_DISTINCT: SqlCase = SqlCase::new(
                "query.nearest.union_distinct",
                concat!(
                    "SELECT id, score FROM (SELECT id, qdrant_nearest_score(vector, 1.0, 0.0) AS \
                     score FROM vectors ORDER BY score DESC LIMIT 2) a ",
                    "UNION SELECT id, score FROM (SELECT id, qdrant_nearest_score(vector, 0.0, \
                     1.0) AS score FROM vectors ORDER BY score DESC LIMIT 2) b"
                ),
            );
            pub(crate) const INTERSECT: SqlCase = SqlCase::new(
                "query.nearest.intersect",
                concat!(
                    "SELECT id, score FROM (SELECT id, qdrant_nearest_score(vector, 1.0, 0.0) AS \
                     score FROM vectors ORDER BY score DESC LIMIT 2) a ",
                    "INTERSECT SELECT id, score FROM (SELECT id, qdrant_nearest_score(vector, \
                     1.0, 0.0) AS score FROM vectors WHERE id <> '3' ORDER BY score DESC LIMIT 2) \
                     b"
                ),
            );
            pub(crate) const INTERSECT_DIRECT: SqlCase = SqlCase::new(
                "query.nearest.intersect_direct",
                "SELECT qdrant_nearest_score(vector, 1.0, 0.0) AS score FROM vectors INTERSECT \
                 SELECT qdrant_nearest_score(vector, 0.0, 1.0) AS score FROM vectors",
            );
            pub(crate) const EXCEPT: SqlCase = SqlCase::new(
                "query.nearest.except",
                concat!(
                    "SELECT id, score FROM (SELECT id, qdrant_nearest_score(vector, 1.0, 0.0) AS \
                     score FROM vectors ORDER BY score DESC LIMIT 3) a ",
                    "EXCEPT SELECT id, score FROM (SELECT id, qdrant_nearest_score(vector, 1.0, \
                     0.0) AS score FROM vectors WHERE id = '3' ORDER BY score DESC LIMIT 1) b"
                ),
            );
            pub(crate) const IN_SUBQUERY: SqlCase = SqlCase::new(
                "query.nearest.in_subquery",
                "SELECT id FROM (SELECT id, qdrant_nearest_score(vector, 1.0, 0.0) AS score FROM \
                 vectors) ranked WHERE id IN (SELECT id FROM vectors WHERE id <> '3') ORDER BY id",
            );
            pub(crate) const EXISTS_CORRELATED: SqlCase = SqlCase::new(
                "query.nearest.exists_correlated",
                "SELECT outer_r.id FROM (SELECT id, qdrant_nearest_score(vector, 1.0, 0.0) AS \
                 score FROM vectors) outer_r WHERE EXISTS (SELECT 1 FROM vectors inner_v WHERE \
                 inner_v.id = outer_r.id AND inner_v.id <> '3') ORDER BY outer_r.id",
            );
            pub(crate) const HAVING_SUBQUERY: SqlCase = SqlCase::new(
                "query.nearest.having_subquery",
                "SELECT id, score FROM (SELECT id, qdrant_nearest_score(vector, 1.0, 0.0) AS \
                 score FROM vectors) ranked GROUP BY id, score HAVING MAX(score) >= 0.0 ORDER BY \
                 id",
            );
            pub(crate) const HAVING_DIRECT: SqlCase = SqlCase::new(
                "query.nearest.having_direct",
                "SELECT qdrant_nearest_score(vector, 1.0, 0.0) AS score FROM vectors GROUP BY \
                 qdrant_nearest_score(vector, 1.0, 0.0) HAVING MAX(qdrant_nearest_score(vector, \
                 1.0, 0.0)) >= 0.0",
            );
            pub(crate) const LEFT_JOIN: SqlCase = SqlCase::new(
                "query.nearest.left_join",
                concat!(
                    "SELECT lhs.id, COALESCE(rhs.score, 0.0) AS rhs_score FROM (SELECT id, \
                     qdrant_nearest_score(vector, 1.0, 0.0) AS score FROM vectors ORDER BY score \
                     DESC LIMIT 2) lhs ",
                    "LEFT JOIN (SELECT id, qdrant_nearest_score(vector, 0.0, 1.0) AS score FROM \
                     vectors ORDER BY score DESC LIMIT 2) rhs ON lhs.id = rhs.id ORDER BY lhs.id"
                ),
            );
            pub(crate) const INNER_JOIN_USING: SqlCase = SqlCase::new(
                "query.nearest.inner_join_using",
                concat!(
                    "SELECT id FROM (SELECT id, qdrant_nearest_score(vector, 1.0, 0.0) AS score \
                     FROM vectors ORDER BY score DESC LIMIT 2) lhs ",
                    "JOIN (SELECT id, qdrant_nearest_score(vector, 0.0, 1.0) AS score FROM \
                     vectors ORDER BY score DESC LIMIT 2) rhs USING (id) ORDER BY id"
                ),
            );
            pub(crate) const RIGHT_JOIN: SqlCase = SqlCase::new(
                "query.nearest.right_join",
                concat!(
                    "SELECT rhs.id, COALESCE(lhs.score, 0.0) AS lhs_score FROM (SELECT id, \
                     qdrant_nearest_score(vector, 1.0, 0.0) AS score FROM vectors ORDER BY score \
                     DESC LIMIT 2) lhs ",
                    "RIGHT JOIN (SELECT id, qdrant_nearest_score(vector, 0.0, 1.0) AS score FROM \
                     vectors ORDER BY score DESC LIMIT 2) rhs ON lhs.id = rhs.id ORDER BY rhs.id"
                ),
            );
            pub(crate) const FULL_JOIN: SqlCase = SqlCase::new(
                "query.nearest.full_join",
                concat!(
                    "SELECT COALESCE(lhs.id, rhs.id) AS id FROM (SELECT id, \
                     qdrant_nearest_score(vector, 1.0, 0.0) AS score FROM vectors ORDER BY score \
                     DESC LIMIT 2) lhs ",
                    "FULL OUTER JOIN (SELECT id, qdrant_nearest_score(vector, 0.0, 1.0) AS score \
                     FROM vectors ORDER BY score DESC LIMIT 2) rhs ON lhs.id = rhs.id ORDER BY id"
                ),
            );
            pub(crate) const CROSS_JOIN: SqlCase = SqlCase::new(
                "query.nearest.cross_join",
                concat!(
                    "SELECT lhs.id, rhs.id AS rhs_id FROM (SELECT id, \
                     qdrant_nearest_score(vector, 1.0, 0.0) AS score FROM vectors ORDER BY score \
                     DESC LIMIT 2) lhs ",
                    "CROSS JOIN (SELECT id, qdrant_nearest_score(vector, 0.0, 1.0) AS score FROM \
                     vectors ORDER BY score DESC LIMIT 2) rhs ORDER BY lhs.id, rhs_id"
                ),
            );
            pub(crate) const LEFT_SEMI_JOIN: SqlCase = SqlCase::new(
                "query.nearest.left_semi_join",
                concat!(
                    "SELECT lhs.id FROM (SELECT id, qdrant_nearest_score(vector, 1.0, 0.0) AS \
                     score FROM vectors ORDER BY score DESC LIMIT 2) lhs ",
                    "LEFT SEMI JOIN (SELECT id, qdrant_nearest_score(vector, 0.0, 1.0) AS score \
                     FROM vectors ORDER BY score DESC LIMIT 2) rhs ON lhs.id = rhs.id ORDER BY \
                     lhs.id"
                ),
            );
            pub(crate) const LEFT_ANTI_JOIN: SqlCase = SqlCase::new(
                "query.nearest.left_anti_join",
                concat!(
                    "SELECT lhs.id FROM (SELECT id, qdrant_nearest_score(vector, 1.0, 0.0) AS \
                     score FROM vectors ORDER BY score DESC LIMIT 2) lhs ",
                    "LEFT ANTI JOIN (SELECT id, qdrant_nearest_score(vector, 0.0, 1.0) AS score \
                     FROM vectors ORDER BY score DESC LIMIT 1) rhs ON lhs.id = rhs.id ORDER BY \
                     lhs.id"
                ),
            );
            pub(crate) const RIGHT_SEMI_JOIN: SqlCase = SqlCase::new(
                "query.nearest.right_semi_join",
                concat!(
                    "SELECT rhs.id FROM (SELECT id, qdrant_nearest_score(vector, 1.0, 0.0) AS \
                     score FROM vectors ORDER BY score DESC LIMIT 1) lhs ",
                    "RIGHT SEMI JOIN (SELECT id, qdrant_nearest_score(vector, 0.0, 1.0) AS score \
                     FROM vectors ORDER BY score DESC LIMIT 2) rhs ON lhs.id = rhs.id ORDER BY \
                     rhs.id"
                ),
            );
            pub(crate) const RIGHT_ANTI_JOIN: SqlCase = SqlCase::new(
                "query.nearest.right_anti_join",
                concat!(
                    "SELECT rhs.id FROM (SELECT id, qdrant_nearest_score(vector, 1.0, 0.0) AS \
                     score FROM vectors WHERE id = '9' ORDER BY score DESC LIMIT 1) lhs ",
                    "RIGHT ANTI JOIN (SELECT id, qdrant_nearest_score(vector, 0.0, 1.0) AS score \
                     FROM vectors ORDER BY score DESC LIMIT 2) rhs ON lhs.id = rhs.id ORDER BY \
                     rhs.id"
                ),
            );
            pub(crate) const SCALAR_SUBQUERY_THRESHOLD: SqlCase = SqlCase::new(
                "query.nearest.scalar_subquery_threshold",
                "SELECT id FROM (SELECT id, qdrant_nearest_score(vector, 1.0, 0.0) AS score FROM \
                 vectors) ranked WHERE score >= (SELECT AVG(score) FROM (SELECT \
                 qdrant_nearest_score(vector, 1.0, 0.0) AS score FROM vectors) scores) ORDER BY id",
            );

            pub(crate) const ALL: &[SqlCase] = &[
                WITHOUT_LIMIT,
                CANONICAL,
                LOCAL_PROJECTION,
                LOCAL_FILTER,
                LOCAL_FILTER_AND_PROJECTION,
                ASC_LOCAL_SORT,
                ABS_LOCAL_PROJECTION,
                CASE_LOCAL_PROJECTION,
                WINDOW_DIRECT,
                WINDOW_OVER_SUBQUERY,
                AGGREGATE_SUBQUERY,
                AGGREGATE_CTE,
                FILTERED_SUBQUERY_FUNCTION,
                PAYLOAD_PROJECTION_SIMPLE,
                SUBQUERY,
                CTE,
                WINDOW,
                UNION_ALL,
                UNION_DISTINCT,
                INTERSECT,
                INTERSECT_DIRECT,
                EXCEPT,
                INNER_JOIN,
                LEFT_JOIN,
                INNER_JOIN_USING,
                RIGHT_JOIN,
                FULL_JOIN,
                CROSS_JOIN,
                LEFT_SEMI_JOIN,
                LEFT_ANTI_JOIN,
                RIGHT_SEMI_JOIN,
                RIGHT_ANTI_JOIN,
                IN_SUBQUERY,
                EXISTS_CORRELATED,
                HAVING_SUBQUERY,
                HAVING_DIRECT,
                SCALAR_SUBQUERY_THRESHOLD,
            ];
        }

        pub(crate) mod sample {
            use super::SqlCase;

            pub(crate) const EXPLICIT: SqlCase = SqlCase::new(
                "query.sample.explicit",
                "SELECT id, qdrant_sample_score('random') AS score FROM vectors ORDER BY score \
                 DESC LIMIT 2",
            );
            pub(crate) const DEFAULT: SqlCase = SqlCase::new(
                "query.sample.default",
                "SELECT id, qdrant_sample_score() AS score FROM vectors ORDER BY score DESC LIMIT \
                 2",
            );
            pub(crate) const SUBQUERY: SqlCase = SqlCase::new(
                "query.sample.subquery",
                "SELECT id, score FROM (SELECT id, qdrant_sample_score() AS score FROM vectors) \
                 sampled LIMIT 2",
            );
            pub(crate) const CTE: SqlCase = SqlCase::new(
                "query.sample.cte",
                "WITH sampled AS (SELECT id, qdrant_sample_score() AS score FROM vectors) SELECT \
                 id, score FROM sampled LIMIT 2",
            );
            pub(crate) const ASC_LOCAL_SORT: SqlCase = SqlCase::new(
                "query.sample.asc_local_sort",
                "SELECT id, qdrant_sample_score() AS score FROM vectors ORDER BY score ASC LIMIT 2",
            );
            pub(crate) const WINDOW_DIRECT: SqlCase = SqlCase::new(
                "query.sample.window_direct",
                "SELECT id, AVG(qdrant_sample_score()) OVER () AS avg_score FROM vectors",
            );
            pub(crate) const WINDOW_OVER_SUBQUERY: SqlCase = SqlCase::new(
                "query.sample.window_over_subquery",
                "SELECT id, AVG(score) OVER () AS avg_score FROM (SELECT id, \
                 qdrant_sample_score() AS score FROM vectors) sampled ORDER BY id",
            );
            pub(crate) const AGGREGATE_SUBQUERY: SqlCase = SqlCase::new(
                "query.sample.aggregate_subquery",
                "SELECT * FROM (SELECT AVG(qdrant_sample_score()) AS avg_score FROM vectors) \
                 ranked",
            );
            pub(crate) const AGGREGATE_CTE: SqlCase = SqlCase::new(
                "query.sample.aggregate_cte",
                "WITH ranked AS (SELECT AVG(qdrant_sample_score()) AS avg_score FROM vectors) \
                 SELECT * FROM ranked",
            );
            pub(crate) const LOCAL_PROJECTION: SqlCase = SqlCase::new(
                "query.sample.local_projection",
                "SELECT id, qdrant_sample_score() + CAST(1.0 AS FLOAT) AS shifted_score FROM \
                 vectors ORDER BY shifted_score DESC LIMIT 2",
            );
            pub(crate) const LEFT_JOIN: SqlCase = SqlCase::new(
                "query.sample.left_join",
                "SELECT lhs.id, COALESCE(rhs.score, 0.0) AS rhs_score FROM (SELECT id, \
                 qdrant_sample_score() AS score FROM vectors ORDER BY score DESC LIMIT 2) lhs \
                 LEFT JOIN (SELECT id, qdrant_sample_score('random') AS score FROM vectors ORDER \
                 BY score DESC LIMIT 2) rhs ON lhs.id = rhs.id ORDER BY lhs.id",
            );
            pub(crate) const INNER_JOIN_USING: SqlCase = SqlCase::new(
                "query.sample.inner_join_using",
                "SELECT id FROM (SELECT id, qdrant_sample_score() AS score FROM vectors ORDER BY \
                 score DESC LIMIT 2) lhs JOIN (SELECT id, qdrant_sample_score('random') AS score \
                 FROM vectors ORDER BY score DESC LIMIT 2) rhs USING (id) ORDER BY id",
            );
            pub(crate) const RIGHT_JOIN: SqlCase = SqlCase::new(
                "query.sample.right_join",
                "SELECT rhs.id, COALESCE(lhs.score, 0.0) AS lhs_score FROM (SELECT id, \
                 qdrant_sample_score() AS score FROM vectors ORDER BY score DESC LIMIT 2) lhs \
                 RIGHT JOIN (SELECT id, qdrant_sample_score('random') AS score FROM vectors ORDER \
                 BY score DESC LIMIT 2) rhs ON lhs.id = rhs.id ORDER BY rhs.id",
            );
            pub(crate) const FULL_JOIN: SqlCase = SqlCase::new(
                "query.sample.full_join",
                "SELECT COALESCE(lhs.id, rhs.id) AS id FROM (SELECT id, qdrant_sample_score() AS \
                 score FROM vectors ORDER BY score DESC LIMIT 2) lhs FULL OUTER JOIN (SELECT id, \
                 qdrant_sample_score('random') AS score FROM vectors ORDER BY score DESC LIMIT 2) \
                 rhs ON lhs.id = rhs.id ORDER BY id",
            );
            pub(crate) const CROSS_JOIN: SqlCase = SqlCase::new(
                "query.sample.cross_join",
                "SELECT lhs.id, rhs.id AS rhs_id FROM (SELECT id, qdrant_sample_score() AS score \
                 FROM vectors ORDER BY score DESC LIMIT 2) lhs CROSS JOIN (SELECT id, \
                 qdrant_sample_score('random') AS score FROM vectors ORDER BY score DESC LIMIT 2) \
                 rhs ORDER BY lhs.id, rhs_id",
            );
            pub(crate) const LEFT_SEMI_JOIN: SqlCase = SqlCase::new(
                "query.sample.left_semi_join",
                "SELECT lhs.id FROM (SELECT id, qdrant_sample_score() AS score FROM vectors ORDER \
                 BY score DESC LIMIT 2) lhs LEFT SEMI JOIN (SELECT id, \
                 qdrant_sample_score('random') AS score FROM vectors ORDER BY score DESC LIMIT 2) \
                 rhs ON lhs.id = rhs.id ORDER BY lhs.id",
            );
            pub(crate) const LEFT_ANTI_JOIN: SqlCase = SqlCase::new(
                "query.sample.left_anti_join",
                "SELECT lhs.id FROM (SELECT id, qdrant_sample_score() AS score FROM vectors ORDER \
                 BY score DESC LIMIT 2) lhs LEFT ANTI JOIN (SELECT id, \
                 qdrant_sample_score('random') AS score FROM vectors ORDER BY score DESC LIMIT 1) \
                 rhs ON lhs.id = rhs.id ORDER BY lhs.id",
            );
            pub(crate) const RIGHT_SEMI_JOIN: SqlCase = SqlCase::new(
                "query.sample.right_semi_join",
                "SELECT rhs.id FROM (SELECT id, qdrant_sample_score() AS score FROM vectors ORDER \
                 BY score DESC LIMIT 1) lhs RIGHT SEMI JOIN (SELECT id, \
                 qdrant_sample_score('random') AS score FROM vectors ORDER BY score DESC LIMIT 2) \
                 rhs ON lhs.id = rhs.id ORDER BY rhs.id",
            );
            pub(crate) const RIGHT_ANTI_JOIN: SqlCase = SqlCase::new(
                "query.sample.right_anti_join",
                "SELECT rhs.id FROM (SELECT id, qdrant_sample_score() AS score FROM vectors WHERE \
                 id = '9' ORDER BY score DESC LIMIT 1) lhs RIGHT ANTI JOIN (SELECT id, \
                 qdrant_sample_score('random') AS score FROM vectors ORDER BY score DESC LIMIT 2) \
                 rhs ON lhs.id = rhs.id ORDER BY rhs.id",
            );

            pub(crate) const ALL: &[SqlCase] = &[
                EXPLICIT,
                DEFAULT,
                SUBQUERY,
                CTE,
                ASC_LOCAL_SORT,
                WINDOW_DIRECT,
                WINDOW_OVER_SUBQUERY,
                AGGREGATE_SUBQUERY,
                AGGREGATE_CTE,
                LOCAL_PROJECTION,
                LEFT_JOIN,
                INNER_JOIN_USING,
                RIGHT_JOIN,
                FULL_JOIN,
                CROSS_JOIN,
                LEFT_SEMI_JOIN,
                LEFT_ANTI_JOIN,
                RIGHT_SEMI_JOIN,
                RIGHT_ANTI_JOIN,
            ];
        }

        pub(crate) mod recommend {
            use super::SqlCase;

            pub(crate) const DEFAULT: SqlCase = SqlCase::new(
                "query.recommend.default",
                "SELECT id, qdrant_recommend_score(embedding, [[1.0, 0.0]], [[0.0, 1.0]]) AS \
                 score FROM vectors ORDER BY score DESC LIMIT 2",
            );
            pub(crate) const STRATEGY: SqlCase = SqlCase::new(
                "query.recommend.strategy",
                "SELECT id, qdrant_recommend_score(embedding, 'average_vector', [[1.0, 0.0]], \
                 [[0.0, 1.0]]) AS score FROM vectors ORDER BY score DESC LIMIT 2",
            );
            pub(crate) const SUBQUERY: SqlCase = SqlCase::new(
                "query.recommend.subquery",
                "SELECT id, score FROM (SELECT id, qdrant_recommend_score(embedding, [[1.0, \
                 0.0]], [[0.0, 1.0]]) AS score FROM vectors) ranked ORDER BY score DESC LIMIT 2",
            );
            pub(crate) const CTE: SqlCase = SqlCase::new(
                "query.recommend.cte",
                "WITH ranked AS (SELECT id, qdrant_recommend_score(embedding, [[1.0, 0.0]], \
                 [[0.0, 1.0]]) AS score FROM vectors) SELECT id, score FROM ranked ORDER BY score \
                 DESC LIMIT 2",
            );
            pub(crate) const WINDOW_DIRECT: SqlCase = SqlCase::new(
                "query.recommend.window_direct",
                "SELECT id, AVG(qdrant_recommend_score(embedding, [[1.0, 0.0]], [[0.0, 1.0]])) \
                 OVER () AS avg_score FROM vectors",
            );
            pub(crate) const WINDOW_OVER_SUBQUERY: SqlCase = SqlCase::new(
                "query.recommend.window_over_subquery",
                "SELECT id, AVG(score) OVER () AS avg_score FROM (SELECT id, \
                 qdrant_recommend_score(embedding, [[1.0, 0.0]], [[0.0, 1.0]]) AS score FROM \
                 vectors) ranked ORDER BY id",
            );
            pub(crate) const AGGREGATE_SUBQUERY: SqlCase = SqlCase::new(
                "query.recommend.aggregate_subquery",
                "SELECT * FROM (SELECT AVG(qdrant_recommend_score(embedding, [[1.0, 0.0]], [[0.0, \
                 1.0]])) AS avg_score FROM vectors) ranked",
            );
            pub(crate) const AGGREGATE_CTE: SqlCase = SqlCase::new(
                "query.recommend.aggregate_cte",
                "WITH ranked AS (SELECT AVG(qdrant_recommend_score(embedding, [[1.0, 0.0]], \
                 [[0.0, 1.0]])) AS avg_score FROM vectors) SELECT * FROM ranked",
            );
            pub(crate) const LOCAL_FILTER: SqlCase = SqlCase::new(
                "query.recommend.local_filter",
                "SELECT id FROM vectors WHERE qdrant_recommend_score(embedding, [[1.0, 0.0]], \
                 [[0.0, 1.0]]) <= 1.0 ORDER BY id",
            );
            pub(crate) const LEFT_JOIN: SqlCase = SqlCase::new(
                "query.recommend.left_join",
                "SELECT lhs.id, COALESCE(rhs.score, 0.0) AS rhs_score FROM (SELECT id, \
                 qdrant_recommend_score(embedding, [[1.0, 0.0]], [[0.0, 1.0]]) AS score FROM \
                 vectors ORDER BY score DESC LIMIT 2) lhs LEFT JOIN (SELECT id, \
                 qdrant_recommend_score(embedding, [[0.0, 1.0]], [[1.0, 0.0]]) AS score FROM \
                 vectors ORDER BY score DESC LIMIT 2) rhs ON lhs.id = rhs.id ORDER BY lhs.id",
            );
            pub(crate) const INNER_JOIN_USING: SqlCase = SqlCase::new(
                "query.recommend.inner_join_using",
                "SELECT id FROM (SELECT id, qdrant_recommend_score(embedding, [[1.0, 0.0]], \
                 [[0.0, 1.0]]) AS score FROM vectors ORDER BY score DESC LIMIT 2) lhs JOIN \
                 (SELECT id, qdrant_recommend_score(embedding, [[0.0, 1.0]], [[1.0, 0.0]]) AS \
                 score FROM vectors ORDER BY score DESC LIMIT 2) rhs USING (id) ORDER BY id",
            );
            pub(crate) const RIGHT_JOIN: SqlCase = SqlCase::new(
                "query.recommend.right_join",
                "SELECT rhs.id, COALESCE(lhs.score, 0.0) AS lhs_score FROM (SELECT id, \
                 qdrant_recommend_score(embedding, [[1.0, 0.0]], [[0.0, 1.0]]) AS score FROM \
                 vectors ORDER BY score DESC LIMIT 2) lhs RIGHT JOIN (SELECT id, \
                 qdrant_recommend_score(embedding, [[0.0, 1.0]], [[1.0, 0.0]]) AS score FROM \
                 vectors ORDER BY score DESC LIMIT 2) rhs ON lhs.id = rhs.id ORDER BY rhs.id",
            );
            pub(crate) const FULL_JOIN: SqlCase = SqlCase::new(
                "query.recommend.full_join",
                "SELECT COALESCE(lhs.id, rhs.id) AS id FROM (SELECT id, \
                 qdrant_recommend_score(embedding, [[1.0, 0.0]], [[0.0, 1.0]]) AS score FROM \
                 vectors ORDER BY score DESC LIMIT 2) lhs FULL OUTER JOIN (SELECT id, \
                 qdrant_recommend_score(embedding, [[0.0, 1.0]], [[1.0, 0.0]]) AS score FROM \
                 vectors ORDER BY score DESC LIMIT 2) rhs ON lhs.id = rhs.id ORDER BY id",
            );
            pub(crate) const CROSS_JOIN: SqlCase = SqlCase::new(
                "query.recommend.cross_join",
                "SELECT lhs.id, rhs.id AS rhs_id FROM (SELECT id, \
                 qdrant_recommend_score(embedding, [[1.0, 0.0]], [[0.0, 1.0]]) AS score FROM \
                 vectors ORDER BY score DESC LIMIT 2) lhs CROSS JOIN (SELECT id, \
                 qdrant_recommend_score(embedding, [[0.0, 1.0]], [[1.0, 0.0]]) AS score FROM \
                 vectors ORDER BY score DESC LIMIT 2) rhs ORDER BY lhs.id, rhs_id",
            );
            pub(crate) const LEFT_SEMI_JOIN: SqlCase = SqlCase::new(
                "query.recommend.left_semi_join",
                "SELECT lhs.id FROM (SELECT id, qdrant_recommend_score(embedding, [[1.0, 0.0]], \
                 [[0.0, 1.0]]) AS score FROM vectors ORDER BY score DESC LIMIT 2) lhs LEFT SEMI \
                 JOIN (SELECT id, qdrant_recommend_score(embedding, [[0.0, 1.0]], [[1.0, 0.0]]) \
                 AS score FROM vectors ORDER BY score DESC LIMIT 2) rhs ON lhs.id = rhs.id ORDER \
                 BY lhs.id",
            );
            pub(crate) const LEFT_ANTI_JOIN: SqlCase = SqlCase::new(
                "query.recommend.left_anti_join",
                "SELECT lhs.id FROM (SELECT id, qdrant_recommend_score(embedding, [[1.0, 0.0]], \
                 [[0.0, 1.0]]) AS score FROM vectors ORDER BY score DESC LIMIT 2) lhs LEFT ANTI \
                 JOIN (SELECT id, qdrant_recommend_score(embedding, [[0.0, 1.0]], [[1.0, 0.0]]) \
                 AS score FROM vectors ORDER BY score DESC LIMIT 1) rhs ON lhs.id = rhs.id ORDER \
                 BY lhs.id",
            );
            pub(crate) const RIGHT_SEMI_JOIN: SqlCase = SqlCase::new(
                "query.recommend.right_semi_join",
                "SELECT rhs.id FROM (SELECT id, qdrant_recommend_score(embedding, [[1.0, 0.0]], \
                 [[0.0, 1.0]]) AS score FROM vectors ORDER BY score DESC LIMIT 1) lhs RIGHT SEMI \
                 JOIN (SELECT id, qdrant_recommend_score(embedding, [[0.0, 1.0]], [[1.0, 0.0]]) \
                 AS score FROM vectors ORDER BY score DESC LIMIT 2) rhs ON lhs.id = rhs.id ORDER \
                 BY rhs.id",
            );
            pub(crate) const RIGHT_ANTI_JOIN: SqlCase = SqlCase::new(
                "query.recommend.right_anti_join",
                "SELECT rhs.id FROM (SELECT id, qdrant_recommend_score(embedding, [[1.0, 0.0]], \
                 [[0.0, 1.0]]) AS score FROM vectors WHERE id = '9' ORDER BY score DESC LIMIT 1) \
                 lhs RIGHT ANTI JOIN (SELECT id, qdrant_recommend_score(embedding, [[0.0, 1.0]], \
                 [[1.0, 0.0]]) AS score FROM vectors ORDER BY score DESC LIMIT 2) rhs ON lhs.id = \
                 rhs.id ORDER BY rhs.id",
            );

            pub(crate) const ALL: &[SqlCase] = &[
                DEFAULT,
                STRATEGY,
                SUBQUERY,
                CTE,
                WINDOW_DIRECT,
                WINDOW_OVER_SUBQUERY,
                AGGREGATE_SUBQUERY,
                AGGREGATE_CTE,
                LOCAL_FILTER,
                LEFT_JOIN,
                INNER_JOIN_USING,
                RIGHT_JOIN,
                FULL_JOIN,
                CROSS_JOIN,
                LEFT_SEMI_JOIN,
                LEFT_ANTI_JOIN,
                RIGHT_SEMI_JOIN,
                RIGHT_ANTI_JOIN,
            ];
        }

        pub(crate) mod discover {
            use super::SqlCase;

            pub(crate) const CANONICAL: SqlCase = SqlCase::new(
                "query.discover.canonical",
                "SELECT id, qdrant_discover_score(embedding, [1.0, 0.0], [[[1.0, 0.0], [0.0, \
                 1.0]]]) AS score FROM vectors ORDER BY score DESC LIMIT 2",
            );
            pub(crate) const SUBQUERY: SqlCase = SqlCase::new(
                "query.discover.subquery",
                "SELECT id, score FROM (SELECT id, qdrant_discover_score(embedding, [1.0, 0.0], \
                 [[[1.0, 0.0], [0.0, 1.0]]]) AS score FROM vectors) ranked ORDER BY score DESC \
                 LIMIT 2",
            );
            pub(crate) const CTE: SqlCase = SqlCase::new(
                "query.discover.cte",
                "WITH ranked AS (SELECT id, qdrant_discover_score(embedding, [1.0, 0.0], [[[1.0, \
                 0.0], [0.0, 1.0]]]) AS score FROM vectors) SELECT id, score FROM ranked ORDER BY \
                 score DESC LIMIT 2",
            );
            pub(crate) const WINDOW_DIRECT: SqlCase = SqlCase::new(
                "query.discover.window_direct",
                "SELECT id, AVG(qdrant_discover_score(embedding, [1.0, 0.0], [[[1.0, 0.0], [0.0, \
                 1.0]]])) OVER () AS avg_score FROM vectors",
            );
            pub(crate) const WINDOW_OVER_SUBQUERY: SqlCase = SqlCase::new(
                "query.discover.window_over_subquery",
                "SELECT id, AVG(score) OVER () AS avg_score FROM (SELECT id, \
                 qdrant_discover_score(embedding, [1.0, 0.0], [[[1.0, 0.0], [0.0, 1.0]]]) AS \
                 score FROM vectors) ranked ORDER BY id",
            );
            pub(crate) const AGGREGATE_SUBQUERY: SqlCase = SqlCase::new(
                "query.discover.aggregate_subquery",
                "SELECT * FROM (SELECT AVG(qdrant_discover_score(embedding, [1.0, 0.0], [[[1.0, \
                 0.0], [0.0, 1.0]]])) AS avg_score FROM vectors) ranked",
            );
            pub(crate) const AGGREGATE_CTE: SqlCase = SqlCase::new(
                "query.discover.aggregate_cte",
                "WITH ranked AS (SELECT AVG(qdrant_discover_score(embedding, [1.0, 0.0], [[[1.0, \
                 0.0], [0.0, 1.0]]])) AS avg_score FROM vectors) SELECT * FROM ranked",
            );
            pub(crate) const LEFT_JOIN: SqlCase = SqlCase::new(
                "query.discover.left_join",
                "SELECT lhs.id, COALESCE(rhs.score, 0.0) AS rhs_score FROM (SELECT id, \
                 qdrant_discover_score(embedding, [1.0, 0.0], [[[1.0, 0.0], [0.0, 1.0]]]) AS \
                 score FROM vectors ORDER BY score DESC LIMIT 2) lhs LEFT JOIN (SELECT id, \
                 qdrant_discover_score(embedding, [0.0, 1.0], [[[1.0, 0.0], [0.0, 1.0]]]) AS \
                 score FROM vectors ORDER BY score DESC LIMIT 2) rhs ON lhs.id = rhs.id ORDER BY \
                 lhs.id",
            );
            pub(crate) const INNER_JOIN_USING: SqlCase = SqlCase::new(
                "query.discover.inner_join_using",
                "SELECT id FROM (SELECT id, qdrant_discover_score(embedding, [1.0, 0.0], [[[1.0, \
                 0.0], [0.0, 1.0]]]) AS score FROM vectors ORDER BY score DESC LIMIT 2) lhs JOIN \
                 (SELECT id, qdrant_discover_score(embedding, [0.0, 1.0], [[[1.0, 0.0], [0.0, \
                 1.0]]]) AS score FROM vectors ORDER BY score DESC LIMIT 2) rhs USING (id) ORDER \
                 BY id",
            );
            pub(crate) const RIGHT_JOIN: SqlCase = SqlCase::new(
                "query.discover.right_join",
                "SELECT rhs.id, COALESCE(lhs.score, 0.0) AS lhs_score FROM (SELECT id, \
                 qdrant_discover_score(embedding, [1.0, 0.0], [[[1.0, 0.0], [0.0, 1.0]]]) AS \
                 score FROM vectors ORDER BY score DESC LIMIT 2) lhs RIGHT JOIN (SELECT id, \
                 qdrant_discover_score(embedding, [0.0, 1.0], [[[1.0, 0.0], [0.0, 1.0]]]) AS \
                 score FROM vectors ORDER BY score DESC LIMIT 2) rhs ON lhs.id = rhs.id ORDER BY \
                 rhs.id",
            );
            pub(crate) const FULL_JOIN: SqlCase = SqlCase::new(
                "query.discover.full_join",
                "SELECT COALESCE(lhs.id, rhs.id) AS id FROM (SELECT id, \
                 qdrant_discover_score(embedding, [1.0, 0.0], [[[1.0, 0.0], [0.0, 1.0]]]) AS \
                 score FROM vectors ORDER BY score DESC LIMIT 2) lhs FULL OUTER JOIN (SELECT id, \
                 qdrant_discover_score(embedding, [0.0, 1.0], [[[1.0, 0.0], [0.0, 1.0]]]) AS \
                 score FROM vectors ORDER BY score DESC LIMIT 2) rhs ON lhs.id = rhs.id ORDER BY \
                 id",
            );
            pub(crate) const CROSS_JOIN: SqlCase = SqlCase::new(
                "query.discover.cross_join",
                "SELECT lhs.id, rhs.id AS rhs_id FROM (SELECT id, \
                 qdrant_discover_score(embedding, [1.0, 0.0], [[[1.0, 0.0], [0.0, 1.0]]]) AS \
                 score FROM vectors ORDER BY score DESC LIMIT 2) lhs CROSS JOIN (SELECT id, \
                 qdrant_discover_score(embedding, [0.0, 1.0], [[[1.0, 0.0], [0.0, 1.0]]]) AS \
                 score FROM vectors ORDER BY score DESC LIMIT 2) rhs ORDER BY lhs.id, rhs_id",
            );
            pub(crate) const LEFT_SEMI_JOIN: SqlCase = SqlCase::new(
                "query.discover.left_semi_join",
                "SELECT lhs.id FROM (SELECT id, qdrant_discover_score(embedding, [1.0, 0.0], \
                 [[[1.0, 0.0], [0.0, 1.0]]]) AS score FROM vectors ORDER BY score DESC LIMIT 2) \
                 lhs LEFT SEMI JOIN (SELECT id, qdrant_discover_score(embedding, [0.0, 1.0], \
                 [[[1.0, 0.0], [0.0, 1.0]]]) AS score FROM vectors ORDER BY score DESC LIMIT 2) \
                 rhs ON lhs.id = rhs.id ORDER BY lhs.id",
            );
            pub(crate) const LEFT_ANTI_JOIN: SqlCase = SqlCase::new(
                "query.discover.left_anti_join",
                "SELECT lhs.id FROM (SELECT id, qdrant_discover_score(embedding, [1.0, 0.0], \
                 [[[1.0, 0.0], [0.0, 1.0]]]) AS score FROM vectors ORDER BY score DESC LIMIT 2) \
                 lhs LEFT ANTI JOIN (SELECT id, qdrant_discover_score(embedding, [0.0, 1.0], \
                 [[[1.0, 0.0], [0.0, 1.0]]]) AS score FROM vectors ORDER BY score DESC LIMIT 1) \
                 rhs ON lhs.id = rhs.id ORDER BY lhs.id",
            );
            pub(crate) const RIGHT_SEMI_JOIN: SqlCase = SqlCase::new(
                "query.discover.right_semi_join",
                "SELECT rhs.id FROM (SELECT id, qdrant_discover_score(embedding, [1.0, 0.0], \
                 [[[1.0, 0.0], [0.0, 1.0]]]) AS score FROM vectors ORDER BY score DESC LIMIT 1) \
                 lhs RIGHT SEMI JOIN (SELECT id, qdrant_discover_score(embedding, [0.0, 1.0], \
                 [[[1.0, 0.0], [0.0, 1.0]]]) AS score FROM vectors ORDER BY score DESC LIMIT 2) \
                 rhs ON lhs.id = rhs.id ORDER BY rhs.id",
            );
            pub(crate) const RIGHT_ANTI_JOIN: SqlCase = SqlCase::new(
                "query.discover.right_anti_join",
                "SELECT rhs.id FROM (SELECT id, qdrant_discover_score(embedding, [1.0, 0.0], \
                 [[[1.0, 0.0], [0.0, 1.0]]]) AS score FROM vectors WHERE id = '9' ORDER BY score \
                 DESC LIMIT 1) lhs RIGHT ANTI JOIN (SELECT id, qdrant_discover_score(embedding, \
                 [0.0, 1.0], [[[1.0, 0.0], [0.0, 1.0]]]) AS score FROM vectors ORDER BY score \
                 DESC LIMIT 2) rhs ON lhs.id = rhs.id ORDER BY rhs.id",
            );

            pub(crate) const ALL: &[SqlCase] = &[
                CANONICAL,
                SUBQUERY,
                CTE,
                WINDOW_DIRECT,
                WINDOW_OVER_SUBQUERY,
                AGGREGATE_SUBQUERY,
                AGGREGATE_CTE,
                LEFT_JOIN,
                INNER_JOIN_USING,
                RIGHT_JOIN,
                FULL_JOIN,
                CROSS_JOIN,
                LEFT_SEMI_JOIN,
                LEFT_ANTI_JOIN,
                RIGHT_SEMI_JOIN,
                RIGHT_ANTI_JOIN,
            ];
        }

        pub(crate) mod context {
            use super::SqlCase;

            pub(crate) const CANONICAL: SqlCase = SqlCase::new(
                "query.context.canonical",
                "SELECT id, qdrant_context_score(embedding, [[[1.0, 0.0], [0.0, 1.0]]]) AS score \
                 FROM vectors ORDER BY score DESC LIMIT 2",
            );
            pub(crate) const SUBQUERY: SqlCase = SqlCase::new(
                "query.context.subquery",
                "SELECT id, score FROM (SELECT id, qdrant_context_score(embedding, [[[1.0, 0.0], \
                 [0.0, 1.0]]]) AS score FROM vectors) ranked ORDER BY score DESC LIMIT 2",
            );
            pub(crate) const CTE: SqlCase = SqlCase::new(
                "query.context.cte",
                "WITH ranked AS (SELECT id, qdrant_context_score(embedding, [[[1.0, 0.0], [0.0, \
                 1.0]]]) AS score FROM vectors) SELECT id, score FROM ranked ORDER BY score DESC \
                 LIMIT 2",
            );
            pub(crate) const WINDOW_DIRECT: SqlCase = SqlCase::new(
                "query.context.window_direct",
                "SELECT id, AVG(qdrant_context_score(embedding, [[[1.0, 0.0], [0.0, 1.0]]])) OVER \
                 () AS avg_score FROM vectors",
            );
            pub(crate) const WINDOW_OVER_SUBQUERY: SqlCase = SqlCase::new(
                "query.context.window_over_subquery",
                "SELECT id, AVG(score) OVER () AS avg_score FROM (SELECT id, \
                 qdrant_context_score(embedding, [[[1.0, 0.0], [0.0, 1.0]]]) AS score FROM \
                 vectors) ranked ORDER BY id",
            );
            pub(crate) const AGGREGATE_SUBQUERY: SqlCase = SqlCase::new(
                "query.context.aggregate_subquery",
                "SELECT * FROM (SELECT AVG(qdrant_context_score(embedding, [[[1.0, 0.0], [0.0, \
                 1.0]]])) AS avg_score FROM vectors) ranked",
            );
            pub(crate) const AGGREGATE_CTE: SqlCase = SqlCase::new(
                "query.context.aggregate_cte",
                "WITH ranked AS (SELECT AVG(qdrant_context_score(embedding, [[[1.0, 0.0], [0.0, \
                 1.0]]])) AS avg_score FROM vectors) SELECT * FROM ranked",
            );
            pub(crate) const LEFT_JOIN: SqlCase = SqlCase::new(
                "query.context.left_join",
                "SELECT lhs.id, COALESCE(rhs.score, 0.0) AS rhs_score FROM (SELECT id, \
                 qdrant_context_score(embedding, [[[1.0, 0.0], [0.0, 1.0]]]) AS score FROM \
                 vectors ORDER BY score DESC LIMIT 2) lhs LEFT JOIN (SELECT id, \
                 qdrant_context_score(embedding, [[[0.0, 1.0], [1.0, 0.0]]]) AS score FROM \
                 vectors ORDER BY score DESC LIMIT 2) rhs ON lhs.id = rhs.id ORDER BY lhs.id",
            );
            pub(crate) const INNER_JOIN_USING: SqlCase = SqlCase::new(
                "query.context.inner_join_using",
                "SELECT id FROM (SELECT id, qdrant_context_score(embedding, [[[1.0, 0.0], [0.0, \
                 1.0]]]) AS score FROM vectors ORDER BY score DESC LIMIT 2) lhs JOIN (SELECT id, \
                 qdrant_context_score(embedding, [[[0.0, 1.0], [1.0, 0.0]]]) AS score FROM \
                 vectors ORDER BY score DESC LIMIT 2) rhs USING (id) ORDER BY id",
            );
            pub(crate) const RIGHT_JOIN: SqlCase = SqlCase::new(
                "query.context.right_join",
                "SELECT rhs.id, COALESCE(lhs.score, 0.0) AS lhs_score FROM (SELECT id, \
                 qdrant_context_score(embedding, [[[1.0, 0.0], [0.0, 1.0]]]) AS score FROM \
                 vectors ORDER BY score DESC LIMIT 2) lhs RIGHT JOIN (SELECT id, \
                 qdrant_context_score(embedding, [[[0.0, 1.0], [1.0, 0.0]]]) AS score FROM \
                 vectors ORDER BY score DESC LIMIT 2) rhs ON lhs.id = rhs.id ORDER BY rhs.id",
            );
            pub(crate) const FULL_JOIN: SqlCase = SqlCase::new(
                "query.context.full_join",
                "SELECT COALESCE(lhs.id, rhs.id) AS id FROM (SELECT id, \
                 qdrant_context_score(embedding, [[[1.0, 0.0], [0.0, 1.0]]]) AS score FROM \
                 vectors ORDER BY score DESC LIMIT 2) lhs FULL OUTER JOIN (SELECT id, \
                 qdrant_context_score(embedding, [[[0.0, 1.0], [1.0, 0.0]]]) AS score FROM \
                 vectors ORDER BY score DESC LIMIT 2) rhs ON lhs.id = rhs.id ORDER BY id",
            );
            pub(crate) const CROSS_JOIN: SqlCase = SqlCase::new(
                "query.context.cross_join",
                "SELECT lhs.id, rhs.id AS rhs_id FROM (SELECT id, qdrant_context_score(embedding, \
                 [[[1.0, 0.0], [0.0, 1.0]]]) AS score FROM vectors ORDER BY score DESC LIMIT 2) \
                 lhs CROSS JOIN (SELECT id, qdrant_context_score(embedding, [[[0.0, 1.0], [1.0, \
                 0.0]]]) AS score FROM vectors ORDER BY score DESC LIMIT 2) rhs ORDER BY lhs.id, \
                 rhs_id",
            );
            pub(crate) const LEFT_SEMI_JOIN: SqlCase = SqlCase::new(
                "query.context.left_semi_join",
                "SELECT lhs.id FROM (SELECT id, qdrant_context_score(embedding, [[[1.0, 0.0], \
                 [0.0, 1.0]]]) AS score FROM vectors ORDER BY score DESC LIMIT 2) lhs LEFT SEMI \
                 JOIN (SELECT id, qdrant_context_score(embedding, [[[0.0, 1.0], [1.0, 0.0]]]) AS \
                 score FROM vectors ORDER BY score DESC LIMIT 2) rhs ON lhs.id = rhs.id ORDER BY \
                 lhs.id",
            );
            pub(crate) const LEFT_ANTI_JOIN: SqlCase = SqlCase::new(
                "query.context.left_anti_join",
                "SELECT lhs.id FROM (SELECT id, qdrant_context_score(embedding, [[[1.0, 0.0], \
                 [0.0, 1.0]]]) AS score FROM vectors ORDER BY score DESC LIMIT 2) lhs LEFT ANTI \
                 JOIN (SELECT id, qdrant_context_score(embedding, [[[0.0, 1.0], [1.0, 0.0]]]) AS \
                 score FROM vectors ORDER BY score DESC LIMIT 1) rhs ON lhs.id = rhs.id ORDER BY \
                 lhs.id",
            );
            pub(crate) const RIGHT_SEMI_JOIN: SqlCase = SqlCase::new(
                "query.context.right_semi_join",
                "SELECT rhs.id FROM (SELECT id, qdrant_context_score(embedding, [[[1.0, 0.0], \
                 [0.0, 1.0]]]) AS score FROM vectors ORDER BY score DESC LIMIT 1) lhs RIGHT SEMI \
                 JOIN (SELECT id, qdrant_context_score(embedding, [[[0.0, 1.0], [1.0, 0.0]]]) AS \
                 score FROM vectors ORDER BY score DESC LIMIT 2) rhs ON lhs.id = rhs.id ORDER BY \
                 rhs.id",
            );
            pub(crate) const RIGHT_ANTI_JOIN: SqlCase = SqlCase::new(
                "query.context.right_anti_join",
                "SELECT rhs.id FROM (SELECT id, qdrant_context_score(embedding, [[[1.0, 0.0], \
                 [0.0, 1.0]]]) AS score FROM vectors WHERE id = '9' ORDER BY score DESC LIMIT 1) \
                 lhs RIGHT ANTI JOIN (SELECT id, qdrant_context_score(embedding, [[[0.0, 1.0], \
                 [1.0, 0.0]]]) AS score FROM vectors ORDER BY score DESC LIMIT 2) rhs ON lhs.id = \
                 rhs.id ORDER BY rhs.id",
            );

            pub(crate) const ALL: &[SqlCase] = &[
                CANONICAL,
                SUBQUERY,
                CTE,
                WINDOW_DIRECT,
                WINDOW_OVER_SUBQUERY,
                AGGREGATE_SUBQUERY,
                AGGREGATE_CTE,
                LEFT_JOIN,
                INNER_JOIN_USING,
                RIGHT_JOIN,
                FULL_JOIN,
                CROSS_JOIN,
                LEFT_SEMI_JOIN,
                LEFT_ANTI_JOIN,
                RIGHT_SEMI_JOIN,
                RIGHT_ANTI_JOIN,
            ];
        }

        pub(crate) mod mmr {
            use super::SqlCase;

            pub(crate) const CANONICAL: SqlCase = SqlCase::new(
                "query.mmr.canonical",
                "SELECT id, qdrant_nearest_with_mmr_score(embedding, 0.9, 8, 1.0, 0.0) AS score \
                 FROM vectors ORDER BY score DESC LIMIT 2",
            );
            pub(crate) const SUBQUERY: SqlCase = SqlCase::new(
                "query.mmr.subquery",
                "SELECT id, score FROM (SELECT id, qdrant_nearest_with_mmr_score(embedding, 0.9, \
                 8, 1.0, 0.0) AS score FROM vectors) ranked ORDER BY score DESC LIMIT 2",
            );
            pub(crate) const CTE: SqlCase = SqlCase::new(
                "query.mmr.cte",
                "WITH ranked AS (SELECT id, qdrant_nearest_with_mmr_score(embedding, 0.9, 8, 1.0, \
                 0.0) AS score FROM vectors) SELECT id, score FROM ranked ORDER BY score DESC \
                 LIMIT 2",
            );
            pub(crate) const WINDOW_DIRECT: SqlCase = SqlCase::new(
                "query.mmr.window_direct",
                "SELECT id, AVG(qdrant_nearest_with_mmr_score(embedding, 0.9, 8, 1.0, 0.0)) OVER \
                 () AS avg_score FROM vectors",
            );
            pub(crate) const WINDOW_OVER_SUBQUERY: SqlCase = SqlCase::new(
                "query.mmr.window_over_subquery",
                "SELECT id, AVG(score) OVER () AS avg_score FROM (SELECT id, \
                 qdrant_nearest_with_mmr_score(embedding, 0.9, 8, 1.0, 0.0) AS score FROM \
                 vectors) ranked ORDER BY id",
            );
            pub(crate) const AGGREGATE_SUBQUERY: SqlCase = SqlCase::new(
                "query.mmr.aggregate_subquery",
                "SELECT * FROM (SELECT AVG(qdrant_nearest_with_mmr_score(embedding, 0.9, 8, 1.0, \
                 0.0)) AS avg_score FROM vectors) ranked",
            );
            pub(crate) const AGGREGATE_CTE: SqlCase = SqlCase::new(
                "query.mmr.aggregate_cte",
                "WITH ranked AS (SELECT AVG(qdrant_nearest_with_mmr_score(embedding, 0.9, 8, 1.0, \
                 0.0)) AS avg_score FROM vectors) SELECT * FROM ranked",
            );
            pub(crate) const LEFT_JOIN: SqlCase = SqlCase::new(
                "query.mmr.left_join",
                "SELECT lhs.id, COALESCE(rhs.score, 0.0) AS rhs_score FROM (SELECT id, \
                 qdrant_nearest_with_mmr_score(embedding, 0.9, 8, 1.0, 0.0) AS score FROM vectors \
                 ORDER BY score DESC LIMIT 2) lhs LEFT JOIN (SELECT id, \
                 qdrant_nearest_with_mmr_score(embedding, 0.5, 8, 0.0, 1.0) AS score FROM vectors \
                 ORDER BY score DESC LIMIT 2) rhs ON lhs.id = rhs.id ORDER BY lhs.id",
            );
            pub(crate) const INNER_JOIN_USING: SqlCase = SqlCase::new(
                "query.mmr.inner_join_using",
                "SELECT id FROM (SELECT id, qdrant_nearest_with_mmr_score(embedding, 0.9, 8, 1.0, \
                 0.0) AS score FROM vectors ORDER BY score DESC LIMIT 2) lhs JOIN (SELECT id, \
                 qdrant_nearest_with_mmr_score(embedding, 0.5, 8, 0.0, 1.0) AS score FROM vectors \
                 ORDER BY score DESC LIMIT 2) rhs USING (id) ORDER BY id",
            );
            pub(crate) const RIGHT_JOIN: SqlCase = SqlCase::new(
                "query.mmr.right_join",
                "SELECT rhs.id, COALESCE(lhs.score, 0.0) AS lhs_score FROM (SELECT id, \
                 qdrant_nearest_with_mmr_score(embedding, 0.9, 8, 1.0, 0.0) AS score FROM vectors \
                 ORDER BY score DESC LIMIT 2) lhs RIGHT JOIN (SELECT id, \
                 qdrant_nearest_with_mmr_score(embedding, 0.5, 8, 0.0, 1.0) AS score FROM vectors \
                 ORDER BY score DESC LIMIT 2) rhs ON lhs.id = rhs.id ORDER BY rhs.id",
            );
            pub(crate) const FULL_JOIN: SqlCase = SqlCase::new(
                "query.mmr.full_join",
                "SELECT COALESCE(lhs.id, rhs.id) AS id FROM (SELECT id, \
                 qdrant_nearest_with_mmr_score(embedding, 0.9, 8, 1.0, 0.0) AS score FROM vectors \
                 ORDER BY score DESC LIMIT 2) lhs FULL OUTER JOIN (SELECT id, \
                 qdrant_nearest_with_mmr_score(embedding, 0.5, 8, 0.0, 1.0) AS score FROM vectors \
                 ORDER BY score DESC LIMIT 2) rhs ON lhs.id = rhs.id ORDER BY id",
            );
            pub(crate) const CROSS_JOIN: SqlCase = SqlCase::new(
                "query.mmr.cross_join",
                "SELECT lhs.id, rhs.id AS rhs_id FROM (SELECT id, \
                 qdrant_nearest_with_mmr_score(embedding, 0.9, 8, 1.0, 0.0) AS score FROM vectors \
                 ORDER BY score DESC LIMIT 2) lhs CROSS JOIN (SELECT id, \
                 qdrant_nearest_with_mmr_score(embedding, 0.5, 8, 0.0, 1.0) AS score FROM vectors \
                 ORDER BY score DESC LIMIT 2) rhs ORDER BY lhs.id, rhs_id",
            );
            pub(crate) const LEFT_SEMI_JOIN: SqlCase = SqlCase::new(
                "query.mmr.left_semi_join",
                "SELECT lhs.id FROM (SELECT id, qdrant_nearest_with_mmr_score(embedding, 0.9, 8, \
                 1.0, 0.0) AS score FROM vectors ORDER BY score DESC LIMIT 2) lhs LEFT SEMI JOIN \
                 (SELECT id, qdrant_nearest_with_mmr_score(embedding, 0.5, 8, 0.0, 1.0) AS score \
                 FROM vectors ORDER BY score DESC LIMIT 2) rhs ON lhs.id = rhs.id ORDER BY lhs.id",
            );
            pub(crate) const LEFT_ANTI_JOIN: SqlCase = SqlCase::new(
                "query.mmr.left_anti_join",
                "SELECT lhs.id FROM (SELECT id, qdrant_nearest_with_mmr_score(embedding, 0.9, 8, \
                 1.0, 0.0) AS score FROM vectors ORDER BY score DESC LIMIT 2) lhs LEFT ANTI JOIN \
                 (SELECT id, qdrant_nearest_with_mmr_score(embedding, 0.5, 8, 0.0, 1.0) AS score \
                 FROM vectors ORDER BY score DESC LIMIT 1) rhs ON lhs.id = rhs.id ORDER BY lhs.id",
            );
            pub(crate) const RIGHT_SEMI_JOIN: SqlCase = SqlCase::new(
                "query.mmr.right_semi_join",
                "SELECT rhs.id FROM (SELECT id, qdrant_nearest_with_mmr_score(embedding, 0.9, 8, \
                 1.0, 0.0) AS score FROM vectors ORDER BY score DESC LIMIT 1) lhs RIGHT SEMI JOIN \
                 (SELECT id, qdrant_nearest_with_mmr_score(embedding, 0.5, 8, 0.0, 1.0) AS score \
                 FROM vectors ORDER BY score DESC LIMIT 2) rhs ON lhs.id = rhs.id ORDER BY rhs.id",
            );
            pub(crate) const RIGHT_ANTI_JOIN: SqlCase = SqlCase::new(
                "query.mmr.right_anti_join",
                "SELECT rhs.id FROM (SELECT id, qdrant_nearest_with_mmr_score(embedding, 0.9, 8, \
                 1.0, 0.0) AS score FROM vectors WHERE id = '9' ORDER BY score DESC LIMIT 1) lhs \
                 RIGHT ANTI JOIN (SELECT id, qdrant_nearest_with_mmr_score(embedding, 0.5, 8, \
                 0.0, 1.0) AS score FROM vectors ORDER BY score DESC LIMIT 2) rhs ON lhs.id = \
                 rhs.id ORDER BY rhs.id",
            );

            pub(crate) const ALL: &[SqlCase] = &[
                CANONICAL,
                SUBQUERY,
                CTE,
                WINDOW_DIRECT,
                WINDOW_OVER_SUBQUERY,
                AGGREGATE_SUBQUERY,
                AGGREGATE_CTE,
                LEFT_JOIN,
                INNER_JOIN_USING,
                RIGHT_JOIN,
                FULL_JOIN,
                CROSS_JOIN,
                LEFT_SEMI_JOIN,
                LEFT_ANTI_JOIN,
                RIGHT_SEMI_JOIN,
                RIGHT_ANTI_JOIN,
            ];
        }

        pub(crate) mod relevance {
            use super::SqlCase;

            pub(crate) const CANONICAL: SqlCase = SqlCase::new(
                "query.relevance.canonical",
                "SELECT id, qdrant_relevance_feedback_score(embedding, [1.0, 0.0], [struct([1.0, \
                 0.0], 1.0), struct([0.0, 1.0], -0.5)], 1.0, 0.5, 0.25) AS score FROM vectors \
                 ORDER BY score DESC LIMIT 2",
            );
            pub(crate) const SUBQUERY: SqlCase = SqlCase::new(
                "query.relevance.subquery",
                "SELECT id, score FROM (SELECT id, qdrant_relevance_feedback_score(embedding, \
                 [1.0, 0.0], [struct([1.0, 0.0], 1.0), struct([0.0, 1.0], -0.5)], 1.0, 0.5, 0.25) \
                 AS score FROM vectors) ranked ORDER BY score DESC LIMIT 2",
            );
            pub(crate) const CTE: SqlCase = SqlCase::new(
                "query.relevance.cte",
                "WITH ranked AS (SELECT id, qdrant_relevance_feedback_score(embedding, [1.0, \
                 0.0], [struct([1.0, 0.0], 1.0), struct([0.0, 1.0], -0.5)], 1.0, 0.5, 0.25) AS \
                 score FROM vectors) SELECT id, score FROM ranked ORDER BY score DESC LIMIT 2",
            );
            pub(crate) const WINDOW_DIRECT: SqlCase = SqlCase::new(
                "query.relevance.window_direct",
                "SELECT id, AVG(qdrant_relevance_feedback_score(embedding, [1.0, 0.0], \
                 [struct([1.0, 0.0], 1.0), struct([0.0, 1.0], -0.5)], 1.0, 0.5, 0.25)) OVER () AS \
                 avg_score FROM vectors",
            );
            pub(crate) const WINDOW_OVER_SUBQUERY: SqlCase = SqlCase::new(
                "query.relevance.window_over_subquery",
                "SELECT id, AVG(score) OVER () AS avg_score FROM (SELECT id, \
                 qdrant_relevance_feedback_score(embedding, [1.0, 0.0], [struct([1.0, 0.0], 1.0), \
                 struct([0.0, 1.0], -0.5)], 1.0, 0.5, 0.25) AS score FROM vectors) ranked ORDER \
                 BY id",
            );
            pub(crate) const AGGREGATE_SUBQUERY: SqlCase = SqlCase::new(
                "query.relevance.aggregate_subquery",
                "SELECT * FROM (SELECT AVG(qdrant_relevance_feedback_score(embedding, [1.0, 0.0], \
                 [struct([1.0, 0.0], 1.0), struct([0.0, 1.0], -0.5)], 1.0, 0.5, 0.25)) AS \
                 avg_score FROM vectors) ranked",
            );
            pub(crate) const AGGREGATE_CTE: SqlCase = SqlCase::new(
                "query.relevance.aggregate_cte",
                "WITH ranked AS (SELECT AVG(qdrant_relevance_feedback_score(embedding, [1.0, \
                 0.0], [struct([1.0, 0.0], 1.0), struct([0.0, 1.0], -0.5)], 1.0, 0.5, 0.25)) AS \
                 avg_score FROM vectors) SELECT * FROM ranked",
            );
            pub(crate) const LEFT_JOIN: SqlCase = SqlCase::new(
                "query.relevance.left_join",
                "SELECT lhs.id, COALESCE(rhs.score, 0.0) AS rhs_score FROM (SELECT id, \
                 qdrant_relevance_feedback_score(embedding, [1.0, 0.0], [struct([1.0, 0.0], 1.0), \
                 struct([0.0, 1.0], -0.5)], 1.0, 0.5, 0.25) AS score FROM vectors ORDER BY score \
                 DESC LIMIT 2) lhs LEFT JOIN (SELECT id, \
                 qdrant_relevance_feedback_score(embedding, [0.0, 1.0], [struct([0.0, 1.0], 1.0), \
                 struct([1.0, 0.0], -0.5)], 1.0, 0.5, 0.25) AS score FROM vectors ORDER BY score \
                 DESC LIMIT 2) rhs ON lhs.id = rhs.id ORDER BY lhs.id",
            );
            pub(crate) const INNER_JOIN_USING: SqlCase = SqlCase::new(
                "query.relevance.inner_join_using",
                "SELECT id FROM (SELECT id, qdrant_relevance_feedback_score(embedding, [1.0, \
                 0.0], [struct([1.0, 0.0], 1.0), struct([0.0, 1.0], -0.5)], 1.0, 0.5, 0.25) AS \
                 score FROM vectors ORDER BY score DESC LIMIT 2) lhs JOIN (SELECT id, \
                 qdrant_relevance_feedback_score(embedding, [0.0, 1.0], [struct([0.0, 1.0], 1.0), \
                 struct([1.0, 0.0], -0.5)], 1.0, 0.5, 0.25) AS score FROM vectors ORDER BY score \
                 DESC LIMIT 2) rhs USING (id) ORDER BY id",
            );
            pub(crate) const RIGHT_JOIN: SqlCase = SqlCase::new(
                "query.relevance.right_join",
                "SELECT rhs.id, COALESCE(lhs.score, 0.0) AS lhs_score FROM (SELECT id, \
                 qdrant_relevance_feedback_score(embedding, [1.0, 0.0], [struct([1.0, 0.0], 1.0), \
                 struct([0.0, 1.0], -0.5)], 1.0, 0.5, 0.25) AS score FROM vectors ORDER BY score \
                 DESC LIMIT 2) lhs RIGHT JOIN (SELECT id, \
                 qdrant_relevance_feedback_score(embedding, [0.0, 1.0], [struct([0.0, 1.0], 1.0), \
                 struct([1.0, 0.0], -0.5)], 1.0, 0.5, 0.25) AS score FROM vectors ORDER BY score \
                 DESC LIMIT 2) rhs ON lhs.id = rhs.id ORDER BY rhs.id",
            );
            pub(crate) const FULL_JOIN: SqlCase = SqlCase::new(
                "query.relevance.full_join",
                "SELECT COALESCE(lhs.id, rhs.id) AS id FROM (SELECT id, \
                 qdrant_relevance_feedback_score(embedding, [1.0, 0.0], [struct([1.0, 0.0], 1.0), \
                 struct([0.0, 1.0], -0.5)], 1.0, 0.5, 0.25) AS score FROM vectors ORDER BY score \
                 DESC LIMIT 2) lhs FULL OUTER JOIN (SELECT id, \
                 qdrant_relevance_feedback_score(embedding, [0.0, 1.0], [struct([0.0, 1.0], 1.0), \
                 struct([1.0, 0.0], -0.5)], 1.0, 0.5, 0.25) AS score FROM vectors ORDER BY score \
                 DESC LIMIT 2) rhs ON lhs.id = rhs.id ORDER BY id",
            );
            pub(crate) const CROSS_JOIN: SqlCase = SqlCase::new(
                "query.relevance.cross_join",
                "SELECT lhs.id, rhs.id AS rhs_id FROM (SELECT id, \
                 qdrant_relevance_feedback_score(embedding, [1.0, 0.0], [struct([1.0, 0.0], 1.0), \
                 struct([0.0, 1.0], -0.5)], 1.0, 0.5, 0.25) AS score FROM vectors ORDER BY score \
                 DESC LIMIT 2) lhs CROSS JOIN (SELECT id, \
                 qdrant_relevance_feedback_score(embedding, [0.0, 1.0], [struct([0.0, 1.0], 1.0), \
                 struct([1.0, 0.0], -0.5)], 1.0, 0.5, 0.25) AS score FROM vectors ORDER BY score \
                 DESC LIMIT 2) rhs ORDER BY lhs.id, rhs_id",
            );
            pub(crate) const LEFT_SEMI_JOIN: SqlCase = SqlCase::new(
                "query.relevance.left_semi_join",
                "SELECT lhs.id FROM (SELECT id, qdrant_relevance_feedback_score(embedding, [1.0, \
                 0.0], [struct([1.0, 0.0], 1.0), struct([0.0, 1.0], -0.5)], 1.0, 0.5, 0.25) AS \
                 score FROM vectors ORDER BY score DESC LIMIT 2) lhs LEFT SEMI JOIN (SELECT id, \
                 qdrant_relevance_feedback_score(embedding, [0.0, 1.0], [struct([0.0, 1.0], 1.0), \
                 struct([1.0, 0.0], -0.5)], 1.0, 0.5, 0.25) AS score FROM vectors ORDER BY score \
                 DESC LIMIT 2) rhs ON lhs.id = rhs.id ORDER BY lhs.id",
            );
            pub(crate) const LEFT_ANTI_JOIN: SqlCase = SqlCase::new(
                "query.relevance.left_anti_join",
                "SELECT lhs.id FROM (SELECT id, qdrant_relevance_feedback_score(embedding, [1.0, \
                 0.0], [struct([1.0, 0.0], 1.0), struct([0.0, 1.0], -0.5)], 1.0, 0.5, 0.25) AS \
                 score FROM vectors ORDER BY score DESC LIMIT 2) lhs LEFT ANTI JOIN (SELECT id, \
                 qdrant_relevance_feedback_score(embedding, [0.0, 1.0], [struct([0.0, 1.0], 1.0), \
                 struct([1.0, 0.0], -0.5)], 1.0, 0.5, 0.25) AS score FROM vectors ORDER BY score \
                 DESC LIMIT 1) rhs ON lhs.id = rhs.id ORDER BY lhs.id",
            );
            pub(crate) const RIGHT_SEMI_JOIN: SqlCase = SqlCase::new(
                "query.relevance.right_semi_join",
                "SELECT rhs.id FROM (SELECT id, qdrant_relevance_feedback_score(embedding, [1.0, \
                 0.0], [struct([1.0, 0.0], 1.0), struct([0.0, 1.0], -0.5)], 1.0, 0.5, 0.25) AS \
                 score FROM vectors ORDER BY score DESC LIMIT 1) lhs RIGHT SEMI JOIN (SELECT id, \
                 qdrant_relevance_feedback_score(embedding, [0.0, 1.0], [struct([0.0, 1.0], 1.0), \
                 struct([1.0, 0.0], -0.5)], 1.0, 0.5, 0.25) AS score FROM vectors ORDER BY score \
                 DESC LIMIT 2) rhs ON lhs.id = rhs.id ORDER BY rhs.id",
            );
            pub(crate) const RIGHT_ANTI_JOIN: SqlCase = SqlCase::new(
                "query.relevance.right_anti_join",
                "SELECT rhs.id FROM (SELECT id, qdrant_relevance_feedback_score(embedding, [1.0, \
                 0.0], [struct([1.0, 0.0], 1.0), struct([0.0, 1.0], -0.5)], 1.0, 0.5, 0.25) AS \
                 score FROM vectors WHERE id = '9' ORDER BY score DESC LIMIT 1) lhs RIGHT ANTI \
                 JOIN (SELECT id, qdrant_relevance_feedback_score(embedding, [0.0, 1.0], \
                 [struct([0.0, 1.0], 1.0), struct([1.0, 0.0], -0.5)], 1.0, 0.5, 0.25) AS score \
                 FROM vectors ORDER BY score DESC LIMIT 2) rhs ON lhs.id = rhs.id ORDER BY rhs.id",
            );

            pub(crate) const ALL: &[SqlCase] = &[
                CANONICAL,
                SUBQUERY,
                CTE,
                WINDOW_DIRECT,
                WINDOW_OVER_SUBQUERY,
                AGGREGATE_SUBQUERY,
                AGGREGATE_CTE,
                LEFT_JOIN,
                INNER_JOIN_USING,
                RIGHT_JOIN,
                FULL_JOIN,
                CROSS_JOIN,
                LEFT_SEMI_JOIN,
                LEFT_ANTI_JOIN,
                RIGHT_SEMI_JOIN,
                RIGHT_ANTI_JOIN,
            ];
        }

        pub(crate) mod grouped {
            use super::SqlCase;

            pub(crate) const NEAREST_ASC: SqlCase = SqlCase::new(
                "query.grouped.nearest_asc",
                concat!(
                    "SELECT id, payload:tag AS tag, score FROM (",
                    "SELECT DISTINCT ON (payload:tag) id, payload, \
                     qdrant_nearest_score(embedding, 1.0, 0.0) AS score ",
                    "FROM vectors ORDER BY payload:tag, qdrant_nearest_score(embedding, 1.0, 0.0) \
                     DESC",
                    ") grouped"
                ),
            );
            pub(crate) const NEAREST_ASC_NO_TIEBREAK: SqlCase = SqlCase::new(
                "query.grouped.nearest_asc_no_tiebreak",
                concat!(
                    "SELECT id, payload:tag AS tag, score FROM (",
                    "SELECT DISTINCT ON (payload:tag) id, payload, \
                     qdrant_nearest_score(embedding, 1.0, 0.0) AS score ",
                    "FROM vectors ORDER BY payload:tag",
                    ") grouped"
                ),
            );
            pub(crate) const NEAREST_DESC: SqlCase = SqlCase::new(
                "query.grouped.nearest_desc",
                concat!(
                    "SELECT id, payload:tag AS tag, score FROM (",
                    "SELECT DISTINCT ON (payload:tag) id, payload, \
                     qdrant_nearest_score(embedding, 1.0, 0.0) AS score ",
                    "FROM vectors ORDER BY payload:tag DESC, qdrant_nearest_score(embedding, 1.0, \
                     0.0) DESC",
                    ") grouped LIMIT 2"
                ),
            );
            pub(crate) const RECOMMEND: SqlCase = SqlCase::new(
                "query.grouped.recommend",
                concat!(
                    "SELECT id, payload:tag AS tag, score FROM (",
                    "SELECT DISTINCT ON (payload:tag) id, payload, \
                     qdrant_recommend_score(embedding, [[1.0, 0.0]], [[0.0, 1.0]]) AS score ",
                    "FROM vectors ORDER BY payload:tag",
                    ") grouped"
                ),
            );
            pub(crate) const DISCOVER: SqlCase = SqlCase::new(
                "query.grouped.discover",
                concat!(
                    "SELECT id, payload:tag AS tag, score FROM (",
                    "SELECT DISTINCT ON (payload:tag) id, payload, \
                     qdrant_discover_score(embedding, [1.0, 0.0], [[[1.0, 0.0], [0.0, 1.0]]]) AS \
                     score ",
                    "FROM vectors ORDER BY payload:tag",
                    ") grouped"
                ),
            );
            pub(crate) const CONTEXT: SqlCase = SqlCase::new(
                "query.grouped.context",
                concat!(
                    "SELECT id, payload:tag AS tag, score FROM (",
                    "SELECT DISTINCT ON (payload:tag) id, payload, \
                     qdrant_context_score(embedding, [[[1.0, 0.0], [0.0, 1.0]]]) AS score ",
                    "FROM vectors ORDER BY payload:tag",
                    ") grouped"
                ),
            );
            pub(crate) const CTE: SqlCase = SqlCase::new(
                "query.grouped.cte",
                concat!(
                    "WITH grouped AS (",
                    "SELECT DISTINCT ON (payload:tag) id, payload, \
                     qdrant_nearest_score(embedding, 1.0, 0.0) AS score ",
                    "FROM vectors ORDER BY payload:tag) ",
                    "SELECT id, payload:tag AS tag, score FROM grouped ORDER BY tag"
                ),
            );
            pub(crate) const WINDOW_OVER_GROUPED: SqlCase = SqlCase::new(
                "query.grouped.window_over_grouped",
                concat!(
                    "WITH grouped AS (",
                    "SELECT DISTINCT ON (payload:tag) id, payload, \
                     qdrant_nearest_score(embedding, 1.0, 0.0) AS score ",
                    "FROM vectors ORDER BY payload:tag) ",
                    "SELECT id, tag, score, ROW_NUMBER() OVER (ORDER BY tag, id) AS row_num FROM \
                     (SELECT id, payload:tag AS tag, score FROM grouped) labeled ORDER BY row_num"
                ),
            );
            pub(crate) const SUBQUERY: SqlCase = SqlCase::new(
                "query.grouped.subquery",
                concat!(
                    "SELECT id, tag FROM (",
                    "SELECT id, payload:tag AS tag, score FROM (",
                    "SELECT DISTINCT ON (payload:tag) id, payload, \
                     qdrant_nearest_score(embedding, 1.0, 0.0) AS score ",
                    "FROM vectors ORDER BY payload:tag",
                    ") grouped",
                    ") labeled ORDER BY tag, id"
                ),
            );
            pub(crate) const HAVING_SUBQUERY: SqlCase = SqlCase::new(
                "query.grouped.having_subquery",
                concat!(
                    "SELECT tag, MAX(score) AS max_score FROM (",
                    "SELECT id, payload:tag AS tag, score FROM (",
                    "SELECT DISTINCT ON (payload:tag) id, payload, \
                     qdrant_nearest_score(embedding, 1.0, 0.0) AS score ",
                    "FROM vectors ORDER BY payload:tag",
                    ") grouped",
                    ") labeled GROUP BY tag HAVING MAX(score) >= 0.0 ORDER BY tag"
                ),
            );
            pub(crate) const UNION_ALL_SUBQUERY: SqlCase = SqlCase::new(
                "query.grouped.union_all_subquery",
                concat!(
                    "SELECT * FROM (",
                    "SELECT DISTINCT ON (payload:tag) id, payload, \
                     qdrant_nearest_score(embedding, 1.0, 0.0) AS score ",
                    "FROM vectors ORDER BY payload:tag",
                    ") grouped_a UNION ALL SELECT * FROM (",
                    "SELECT DISTINCT ON (payload:tag) id, payload, \
                     qdrant_nearest_score(embedding, 1.0, 0.0) AS score ",
                    "FROM vectors ORDER BY payload:tag",
                    ") grouped_b"
                ),
            );

            pub(crate) const ALL: &[SqlCase] = &[
                NEAREST_ASC,
                NEAREST_ASC_NO_TIEBREAK,
                NEAREST_DESC,
                RECOMMEND,
                DISCOVER,
                CONTEXT,
                SUBQUERY,
                HAVING_SUBQUERY,
                UNION_ALL_SUBQUERY,
                CTE,
                WINDOW_OVER_GROUPED,
            ];
        }
    }

    pub(crate) mod coordination {
        use super::SqlCase;

        pub(crate) mod formula {
            use super::SqlCase;

            pub(crate) const WITHOUT_LIMIT: SqlCase = SqlCase::new(
                "coordination.formula.without_limit",
                concat!(
                    "SELECT dense.id, qdrant_formula_score(dense.score + sparse.score) AS score ",
                    "FROM (SELECT id, qdrant_nearest_score(embedding, 1.0, 0.0) AS score FROM \
                     vectors) dense ",
                    "FULL OUTER JOIN ",
                    "(SELECT id, qdrant_nearest_score(aux, 0.0, 1.0) AS score FROM vectors) \
                     sparse ",
                    "USING (id) ORDER BY score DESC"
                ),
            );
            pub(crate) const CANONICAL: SqlCase = SqlCase::new(
                "coordination.formula.canonical",
                concat!(
                    "SELECT dense.id, qdrant_formula_score(dense.score + sparse.score) AS score ",
                    "FROM (SELECT id, qdrant_nearest_score(embedding, 1.0, 0.0) AS score ",
                    "FROM vectors ORDER BY score DESC LIMIT 5) dense ",
                    "FULL OUTER JOIN ",
                    "(SELECT id, qdrant_nearest_score(aux, 0.0, 1.0) AS score ",
                    "FROM vectors ORDER BY score DESC LIMIT 5) sparse ",
                    "USING (id) ORDER BY score DESC LIMIT 2"
                ),
            );
            pub(crate) const ALIAS_WRAPPED: SqlCase = SqlCase::new(
                "coordination.formula.alias_wrapped",
                concat!(
                    "SELECT ranked.id, ranked.score FROM (",
                    "SELECT dense.id AS id, qdrant_formula_score(dense.score + sparse.score) AS \
                     score ",
                    "FROM (SELECT id, qdrant_nearest_score(embedding, 1.0, 0.0) AS score ",
                    "FROM vectors ORDER BY score DESC LIMIT 5) dense ",
                    "FULL OUTER JOIN ",
                    "(SELECT id, qdrant_nearest_score(aux, 0.0, 1.0) AS score ",
                    "FROM vectors ORDER BY score DESC LIMIT 5) sparse ",
                    "USING (id)) ranked ORDER BY ranked.score DESC LIMIT 2"
                ),
            );
            pub(crate) const REDUNDANT_SORT: SqlCase = SqlCase::new(
                "coordination.formula.redundant_sort",
                concat!(
                    "SELECT ranked.id, ranked.score FROM (",
                    "SELECT dense.id AS id, qdrant_formula_score(dense.score + sparse.score) AS \
                     score ",
                    "FROM (SELECT id, qdrant_nearest_score(embedding, 1.0, 0.0) AS score ",
                    "FROM vectors ORDER BY score DESC LIMIT 5) dense ",
                    "FULL OUTER JOIN ",
                    "(SELECT id, qdrant_nearest_score(aux, 0.0, 1.0) AS score ",
                    "FROM vectors ORDER BY score DESC LIMIT 5) sparse ",
                    "USING (id) ORDER BY score DESC) ranked ORDER BY ranked.score DESC LIMIT 2"
                ),
            );
            pub(crate) const ALIAS_THREADING: SqlCase = SqlCase::new(
                "coordination.formula.alias_threading",
                concat!(
                    "SELECT final.id, final.score FROM (",
                    "SELECT ranked.id AS id, ranked.score AS score FROM (",
                    "SELECT dense.id AS id, qdrant_formula_score(dense.score + sparse.score) AS \
                     score ",
                    "FROM (SELECT id, qdrant_nearest_score(embedding, 1.0, 0.0) AS score ",
                    "FROM vectors ORDER BY score DESC LIMIT 5) dense ",
                    "FULL OUTER JOIN ",
                    "(SELECT id, qdrant_nearest_score(aux, 0.0, 1.0) AS score ",
                    "FROM vectors ORDER BY score DESC LIMIT 5) sparse ",
                    "USING (id)) ranked) final ORDER BY final.score DESC LIMIT 2"
                ),
            );
            pub(crate) const SORT_ONLY: SqlCase = SqlCase::new(
                "coordination.formula.sort_only",
                concat!(
                    "SELECT dense.id AS id FROM ",
                    "(SELECT id, qdrant_nearest_score(embedding, 1.0, 0.0) AS score ",
                    "FROM vectors ORDER BY score DESC LIMIT 5) dense ",
                    "FULL OUTER JOIN ",
                    "(SELECT id, qdrant_nearest_score(aux, 0.0, 1.0) AS score ",
                    "FROM vectors ORDER BY score DESC LIMIT 5) sparse ",
                    "USING (id) ORDER BY qdrant_formula_score(dense.score + sparse.score) DESC \
                     LIMIT 2"
                ),
            );
            pub(crate) const LEFT_JOIN: SqlCase = SqlCase::new(
                "coordination.formula.left_join",
                concat!(
                    "SELECT dense.id, qdrant_formula_score(dense.score + sparse.score) AS score \
                     FROM ",
                    "(SELECT id, qdrant_nearest_score(embedding, 1.0, 0.0) AS score FROM vectors \
                     ORDER BY score DESC LIMIT 5) dense ",
                    "LEFT JOIN (SELECT id, qdrant_nearest_score(aux, 0.0, 1.0) AS score FROM \
                     vectors ORDER BY score DESC LIMIT 5) sparse ON dense.id = sparse.id ",
                    "ORDER BY score DESC LIMIT 2"
                ),
            );
            pub(crate) const CROSS_JOIN: SqlCase = SqlCase::new(
                "coordination.formula.cross_join",
                concat!(
                    "SELECT dense.id, qdrant_formula_score(dense.score + sparse.score) AS score \
                     FROM ",
                    "(SELECT id, qdrant_nearest_score(embedding, 1.0, 0.0) AS score FROM vectors \
                     ORDER BY score DESC LIMIT 5) dense ",
                    "CROSS JOIN (SELECT id, qdrant_nearest_score(aux, 0.0, 1.0) AS score FROM \
                     vectors ORDER BY score DESC LIMIT 5) sparse ",
                    "ORDER BY score DESC LIMIT 2"
                ),
            );
            pub(crate) const RIGHT_JOIN: SqlCase = SqlCase::new(
                "coordination.formula.right_join",
                concat!(
                    "SELECT sparse.id, qdrant_formula_score(dense.score + sparse.score) AS score \
                     FROM ",
                    "(SELECT id, qdrant_nearest_score(embedding, 1.0, 0.0) AS score FROM vectors \
                     ORDER BY score DESC LIMIT 5) dense ",
                    "RIGHT JOIN (SELECT id, qdrant_nearest_score(aux, 0.0, 1.0) AS score FROM \
                     vectors ORDER BY score DESC LIMIT 5) sparse ON dense.id = sparse.id ",
                    "ORDER BY score DESC LIMIT 2"
                ),
            );
            pub(crate) const INNER_JOIN_QDRANT_ONLY_LEAF: SqlCase = SqlCase::new(
                "coordination.formula.inner_join_qdrant_only_leaf",
                concat!(
                    "SELECT dense.id, qdrant_formula_score(dense.score + \
                     qdrant_condition(qdrant_payload_num('rank') > 0)) AS score FROM ",
                    "(SELECT id, qdrant_nearest_score(embedding, 1.0, 0.0) AS score FROM vectors \
                     ORDER BY score DESC LIMIT 5) dense ",
                    "JOIN (SELECT id, qdrant_nearest_score(aux, 0.0, 1.0) AS score FROM vectors \
                     ORDER BY score DESC LIMIT 5) sparse ON dense.id = sparse.id ",
                    "ORDER BY score DESC LIMIT 2"
                ),
            );

            pub(crate) const ALL: &[SqlCase] = &[
                WITHOUT_LIMIT,
                CANONICAL,
                ALIAS_WRAPPED,
                REDUNDANT_SORT,
                ALIAS_THREADING,
                SORT_ONLY,
                LEFT_JOIN,
                CROSS_JOIN,
                RIGHT_JOIN,
                INNER_JOIN_QDRANT_ONLY_LEAF,
            ];
        }

        pub(crate) mod fusion {
            use super::SqlCase;

            pub(crate) const WITHOUT_LIMIT: SqlCase = SqlCase::new(
                "coordination.fusion.without_limit",
                concat!(
                    "SELECT id, qdrant_fusion_score('RRF', dense.score, sparse.score) AS score \
                     FROM ",
                    "(SELECT id, qdrant_nearest_score(embedding, 1.0, 0.0) AS score FROM vectors) \
                     dense ",
                    "FULL OUTER JOIN ",
                    "(SELECT id, qdrant_nearest_score(aux, 0.0, 1.0) AS score FROM vectors) \
                     sparse ",
                    "USING (id) ORDER BY score DESC"
                ),
            );
            pub(crate) const CANONICAL: SqlCase = SqlCase::new(
                "coordination.fusion.canonical",
                concat!(
                    "SELECT id, qdrant_fusion_score('RRF', dense.score, sparse.score) AS score ",
                    "FROM (SELECT id, qdrant_nearest_score(embedding, 1.0, 0.0) AS score ",
                    "FROM vectors ORDER BY score DESC LIMIT 5) dense ",
                    "FULL OUTER JOIN ",
                    "(SELECT id, qdrant_nearest_score(aux, 0.0, 1.0) AS score ",
                    "FROM vectors ORDER BY score DESC LIMIT 5) sparse ",
                    "USING (id) ORDER BY score DESC LIMIT 2"
                ),
            );
            pub(crate) const ALIAS_WRAPPED: SqlCase = SqlCase::new(
                "coordination.fusion.alias_wrapped",
                concat!(
                    "SELECT ranked.id, ranked.score FROM (",
                    "SELECT dense.id AS id, qdrant_fusion_score('RRF', dense.score, sparse.score) \
                     AS score ",
                    "FROM (SELECT id, qdrant_nearest_score(embedding, 1.0, 0.0) AS score ",
                    "FROM vectors ORDER BY score DESC LIMIT 5) dense ",
                    "FULL OUTER JOIN ",
                    "(SELECT id, qdrant_nearest_score(aux, 0.0, 1.0) AS score ",
                    "FROM vectors ORDER BY score DESC LIMIT 5) sparse ",
                    "USING (id)) ranked ORDER BY ranked.score DESC LIMIT 2"
                ),
            );
            pub(crate) const ALIAS_THREADING: SqlCase = SqlCase::new(
                "coordination.fusion.alias_threading",
                concat!(
                    "SELECT final.id, final.score FROM (",
                    "SELECT ranked.id AS id, ranked.score AS score FROM (",
                    "SELECT dense.id AS id, qdrant_fusion_score('RRF', dense.score, sparse.score) \
                     AS score ",
                    "FROM (SELECT id, qdrant_nearest_score(embedding, 1.0, 0.0) AS score ",
                    "FROM vectors ORDER BY score DESC LIMIT 5) dense ",
                    "FULL OUTER JOIN ",
                    "(SELECT id, qdrant_nearest_score(aux, 0.0, 1.0) AS score ",
                    "FROM vectors ORDER BY score DESC LIMIT 5) sparse ",
                    "USING (id)) ranked) final ORDER BY final.score DESC LIMIT 2"
                ),
            );

            pub(crate) const ALL: &[SqlCase] =
                &[WITHOUT_LIMIT, CANONICAL, ALIAS_WRAPPED, ALIAS_THREADING];
        }
    }
}

pub(crate) mod unsupported {
    use super::UnsupportedSqlCase;

    pub(crate) mod scan {
        use super::UnsupportedSqlCase;

        pub(crate) mod projection {
            use super::UnsupportedSqlCase;

            pub(crate) const UNNEST_RAW_PAYLOAD_ARITHMETIC: UnsupportedSqlCase =
                UnsupportedSqlCase::upstream(
                    "scan.projection.unnest_raw_payload_arithmetic",
                    "SELECT id, item FROM vectors CROSS JOIN UNNEST([payload:rank + 1]) AS \
                     t(item) ORDER BY id",
                    "unnest inputs are not lateral, so raw qdrant payload expressions are not \
                     admitted there today",
                    "No field named payload",
                );
            pub(crate) const TEXT_MATCH_WRAPPED_PROJECTION: UnsupportedSqlCase =
                UnsupportedSqlCase::by_design(
                    "scan.projection.text_match_wrapped_projection",
                    "SELECT COALESCE(payload_text_match(payload:description, 'good cheap'), \
                     false) AS matched FROM vectors ORDER BY id",
                    "text-match stays a remote-only predicate and is not locally executable when \
                     wrapped as a projection expression",
                    "payload_text_match",
                );

            pub(crate) const TEXT_MATCH_CASE_PROJECTION: UnsupportedSqlCase =
                UnsupportedSqlCase::by_design(
                    "scan.projection.text_match_case_projection",
                    "SELECT CASE WHEN payload_text_match(payload:description, 'good cheap') THEN \
                     1 ELSE 0 END AS matched FROM vectors ORDER BY id",
                    "text-match stays a remote-only predicate and is not locally executable when \
                     nested inside projection expressions",
                    "payload_text_match",
                );

            pub(crate) const ALL: &[UnsupportedSqlCase] = &[
                UNNEST_RAW_PAYLOAD_ARITHMETIC,
                TEXT_MATCH_WRAPPED_PROJECTION,
                TEXT_MATCH_CASE_PROJECTION,
            ];
        }

        pub(crate) mod ordering {
            use super::UnsupportedSqlCase;

            pub(crate) const REMOTE_ONLY_PREDICATE: UnsupportedSqlCase =
                UnsupportedSqlCase::by_design(
                    "scan.ordering.remote_only_predicate",
                    "SELECT id FROM vectors ORDER BY payload_text_match(payload:description, \
                     'good cheap')",
                    "text-match stays a remote-only predicate and is not locally executable as an \
                     ordering key",
                    "payload_text_match",
                );
            pub(crate) const ORDER_BY_SCORE_NON_PATH: UnsupportedSqlCase =
                UnsupportedSqlCase::by_design(
                    "scan.ordering.order_by_score_non_path",
                    "SELECT id, qdrant_order_by_score(ABS(CAST(payload:rank AS DOUBLE)), true) AS \
                     score FROM vectors ORDER BY score DESC",
                    "qdrant_order_by_score still requires a canonical payload path input rather \
                     than an arbitrary scalar expression",
                    "qdrant_order_by_score requires a qdrant payload path",
                );
            pub(crate) const TEXT_MATCH_NULLS_LAST: UnsupportedSqlCase =
                UnsupportedSqlCase::by_design(
                    "scan.ordering.text_match_nulls_last",
                    "SELECT id FROM vectors ORDER BY payload_text_match(payload:description, \
                     'good cheap') NULLS LAST, id",
                    "text-match stays a remote-only predicate and is not locally executable as an \
                     ordering key, including NULLS FIRST/LAST variants",
                    "payload_text_match",
                );
            pub(crate) const ALL: &[UnsupportedSqlCase] =
                &[REMOTE_ONLY_PREDICATE, ORDER_BY_SCORE_NON_PATH, TEXT_MATCH_NULLS_LAST];
        }

        pub(crate) mod filters {
            use super::UnsupportedSqlCase;
            pub(crate) const TEXT_MATCH_PROJECTION: UnsupportedSqlCase =
                UnsupportedSqlCase::by_design(
                    "scan.filters.text_match_projection",
                    "SELECT payload_text_match(payload:description, 'good cheap') AS matched FROM \
                     vectors ORDER BY id",
                    "text-match stays a remote-only predicate and is not locally executable as a \
                     projection",
                    "payload_text_match",
                );
            pub(crate) const PHRASE_MATCH_PROJECTION: UnsupportedSqlCase =
                UnsupportedSqlCase::by_design(
                    "scan.filters.phrase_match_projection",
                    "SELECT payload_phrase_match(payload:description, 'time is a flat circle') AS \
                     matched FROM vectors ORDER BY id",
                    "phrase-match stays a remote-only predicate and is not locally executable as \
                     a projection",
                    "payload_phrase_match",
                );
            pub(crate) const NESTED_MATCH_PROJECTION: UnsupportedSqlCase =
                UnsupportedSqlCase::by_design(
                    "scan.filters.nested_match_projection",
                    "SELECT payload_nested_match(payload:metadata, payload:rank >= 20 AND \
                     payload:tag = 'red') AS matched FROM vectors ORDER BY id",
                    "nested-match currently stays a remote-only predicate and is not locally \
                     executable as a projection",
                    "payload_nested_match",
                );
            pub(crate) const ALL: &[UnsupportedSqlCase] =
                &[TEXT_MATCH_PROJECTION, PHRASE_MATCH_PROJECTION, NESTED_MATCH_PROJECTION];
        }

        pub(crate) mod aggregates {
            use super::UnsupportedSqlCase;

            pub(crate) const TEXT_MATCH_AGGREGATE: UnsupportedSqlCase =
                UnsupportedSqlCase::by_design(
                    "scan.aggregates.text_match_aggregate",
                    "SELECT COUNT(payload_text_match(payload:description, 'good cheap')) AS total \
                     FROM vectors",
                    "text-match stays a remote-only predicate and is not locally executable as an \
                     aggregate input",
                    "payload_text_match",
                );
            pub(crate) const TEXT_MATCH_CASE_AGGREGATE: UnsupportedSqlCase =
                UnsupportedSqlCase::by_design(
                    "scan.aggregates.text_match_case_aggregate",
                    "SELECT SUM(CASE WHEN payload_text_match(payload:description, 'good cheap') \
                     THEN 1 ELSE 0 END) AS total FROM vectors",
                    "text-match stays a remote-only predicate and is not locally executable when \
                     nested inside aggregate input expressions",
                    "payload_text_match",
                );
            pub(crate) const TEXT_MATCH_HAVING: UnsupportedSqlCase = UnsupportedSqlCase::by_design(
                "scan.aggregates.text_match_having",
                "SELECT payload:tag AS tag, COUNT(*) AS total FROM vectors GROUP BY payload:tag \
                 HAVING MAX(payload_text_match(payload:description, 'good cheap'))",
                "text-match stays a remote-only predicate and is not locally executable inside \
                 HAVING aggregate semantics",
                "payload_text_match requires exact qdrant text-index filter pushdown",
            );

            pub(crate) const ALL: &[UnsupportedSqlCase] =
                &[TEXT_MATCH_AGGREGATE, TEXT_MATCH_CASE_AGGREGATE, TEXT_MATCH_HAVING];
        }
    }

    pub(crate) mod writes {
        use super::UnsupportedSqlCase;

        pub(crate) mod append {
            use super::UnsupportedSqlCase;

            pub(crate) const SCHEMA_MISMATCH: UnsupportedSqlCase = UnsupportedSqlCase::new(
                "writes.append.schema_mismatch",
                "INSERT INTO vectors SELECT id, vector FROM staging",
                "Q-041",
                "insert_into requires schema-equivalent append input today",
                "Column count doesn't match insert query!",
            );
            pub(crate) const SUBQUERY_SCHEMA_MISMATCH: UnsupportedSqlCase = UnsupportedSqlCase::new(
                "writes.append.subquery_schema_mismatch",
                "INSERT INTO vectors SELECT * FROM (SELECT id, vector FROM staging) staged",
                "Q-041",
                "insert_into requires schema-equivalent append input today",
                "Column count doesn't match insert query!",
            );
            pub(crate) const CTE_INSERT: UnsupportedSqlCase = UnsupportedSqlCase::upstream(
                "writes.append.cte_insert",
                "WITH staged AS (SELECT id, payload, vector FROM staging) INSERT INTO vectors \
                 SELECT * FROM staged",
                "DataFusion still does not admit INSERT INTO with a preceding CTE in this shape",
                "not implemented yet",
            );
            pub(crate) const UNION_ALL_SCHEMA_MISMATCH: UnsupportedSqlCase =
                UnsupportedSqlCase::new(
                    "writes.append.union_all_schema_mismatch",
                    "INSERT INTO vectors SELECT id, vector FROM staging UNION ALL SELECT id, \
                     vector FROM staging",
                    "Q-041",
                    "insert_into requires schema-equivalent append input today",
                    "Column count doesn't match insert query!",
                );
            pub(crate) const EXTRA_COLUMN: UnsupportedSqlCase = UnsupportedSqlCase::new(
                "writes.append.extra_column",
                "INSERT INTO vectors SELECT id, payload, vector, id AS copy_id FROM staging",
                "Q-041",
                "insert_into requires schema-equivalent append input today",
                "Column count doesn't match insert query!",
            );
            pub(crate) const WINDOW_EXTRA_COLUMN: UnsupportedSqlCase = UnsupportedSqlCase::new(
                "writes.append.window_extra_column",
                "INSERT INTO vectors SELECT id, payload, vector, ROW_NUMBER() OVER (ORDER BY id) \
                 AS row_num FROM staging",
                "Q-041",
                "insert_into requires schema-equivalent append input today",
                "Column count doesn't match insert query!",
            );
            pub(crate) const EXISTS_EXTRA_COLUMN: UnsupportedSqlCase = UnsupportedSqlCase::new(
                "writes.append.exists_extra_column",
                "INSERT INTO vectors SELECT id, payload, vector, EXISTS (SELECT 1 FROM staging \
                 other WHERE other.id = staging.id) AS seen FROM staging",
                "Q-041",
                "insert_into requires schema-equivalent append input today",
                "Column count doesn't match insert query!",
            );

            pub(crate) const ALL: &[UnsupportedSqlCase] = &[
                SCHEMA_MISMATCH,
                SUBQUERY_SCHEMA_MISMATCH,
                CTE_INSERT,
                UNION_ALL_SCHEMA_MISMATCH,
                EXTRA_COLUMN,
                WINDOW_EXTRA_COLUMN,
                EXISTS_EXTRA_COLUMN,
            ];
        }
    }

    pub(crate) mod query {
        use super::UnsupportedSqlCase;

        pub(crate) mod nearest {
            use super::UnsupportedSqlCase;
            pub(crate) const WIDTH_MISMATCH: UnsupportedSqlCase = UnsupportedSqlCase::invalid(
                "query.nearest.width_mismatch",
                "SELECT id, qdrant_nearest_score(vector, 1.0) AS score FROM vectors ORDER BY \
                 score DESC LIMIT 2",
                "nearest query vectors must still match the declared source vector width",
                "query vector width does not match source vector width",
            );

            pub(crate) const ALL: &[UnsupportedSqlCase] = &[WIDTH_MISMATCH];
        }

        pub(crate) mod sample {
            use super::UnsupportedSqlCase;
            pub(crate) const ALL: &[UnsupportedSqlCase] = &[];
        }

        pub(crate) mod recommend {
            use super::UnsupportedSqlCase;
            pub(crate) const ALL: &[UnsupportedSqlCase] = &[];
        }

        pub(crate) mod discover {
            use super::UnsupportedSqlCase;
            pub(crate) const ALL: &[UnsupportedSqlCase] = &[];
        }

        pub(crate) mod context {
            use super::UnsupportedSqlCase;
            pub(crate) const ALL: &[UnsupportedSqlCase] = &[];
        }

        pub(crate) mod mmr {
            use super::UnsupportedSqlCase;
            pub(crate) const ALL: &[UnsupportedSqlCase] = &[];
        }

        pub(crate) mod relevance {
            use super::UnsupportedSqlCase;
            pub(crate) const ALL: &[UnsupportedSqlCase] = &[];
        }

        pub(crate) mod grouped {
            use super::UnsupportedSqlCase;

            pub(crate) const MULTI_KEY_SUBQUERY: UnsupportedSqlCase = UnsupportedSqlCase::new(
                "query.grouped.multi_key_subquery",
                concat!(
                    "SELECT * FROM (",
                    "SELECT DISTINCT ON (payload:tag, id) id, payload, \
                     qdrant_nearest_score(embedding, 1.0, 0.0) AS score ",
                    "FROM vectors ORDER BY payload:tag, id",
                    ") grouped"
                ),
                "Q-049",
                "grouped retrieval still only admits the current single-key DISTINCT ON subset \
                 that can close to a grouped kernel",
                "qdrant processing must close to a kernel or stay region-owned",
            );
            pub(crate) const ALL: &[UnsupportedSqlCase] = &[MULTI_KEY_SUBQUERY];
        }
    }

    pub(crate) mod coordination {
        use super::UnsupportedSqlCase;

        pub(crate) mod formula {
            use super::UnsupportedSqlCase;

            pub(crate) const ALL: &[UnsupportedSqlCase] = &[];
        }

        pub(crate) mod fusion {
            use super::UnsupportedSqlCase;
            pub(crate) const INNER_JOIN: UnsupportedSqlCase = UnsupportedSqlCase::new(
                "coordination.fusion.inner_join",
                concat!(
                    "SELECT dense.id AS id, qdrant_fusion_score('RRF', dense.score, sparse.score) \
                     AS score FROM ",
                    "(SELECT id, qdrant_nearest_score(embedding, 1.0, 0.0) AS score FROM vectors \
                     ORDER BY score DESC LIMIT 5) dense ",
                    "JOIN (SELECT id, qdrant_nearest_score(aux, 0.0, 1.0) AS score FROM vectors \
                     ORDER BY score DESC LIMIT 5) sparse ON dense.id = sparse.id ",
                    "ORDER BY score DESC LIMIT 2"
                ),
                "Q-038",
                "coordinated fusion rewrite remains intentionally narrow outside admitted \
                 full-outer coordination",
                "unsupported coordinated qdrant_fusion_score shape",
            );
            pub(crate) const LEFT_JOIN: UnsupportedSqlCase = UnsupportedSqlCase::new(
                "coordination.fusion.left_join",
                concat!(
                    "SELECT dense.id AS id, qdrant_fusion_score('RRF', dense.score, sparse.score) \
                     AS score FROM ",
                    "(SELECT id, qdrant_nearest_score(embedding, 1.0, 0.0) AS score FROM vectors \
                     ORDER BY score DESC LIMIT 5) dense ",
                    "LEFT JOIN (SELECT id, qdrant_nearest_score(aux, 0.0, 1.0) AS score FROM \
                     vectors ORDER BY score DESC LIMIT 5) sparse ON dense.id = sparse.id ",
                    "ORDER BY score DESC LIMIT 2"
                ),
                "Q-038",
                "coordinated fusion rewrite remains intentionally narrow outside admitted \
                 full-outer coordination",
                "unsupported coordinated qdrant_fusion_score shape",
            );
            pub(crate) const CROSS_JOIN: UnsupportedSqlCase = UnsupportedSqlCase::new(
                "coordination.fusion.cross_join",
                concat!(
                    "SELECT dense.id AS id, qdrant_fusion_score('RRF', dense.score, sparse.score) \
                     AS score FROM ",
                    "(SELECT id, qdrant_nearest_score(embedding, 1.0, 0.0) AS score FROM vectors \
                     ORDER BY score DESC LIMIT 5) dense ",
                    "CROSS JOIN (SELECT id, qdrant_nearest_score(aux, 0.0, 1.0) AS score FROM \
                     vectors ORDER BY score DESC LIMIT 5) sparse ",
                    "ORDER BY score DESC LIMIT 2"
                ),
                "Q-038",
                "coordinated fusion rewrite remains intentionally narrow outside admitted \
                 full-outer coordination",
                "unsupported coordinated qdrant_fusion_score shape",
            );
            pub(crate) const RIGHT_JOIN: UnsupportedSqlCase = UnsupportedSqlCase::new(
                "coordination.fusion.right_join",
                concat!(
                    "SELECT sparse.id AS id, qdrant_fusion_score('RRF', dense.score, \
                     sparse.score) AS score FROM ",
                    "(SELECT id, qdrant_nearest_score(embedding, 1.0, 0.0) AS score FROM vectors \
                     ORDER BY score DESC LIMIT 5) dense ",
                    "RIGHT JOIN (SELECT id, qdrant_nearest_score(aux, 0.0, 1.0) AS score FROM \
                     vectors ORDER BY score DESC LIMIT 5) sparse ON dense.id = sparse.id ",
                    "ORDER BY score DESC LIMIT 2"
                ),
                "Q-038",
                "coordinated fusion rewrite remains intentionally narrow outside admitted \
                 full-outer coordination",
                "unsupported coordinated qdrant_fusion_score shape",
            );

            pub(crate) const ALL: &[UnsupportedSqlCase] =
                &[INNER_JOIN, LEFT_JOIN, CROSS_JOIN, RIGHT_JOIN];
        }
    }
}
