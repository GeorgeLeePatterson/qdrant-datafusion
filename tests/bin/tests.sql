-- qdrant_test_queries.sql
-- Comprehensive test suite for DataFusion-Qdrant integration
-- Tests all major Qdrant operations through SQL interface

-- ============================================================================
-- BASIC VECTOR SEARCH
-- ============================================================================

-- Single vector search (unnamed/default vector)
SELECT id, payload, V_SEARCH([0.1, 0.2, 0.3, 0.4]) as score
FROM collection_name
ORDER BY score DESC
LIMIT 10;

-- Named vector search
SELECT id, payload, V_SEARCH('text_embedding', [0.1, 0.2, 0.3]) as score
FROM collection_name
ORDER BY score DESC
LIMIT 10;

-- Search by point ID (finds similar points to given ID)
SELECT id, payload, V_SEARCH('43cf51e2-8777-4f52-bc74-c2cbde0c8b04') as score
FROM collection_name
ORDER BY score DESC
LIMIT 10;

-- Multiple named vector searches in single query
SELECT
    id,
    V_SEARCH('text_embedding', [0.1, 0.2]) as text_score,
    V_SEARCH('image_embedding', [0.3, 0.4, 0.5]) as image_score,
    payload
FROM collection_name
ORDER BY text_score * 0.7 + image_score * 0.3 DESC
LIMIT 20;

-- ============================================================================
-- DISTANCE AND FILTERING
-- ============================================================================

-- Distance calculation with specific metric
SELECT id, payload, V_DISTANCE([0.1, 0.2, 0.3], 'cosine') as distance
FROM collection_name
WHERE V_DISTANCE([0.1, 0.2, 0.3], 'cosine') < 0.5
ORDER BY distance ASC;

-- Radius search (boolean within)
SELECT id, payload
FROM collection_name
WHERE V_WITHIN([0.1, 0.2, 0.3], 0.5)
LIMIT 100;

-- Combined vector and payload filtering
SELECT id, payload, V_SEARCH([0.1, 0.2]) as score
FROM collection_name
WHERE payload->'category' = 'electronics'
  AND payload->'price' > 100
  AND payload->'price' < 500
ORDER BY score DESC
LIMIT 10;

-- ============================================================================
-- SPARSE VECTOR SEARCH
-- ============================================================================

-- Basic sparse vector search
SELECT id, payload, V_SPARSE_SEARCH([1, 100, 500], [0.1, 0.8, 0.2]) as score
FROM collection_name
ORDER BY score DESC
LIMIT 10;

-- Named sparse vector search
SELECT id, payload, V_SPARSE_SEARCH('sparse_text', [10, 20, 30], [0.5, 0.3, 0.7]) as score
FROM collection_name
ORDER BY score DESC
LIMIT 10;

-- ============================================================================
-- RECOMMENDATION
-- ============================================================================

-- Recommend with positive examples only (using point IDs)
SELECT id, payload, V_RECOMMEND(['id1', 'id2', 'id3'], NULL) as score
FROM collection_name
ORDER BY score DESC
LIMIT 10;

-- Recommend with positive and negative examples
SELECT id, payload, V_RECOMMEND(
    ['id1', 'id2'],  -- positive
    ['id3', 'id4']   -- negative
) as score
FROM collection_name
ORDER BY score DESC
LIMIT 10;

-- Recommend with vector literals instead of IDs
SELECT id, payload, V_RECOMMEND(
    [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]],  -- positive vectors
    [[0.7, 0.8, 0.9]]                     -- negative vectors
) as score
FROM collection_name
ORDER BY score DESC
LIMIT 10;

-- Named vector recommendation
SELECT id, payload, V_RECOMMEND(
    'image_embedding',
    ['img_id1', 'img_id2'],
    ['img_id3']
) as score
FROM collection_name
ORDER BY score DESC
LIMIT 10;

-- ============================================================================
-- DISCOVERY SEARCH
-- ============================================================================

-- Basic discovery (target with context)
SELECT id, payload, V_DISCOVER(
    [0.1, 0.2, 0.3],  -- target
    [['ctx_id1', 0.5], ['ctx_id2', 0.3]]  -- context pairs
) as score
FROM collection_name
ORDER BY score DESC
LIMIT 10;

-- Discovery with named vector
SELECT id, payload, V_DISCOVER(
    'text_embedding',
    [0.1, 0.2, 0.3],
    [['id1', 0.8], ['id2', 0.2]]
) as score
FROM collection_name
ORDER BY score DESC
LIMIT 10;

-- ============================================================================
-- MULTI-STAGE QUERIES (PREFETCH PATTERNS)
-- ============================================================================

-- Two-stage search: broad search then rerank
WITH prefetch AS (
    SELECT id, payload, V_SEARCH([0.1, 0.2, 0.3]) as initial_score
    FROM collection_name
    ORDER BY initial_score DESC
    LIMIT 100
)
SELECT id, payload, V_SEARCH([0.4, 0.5, 0.6]) as final_score
FROM collection_name
WHERE id IN (SELECT id FROM prefetch)
ORDER BY final_score DESC
LIMIT 10;

-- ColBERT-style multi-vector reranking
WITH prefetch AS (
    SELECT * FROM collection_name
    ORDER BY V_SEARCH([1, 23, 45, 67]) DESC
    LIMIT 100
)
SELECT id, payload, V_COLBERT(
    'colbert_vectors',
    [[0.1, 0.2, 0.3], [0.2, 0.1, 0.35], [0.8, 0.9, 0.53]]
) as score
FROM prefetch
ORDER BY score DESC
LIMIT 10;

-- ============================================================================
-- FUSION QUERIES
-- ============================================================================

-- RRF fusion of multiple searches
WITH
sparse_results AS (
    SELECT id, V_SPARSE_SEARCH([1, 42], [0.22, 0.8]) as sparse_score
    FROM collection_name
    ORDER BY sparse_score DESC
    LIMIT 20
),
dense_results AS (
    SELECT id, V_SEARCH([0.01, 0.45, 0.67]) as dense_score
    FROM collection_name
    ORDER BY dense_score DESC
    LIMIT 20
)
SELECT
    c.id,
    c.payload,
    V_FUSION([s.sparse_score, d.dense_score], 'rrf') as fused_score
FROM collection_name c
JOIN sparse_results s ON c.id = s.id
JOIN dense_results d ON c.id = d.id
ORDER BY fused_score DESC
LIMIT 10;

-- ============================================================================
-- PAYLOAD OPERATIONS
-- ============================================================================

-- Check field existence
SELECT id, payload
FROM collection_name
WHERE HAS_FIELD(payload, 'discontinued') = false
  AND V_SEARCH([0.1, 0.2]) > 0.5
LIMIT 10;

-- Full text search on payload field
SELECT id, payload, MATCH_TEXT(payload->'description', 'machine learning') as text_score
FROM collection_name
WHERE MATCH_TEXT(payload->'description', 'machine learning') > 0.0
ORDER BY text_score DESC
LIMIT 10;

-- Nested payload access
SELECT
    id,
    payload->'address'->'city' as city,
    payload->'tags' as tags,
    V_SEARCH([0.1, 0.2]) as score
FROM collection_name
WHERE payload->'address'->'country' = 'USA'
  AND payload->'tags' @> '["electronics"]'
ORDER BY score DESC
LIMIT 10;

-- ============================================================================
-- AGGREGATIONS WITH VECTOR SEARCH
-- ============================================================================

-- Group by with vector search
SELECT
    payload->'author' as author,
    COUNT(*) as point_count,
    MAX(V_SEARCH([0.1, 0.2, 0.3])) as best_score,
    AVG(V_SEARCH([0.1, 0.2, 0.3])) as avg_score
FROM collection_name
GROUP BY payload->'author'
HAVING COUNT(*) >= 3
ORDER BY best_score DESC
LIMIT 10;

-- Count vectors in radius by category
SELECT
    payload->'category' as category,
    COUNT(*) as count_in_radius
FROM collection_name
WHERE V_WITHIN([0.1, 0.2, 0.3], 0.5)
GROUP BY payload->'category'
ORDER BY count_in_radius DESC;

-- ============================================================================
-- SCORE BOOSTING
-- ============================================================================

-- Boost score based on payload conditions
WITH base_search AS (
    SELECT
        id,
        payload,
        V_SEARCH([0.2, 0.8, 0.1]) as base_score
    FROM collection_name
    ORDER BY base_score DESC
    LIMIT 50
)
SELECT
    id,
    payload,
    base_score,
    base_score +
    CASE
        WHEN payload->'tag' IN ('h1', 'h2', 'h3', 'h4') THEN 0.5
        WHEN payload->'tag' IN ('p', 'li') THEN 0.25
        ELSE 0
    END as boosted_score
FROM base_search
ORDER BY boosted_score DESC
LIMIT 10;

-- Geographic distance decay boosting
WITH base_search AS (
    SELECT
        id,
        payload,
        V_SEARCH([0.2, 0.8]) as base_score
    FROM collection_name
    LIMIT 50
)
SELECT
    id,
    payload,
    base_score,
    base_score + V_GAUSS_DECAY(
        V_GEO_DISTANCE(
            payload->'geo'->'location',
            POINT(52.504043, 13.393236)  -- Berlin
        ),
        5000  -- 5km scale
    ) as final_score
FROM base_search
ORDER BY final_score DESC
LIMIT 10;

-- ============================================================================
-- RANDOM SAMPLING
-- ============================================================================

-- Random sampling of points
SELECT id, payload
FROM collection_name
ORDER BY V_RANDOM()
LIMIT 10;

-- Random sample with filtering
SELECT id, payload
FROM collection_name
WHERE payload->'status' = 'active'
ORDER BY V_RANDOM()
LIMIT 5;

-- ============================================================================
-- BATCH OPERATIONS
-- ============================================================================

-- Batch similarity search (multiple queries)
WITH queries AS (
    SELECT * FROM (VALUES
        (1, [0.1, 0.2, 0.3]),
        (2, [0.4, 0.5, 0.6]),
        (3, [0.7, 0.8, 0.9])
    ) AS t(query_id, query_vector)
)
SELECT
    q.query_id,
    c.id as point_id,
    c.payload,
    V_SEARCH(q.query_vector) as score
FROM queries q
CROSS JOIN LATERAL (
    SELECT * FROM collection_name
    ORDER BY V_SEARCH(q.query_vector) DESC
    LIMIT 5
) c;

-- ============================================================================
-- COMPLEX COMBINATIONS
-- ============================================================================

-- Multi-vector fusion with payload filtering and score boosting
WITH
text_search AS (
    SELECT
        id,
        V_SEARCH('text_embedding', [0.1, 0.2]) as text_score
    FROM collection_name
    WHERE payload->'language' = 'en'
    ORDER BY text_score DESC
    LIMIT 50
),
image_search AS (
    SELECT
        id,
        V_SEARCH('image_embedding', [0.3, 0.4, 0.5]) as image_score
    FROM collection_name
    WHERE payload->'has_thumbnail' = true
    ORDER BY image_score DESC
    LIMIT 50
)
SELECT
    c.id,
    c.payload,
    V_FUSION([t.text_score, i.image_score], 'rrf') *
    CASE
        WHEN c.payload->'verified' = true THEN 1.2
        ELSE 1.0
    END as final_score
FROM collection_name c
JOIN text_search t ON c.id = t.id
JOIN image_search i ON c.id = i.id
WHERE c.payload->'status' = 'published'
ORDER BY final_score DESC
LIMIT 10;

-- ============================================================================
-- EDGE CASES AND SPECIAL QUERIES
-- ============================================================================

-- Empty vector search (should handle gracefully)
SELECT id FROM collection_name
WHERE V_SEARCH([]) IS NOT NULL
LIMIT 1;

-- Null handling in recommendations
SELECT id, V_RECOMMEND(['valid_id'], NULL) as score
FROM collection_name
ORDER BY score DESC
LIMIT 5;

-- Very high dimensional sparse vector
SELECT id, V_SPARSE_SEARCH(
    [1, 100, 500, 1000, 5000, 10000, 50000, 100000],
    [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]
) as score
FROM collection_name
ORDER BY score DESC
LIMIT 10;
