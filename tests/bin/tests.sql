-- Current admitted SQL examples for the collection-scan baseline.
-- This file intentionally avoids speculative search/recommend/discover syntax.

SELECT id, payload
FROM collection_name
LIMIT 10;

SELECT id, vector
FROM collection_name
ORDER BY id
LIMIT 10;

SELECT text_embedding
FROM collection_name
WHERE text_embedding IS NOT NULL
ORDER BY id;

SELECT multi_embedding, keywords
FROM collection_name
WHERE multi_embedding IS NOT NULL OR keywords IS NOT NULL;

SELECT COUNT(*)
FROM collection_name;
