DROP INDEX IF EXISTS documents_body_gin_idx;
\timing on
CREATE INDEX documents_body_gin_idx ON documents USING gin (body_tsv);
\timing off
SELECT pg_relation_size('documents_body_gin_idx') AS index_bytes;
VACUUM (ANALYZE) documents;
CHECKPOINT;
