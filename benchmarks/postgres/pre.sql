CREATE EXTENSION IF NOT EXISTS pg_prewarm;
DROP TABLE IF EXISTS documents CASCADE;
CREATE TABLE documents (
    id TEXT NOT NULL,
    body TEXT NOT NULL,
    body_tsv TSVECTOR GENERATED ALWAYS AS (to_tsvector('simple', body)) STORED
);
