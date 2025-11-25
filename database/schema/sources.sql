CREATE SCHEMA IF NOT EXISTS sources;

CREATE TABLE IF NOT EXISTS sources.persons_searched (
    id SERIAL PRIMARY KEY,
    person_name TEXT NOT NULL,
    search_query TEXT NOT NULL,
    searched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(person_name, search_query, searched_at)
);

CREATE TABLE IF NOT EXISTS sources.search_results (
    id SERIAL PRIMARY KEY,
    person_search_id INTEGER NOT NULL REFERENCES sources.persons_searched(id) ON DELETE CASCADE,
    rank INTEGER NOT NULL,
    url TEXT NOT NULL,
    title TEXT,
    fetch_status TEXT NOT NULL CHECK (fetch_status IN ('pending', 'success', 'failed')),
    fetch_error TEXT,
    full_text TEXT,
    fetched_at TIMESTAMP,
    extraction_method TEXT CHECK (extraction_method IN ('html', 'pdf_basic', 'pdf_ocr')),
    extraction_quality TEXT CHECK (extraction_quality IN ('good', 'poor', 'failed')),
    needs_ocr BOOLEAN DEFAULT FALSE,
    provenance_narrative TEXT,
    UNIQUE(person_search_id, url)
);

CREATE TABLE IF NOT EXISTS sources.chunks (
    id SERIAL PRIMARY KEY,
    search_result_id INTEGER NOT NULL REFERENCES sources.search_results(id) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL,
    start_token INTEGER NOT NULL,
    end_token INTEGER NOT NULL,
    char_start INTEGER NOT NULL,
    char_end INTEGER NOT NULL,
    token_count INTEGER NOT NULL,
    text TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(search_result_id, chunk_index)
);

CREATE TABLE IF NOT EXISTS sources.embeddings (
    id SERIAL PRIMARY KEY,
    chunk_id INTEGER NOT NULL REFERENCES sources.chunks(id) ON DELETE CASCADE,
    model TEXT NOT NULL,
    embedding FLOAT8[] NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(chunk_id)
);

CREATE INDEX idx_search_results_person ON sources.search_results(person_search_id);
CREATE INDEX idx_search_results_status ON sources.search_results(fetch_status);
CREATE INDEX idx_persons_searched_name ON sources.persons_searched(person_name);
CREATE INDEX idx_chunks_search_result ON sources.chunks(search_result_id);
CREATE INDEX idx_embeddings_chunk ON sources.embeddings(chunk_id);