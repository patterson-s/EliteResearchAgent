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
    UNIQUE(person_search_id, url)
);

CREATE INDEX idx_search_results_person ON sources.search_results(person_search_id);
CREATE INDEX idx_search_results_status ON sources.search_results(fetch_status);
CREATE INDEX idx_persons_searched_name ON sources.persons_searched(person_name);
