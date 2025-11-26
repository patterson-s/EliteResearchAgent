-- Birth year extraction and verification tables
-- These tables store LLM-based extraction attempts and verified outcomes

CREATE SCHEMA IF NOT EXISTS services;

-- Raw extraction attempts from individual chunks
CREATE TABLE IF NOT EXISTS services.birth_extractions (
    extraction_id SERIAL PRIMARY KEY,
    chunk_id INT REFERENCES sources.chunks(chunk_id) ON DELETE CASCADE,
    person_name TEXT NOT NULL,
    extracted_year INT,
    contains_birth_info BOOLEAN NOT NULL,
    evidence_type TEXT,
    extraction_timestamp TIMESTAMPTZ DEFAULT NOW(),
    model_used TEXT,
    raw_llm_output TEXT,
    reasoning TEXT
);

CREATE INDEX idx_birth_extractions_person ON services.birth_extractions(person_name);
CREATE INDEX idx_birth_extractions_chunk ON services.birth_extractions(chunk_id);
CREATE INDEX idx_birth_extractions_year ON services.birth_extractions(extracted_year);

-- Verified birth year outcomes
CREATE TABLE IF NOT EXISTS services.birth_verifications (
    verification_id SERIAL PRIMARY KEY,
    person_name TEXT NOT NULL UNIQUE,
    birth_year INT,
    verification_status TEXT NOT NULL,
    independent_source_count INT,
    total_extractions_attempted INT,
    provenance_narrative TEXT,
    verified_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_birth_verifications_person ON services.birth_verifications(person_name);
CREATE INDEX idx_birth_verifications_status ON services.birth_verifications(verification_status);
CREATE INDEX idx_birth_verifications_year ON services.birth_verifications(birth_year);

-- Link extractions to their verification (many-to-one)
CREATE TABLE IF NOT EXISTS services.birth_verification_evidence (
    verification_id INT REFERENCES services.birth_verifications(verification_id) ON DELETE CASCADE,
    extraction_id INT REFERENCES services.birth_extractions(extraction_id) ON DELETE CASCADE,
    evidence_weight INT DEFAULT 1,
    PRIMARY KEY (verification_id, extraction_id)
);

CREATE INDEX idx_birth_verification_evidence_verification ON services.birth_verification_evidence(verification_id);
CREATE INDEX idx_birth_verification_evidence_extraction ON services.birth_verification_evidence(extraction_id);

-- View: complete birth year data with provenance chain
CREATE OR REPLACE VIEW services.birth_years_complete AS
SELECT 
    bv.verification_id,
    bv.person_name,
    bv.birth_year,
    bv.verification_status,
    bv.independent_source_count,
    bv.verified_at,
    COUNT(DISTINCT be.extraction_id) as extraction_count,
    COUNT(DISTINCT c.chunk_id) as chunk_count,
    COUNT(DISTINCT sr.search_result_id) as source_count,
    STRING_AGG(DISTINCT sr.url, '; ' ORDER BY sr.url) as source_urls
FROM services.birth_verifications bv
LEFT JOIN services.birth_verification_evidence bve ON bv.verification_id = bve.verification_id
LEFT JOIN services.birth_extractions be ON bve.extraction_id = be.extraction_id
LEFT JOIN sources.chunks c ON be.chunk_id = c.chunk_id
LEFT JOIN sources.search_results sr ON c.search_result_id = sr.search_result_id
GROUP BY 
    bv.verification_id,
    bv.person_name,
    bv.birth_year,
    bv.verification_status,
    bv.independent_source_count,
    bv.verified_at;