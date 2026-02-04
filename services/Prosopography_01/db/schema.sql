-- Prosopography Tool Database Schema
-- Creates tables for career event extraction, validation, and supplementation

CREATE SCHEMA IF NOT EXISTS prosopography;

-- ========================================
-- CORE TABLES
-- ========================================

-- Persons being researched
CREATE TABLE IF NOT EXISTS prosopography.persons (
    person_id SERIAL PRIMARY KEY,
    person_name TEXT NOT NULL UNIQUE,
    workflow_status TEXT NOT NULL DEFAULT 'pending', -- pending, phase1_complete, phase2_reviewed, phase3_in_progress, complete
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_persons_status ON prosopography.persons(workflow_status);
CREATE INDEX IF NOT EXISTS idx_persons_name ON prosopography.persons(person_name);

-- ========================================
-- CANONICAL ORGANIZATIONS
-- ========================================

CREATE TABLE IF NOT EXISTS prosopography.canonical_organizations (
    org_id SERIAL PRIMARY KEY,
    person_id INT REFERENCES prosopography.persons(person_id) ON DELETE CASCADE,
    canonical_id TEXT NOT NULL, -- e.g., "ORG_001"
    canonical_name TEXT NOT NULL,
    org_type TEXT, -- university, government, international_org, company, research_center, ngo, commission, other
    country TEXT,
    parent_org_id INT REFERENCES prosopography.canonical_organizations(org_id),
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(person_id, canonical_id)
);

CREATE INDEX IF NOT EXISTS idx_orgs_person ON prosopography.canonical_organizations(person_id);

-- Name variations that map to canonical orgs
CREATE TABLE IF NOT EXISTS prosopography.organization_aliases (
    alias_id SERIAL PRIMARY KEY,
    org_id INT REFERENCES prosopography.canonical_organizations(org_id) ON DELETE CASCADE,
    alias_name TEXT NOT NULL,
    source_chunk_id INT, -- Reference to sources.chunks where this alias was found
    UNIQUE(org_id, alias_name)
);

-- ========================================
-- CAREER EVENTS
-- ========================================

CREATE TABLE IF NOT EXISTS prosopography.career_events (
    event_id SERIAL PRIMARY KEY,
    person_id INT REFERENCES prosopography.persons(person_id) ON DELETE CASCADE,
    event_code TEXT NOT NULL, -- e.g., "E001", "E_NEW_001"
    event_type TEXT NOT NULL, -- career_position, award
    org_id INT REFERENCES prosopography.canonical_organizations(org_id),

    -- Time period
    time_start TEXT, -- YYYY or YYYY-MM-DD
    time_end TEXT,   -- YYYY, YYYY-MM-DD, "present", NULL
    time_text TEXT,  -- Original text: "from 1986 to 1989"

    -- Event details (arrays stored as JSONB for flexibility)
    roles JSONB DEFAULT '[]',
    locations JSONB DEFAULT '[]',

    -- Status tracking
    confidence TEXT DEFAULT 'medium', -- high, medium, low
    llm_status TEXT DEFAULT 'valid', -- valid, warning, error
    validation_status TEXT DEFAULT 'pending', -- pending, validated, rejected, needs_review

    -- Audit
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_source TEXT DEFAULT 'phase1_extraction', -- phase1_extraction, phase2_correction, phase3_supplementation

    UNIQUE(person_id, event_code)
);

CREATE INDEX IF NOT EXISTS idx_events_person ON prosopography.career_events(person_id);
CREATE INDEX IF NOT EXISTS idx_events_status ON prosopography.career_events(validation_status);
CREATE INDEX IF NOT EXISTS idx_events_llm_status ON prosopography.career_events(llm_status);
CREATE INDEX IF NOT EXISTS idx_events_org ON prosopography.career_events(org_id);

-- ========================================
-- SOURCE EVIDENCE / PROVENANCE
-- ========================================

CREATE TABLE IF NOT EXISTS prosopography.source_evidence (
    evidence_id SERIAL PRIMARY KEY,
    event_id INT REFERENCES prosopography.career_events(event_id) ON DELETE CASCADE,

    -- Source identification (references existing sources.chunks table)
    chunk_id INT, -- FK to sources.chunks (not enforced to allow flexibility)
    source_url TEXT NOT NULL,
    source_type TEXT NOT NULL, -- wikipedia, news, official, academic, other

    -- The actual evidence
    verbatim_quote TEXT NOT NULL,
    quote_context TEXT, -- Surrounding context for disambiguation

    -- Evidence role
    evidence_type TEXT NOT NULL, -- original, validation, supplementation
    contribution TEXT, -- What this evidence adds: time, role, location, confirmation

    -- Processing metadata
    extraction_phase TEXT NOT NULL, -- phase1, phase2_manual, phase3
    processing_timestamp TIMESTAMPTZ DEFAULT NOW(),
    model_used TEXT
);

CREATE INDEX IF NOT EXISTS idx_evidence_event ON prosopography.source_evidence(event_id);
CREATE INDEX IF NOT EXISTS idx_evidence_source ON prosopography.source_evidence(source_url);
CREATE INDEX IF NOT EXISTS idx_evidence_type ON prosopography.source_evidence(evidence_type);
CREATE INDEX IF NOT EXISTS idx_evidence_chunk ON prosopography.source_evidence(chunk_id);

-- ========================================
-- USER CORRECTIONS
-- ========================================

CREATE TABLE IF NOT EXISTS prosopography.user_corrections (
    correction_id SERIAL PRIMARY KEY,
    event_id INT REFERENCES prosopography.career_events(event_id) ON DELETE CASCADE,

    -- What was corrected
    field_name TEXT NOT NULL, -- organization, time_start, time_end, time_text, roles, locations, event_type

    -- Original and corrected values
    original_value TEXT,
    corrected_value TEXT,

    -- Validation context
    is_valid BOOLEAN NOT NULL, -- Was the original value marked as correct?
    correction_notes TEXT,

    -- Audit
    corrected_by TEXT DEFAULT 'user',
    corrected_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_corrections_event ON prosopography.user_corrections(event_id);
CREATE INDEX IF NOT EXISTS idx_corrections_field ON prosopography.user_corrections(field_name);

-- ========================================
-- LLM VERIFICATION ISSUES
-- ========================================

CREATE TABLE IF NOT EXISTS prosopography.verification_issues (
    issue_id SERIAL PRIMARY KEY,
    event_id INT REFERENCES prosopography.career_events(event_id) ON DELETE CASCADE,

    issue_type TEXT NOT NULL, -- temporal_coherence, completeness, quote_support, duplicate_candidate, classification
    severity TEXT NOT NULL, -- error, warning, info
    description TEXT NOT NULL,

    -- Resolution tracking
    resolved BOOLEAN DEFAULT FALSE,
    resolution_notes TEXT,
    resolved_at TIMESTAMPTZ,

    detected_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_issues_event ON prosopography.verification_issues(event_id);
CREATE INDEX IF NOT EXISTS idx_issues_resolved ON prosopography.verification_issues(resolved);
CREATE INDEX IF NOT EXISTS idx_issues_severity ON prosopography.verification_issues(severity);

-- ========================================
-- DECISION LOG (for Phase 3 incremental processing)
-- ========================================

CREATE TABLE IF NOT EXISTS prosopography.processing_decisions (
    decision_id SERIAL PRIMARY KEY,
    person_id INT REFERENCES prosopography.persons(person_id) ON DELETE CASCADE,

    -- Source processing context
    source_url TEXT NOT NULL,
    chunk_id INT,
    processing_phase TEXT NOT NULL, -- phase3_supplementation

    -- Decision details
    decision_type TEXT NOT NULL, -- merge, new, skip, validate
    target_event_id INT REFERENCES prosopography.career_events(event_id),
    reasoning TEXT NOT NULL,

    -- What was found/changed
    candidate_data JSONB NOT NULL, -- The extracted candidate event
    changes_made BOOLEAN DEFAULT FALSE,
    changes_summary TEXT,

    -- Raw LLM output for debugging
    raw_llm_output TEXT,
    model_used TEXT,

    processed_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_decisions_person ON prosopography.processing_decisions(person_id);
CREATE INDEX IF NOT EXISTS idx_decisions_type ON prosopography.processing_decisions(decision_type);
CREATE INDEX IF NOT EXISTS idx_decisions_source ON prosopography.processing_decisions(source_url);

-- ========================================
-- SOURCES PROCESSED TRACKING
-- ========================================

CREATE TABLE IF NOT EXISTS prosopography.sources_processed (
    id SERIAL PRIMARY KEY,
    person_id INT REFERENCES prosopography.persons(person_id) ON DELETE CASCADE,
    source_url TEXT NOT NULL,
    source_type TEXT NOT NULL, -- wikipedia, news, official, academic, other
    processing_phase TEXT NOT NULL, -- phase1, phase3

    -- Processing results
    chunks_processed INT DEFAULT 0,
    events_extracted INT DEFAULT 0,
    events_merged INT DEFAULT 0,
    events_created INT DEFAULT 0,

    processed_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(person_id, source_url)
);

CREATE INDEX IF NOT EXISTS idx_sources_person ON prosopography.sources_processed(person_id);

-- ========================================
-- EVALUATION METRICS
-- ========================================

CREATE TABLE IF NOT EXISTS prosopography.evaluation_metrics (
    metric_id SERIAL PRIMARY KEY,
    person_id INT REFERENCES prosopography.persons(person_id) ON DELETE CASCADE,

    -- Extraction quality
    total_events INT DEFAULT 0,
    events_validated INT DEFAULT 0,
    events_rejected INT DEFAULT 0,
    events_corrected INT DEFAULT 0,

    -- Source coverage
    sources_processed INT DEFAULT 0,
    source_validation_rate DECIMAL(5,2), -- % of events validated by multiple sources

    -- Field-level accuracy
    org_accuracy DECIMAL(5,2),
    time_accuracy DECIMAL(5,2),
    roles_accuracy DECIMAL(5,2),
    locations_accuracy DECIMAL(5,2),

    -- Issues
    total_issues INT DEFAULT 0,
    errors_count INT DEFAULT 0,
    warnings_count INT DEFAULT 0,

    calculated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_metrics_person ON prosopography.evaluation_metrics(person_id);

-- ========================================
-- VIEWS
-- ========================================

-- Complete event view with evidence count and org name
CREATE OR REPLACE VIEW prosopography.events_complete AS
SELECT
    ce.*,
    co.canonical_name as org_name,
    co.org_type,
    p.person_name,
    p.workflow_status,
    COUNT(DISTINCT se.evidence_id) as evidence_count,
    COUNT(DISTINCT CASE WHEN se.evidence_type = 'validation' THEN se.evidence_id END) as validation_count,
    COUNT(DISTINCT se.source_url) as unique_source_count
FROM prosopography.career_events ce
JOIN prosopography.persons p ON ce.person_id = p.person_id
LEFT JOIN prosopography.canonical_organizations co ON ce.org_id = co.org_id
LEFT JOIN prosopography.source_evidence se ON ce.event_id = se.event_id
GROUP BY ce.event_id, co.canonical_name, co.org_type, p.person_name, p.workflow_status;

-- Issues dashboard view
CREATE OR REPLACE VIEW prosopography.issues_dashboard AS
SELECT
    p.person_name,
    ce.event_code,
    vi.issue_type,
    vi.severity,
    vi.description,
    vi.resolved,
    vi.resolution_notes,
    ce.validation_status,
    vi.detected_at
FROM prosopography.verification_issues vi
JOIN prosopography.career_events ce ON vi.event_id = ce.event_id
JOIN prosopography.persons p ON ce.person_id = p.person_id
ORDER BY
    CASE vi.severity WHEN 'error' THEN 1 WHEN 'warning' THEN 2 ELSE 3 END,
    p.person_name,
    ce.event_code;

-- Person summary view
CREATE OR REPLACE VIEW prosopography.person_summary AS
SELECT
    p.person_id,
    p.person_name,
    p.workflow_status,
    COUNT(DISTINCT ce.event_id) as event_count,
    COUNT(DISTINCT sp.source_url) as sources_processed,
    COUNT(DISTINCT CASE WHEN ce.validation_status = 'validated' THEN ce.event_id END) as validated_count,
    COUNT(DISTINCT CASE WHEN vi.severity = 'error' AND NOT vi.resolved THEN vi.issue_id END) as open_errors,
    COUNT(DISTINCT CASE WHEN vi.severity = 'warning' AND NOT vi.resolved THEN vi.issue_id END) as open_warnings
FROM prosopography.persons p
LEFT JOIN prosopography.career_events ce ON p.person_id = ce.person_id
LEFT JOIN prosopography.sources_processed sp ON p.person_id = sp.person_id
LEFT JOIN prosopography.verification_issues vi ON ce.event_id = vi.event_id
GROUP BY p.person_id, p.person_name, p.workflow_status;
