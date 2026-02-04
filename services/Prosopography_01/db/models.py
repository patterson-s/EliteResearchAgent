"""Data models for Prosopography Tool."""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from datetime import datetime


@dataclass
class Person:
    """A person being researched."""
    person_id: Optional[int] = None
    person_name: str = ""
    workflow_status: str = "pending"  # pending, phase1_complete, phase2_reviewed, phase3_in_progress, complete
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class CanonicalOrganization:
    """A canonical (deduplicated) organization."""
    org_id: Optional[int] = None
    person_id: int = 0
    canonical_id: str = ""  # e.g., "ORG_001"
    canonical_name: str = ""
    org_type: Optional[str] = None  # university, government, international_org, company, etc.
    country: Optional[str] = None
    parent_org_id: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: Optional[datetime] = None


@dataclass
class CareerEvent:
    """A career event (position or award)."""
    event_id: Optional[int] = None
    person_id: int = 0
    event_code: str = ""  # e.g., "E001", "E_NEW_001"
    event_type: str = "career_position"  # career_position, award
    org_id: Optional[int] = None

    # Time period
    time_start: Optional[str] = None  # YYYY or YYYY-MM-DD
    time_end: Optional[str] = None    # YYYY, YYYY-MM-DD, "present", None
    time_text: Optional[str] = None   # Original text: "from 1986 to 1989"

    # Details
    roles: List[str] = field(default_factory=list)
    locations: List[str] = field(default_factory=list)

    # Status
    confidence: str = "medium"  # high, medium, low
    llm_status: str = "valid"   # valid, warning, error
    validation_status: str = "pending"  # pending, validated, rejected, needs_review

    # Audit
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    created_source: str = "phase1_extraction"  # phase1_extraction, phase2_correction, phase3_supplementation

    # Optional: organization name (for display, not stored in this field)
    org_name: Optional[str] = None


@dataclass
class SourceEvidence:
    """Evidence from a source supporting a career event."""
    evidence_id: Optional[int] = None
    event_id: int = 0

    # Source identification
    chunk_id: Optional[int] = None  # FK to sources.chunks
    source_url: str = ""
    source_type: str = ""  # wikipedia, news, official, academic

    # The actual evidence
    verbatim_quote: str = ""
    quote_context: Optional[str] = None  # Surrounding context for disambiguation

    # Evidence role
    evidence_type: str = "original"  # original, validation, supplementation
    contribution: Optional[str] = None  # time, role, location, confirmation

    # Processing metadata
    extraction_phase: str = "phase1"  # phase1, phase2_manual, phase3
    processing_timestamp: Optional[datetime] = None
    model_used: Optional[str] = None


@dataclass
class UserCorrection:
    """A user correction to a career event."""
    correction_id: Optional[int] = None
    event_id: int = 0

    # What was corrected
    field_name: str = ""  # organization, time_start, time_end, roles, locations, event_type

    # Original and corrected values
    original_value: Optional[str] = None
    corrected_value: Optional[str] = None

    # Validation context
    is_valid: bool = True  # Was the original value marked as correct?
    correction_notes: Optional[str] = None

    # Audit
    corrected_by: str = "user"
    corrected_at: Optional[datetime] = None


@dataclass
class VerificationIssue:
    """An issue detected during LLM verification."""
    issue_id: Optional[int] = None
    event_id: int = 0

    issue_type: str = ""  # temporal_coherence, completeness, quote_support, duplicate_candidate
    severity: str = "warning"  # error, warning, info
    description: str = ""

    # Resolution tracking
    resolved: bool = False
    resolution_notes: Optional[str] = None
    resolved_at: Optional[datetime] = None

    detected_at: Optional[datetime] = None


@dataclass
class ProcessingDecision:
    """A decision made during Phase 3 supplementation."""
    decision_id: Optional[int] = None
    person_id: int = 0

    # Source processing context
    source_url: str = ""
    chunk_id: Optional[int] = None
    processing_phase: str = "phase3_supplementation"

    # Decision details
    decision_type: str = ""  # merge, new, skip, validate
    target_event_id: Optional[int] = None
    reasoning: str = ""

    # What was found/changed
    candidate_data: Dict[str, Any] = field(default_factory=dict)
    changes_made: bool = False
    changes_summary: Optional[str] = None

    # Raw LLM output for debugging
    raw_llm_output: Optional[str] = None
    model_used: Optional[str] = None

    processed_at: Optional[datetime] = None


@dataclass
class EvaluationMetrics:
    """Evaluation metrics for a person's career events."""
    metric_id: Optional[int] = None
    person_id: int = 0

    # Extraction quality
    total_events: int = 0
    events_validated: int = 0
    events_rejected: int = 0
    events_corrected: int = 0

    # Source coverage
    sources_processed: int = 0
    source_validation_rate: float = 0.0  # % of events validated by multiple sources

    # Field-level accuracy
    org_accuracy: float = 0.0
    time_accuracy: float = 0.0
    roles_accuracy: float = 0.0
    locations_accuracy: float = 0.0

    # Issues
    total_issues: int = 0
    errors_count: int = 0
    warnings_count: int = 0

    calculated_at: Optional[datetime] = None
