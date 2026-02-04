# Database layer for Prosopography Tool
from .connection import get_connection, release_connection, close_all_connections
from .models import (
    Person, CanonicalOrganization, CareerEvent,
    SourceEvidence, UserCorrection, VerificationIssue,
    ProcessingDecision, EvaluationMetrics
)
from .person_repo import PersonRepository
from .event_repo import EventRepository
from .evidence_repo import EvidenceRepository
from .correction_repo import CorrectionRepository
from .org_repo import OrganizationRepository
from .issue_repo import IssueRepository

__all__ = [
    # Connection
    'get_connection', 'release_connection', 'close_all_connections',
    # Models
    'Person', 'CanonicalOrganization', 'CareerEvent',
    'SourceEvidence', 'UserCorrection', 'VerificationIssue',
    'ProcessingDecision', 'EvaluationMetrics',
    # Repositories
    'PersonRepository', 'EventRepository', 'EvidenceRepository',
    'CorrectionRepository', 'OrganizationRepository', 'IssueRepository'
]
