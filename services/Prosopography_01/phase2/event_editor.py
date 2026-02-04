"""Service for adding, deleting, and modifying career events."""

from typing import Dict, Any, List, Optional
import json

from ..db import (
    EventRepository, EvidenceRepository, OrganizationRepository,
    IssueRepository, CorrectionRepository,
    CareerEvent, SourceEvidence, CanonicalOrganization
)


class EventEditor:
    """Service for editing career events (add, delete, modify)."""

    def __init__(self):
        """Initialize the event editor."""
        self.event_repo = EventRepository()
        self.evidence_repo = EvidenceRepository()
        self.org_repo = OrganizationRepository()
        self.issue_repo = IssueRepository()
        self.correction_repo = CorrectionRepository()

    def add_event(
        self,
        person_id: int,
        event_type: str,
        roles: List[str],
        organization_name: Optional[str] = None,
        time_start: Optional[str] = None,
        time_end: Optional[str] = None,
        time_text: Optional[str] = None,
        locations: Optional[List[str]] = None,
        supporting_quote: Optional[str] = None,
        source_url: Optional[str] = None,
        notes: Optional[str] = None
    ) -> int:
        """Add a new career event manually.

        Args:
            person_id: ID of the person
            event_type: Type of event (career_position or award)
            roles: List of role titles
            organization_name: Name of the organization
            time_start: Start year/date
            time_end: End year/date
            time_text: Original time text
            locations: List of locations
            supporting_quote: Manual quote/evidence
            source_url: Source URL for the evidence
            notes: Notes about why this was added

        Returns:
            The new event_id
        """
        # Generate event code
        event_code = self.event_repo.get_next_event_code(person_id, "E_MANUAL_")

        # Find or create organization
        org_id = None
        if organization_name:
            # Try to find existing org by name
            orgs = self.org_repo.get_for_person(person_id)
            for org in orgs:
                if org.canonical_name.lower() == organization_name.lower():
                    org_id = org.org_id
                    break

            # Create new org if not found
            if org_id is None:
                canonical_id = self.org_repo.get_next_canonical_id(person_id)
                new_org = CanonicalOrganization(
                    person_id=person_id,
                    canonical_id=canonical_id,
                    canonical_name=organization_name,
                    org_type="other",
                    metadata={"added_manually": True}
                )
                org_id = self.org_repo.create(new_org)

        # Create the event
        event = CareerEvent(
            person_id=person_id,
            event_code=event_code,
            event_type=event_type,
            org_id=org_id,
            time_start=time_start,
            time_end=time_end,
            time_text=time_text,
            roles=roles,
            locations=locations or [],
            confidence="medium",
            llm_status="valid",
            validation_status="validated",  # User-added events are pre-validated
            created_source="phase2_correction"
        )
        event_id = self.event_repo.create(event)

        # Add evidence if provided
        if supporting_quote:
            evidence = SourceEvidence(
                event_id=event_id,
                source_url=source_url or "manual_entry",
                source_type="manual",
                verbatim_quote=supporting_quote,
                evidence_type="original",
                extraction_phase="phase2_manual"
            )
            self.evidence_repo.create(evidence)

        return event_id

    def delete_event(self, event_id: int, reason: str) -> None:
        """Delete an event (marks as rejected rather than hard delete).

        Args:
            event_id: ID of the event to delete
            reason: Reason for deletion
        """
        event = self.event_repo.get_by_id(event_id)
        if not event:
            raise ValueError(f"Event {event_id} not found")

        # Mark as rejected rather than hard delete (preserves audit trail)
        self.event_repo.update_status(event_id, "rejected")

        # Resolve any issues
        self.issue_repo.resolve_for_event(event_id, f"Event rejected: {reason}")

    def hard_delete_event(self, event_id: int) -> None:
        """Permanently delete an event and all related records.

        Use with caution - this removes all data including evidence and corrections.

        Args:
            event_id: ID of the event to delete
        """
        # Delete in order to respect foreign keys
        self.evidence_repo.delete_for_event(event_id)
        self.issue_repo.delete_for_event(event_id)
        self.correction_repo.delete_for_event(event_id)
        self.event_repo.delete(event_id)

    def merge_events(
        self,
        primary_event_id: int,
        secondary_event_id: int,
        merge_strategy: str = "union"
    ) -> int:
        """Merge two events into one.

        Args:
            primary_event_id: ID of the event to keep
            secondary_event_id: ID of the event to merge in
            merge_strategy: How to merge (union, primary_wins)

        Returns:
            The primary_event_id
        """
        primary = self.event_repo.get_by_id(primary_event_id)
        secondary = self.event_repo.get_by_id(secondary_event_id)

        if not primary or not secondary:
            raise ValueError("One or both events not found")

        if primary.person_id != secondary.person_id:
            raise ValueError("Cannot merge events from different persons")

        # Merge roles
        if merge_strategy == "union":
            merged_roles = list(set(primary.roles + secondary.roles))
            merged_locations = list(set(primary.locations + secondary.locations))
        else:  # primary_wins
            merged_roles = primary.roles
            merged_locations = primary.locations

        # Update primary event
        primary.roles = merged_roles
        primary.locations = merged_locations

        # Take earlier start date and later end date
        if secondary.time_start and (not primary.time_start or secondary.time_start < primary.time_start):
            primary.time_start = secondary.time_start
        if secondary.time_end and (not primary.time_end or secondary.time_end > primary.time_end):
            primary.time_end = secondary.time_end

        self.event_repo.update(primary)

        # Move evidence from secondary to primary
        secondary_evidence = self.evidence_repo.get_for_event(secondary_event_id)
        for evidence in secondary_evidence:
            evidence.event_id = primary_event_id
            # Re-create with new event_id (simpler than update)
            self.evidence_repo.create(evidence)

        # Mark secondary as rejected
        self.event_repo.update_status(secondary_event_id, "rejected")

        return primary_event_id

    def add_evidence(
        self,
        event_id: int,
        quote: str,
        source_url: str,
        source_type: str = "manual",
        contribution: Optional[str] = None
    ) -> int:
        """Add custom evidence to an event.

        Args:
            event_id: ID of the event
            quote: The evidence quote
            source_url: URL of the source
            source_type: Type of source (manual, wikipedia, news, etc.)
            contribution: What this evidence contributes (time, role, location, confirmation)

        Returns:
            The evidence_id
        """
        event = self.event_repo.get_by_id(event_id)
        if not event:
            raise ValueError(f"Event {event_id} not found")

        evidence = SourceEvidence(
            event_id=event_id,
            source_url=source_url,
            source_type=source_type,
            verbatim_quote=quote,
            evidence_type="supplementation",
            contribution=contribution,
            extraction_phase="phase2_manual"
        )
        return self.evidence_repo.create(evidence)

    def update_event(
        self,
        event_id: int,
        updates: Dict[str, Any]
    ) -> None:
        """Update multiple fields on an event.

        Args:
            event_id: ID of the event
            updates: Dictionary of field names to new values
        """
        event = self.event_repo.get_by_id(event_id)
        if not event:
            raise ValueError(f"Event {event_id} not found")

        # Apply updates
        for field, value in updates.items():
            if field == "event_type":
                event.event_type = value
            elif field == "time_start":
                event.time_start = value
            elif field == "time_end":
                event.time_end = value
            elif field == "time_text":
                event.time_text = value
            elif field == "roles":
                event.roles = value if isinstance(value, list) else [value]
            elif field == "locations":
                event.locations = value if isinstance(value, list) else [value]
            elif field == "confidence":
                event.confidence = value
            elif field == "validation_status":
                event.validation_status = value

        self.event_repo.update(event)
