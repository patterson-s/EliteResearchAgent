"""Service for applying user corrections to career events."""

from typing import Dict, Any, List, Optional
import json

from ..db import (
    EventRepository, CorrectionRepository, IssueRepository,
    PersonRepository, OrganizationRepository,
    CareerEvent, UserCorrection
)


class CorrectionService:
    """Service for managing user corrections to career events."""

    def __init__(self):
        """Initialize the correction service."""
        self.event_repo = EventRepository()
        self.correction_repo = CorrectionRepository()
        self.issue_repo = IssueRepository()
        self.person_repo = PersonRepository()
        self.org_repo = OrganizationRepository()

    def apply_correction(
        self,
        event_id: int,
        field_name: str,
        is_valid: bool,
        corrected_value: Optional[str] = None,
        notes: Optional[str] = None
    ) -> int:
        """Apply a correction to an event field.

        Args:
            event_id: ID of the event to correct
            field_name: Name of the field being corrected
            is_valid: Whether the original value is valid
            corrected_value: The corrected value (if is_valid is False)
            notes: Optional notes about the correction

        Returns:
            The correction_id
        """
        # Get the current event
        event = self.event_repo.get_by_id(event_id)
        if not event:
            raise ValueError(f"Event {event_id} not found")

        # Get original value
        original_value = self._get_field_value(event, field_name)

        # Create correction record
        correction = UserCorrection(
            event_id=event_id,
            field_name=field_name,
            original_value=original_value,
            corrected_value=corrected_value if not is_valid else None,
            is_valid=is_valid,
            correction_notes=notes
        )
        correction_id = self.correction_repo.create(correction)

        # Apply correction to event if value was changed
        if not is_valid and corrected_value:
            self._apply_field_correction(event, field_name, corrected_value)
            self.event_repo.update(event)

        return correction_id

    def apply_bulk_corrections(
        self,
        event_id: int,
        corrections: Dict[str, Dict[str, Any]]
    ) -> List[int]:
        """Apply multiple corrections to an event.

        Args:
            event_id: ID of the event
            corrections: Dictionary mapping field_name to correction info
                         {field_name: {is_valid: bool, corrected_value: str, notes: str}}

        Returns:
            List of correction_ids
        """
        correction_ids = []
        for field_name, correction_info in corrections.items():
            correction_id = self.apply_correction(
                event_id=event_id,
                field_name=field_name,
                is_valid=correction_info.get("is_valid", True),
                corrected_value=correction_info.get("corrected_value"),
                notes=correction_info.get("notes")
            )
            correction_ids.append(correction_id)
        return correction_ids

    def validate_event(self, event_id: int, notes: Optional[str] = None) -> None:
        """Mark an event as validated (all fields correct).

        Args:
            event_id: ID of the event
            notes: Optional validation notes
        """
        event = self.event_repo.get_by_id(event_id)
        if not event:
            raise ValueError(f"Event {event_id} not found")

        # Update event status
        self.event_repo.update_status(event_id, "validated")

        # Resolve any open issues
        self.issue_repo.resolve_for_event(event_id, notes or "Validated by user")

    def reject_event(self, event_id: int, reason: str) -> None:
        """Mark an event as rejected (false positive).

        Args:
            event_id: ID of the event
            reason: Reason for rejection
        """
        event = self.event_repo.get_by_id(event_id)
        if not event:
            raise ValueError(f"Event {event_id} not found")

        # Create a correction noting the rejection
        correction = UserCorrection(
            event_id=event_id,
            field_name="event_valid",
            original_value="true",
            corrected_value="false",
            is_valid=False,
            correction_notes=reason
        )
        self.correction_repo.create(correction)

        # Update event status
        self.event_repo.update_status(event_id, "rejected")

    def finalize_review(self, person_id: int) -> Dict[str, Any]:
        """Finalize the Phase 2 review for a person.

        Args:
            person_id: ID of the person

        Returns:
            Summary of the review
        """
        # Get all events for the person
        events = self.event_repo.get_for_person(person_id)

        # Count statuses
        status_counts = {"pending": 0, "validated": 0, "rejected": 0, "needs_review": 0}
        for event in events:
            status = event.validation_status
            if status in status_counts:
                status_counts[status] += 1

        # Get correction summary
        corrections = self.correction_repo.get_for_person(person_id)
        correction_counts = self.correction_repo.count_by_field(person_id)

        # Update person status
        self.person_repo.update_status(person_id, "phase2_reviewed")

        return {
            "total_events": len(events),
            "status_counts": status_counts,
            "total_corrections": len(corrections),
            "corrections_by_field": correction_counts,
            "workflow_status": "phase2_reviewed"
        }

    def get_correction_summary(self, person_id: int) -> Dict[str, Any]:
        """Get a summary of corrections for a person.

        Args:
            person_id: ID of the person

        Returns:
            Dictionary with correction statistics
        """
        corrections = self.correction_repo.get_for_person(person_id)
        by_field = self.correction_repo.count_by_field(person_id)

        total_invalid = sum(1 for c in corrections if not c.is_valid)

        return {
            "total_corrections": len(corrections),
            "total_invalid": total_invalid,
            "by_field": by_field
        }

    def _get_field_value(self, event: CareerEvent, field_name: str) -> Optional[str]:
        """Get the current value of a field from an event."""
        if field_name == "organization":
            return event.org_name or str(event.org_id)
        elif field_name == "time_start":
            return event.time_start
        elif field_name == "time_end":
            return event.time_end
        elif field_name == "time_text":
            return event.time_text
        elif field_name == "roles":
            return json.dumps(event.roles) if event.roles else "[]"
        elif field_name == "locations":
            return json.dumps(event.locations) if event.locations else "[]"
        elif field_name == "event_type":
            return event.event_type
        elif field_name == "confidence":
            return event.confidence
        else:
            return None

    def _apply_field_correction(
        self,
        event: CareerEvent,
        field_name: str,
        corrected_value: str
    ) -> None:
        """Apply a correction to a specific field on an event."""
        if field_name == "time_start":
            event.time_start = corrected_value
        elif field_name == "time_end":
            event.time_end = corrected_value
        elif field_name == "time_text":
            event.time_text = corrected_value
        elif field_name == "roles":
            try:
                event.roles = json.loads(corrected_value)
            except json.JSONDecodeError:
                # Assume comma-separated if not valid JSON
                event.roles = [r.strip() for r in corrected_value.split(",")]
        elif field_name == "locations":
            try:
                event.locations = json.loads(corrected_value)
            except json.JSONDecodeError:
                event.locations = [l.strip() for l in corrected_value.split(",")]
        elif field_name == "event_type":
            event.event_type = corrected_value
        elif field_name == "confidence":
            event.confidence = corrected_value
        # Note: organization corrections require separate handling through org_repo
