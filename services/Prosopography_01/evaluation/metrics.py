"""Metrics calculation for evaluation."""

from typing import Dict, Any, List, Optional

from ..db import (
    PersonRepository, EventRepository, EvidenceRepository,
    CorrectionRepository, IssueRepository,
    get_connection, release_connection
)


class MetricsCalculator:
    """Service for calculating evaluation metrics."""

    def __init__(self):
        """Initialize the metrics calculator."""
        self.person_repo = PersonRepository()
        self.event_repo = EventRepository()
        self.evidence_repo = EvidenceRepository()
        self.correction_repo = CorrectionRepository()
        self.issue_repo = IssueRepository()

    def calculate_extraction_quality(self, person_id: int) -> Dict[str, Any]:
        """Calculate extraction quality metrics for a person.

        Args:
            person_id: ID of the person

        Returns:
            Dictionary with extraction quality metrics
        """
        events = self.event_repo.get_for_person(person_id)
        counts = self.event_repo.count_for_person(person_id)

        total = counts["total"]
        validated = counts.get("validated", 0)
        rejected = counts.get("rejected", 0)

        # Get correction info
        corrections = self.correction_repo.get_for_person(person_id)
        events_with_corrections = len(set(c.event_id for c in corrections))
        invalid_corrections = [c for c in corrections if not c.is_valid]

        return {
            "total_events": total,
            "validated": validated,
            "rejected": rejected,
            "pending": counts.get("pending", 0),
            "validation_rate": validated / total if total > 0 else 0,
            "rejection_rate": rejected / total if total > 0 else 0,
            "events_corrected": events_with_corrections,
            "correction_rate": events_with_corrections / total if total > 0 else 0,
            "fields_marked_invalid": len(invalid_corrections)
        }

    def calculate_field_accuracy(self, person_id: int) -> Dict[str, Any]:
        """Calculate field-level accuracy based on user corrections.

        Args:
            person_id: ID of the person

        Returns:
            Dictionary with field accuracy metrics
        """
        events = self.event_repo.get_for_person(person_id)
        corrections_by_field = self.correction_repo.count_by_field(person_id)

        total_events = len(events)
        fields = ["organization", "time_start", "time_end", "roles", "locations"]

        accuracy = {}
        for field in fields:
            field_data = corrections_by_field.get(field, {"total": 0, "invalid": 0})
            invalid = field_data.get("invalid", 0)
            # Accuracy = (total events - invalid) / total events
            accuracy[field] = (total_events - invalid) / total_events if total_events > 0 else 1.0

        return {
            "field_accuracy": accuracy,
            "corrections_by_field": corrections_by_field,
            "total_events": total_events
        }

    def calculate_source_coverage(self, person_id: int) -> Dict[str, Any]:
        """Calculate source coverage metrics.

        Args:
            person_id: ID of the person

        Returns:
            Dictionary with source coverage metrics
        """
        events = self.event_repo.get_for_person(person_id)
        events = [e for e in events if e.validation_status != "rejected"]

        # Count events validated by multiple sources
        multi_source_events = 0
        for event in events:
            source_count = self.evidence_repo.count_unique_sources_for_event(event.event_id)
            if source_count >= 2:
                multi_source_events += 1

        # Get sources processed
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) FROM prosopography.sources_processed
                    WHERE person_id = %s
                """, (person_id,))
                sources_processed = cur.fetchone()[0]

                cur.execute("""
                    SELECT source_type, COUNT(*) as count
                    FROM prosopography.sources_processed
                    WHERE person_id = %s
                    GROUP BY source_type
                """, (person_id,))
                by_type = {row[0]: row[1] for row in cur.fetchall()}
        finally:
            release_connection(conn)

        total_events = len(events)
        return {
            "total_events": total_events,
            "multi_source_events": multi_source_events,
            "multi_source_rate": multi_source_events / total_events if total_events > 0 else 0,
            "sources_processed": sources_processed,
            "sources_by_type": by_type
        }

    def calculate_issue_metrics(self, person_id: int) -> Dict[str, Any]:
        """Calculate issue-related metrics.

        Args:
            person_id: ID of the person

        Returns:
            Dictionary with issue metrics
        """
        counts = self.issue_repo.count_by_severity(person_id)

        total_issues = sum(c["open"] + c["resolved"] for c in counts.values())
        open_issues = sum(c["open"] for c in counts.values())
        resolved_issues = sum(c["resolved"] for c in counts.values())

        return {
            "total_issues": total_issues,
            "open_issues": open_issues,
            "resolved_issues": resolved_issues,
            "resolution_rate": resolved_issues / total_issues if total_issues > 0 else 1.0,
            "by_severity": counts,
            "open_errors": counts.get("error", {}).get("open", 0),
            "open_warnings": counts.get("warning", {}).get("open", 0)
        }

    def calculate_all_metrics(self, person_id: int) -> Dict[str, Any]:
        """Calculate all metrics for a person.

        Args:
            person_id: ID of the person

        Returns:
            Dictionary with all metrics
        """
        person = self.person_repo.get_by_id(person_id)
        if not person:
            raise ValueError(f"Person {person_id} not found")

        return {
            "person_id": person_id,
            "person_name": person.person_name,
            "workflow_status": person.workflow_status,
            "extraction_quality": self.calculate_extraction_quality(person_id),
            "field_accuracy": self.calculate_field_accuracy(person_id),
            "source_coverage": self.calculate_source_coverage(person_id),
            "issues": self.calculate_issue_metrics(person_id)
        }

    def get_dashboard_summary(self) -> Dict[str, Any]:
        """Get summary metrics for the dashboard.

        Returns:
            Dictionary with dashboard summary
        """
        persons = self.person_repo.get_all()

        status_counts = {}
        total_events = 0
        total_validated = 0
        total_issues = 0

        for person in persons:
            status = person.workflow_status
            status_counts[status] = status_counts.get(status, 0) + 1

            events = self.event_repo.count_for_person(person.person_id)
            total_events += events["total"]
            total_validated += events.get("validated", 0)

            issues = self.issue_repo.count_by_severity(person.person_id)
            total_issues += sum(c["open"] for c in issues.values())

        return {
            "total_persons": len(persons),
            "persons_by_status": status_counts,
            "total_events": total_events,
            "total_validated": total_validated,
            "total_open_issues": total_issues
        }
