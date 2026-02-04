"""Issue tracking service for verification issues."""

from typing import Dict, Any, List, Optional

from ..db import IssueRepository, EventRepository, VerificationIssue


class IssueTracker:
    """Service for tracking and managing verification issues."""

    def __init__(self):
        """Initialize the issue tracker."""
        self.issue_repo = IssueRepository()
        self.event_repo = EventRepository()

    def record_issue(
        self,
        event_id: int,
        issue_type: str,
        severity: str,
        description: str
    ) -> int:
        """Record a new verification issue.

        Args:
            event_id: ID of the event with the issue
            issue_type: Type of issue (temporal_coherence, completeness, quote_support, etc.)
            severity: Severity level (error, warning, info)
            description: Description of the issue

        Returns:
            The issue_id
        """
        issue = VerificationIssue(
            event_id=event_id,
            issue_type=issue_type,
            severity=severity,
            description=description
        )
        return self.issue_repo.create(issue)

    def resolve_issue(self, issue_id: int, notes: Optional[str] = None) -> None:
        """Mark an issue as resolved.

        Args:
            issue_id: ID of the issue
            notes: Optional resolution notes
        """
        self.issue_repo.resolve(issue_id, notes)

    def resolve_all_for_event(self, event_id: int, notes: Optional[str] = None) -> None:
        """Resolve all issues for an event.

        Args:
            event_id: ID of the event
            notes: Optional resolution notes
        """
        self.issue_repo.resolve_for_event(event_id, notes)

    def get_open_issues(self, person_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get all open issues, optionally filtered by person.

        Args:
            person_id: Optional person ID filter

        Returns:
            List of issue dictionaries
        """
        if person_id:
            issues = self.issue_repo.get_for_person(person_id, include_resolved=False)
            return [self._issue_to_dict(i) for i in issues]
        else:
            return self.issue_repo.get_all_open()

    def get_issue_summary(self, person_id: Optional[int] = None) -> Dict[str, Any]:
        """Get summary of issues by severity.

        Args:
            person_id: Optional person ID filter

        Returns:
            Dictionary with counts by severity
        """
        counts = self.issue_repo.count_by_severity(person_id)

        total_open = sum(c["open"] for c in counts.values())
        total_resolved = sum(c["resolved"] for c in counts.values())

        return {
            "by_severity": counts,
            "total_open": total_open,
            "total_resolved": total_resolved,
            "total": total_open + total_resolved
        }

    def check_temporal_coherence(self, person_id: int) -> List[Dict[str, Any]]:
        """Check all events for a person for temporal coherence issues.

        Args:
            person_id: ID of the person

        Returns:
            List of detected issues
        """
        events = self.event_repo.get_for_person(person_id)
        issues = []
        current_year = 2024

        for event in events:
            # Check if start is after end
            if event.time_start and event.time_end and event.time_end != "present":
                try:
                    start = int(event.time_start[:4])
                    end = int(event.time_end[:4])
                    if start > end:
                        issues.append({
                            "event_id": event.event_id,
                            "event_code": event.event_code,
                            "issue_type": "temporal_coherence",
                            "severity": "error",
                            "description": f"Start year ({start}) is after end year ({end})"
                        })
                except (ValueError, TypeError):
                    pass

            # Check for future dates
            if event.time_start:
                try:
                    start = int(event.time_start[:4])
                    if start > current_year:
                        issues.append({
                            "event_id": event.event_id,
                            "event_code": event.event_code,
                            "issue_type": "temporal_coherence",
                            "severity": "error",
                            "description": f"Start year ({start}) is in the future"
                        })
                except (ValueError, TypeError):
                    pass

        return issues

    def run_all_checks(self, person_id: int) -> Dict[str, Any]:
        """Run all validation checks for a person.

        Args:
            person_id: ID of the person

        Returns:
            Dictionary with check results
        """
        results = {
            "temporal_coherence": self.check_temporal_coherence(person_id),
            "issues_created": 0
        }

        # Record any new issues found
        for check_type, issues in results.items():
            if isinstance(issues, list):
                for issue in issues:
                    self.record_issue(
                        event_id=issue["event_id"],
                        issue_type=issue["issue_type"],
                        severity=issue["severity"],
                        description=issue["description"]
                    )
                    results["issues_created"] += 1

        return results

    def _issue_to_dict(self, issue: VerificationIssue) -> Dict[str, Any]:
        """Convert issue to dictionary."""
        return {
            "issue_id": issue.issue_id,
            "event_id": issue.event_id,
            "issue_type": issue.issue_type,
            "severity": issue.severity,
            "description": issue.description,
            "resolved": issue.resolved,
            "resolution_notes": issue.resolution_notes,
            "detected_at": issue.detected_at
        }
