"""Repository for VerificationIssue CRUD operations."""

from typing import List, Optional
from datetime import datetime

from .connection import get_connection, release_connection
from .models import VerificationIssue


class IssueRepository:
    """Repository for managing VerificationIssue records."""

    def create(self, issue: VerificationIssue) -> int:
        """Create a new verification issue. Returns the issue_id."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO prosopography.verification_issues
                    (event_id, issue_type, severity, description)
                    VALUES (%s, %s, %s, %s)
                    RETURNING issue_id
                """, (
                    issue.event_id, issue.issue_type, issue.severity, issue.description
                ))
                issue_id = cur.fetchone()[0]
                conn.commit()
                return issue_id
        finally:
            release_connection(conn)

    def create_many(self, issues: List[VerificationIssue]) -> List[int]:
        """Create multiple issues. Returns list of issue_ids."""
        if not issues:
            return []
        conn = get_connection()
        try:
            issue_ids = []
            with conn.cursor() as cur:
                for issue in issues:
                    cur.execute("""
                        INSERT INTO prosopography.verification_issues
                        (event_id, issue_type, severity, description)
                        VALUES (%s, %s, %s, %s)
                        RETURNING issue_id
                    """, (
                        issue.event_id, issue.issue_type, issue.severity, issue.description
                    ))
                    issue_ids.append(cur.fetchone()[0])
                conn.commit()
            return issue_ids
        finally:
            release_connection(conn)

    def get_by_id(self, issue_id: int) -> Optional[VerificationIssue]:
        """Get an issue by ID."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT issue_id, event_id, issue_type, severity, description,
                           resolved, resolution_notes, resolved_at, detected_at
                    FROM prosopography.verification_issues
                    WHERE issue_id = %s
                """, (issue_id,))
                row = cur.fetchone()
                if row:
                    return self._row_to_issue(row)
                return None
        finally:
            release_connection(conn)

    def get_for_event(self, event_id: int, include_resolved: bool = True) -> List[VerificationIssue]:
        """Get all issues for a specific event."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                if include_resolved:
                    cur.execute("""
                        SELECT issue_id, event_id, issue_type, severity, description,
                               resolved, resolution_notes, resolved_at, detected_at
                        FROM prosopography.verification_issues
                        WHERE event_id = %s
                        ORDER BY CASE severity WHEN 'error' THEN 1 WHEN 'warning' THEN 2 ELSE 3 END
                    """, (event_id,))
                else:
                    cur.execute("""
                        SELECT issue_id, event_id, issue_type, severity, description,
                               resolved, resolution_notes, resolved_at, detected_at
                        FROM prosopography.verification_issues
                        WHERE event_id = %s AND NOT resolved
                        ORDER BY CASE severity WHEN 'error' THEN 1 WHEN 'warning' THEN 2 ELSE 3 END
                    """, (event_id,))
                rows = cur.fetchall()
                return [self._row_to_issue(row) for row in rows]
        finally:
            release_connection(conn)

    def get_for_person(self, person_id: int, include_resolved: bool = False) -> List[VerificationIssue]:
        """Get all issues for a person's events."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                if include_resolved:
                    cur.execute("""
                        SELECT vi.issue_id, vi.event_id, vi.issue_type, vi.severity, vi.description,
                               vi.resolved, vi.resolution_notes, vi.resolved_at, vi.detected_at
                        FROM prosopography.verification_issues vi
                        JOIN prosopography.career_events ce ON vi.event_id = ce.event_id
                        WHERE ce.person_id = %s
                        ORDER BY CASE vi.severity WHEN 'error' THEN 1 WHEN 'warning' THEN 2 ELSE 3 END
                    """, (person_id,))
                else:
                    cur.execute("""
                        SELECT vi.issue_id, vi.event_id, vi.issue_type, vi.severity, vi.description,
                               vi.resolved, vi.resolution_notes, vi.resolved_at, vi.detected_at
                        FROM prosopography.verification_issues vi
                        JOIN prosopography.career_events ce ON vi.event_id = ce.event_id
                        WHERE ce.person_id = %s AND NOT vi.resolved
                        ORDER BY CASE vi.severity WHEN 'error' THEN 1 WHEN 'warning' THEN 2 ELSE 3 END
                    """, (person_id,))
                rows = cur.fetchall()
                return [self._row_to_issue(row) for row in rows]
        finally:
            release_connection(conn)

    def get_all_open(self, severity: Optional[str] = None) -> List[dict]:
        """Get all open issues across all persons (for dashboard)."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                if severity:
                    cur.execute("""
                        SELECT p.person_name, ce.event_code, vi.issue_type, vi.severity,
                               vi.description, vi.detected_at, vi.issue_id, ce.event_id
                        FROM prosopography.verification_issues vi
                        JOIN prosopography.career_events ce ON vi.event_id = ce.event_id
                        JOIN prosopography.persons p ON ce.person_id = p.person_id
                        WHERE NOT vi.resolved AND vi.severity = %s
                        ORDER BY CASE vi.severity WHEN 'error' THEN 1 WHEN 'warning' THEN 2 ELSE 3 END,
                                 p.person_name, ce.event_code
                    """, (severity,))
                else:
                    cur.execute("""
                        SELECT p.person_name, ce.event_code, vi.issue_type, vi.severity,
                               vi.description, vi.detected_at, vi.issue_id, ce.event_id
                        FROM prosopography.verification_issues vi
                        JOIN prosopography.career_events ce ON vi.event_id = ce.event_id
                        JOIN prosopography.persons p ON ce.person_id = p.person_id
                        WHERE NOT vi.resolved
                        ORDER BY CASE vi.severity WHEN 'error' THEN 1 WHEN 'warning' THEN 2 ELSE 3 END,
                                 p.person_name, ce.event_code
                    """)
                rows = cur.fetchall()
                return [
                    {
                        'person_name': row[0],
                        'event_code': row[1],
                        'issue_type': row[2],
                        'severity': row[3],
                        'description': row[4],
                        'detected_at': row[5],
                        'issue_id': row[6],
                        'event_id': row[7]
                    }
                    for row in rows
                ]
        finally:
            release_connection(conn)

    def resolve(self, issue_id: int, resolution_notes: Optional[str] = None) -> None:
        """Mark an issue as resolved."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE prosopography.verification_issues
                    SET resolved = TRUE, resolution_notes = %s, resolved_at = NOW()
                    WHERE issue_id = %s
                """, (resolution_notes, issue_id))
                conn.commit()
        finally:
            release_connection(conn)

    def resolve_for_event(self, event_id: int, resolution_notes: Optional[str] = None) -> None:
        """Mark all issues for an event as resolved."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE prosopography.verification_issues
                    SET resolved = TRUE, resolution_notes = %s, resolved_at = NOW()
                    WHERE event_id = %s AND NOT resolved
                """, (resolution_notes, event_id))
                conn.commit()
        finally:
            release_connection(conn)

    def count_by_severity(self, person_id: Optional[int] = None) -> dict:
        """Get counts of issues by severity."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                if person_id:
                    cur.execute("""
                        SELECT vi.severity, vi.resolved, COUNT(*)
                        FROM prosopography.verification_issues vi
                        JOIN prosopography.career_events ce ON vi.event_id = ce.event_id
                        WHERE ce.person_id = %s
                        GROUP BY vi.severity, vi.resolved
                    """, (person_id,))
                else:
                    cur.execute("""
                        SELECT severity, resolved, COUNT(*)
                        FROM prosopography.verification_issues
                        GROUP BY severity, resolved
                    """)
                rows = cur.fetchall()
                counts = {
                    'error': {'open': 0, 'resolved': 0},
                    'warning': {'open': 0, 'resolved': 0},
                    'info': {'open': 0, 'resolved': 0}
                }
                for row in rows:
                    severity = row[0]
                    resolved = row[1]
                    count = row[2]
                    if severity in counts:
                        key = 'resolved' if resolved else 'open'
                        counts[severity][key] = count
                return counts
        finally:
            release_connection(conn)

    def delete(self, issue_id: int) -> None:
        """Delete an issue."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM prosopography.verification_issues
                    WHERE issue_id = %s
                """, (issue_id,))
                conn.commit()
        finally:
            release_connection(conn)

    def delete_for_event(self, event_id: int) -> None:
        """Delete all issues for an event."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM prosopography.verification_issues
                    WHERE event_id = %s
                """, (event_id,))
                conn.commit()
        finally:
            release_connection(conn)

    def _row_to_issue(self, row) -> VerificationIssue:
        """Convert a database row to a VerificationIssue object."""
        return VerificationIssue(
            issue_id=row[0],
            event_id=row[1],
            issue_type=row[2],
            severity=row[3],
            description=row[4],
            resolved=row[5],
            resolution_notes=row[6],
            resolved_at=row[7],
            detected_at=row[8]
        )
