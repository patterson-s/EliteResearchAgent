"""Repository for CareerEvent CRUD operations."""

from typing import List, Optional
import json

from .connection import get_connection, release_connection
from .models import CareerEvent


class EventRepository:
    """Repository for managing CareerEvent records."""

    def create(self, event: CareerEvent) -> int:
        """Create a new career event. Returns the event_id."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO prosopography.career_events
                    (person_id, event_code, event_type, org_id, time_start, time_end,
                     time_text, roles, locations, confidence, llm_status,
                     validation_status, created_source)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING event_id
                """, (
                    event.person_id, event.event_code, event.event_type, event.org_id,
                    event.time_start, event.time_end, event.time_text,
                    json.dumps(event.roles), json.dumps(event.locations),
                    event.confidence, event.llm_status, event.validation_status,
                    event.created_source
                ))
                event_id = cur.fetchone()[0]
                conn.commit()
                return event_id
        finally:
            release_connection(conn)

    def get_by_id(self, event_id: int) -> Optional[CareerEvent]:
        """Get an event by ID."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT ce.event_id, ce.person_id, ce.event_code, ce.event_type, ce.org_id,
                           ce.time_start, ce.time_end, ce.time_text, ce.roles, ce.locations,
                           ce.confidence, ce.llm_status, ce.validation_status,
                           ce.created_at, ce.updated_at, ce.created_source,
                           co.canonical_name
                    FROM prosopography.career_events ce
                    LEFT JOIN prosopography.canonical_organizations co ON ce.org_id = co.org_id
                    WHERE ce.event_id = %s
                """, (event_id,))
                row = cur.fetchone()
                if row:
                    return self._row_to_event(row)
                return None
        finally:
            release_connection(conn)

    def get_by_code(self, person_id: int, event_code: str) -> Optional[CareerEvent]:
        """Get an event by person_id and event_code."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT ce.event_id, ce.person_id, ce.event_code, ce.event_type, ce.org_id,
                           ce.time_start, ce.time_end, ce.time_text, ce.roles, ce.locations,
                           ce.confidence, ce.llm_status, ce.validation_status,
                           ce.created_at, ce.updated_at, ce.created_source,
                           co.canonical_name
                    FROM prosopography.career_events ce
                    LEFT JOIN prosopography.canonical_organizations co ON ce.org_id = co.org_id
                    WHERE ce.person_id = %s AND ce.event_code = %s
                """, (person_id, event_code))
                row = cur.fetchone()
                if row:
                    return self._row_to_event(row)
                return None
        finally:
            release_connection(conn)

    def get_for_person(self, person_id: int, status_filter: Optional[str] = None) -> List[CareerEvent]:
        """Get all events for a person, optionally filtered by validation status."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                if status_filter:
                    cur.execute("""
                        SELECT ce.event_id, ce.person_id, ce.event_code, ce.event_type, ce.org_id,
                               ce.time_start, ce.time_end, ce.time_text, ce.roles, ce.locations,
                               ce.confidence, ce.llm_status, ce.validation_status,
                               ce.created_at, ce.updated_at, ce.created_source,
                               co.canonical_name
                        FROM prosopography.career_events ce
                        LEFT JOIN prosopography.canonical_organizations co ON ce.org_id = co.org_id
                        WHERE ce.person_id = %s AND ce.validation_status = %s
                        ORDER BY ce.time_start NULLS LAST, ce.event_code
                    """, (person_id, status_filter))
                else:
                    cur.execute("""
                        SELECT ce.event_id, ce.person_id, ce.event_code, ce.event_type, ce.org_id,
                               ce.time_start, ce.time_end, ce.time_text, ce.roles, ce.locations,
                               ce.confidence, ce.llm_status, ce.validation_status,
                               ce.created_at, ce.updated_at, ce.created_source,
                               co.canonical_name
                        FROM prosopography.career_events ce
                        LEFT JOIN prosopography.canonical_organizations co ON ce.org_id = co.org_id
                        WHERE ce.person_id = %s
                        ORDER BY ce.time_start NULLS LAST, ce.event_code
                    """, (person_id,))
                rows = cur.fetchall()
                return [self._row_to_event(row) for row in rows]
        finally:
            release_connection(conn)

    def get_with_issues(self, person_id: int, severity: Optional[str] = None) -> List[CareerEvent]:
        """Get events that have unresolved issues."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                if severity:
                    cur.execute("""
                        SELECT DISTINCT ce.event_id, ce.person_id, ce.event_code, ce.event_type, ce.org_id,
                               ce.time_start, ce.time_end, ce.time_text, ce.roles, ce.locations,
                               ce.confidence, ce.llm_status, ce.validation_status,
                               ce.created_at, ce.updated_at, ce.created_source,
                               co.canonical_name
                        FROM prosopography.career_events ce
                        LEFT JOIN prosopography.canonical_organizations co ON ce.org_id = co.org_id
                        JOIN prosopography.verification_issues vi ON ce.event_id = vi.event_id
                        WHERE ce.person_id = %s AND vi.severity = %s AND NOT vi.resolved
                        ORDER BY ce.time_start NULLS LAST, ce.event_code
                    """, (person_id, severity))
                else:
                    cur.execute("""
                        SELECT DISTINCT ce.event_id, ce.person_id, ce.event_code, ce.event_type, ce.org_id,
                               ce.time_start, ce.time_end, ce.time_text, ce.roles, ce.locations,
                               ce.confidence, ce.llm_status, ce.validation_status,
                               ce.created_at, ce.updated_at, ce.created_source,
                               co.canonical_name
                        FROM prosopography.career_events ce
                        LEFT JOIN prosopography.canonical_organizations co ON ce.org_id = co.org_id
                        JOIN prosopography.verification_issues vi ON ce.event_id = vi.event_id
                        WHERE ce.person_id = %s AND NOT vi.resolved
                        ORDER BY ce.time_start NULLS LAST, ce.event_code
                    """, (person_id,))
                rows = cur.fetchall()
                return [self._row_to_event(row) for row in rows]
        finally:
            release_connection(conn)

    def update(self, event: CareerEvent) -> None:
        """Update an existing career event."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE prosopography.career_events
                    SET event_type = %s, org_id = %s, time_start = %s, time_end = %s,
                        time_text = %s, roles = %s, locations = %s, confidence = %s,
                        llm_status = %s, validation_status = %s, updated_at = NOW()
                    WHERE event_id = %s
                """, (
                    event.event_type, event.org_id, event.time_start, event.time_end,
                    event.time_text, json.dumps(event.roles), json.dumps(event.locations),
                    event.confidence, event.llm_status, event.validation_status,
                    event.event_id
                ))
                conn.commit()
        finally:
            release_connection(conn)

    def update_status(self, event_id: int, validation_status: str) -> None:
        """Update just the validation status of an event."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE prosopography.career_events
                    SET validation_status = %s, updated_at = NOW()
                    WHERE event_id = %s
                """, (validation_status, event_id))
                conn.commit()
        finally:
            release_connection(conn)

    def delete(self, event_id: int) -> None:
        """Delete an event and all related records (cascades)."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM prosopography.career_events
                    WHERE event_id = %s
                """, (event_id,))
                conn.commit()
        finally:
            release_connection(conn)

    def get_next_event_code(self, person_id: int, prefix: str = "E") -> str:
        """Generate the next event code for a person."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT event_code FROM prosopography.career_events
                    WHERE person_id = %s AND event_code LIKE %s
                    ORDER BY event_code DESC
                    LIMIT 1
                """, (person_id, f"{prefix}%"))
                row = cur.fetchone()
                if row:
                    # Extract number and increment
                    code = row[0]
                    try:
                        num = int(code.replace(prefix, "").replace("_NEW_", ""))
                        return f"{prefix}{num + 1:03d}"
                    except ValueError:
                        pass
                return f"{prefix}001"
        finally:
            release_connection(conn)

    def count_for_person(self, person_id: int) -> dict:
        """Get counts of events by validation status for a person."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT validation_status, COUNT(*)
                    FROM prosopography.career_events
                    WHERE person_id = %s
                    GROUP BY validation_status
                """, (person_id,))
                rows = cur.fetchall()
                counts = {'total': 0, 'pending': 0, 'validated': 0, 'rejected': 0, 'needs_review': 0}
                for row in rows:
                    counts[row[0]] = row[1]
                    counts['total'] += row[1]
                return counts
        finally:
            release_connection(conn)

    def _row_to_event(self, row) -> CareerEvent:
        """Convert a database row to a CareerEvent object."""
        roles = row[8] if isinstance(row[8], list) else json.loads(row[8]) if row[8] else []
        locations = row[9] if isinstance(row[9], list) else json.loads(row[9]) if row[9] else []
        return CareerEvent(
            event_id=row[0],
            person_id=row[1],
            event_code=row[2],
            event_type=row[3],
            org_id=row[4],
            time_start=row[5],
            time_end=row[6],
            time_text=row[7],
            roles=roles,
            locations=locations,
            confidence=row[10],
            llm_status=row[11],
            validation_status=row[12],
            created_at=row[13],
            updated_at=row[14],
            created_source=row[15],
            org_name=row[16] if len(row) > 16 else None
        )
