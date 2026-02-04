"""Repository for UserCorrection CRUD operations."""

from typing import List, Optional

from .connection import get_connection, release_connection
from .models import UserCorrection


class CorrectionRepository:
    """Repository for managing UserCorrection records."""

    def create(self, correction: UserCorrection) -> int:
        """Create a new correction record. Returns the correction_id."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO prosopography.user_corrections
                    (event_id, field_name, original_value, corrected_value,
                     is_valid, correction_notes, corrected_by)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING correction_id
                """, (
                    correction.event_id, correction.field_name, correction.original_value,
                    correction.corrected_value, correction.is_valid, correction.correction_notes,
                    correction.corrected_by
                ))
                correction_id = cur.fetchone()[0]
                conn.commit()
                return correction_id
        finally:
            release_connection(conn)

    def create_many(self, corrections: List[UserCorrection]) -> List[int]:
        """Create multiple correction records. Returns list of correction_ids."""
        if not corrections:
            return []
        conn = get_connection()
        try:
            correction_ids = []
            with conn.cursor() as cur:
                for correction in corrections:
                    cur.execute("""
                        INSERT INTO prosopography.user_corrections
                        (event_id, field_name, original_value, corrected_value,
                         is_valid, correction_notes, corrected_by)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        RETURNING correction_id
                    """, (
                        correction.event_id, correction.field_name, correction.original_value,
                        correction.corrected_value, correction.is_valid, correction.correction_notes,
                        correction.corrected_by
                    ))
                    correction_ids.append(cur.fetchone()[0])
                conn.commit()
            return correction_ids
        finally:
            release_connection(conn)

    def get_by_id(self, correction_id: int) -> Optional[UserCorrection]:
        """Get a correction by ID."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT correction_id, event_id, field_name, original_value, corrected_value,
                           is_valid, correction_notes, corrected_by, corrected_at
                    FROM prosopography.user_corrections
                    WHERE correction_id = %s
                """, (correction_id,))
                row = cur.fetchone()
                if row:
                    return self._row_to_correction(row)
                return None
        finally:
            release_connection(conn)

    def get_for_event(self, event_id: int) -> List[UserCorrection]:
        """Get all corrections for a specific event."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT correction_id, event_id, field_name, original_value, corrected_value,
                           is_valid, correction_notes, corrected_by, corrected_at
                    FROM prosopography.user_corrections
                    WHERE event_id = %s
                    ORDER BY corrected_at DESC
                """, (event_id,))
                rows = cur.fetchall()
                return [self._row_to_correction(row) for row in rows]
        finally:
            release_connection(conn)

    def get_for_event_and_field(self, event_id: int, field_name: str) -> Optional[UserCorrection]:
        """Get the most recent correction for a specific event and field."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT correction_id, event_id, field_name, original_value, corrected_value,
                           is_valid, correction_notes, corrected_by, corrected_at
                    FROM prosopography.user_corrections
                    WHERE event_id = %s AND field_name = %s
                    ORDER BY corrected_at DESC
                    LIMIT 1
                """, (event_id, field_name))
                row = cur.fetchone()
                if row:
                    return self._row_to_correction(row)
                return None
        finally:
            release_connection(conn)

    def get_for_person(self, person_id: int) -> List[UserCorrection]:
        """Get all corrections for a person's events."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT uc.correction_id, uc.event_id, uc.field_name, uc.original_value,
                           uc.corrected_value, uc.is_valid, uc.correction_notes,
                           uc.corrected_by, uc.corrected_at
                    FROM prosopography.user_corrections uc
                    JOIN prosopography.career_events ce ON uc.event_id = ce.event_id
                    WHERE ce.person_id = %s
                    ORDER BY uc.corrected_at DESC
                """, (person_id,))
                rows = cur.fetchall()
                return [self._row_to_correction(row) for row in rows]
        finally:
            release_connection(conn)

    def get_invalid_corrections(self, person_id: int) -> List[UserCorrection]:
        """Get all corrections where the original value was marked invalid."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT uc.correction_id, uc.event_id, uc.field_name, uc.original_value,
                           uc.corrected_value, uc.is_valid, uc.correction_notes,
                           uc.corrected_by, uc.corrected_at
                    FROM prosopography.user_corrections uc
                    JOIN prosopography.career_events ce ON uc.event_id = ce.event_id
                    WHERE ce.person_id = %s AND NOT uc.is_valid
                    ORDER BY uc.corrected_at DESC
                """, (person_id,))
                rows = cur.fetchall()
                return [self._row_to_correction(row) for row in rows]
        finally:
            release_connection(conn)

    def count_by_field(self, person_id: int) -> dict:
        """Get counts of corrections by field name for a person."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT uc.field_name,
                           COUNT(*) as total,
                           SUM(CASE WHEN uc.is_valid THEN 0 ELSE 1 END) as invalid_count
                    FROM prosopography.user_corrections uc
                    JOIN prosopography.career_events ce ON uc.event_id = ce.event_id
                    WHERE ce.person_id = %s
                    GROUP BY uc.field_name
                """, (person_id,))
                rows = cur.fetchall()
                return {
                    row[0]: {'total': row[1], 'invalid': row[2]}
                    for row in rows
                }
        finally:
            release_connection(conn)

    def delete(self, correction_id: int) -> None:
        """Delete a correction record."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM prosopography.user_corrections
                    WHERE correction_id = %s
                """, (correction_id,))
                conn.commit()
        finally:
            release_connection(conn)

    def delete_for_event(self, event_id: int) -> None:
        """Delete all corrections for an event."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM prosopography.user_corrections
                    WHERE event_id = %s
                """, (event_id,))
                conn.commit()
        finally:
            release_connection(conn)

    def _row_to_correction(self, row) -> UserCorrection:
        """Convert a database row to a UserCorrection object."""
        return UserCorrection(
            correction_id=row[0],
            event_id=row[1],
            field_name=row[2],
            original_value=row[3],
            corrected_value=row[4],
            is_valid=row[5],
            correction_notes=row[6],
            corrected_by=row[7],
            corrected_at=row[8]
        )
