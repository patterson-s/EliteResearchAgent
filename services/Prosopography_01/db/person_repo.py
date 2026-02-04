"""Repository for Person CRUD operations."""

from typing import List, Optional
from datetime import datetime
import json

from .connection import get_connection, release_connection
from .models import Person


class PersonRepository:
    """Repository for managing Person records."""

    def create(self, person: Person) -> int:
        """Create a new person record. Returns the person_id."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO prosopography.persons (person_name, workflow_status)
                    VALUES (%s, %s)
                    ON CONFLICT (person_name) DO UPDATE SET updated_at = NOW()
                    RETURNING person_id
                """, (person.person_name, person.workflow_status))
                person_id = cur.fetchone()[0]
                conn.commit()
                return person_id
        finally:
            release_connection(conn)

    def get_by_id(self, person_id: int) -> Optional[Person]:
        """Get a person by ID."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT person_id, person_name, workflow_status, created_at, updated_at
                    FROM prosopography.persons
                    WHERE person_id = %s
                """, (person_id,))
                row = cur.fetchone()
                if row:
                    return Person(
                        person_id=row[0],
                        person_name=row[1],
                        workflow_status=row[2],
                        created_at=row[3],
                        updated_at=row[4]
                    )
                return None
        finally:
            release_connection(conn)

    def get_by_name(self, person_name: str) -> Optional[Person]:
        """Get a person by name."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT person_id, person_name, workflow_status, created_at, updated_at
                    FROM prosopography.persons
                    WHERE person_name = %s
                """, (person_name,))
                row = cur.fetchone()
                if row:
                    return Person(
                        person_id=row[0],
                        person_name=row[1],
                        workflow_status=row[2],
                        created_at=row[3],
                        updated_at=row[4]
                    )
                return None
        finally:
            release_connection(conn)

    def get_all(self, status_filter: Optional[str] = None) -> List[Person]:
        """Get all persons, optionally filtered by workflow status."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                if status_filter:
                    cur.execute("""
                        SELECT person_id, person_name, workflow_status, created_at, updated_at
                        FROM prosopography.persons
                        WHERE workflow_status = %s
                        ORDER BY person_name
                    """, (status_filter,))
                else:
                    cur.execute("""
                        SELECT person_id, person_name, workflow_status, created_at, updated_at
                        FROM prosopography.persons
                        ORDER BY person_name
                    """)
                rows = cur.fetchall()
                return [
                    Person(
                        person_id=row[0],
                        person_name=row[1],
                        workflow_status=row[2],
                        created_at=row[3],
                        updated_at=row[4]
                    )
                    for row in rows
                ]
        finally:
            release_connection(conn)

    def update_status(self, person_id: int, status: str) -> None:
        """Update a person's workflow status."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE prosopography.persons
                    SET workflow_status = %s, updated_at = NOW()
                    WHERE person_id = %s
                """, (status, person_id))
                conn.commit()
        finally:
            release_connection(conn)

    def delete(self, person_id: int) -> None:
        """Delete a person and all related records (cascades)."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM prosopography.persons
                    WHERE person_id = %s
                """, (person_id,))
                conn.commit()
        finally:
            release_connection(conn)

    def get_summary(self) -> List[dict]:
        """Get summary view for all persons."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT person_id, person_name, workflow_status,
                           event_count, sources_processed, validated_count,
                           open_errors, open_warnings
                    FROM prosopography.person_summary
                    ORDER BY person_name
                """)
                rows = cur.fetchall()
                return [
                    {
                        'person_id': row[0],
                        'person_name': row[1],
                        'workflow_status': row[2],
                        'event_count': row[3] or 0,
                        'sources_processed': row[4] or 0,
                        'validated_count': row[5] or 0,
                        'open_errors': row[6] or 0,
                        'open_warnings': row[7] or 0
                    }
                    for row in rows
                ]
        finally:
            release_connection(conn)
