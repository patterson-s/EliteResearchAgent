"""Repository for CanonicalOrganization CRUD operations."""

from typing import List, Optional
import json

from .connection import get_connection, release_connection
from .models import CanonicalOrganization


class OrganizationRepository:
    """Repository for managing CanonicalOrganization records."""

    def create(self, org: CanonicalOrganization) -> int:
        """Create a new canonical organization. Returns the org_id."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO prosopography.canonical_organizations
                    (person_id, canonical_id, canonical_name, org_type, country, parent_org_id, metadata)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (person_id, canonical_id) DO UPDATE SET
                        canonical_name = EXCLUDED.canonical_name,
                        org_type = EXCLUDED.org_type,
                        country = EXCLUDED.country,
                        metadata = EXCLUDED.metadata
                    RETURNING org_id
                """, (
                    org.person_id, org.canonical_id, org.canonical_name,
                    org.org_type, org.country, org.parent_org_id,
                    json.dumps(org.metadata) if org.metadata else '{}'
                ))
                org_id = cur.fetchone()[0]
                conn.commit()
                return org_id
        finally:
            release_connection(conn)

    def create_many(self, orgs: List[CanonicalOrganization]) -> List[int]:
        """Create multiple organizations. Returns list of org_ids."""
        if not orgs:
            return []
        conn = get_connection()
        try:
            org_ids = []
            with conn.cursor() as cur:
                for org in orgs:
                    cur.execute("""
                        INSERT INTO prosopography.canonical_organizations
                        (person_id, canonical_id, canonical_name, org_type, country, parent_org_id, metadata)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (person_id, canonical_id) DO UPDATE SET
                            canonical_name = EXCLUDED.canonical_name,
                            org_type = EXCLUDED.org_type,
                            country = EXCLUDED.country,
                            metadata = EXCLUDED.metadata
                        RETURNING org_id
                    """, (
                        org.person_id, org.canonical_id, org.canonical_name,
                        org.org_type, org.country, org.parent_org_id,
                        json.dumps(org.metadata) if org.metadata else '{}'
                    ))
                    org_ids.append(cur.fetchone()[0])
                conn.commit()
            return org_ids
        finally:
            release_connection(conn)

    def get_by_id(self, org_id: int) -> Optional[CanonicalOrganization]:
        """Get an organization by ID."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT org_id, person_id, canonical_id, canonical_name, org_type,
                           country, parent_org_id, metadata, created_at
                    FROM prosopography.canonical_organizations
                    WHERE org_id = %s
                """, (org_id,))
                row = cur.fetchone()
                if row:
                    return self._row_to_org(row)
                return None
        finally:
            release_connection(conn)

    def get_by_canonical_id(self, person_id: int, canonical_id: str) -> Optional[CanonicalOrganization]:
        """Get an organization by person_id and canonical_id."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT org_id, person_id, canonical_id, canonical_name, org_type,
                           country, parent_org_id, metadata, created_at
                    FROM prosopography.canonical_organizations
                    WHERE person_id = %s AND canonical_id = %s
                """, (person_id, canonical_id))
                row = cur.fetchone()
                if row:
                    return self._row_to_org(row)
                return None
        finally:
            release_connection(conn)

    def get_for_person(self, person_id: int) -> List[CanonicalOrganization]:
        """Get all organizations for a person."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT org_id, person_id, canonical_id, canonical_name, org_type,
                           country, parent_org_id, metadata, created_at
                    FROM prosopography.canonical_organizations
                    WHERE person_id = %s
                    ORDER BY canonical_id
                """, (person_id,))
                rows = cur.fetchall()
                return [self._row_to_org(row) for row in rows]
        finally:
            release_connection(conn)

    def get_next_canonical_id(self, person_id: int) -> str:
        """Generate the next canonical ID for a person."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT canonical_id FROM prosopography.canonical_organizations
                    WHERE person_id = %s AND canonical_id LIKE 'ORG_%'
                    ORDER BY canonical_id DESC
                    LIMIT 1
                """, (person_id,))
                row = cur.fetchone()
                if row:
                    try:
                        num = int(row[0].replace("ORG_", ""))
                        return f"ORG_{num + 1:03d}"
                    except ValueError:
                        pass
                return "ORG_001"
        finally:
            release_connection(conn)

    def add_alias(self, org_id: int, alias_name: str, source_chunk_id: Optional[int] = None) -> int:
        """Add an alias for an organization. Returns alias_id."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO prosopography.organization_aliases (org_id, alias_name, source_chunk_id)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (org_id, alias_name) DO NOTHING
                    RETURNING alias_id
                """, (org_id, alias_name, source_chunk_id))
                result = cur.fetchone()
                conn.commit()
                return result[0] if result else 0
        finally:
            release_connection(conn)

    def get_aliases(self, org_id: int) -> List[str]:
        """Get all aliases for an organization."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT alias_name FROM prosopography.organization_aliases
                    WHERE org_id = %s
                    ORDER BY alias_name
                """, (org_id,))
                return [row[0] for row in cur.fetchall()]
        finally:
            release_connection(conn)

    def find_by_alias(self, person_id: int, alias_name: str) -> Optional[CanonicalOrganization]:
        """Find an organization by one of its aliases."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT co.org_id, co.person_id, co.canonical_id, co.canonical_name, co.org_type,
                           co.country, co.parent_org_id, co.metadata, co.created_at
                    FROM prosopography.canonical_organizations co
                    JOIN prosopography.organization_aliases oa ON co.org_id = oa.org_id
                    WHERE co.person_id = %s AND oa.alias_name = %s
                """, (person_id, alias_name))
                row = cur.fetchone()
                if row:
                    return self._row_to_org(row)
                return None
        finally:
            release_connection(conn)

    def update(self, org: CanonicalOrganization) -> None:
        """Update an existing organization."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE prosopography.canonical_organizations
                    SET canonical_name = %s, org_type = %s, country = %s,
                        parent_org_id = %s, metadata = %s
                    WHERE org_id = %s
                """, (
                    org.canonical_name, org.org_type, org.country,
                    org.parent_org_id, json.dumps(org.metadata) if org.metadata else '{}',
                    org.org_id
                ))
                conn.commit()
        finally:
            release_connection(conn)

    def delete(self, org_id: int) -> None:
        """Delete an organization and its aliases."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM prosopography.canonical_organizations
                    WHERE org_id = %s
                """, (org_id,))
                conn.commit()
        finally:
            release_connection(conn)

    def _row_to_org(self, row) -> CanonicalOrganization:
        """Convert a database row to a CanonicalOrganization object."""
        metadata = row[7] if isinstance(row[7], dict) else json.loads(row[7]) if row[7] else {}
        return CanonicalOrganization(
            org_id=row[0],
            person_id=row[1],
            canonical_id=row[2],
            canonical_name=row[3],
            org_type=row[4],
            country=row[5],
            parent_org_id=row[6],
            metadata=metadata,
            created_at=row[8]
        )
