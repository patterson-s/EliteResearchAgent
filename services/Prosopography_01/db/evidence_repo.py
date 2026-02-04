"""Repository for SourceEvidence CRUD operations."""

from typing import List, Optional
from datetime import datetime

from .connection import get_connection, release_connection
from .models import SourceEvidence


class EvidenceRepository:
    """Repository for managing SourceEvidence records."""

    def create(self, evidence: SourceEvidence) -> int:
        """Create a new source evidence record. Returns the evidence_id."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO prosopography.source_evidence
                    (event_id, chunk_id, source_url, source_type, verbatim_quote,
                     quote_context, evidence_type, contribution, extraction_phase, model_used)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING evidence_id
                """, (
                    evidence.event_id, evidence.chunk_id, evidence.source_url,
                    evidence.source_type, evidence.verbatim_quote, evidence.quote_context,
                    evidence.evidence_type, evidence.contribution, evidence.extraction_phase,
                    evidence.model_used
                ))
                evidence_id = cur.fetchone()[0]
                conn.commit()
                return evidence_id
        finally:
            release_connection(conn)

    def create_many(self, evidence_list: List[SourceEvidence]) -> List[int]:
        """Create multiple evidence records. Returns list of evidence_ids."""
        if not evidence_list:
            return []
        conn = get_connection()
        try:
            evidence_ids = []
            with conn.cursor() as cur:
                for evidence in evidence_list:
                    cur.execute("""
                        INSERT INTO prosopography.source_evidence
                        (event_id, chunk_id, source_url, source_type, verbatim_quote,
                         quote_context, evidence_type, contribution, extraction_phase, model_used)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING evidence_id
                    """, (
                        evidence.event_id, evidence.chunk_id, evidence.source_url,
                        evidence.source_type, evidence.verbatim_quote, evidence.quote_context,
                        evidence.evidence_type, evidence.contribution, evidence.extraction_phase,
                        evidence.model_used
                    ))
                    evidence_ids.append(cur.fetchone()[0])
                conn.commit()
            return evidence_ids
        finally:
            release_connection(conn)

    def get_by_id(self, evidence_id: int) -> Optional[SourceEvidence]:
        """Get evidence by ID."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT evidence_id, event_id, chunk_id, source_url, source_type,
                           verbatim_quote, quote_context, evidence_type, contribution,
                           extraction_phase, processing_timestamp, model_used
                    FROM prosopography.source_evidence
                    WHERE evidence_id = %s
                """, (evidence_id,))
                row = cur.fetchone()
                if row:
                    return self._row_to_evidence(row)
                return None
        finally:
            release_connection(conn)

    def get_for_event(self, event_id: int) -> List[SourceEvidence]:
        """Get all evidence for a specific event."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT evidence_id, event_id, chunk_id, source_url, source_type,
                           verbatim_quote, quote_context, evidence_type, contribution,
                           extraction_phase, processing_timestamp, model_used
                    FROM prosopography.source_evidence
                    WHERE event_id = %s
                    ORDER BY processing_timestamp
                """, (event_id,))
                rows = cur.fetchall()
                return [self._row_to_evidence(row) for row in rows]
        finally:
            release_connection(conn)

    def get_for_event_by_type(self, event_id: int, evidence_type: str) -> List[SourceEvidence]:
        """Get evidence for an event filtered by type (original, validation, supplementation)."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT evidence_id, event_id, chunk_id, source_url, source_type,
                           verbatim_quote, quote_context, evidence_type, contribution,
                           extraction_phase, processing_timestamp, model_used
                    FROM prosopography.source_evidence
                    WHERE event_id = %s AND evidence_type = %s
                    ORDER BY processing_timestamp
                """, (event_id, evidence_type))
                rows = cur.fetchall()
                return [self._row_to_evidence(row) for row in rows]
        finally:
            release_connection(conn)

    def get_for_person(self, person_id: int) -> List[SourceEvidence]:
        """Get all evidence for a person's events."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT se.evidence_id, se.event_id, se.chunk_id, se.source_url, se.source_type,
                           se.verbatim_quote, se.quote_context, se.evidence_type, se.contribution,
                           se.extraction_phase, se.processing_timestamp, se.model_used
                    FROM prosopography.source_evidence se
                    JOIN prosopography.career_events ce ON se.event_id = ce.event_id
                    WHERE ce.person_id = %s
                    ORDER BY se.processing_timestamp
                """, (person_id,))
                rows = cur.fetchall()
                return [self._row_to_evidence(row) for row in rows]
        finally:
            release_connection(conn)

    def count_by_source(self, person_id: int) -> List[dict]:
        """Get counts of evidence by source URL for a person."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT se.source_url, se.source_type, COUNT(*) as count
                    FROM prosopography.source_evidence se
                    JOIN prosopography.career_events ce ON se.event_id = ce.event_id
                    WHERE ce.person_id = %s
                    GROUP BY se.source_url, se.source_type
                    ORDER BY count DESC
                """, (person_id,))
                rows = cur.fetchall()
                return [
                    {'source_url': row[0], 'source_type': row[1], 'count': row[2]}
                    for row in rows
                ]
        finally:
            release_connection(conn)

    def count_unique_sources_for_event(self, event_id: int) -> int:
        """Count unique sources that provide evidence for an event."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(DISTINCT source_url)
                    FROM prosopography.source_evidence
                    WHERE event_id = %s
                """, (event_id,))
                return cur.fetchone()[0]
        finally:
            release_connection(conn)

    def delete(self, evidence_id: int) -> None:
        """Delete an evidence record."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM prosopography.source_evidence
                    WHERE evidence_id = %s
                """, (evidence_id,))
                conn.commit()
        finally:
            release_connection(conn)

    def delete_for_event(self, event_id: int) -> None:
        """Delete all evidence for an event."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM prosopography.source_evidence
                    WHERE event_id = %s
                """, (event_id,))
                conn.commit()
        finally:
            release_connection(conn)

    def _row_to_evidence(self, row) -> SourceEvidence:
        """Convert a database row to a SourceEvidence object."""
        return SourceEvidence(
            evidence_id=row[0],
            event_id=row[1],
            chunk_id=row[2],
            source_url=row[3],
            source_type=row[4],
            verbatim_quote=row[5],
            quote_context=row[6],
            evidence_type=row[7],
            contribution=row[8],
            extraction_phase=row[9],
            processing_timestamp=row[10],
            model_used=row[11]
        )
