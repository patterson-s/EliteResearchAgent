"""Phase 3 Pipeline: Supplement events with additional sources."""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from typing import Dict, Any, List, Optional, Callable
import json

from db import (
    PersonRepository, EventRepository, EvidenceRepository,
    OrganizationRepository, IssueRepository,
    Person, CareerEvent, SourceEvidence, CanonicalOrganization,
    get_connection, release_connection
)
from llm_client import LLMClient
from utils import (
    load_config, get_review_dir, save_json_checkpoint,
    extract_source_type, normalize_time_period
)
from phase3.extract_candidates import extract_candidates
from phase3.match_or_new import match_or_new
from phase3.enrich_event import enrich_event
from phase3.create_event import create_event


class Phase3Pipeline:
    """Pipeline for Phase 3: Supplement with additional sources."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the pipeline."""
        self.config = config or load_config()
        self.llm_client = LLMClient(self.config)
        self.person_repo = PersonRepository()
        self.event_repo = EventRepository()
        self.evidence_repo = EvidenceRepository()
        self.org_repo = OrganizationRepository()
        self.issue_repo = IssueRepository()

    def process_source(
        self,
        person_id: int,
        chunks: List[Dict[str, Any]],
        source_url: str,
        review_mode: bool = False,
        decision_callback: Optional[Callable] = None,
        save_checkpoints: bool = True
    ) -> Dict[str, Any]:
        """Process a single source for supplementation.

        Args:
            person_id: ID of the person
            chunks: List of chunk dictionaries with text, chunk_id
            source_url: URL of the source
            review_mode: If True, call decision_callback for each decision
            decision_callback: Function to call for review (receives candidate, decision, returns approved decision)
            save_checkpoints: Whether to save JSON checkpoints

        Returns:
            Dictionary with processing results
        """
        person = self.person_repo.get_by_id(person_id)
        if not person:
            raise ValueError(f"Person {person_id} not found")

        review_dir = get_review_dir(person.person_name)
        source_type = extract_source_type(source_url)

        # Get existing events
        existing_events = self.event_repo.get_for_person(person_id)
        existing_events = [e for e in existing_events if e.validation_status != "rejected"]

        # Track results
        decisions = []
        events_merged = 0
        events_created = 0
        new_event_counter = self._get_next_new_event_number(person_id)

        # Process each chunk
        for chunk in chunks:
            chunk_text = chunk.get("text", "")
            chunk_id = chunk.get("chunk_id")

            if not chunk_text.strip():
                continue

            # Step 1: Extract candidates
            candidates = extract_candidates(
                chunk_text, chunk_id, source_url,
                self.llm_client, self.config
            )

            # Process each candidate
            for candidate in candidates:
                if "error" in candidate:
                    decisions.append({
                        "chunk_id": chunk_id,
                        "candidate": candidate,
                        "action": "error",
                        "error": candidate["error"]
                    })
                    continue

                # Step 2: Match or new decision
                decision_result = match_or_new(
                    candidate, existing_events,
                    self.llm_client, self.config
                )

                # Review mode: get user approval
                if review_mode and decision_callback:
                    decision_result = decision_callback(candidate, decision_result)
                    if decision_result is None:
                        # User skipped
                        decisions.append({
                            "chunk_id": chunk_id,
                            "candidate": candidate,
                            "action": "skipped",
                            "reasoning": "Skipped by user"
                        })
                        continue

                # Process based on decision
                if decision_result["decision"] == "merge":
                    # Step 3a: Enrich existing event
                    target_event = self.event_repo.get_by_id(decision_result["target_event_id"])
                    if target_event:
                        enrichment = enrich_event(
                            target_event, candidate,
                            self.llm_client, self.config
                        )

                        # Apply enrichment
                        self._apply_enrichment(
                            target_event, enrichment, candidate,
                            source_url, source_type, chunk_id
                        )

                        decisions.append({
                            "chunk_id": chunk_id,
                            "candidate": candidate,
                            "action": "merge",
                            "target_event_id": target_event.event_id,
                            "target_event_code": target_event.event_code,
                            "reasoning": decision_result["reasoning"],
                            "changes_made": enrichment["changes_made"],
                            "changes_summary": enrichment["changes_summary"]
                        })
                        events_merged += 1

                else:
                    # Step 3b: Create new event
                    creation = create_event(
                        candidate, new_event_counter,
                        self.llm_client, self.config
                    )

                    new_event_id = self._create_new_event(
                        person_id, creation["new_event"],
                        source_url, source_type, chunk_id
                    )

                    decisions.append({
                        "chunk_id": chunk_id,
                        "candidate": candidate,
                        "action": "new",
                        "new_event_id": new_event_id,
                        "new_event_code": creation["new_event"]["event_code"],
                        "reasoning": decision_result["reasoning"]
                    })

                    # Add to existing events for subsequent matching
                    new_event = self.event_repo.get_by_id(new_event_id)
                    if new_event:
                        existing_events.append(new_event)

                    events_created += 1
                    new_event_counter += 1

        # Save checkpoint
        if save_checkpoints:
            checkpoint = {
                "source_url": source_url,
                "chunks_processed": len(chunks),
                "decisions": decisions,
                "events_merged": events_merged,
                "events_created": events_created
            }
            checkpoint_path = review_dir / f"phase3_{source_url.replace('/', '_')[:50]}.json"
            save_json_checkpoint(checkpoint, checkpoint_path)

        # Record source as processed
        self._record_source_processed(
            person_id, source_url, source_type,
            len(chunks), len(decisions), events_merged, events_created
        )

        # Update person status
        self.person_repo.update_status(person_id, "phase3_in_progress")

        return {
            "source_url": source_url,
            "chunks_processed": len(chunks),
            "candidates_found": len(decisions),
            "events_merged": events_merged,
            "events_created": events_created,
            "decisions": decisions
        }

    def _apply_enrichment(
        self,
        event: CareerEvent,
        enrichment: Dict[str, Any],
        candidate: Dict[str, Any],
        source_url: str,
        source_type: str,
        chunk_id: Optional[int]
    ) -> None:
        """Apply enrichment changes to an event."""
        updated = enrichment.get("updated_event", {})
        new_evidence = enrichment.get("new_evidence", {})

        # Update event fields if changed
        if enrichment.get("changes_made"):
            if updated.get("time_period"):
                tp = updated["time_period"]
                if tp.get("start"):
                    event.time_start = tp["start"]
                if tp.get("end"):
                    event.time_end = tp["end"]
                if tp.get("text"):
                    event.time_text = tp["text"]

            if updated.get("roles"):
                # Union of roles
                new_roles = set(event.roles) | set(updated["roles"])
                event.roles = list(new_roles)

            if updated.get("locations"):
                new_locs = set(event.locations) | set(updated["locations"])
                event.locations = list(new_locs)

            if updated.get("confidence"):
                # Upgrade confidence if new source provides more certainty
                if updated["confidence"] == "high" or (
                    updated["confidence"] == "medium" and event.confidence == "low"
                ):
                    event.confidence = updated["confidence"]

            self.event_repo.update(event)

        # Add evidence
        quote = new_evidence.get("quote") or candidate.get("supporting_quote", "")
        if quote:
            contribution = new_evidence.get("contribution", "validation")
            evidence_type = "validation" if not enrichment.get("changes_made") else "supplementation"

            evidence = SourceEvidence(
                event_id=event.event_id,
                chunk_id=chunk_id,
                source_url=source_url,
                source_type=source_type,
                verbatim_quote=quote,
                evidence_type=evidence_type,
                contribution=contribution,
                extraction_phase="phase3",
                model_used=self.config.get("model")
            )
            self.evidence_repo.create(evidence)

    def _create_new_event(
        self,
        person_id: int,
        event_data: Dict[str, Any],
        source_url: str,
        source_type: str,
        chunk_id: Optional[int]
    ) -> int:
        """Create a new event and its evidence."""
        time_period = event_data.get("time_period", {})
        time_start, time_end = normalize_time_period(time_period.get("text"))
        if not time_start:
            time_start = time_period.get("start")
        if not time_end:
            time_end = time_period.get("end")

        # Try to find or create organization
        org_id = None
        org_name = event_data.get("organization")
        if org_name:
            # Check if org exists
            existing_orgs = self.org_repo.get_for_person(person_id)
            for org in existing_orgs:
                if org.canonical_name.lower() == org_name.lower():
                    org_id = org.org_id
                    break

            if org_id is None:
                # Create new org
                canonical_id = self.org_repo.get_next_canonical_id(person_id)
                new_org = CanonicalOrganization(
                    person_id=person_id,
                    canonical_id=canonical_id,
                    canonical_name=org_name,
                    org_type="other",
                    metadata={"created_in_phase3": True}
                )
                org_id = self.org_repo.create(new_org)

        # Create event
        event = CareerEvent(
            person_id=person_id,
            event_code=event_data.get("event_code", "E_NEW_001"),
            event_type=event_data.get("event_type", "career_position"),
            org_id=org_id,
            time_start=time_start,
            time_end=time_end,
            time_text=time_period.get("text"),
            roles=event_data.get("roles", []),
            locations=event_data.get("locations", []),
            confidence=event_data.get("confidence", "medium"),
            llm_status="valid",
            validation_status="pending",
            created_source="phase3_supplementation"
        )
        event_id = self.event_repo.create(event)

        # Create evidence
        quote = event_data.get("supporting_quote", "")
        if quote:
            evidence = SourceEvidence(
                event_id=event_id,
                chunk_id=chunk_id,
                source_url=source_url,
                source_type=source_type,
                verbatim_quote=quote,
                evidence_type="original",
                extraction_phase="phase3",
                model_used=self.config.get("model")
            )
            self.evidence_repo.create(evidence)

        return event_id

    def _get_next_new_event_number(self, person_id: int) -> int:
        """Get the next number for E_NEW_XXX events."""
        events = self.event_repo.get_for_person(person_id)
        max_num = 0
        for event in events:
            if event.event_code.startswith("E_NEW_"):
                try:
                    num = int(event.event_code.replace("E_NEW_", ""))
                    max_num = max(max_num, num)
                except ValueError:
                    pass
        return max_num + 1

    def _record_source_processed(
        self,
        person_id: int,
        source_url: str,
        source_type: str,
        chunks_processed: int,
        candidates_found: int,
        events_merged: int,
        events_created: int
    ) -> None:
        """Record that a source has been processed."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO prosopography.sources_processed
                    (person_id, source_url, source_type, processing_phase,
                     chunks_processed, events_extracted, events_merged, events_created)
                    VALUES (%s, %s, %s, 'phase3', %s, %s, %s, %s)
                    ON CONFLICT (person_id, source_url) DO UPDATE SET
                        chunks_processed = EXCLUDED.chunks_processed,
                        events_extracted = EXCLUDED.events_extracted,
                        events_merged = EXCLUDED.events_merged,
                        events_created = EXCLUDED.events_created,
                        processed_at = NOW()
                """, (
                    person_id, source_url, source_type,
                    chunks_processed, candidates_found, events_merged, events_created
                ))
                conn.commit()
        finally:
            release_connection(conn)

    def get_unprocessed_sources(self, person_id: int) -> List[Dict[str, Any]]:
        """Get sources that haven't been processed yet for a person."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                # Get all sources for person from sources.chunks
                cur.execute("""
                    SELECT DISTINCT c.source_url, c.title
                    FROM sources.chunks c
                    JOIN sources.search_results sr ON c.source_url = sr.url
                    JOIN sources.persons_searched ps ON sr.person_id = ps.person_id
                    WHERE ps.person_name = (
                        SELECT person_name FROM prosopography.persons WHERE person_id = %s
                    )
                    AND c.source_url NOT IN (
                        SELECT source_url FROM prosopography.sources_processed
                        WHERE person_id = %s
                    )
                """, (person_id, person_id))
                rows = cur.fetchall()
                return [{"source_url": row[0], "title": row[1]} for row in rows]
        finally:
            release_connection(conn)

    def finalize(self, person_id: int) -> Dict[str, Any]:
        """Finalize Phase 3 processing."""
        self.person_repo.update_status(person_id, "complete")

        # Get final stats
        events = self.event_repo.get_for_person(person_id)
        counts = self.event_repo.count_for_person(person_id)

        return {
            "total_events": len(events),
            "status_counts": counts,
            "workflow_status": "complete"
        }
