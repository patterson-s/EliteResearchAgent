"""Phase 1 Pipeline: Extract career events from Wikipedia."""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from typing import Dict, Any, List, Optional
import json

from db import (
    PersonRepository, EventRepository, EvidenceRepository,
    OrganizationRepository, IssueRepository,
    Person, CareerEvent, SourceEvidence, CanonicalOrganization, VerificationIssue
)
from llm_client import LLMClient
from utils import (
    load_config, get_review_dir, save_json_checkpoint,
    chunk_text, extract_source_type, normalize_time_period
)
from phase1.extract_entities import extract_entities_parallel
from phase1.discover_orgs import discover_canonical_orgs, build_org_mapping
from phase1.assemble_events import assemble_events
from phase1.verify_events import verify_events


class Phase1Pipeline:
    """Pipeline for Phase 1: Initial extraction from Wikipedia."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the pipeline."""
        self.config = config or load_config()
        self.llm_client = LLMClient(self.config)
        self.person_repo = PersonRepository()
        self.event_repo = EventRepository()
        self.evidence_repo = EvidenceRepository()
        self.org_repo = OrganizationRepository()
        self.issue_repo = IssueRepository()

    def run(
        self,
        person_name: str,
        wikipedia_text: str,
        source_url: str,
        chunk_ids: Optional[List[int]] = None,
        save_checkpoints: bool = True
    ) -> Dict[str, Any]:
        """Run the full Phase 1 pipeline.

        Args:
            person_name: Name of the person
            wikipedia_text: Full Wikipedia article text
            source_url: URL of the Wikipedia article
            chunk_ids: Optional list of chunk IDs from sources.chunks
            save_checkpoints: Whether to save JSON checkpoints

        Returns:
            Dictionary with pipeline results
        """
        # Create or get person
        person = self.person_repo.get_by_name(person_name)
        if person is None:
            person = Person(person_name=person_name, workflow_status="pending")
            person.person_id = self.person_repo.create(person)
        else:
            person_id = person.person_id

        person_id = person.person_id
        review_dir = get_review_dir(person_name)

        results = {
            "person_id": person_id,
            "person_name": person_name,
            "source_url": source_url,
            "steps": {}
        }

        # Step 1: Extract entities
        print(f"Step 1: Extracting entities from {person_name}...")
        chunks = chunk_text(wikipedia_text)
        step1_result = extract_entities_parallel(chunks, self.llm_client, self.config)

        results["steps"]["step1"] = {
            "total_chunks": step1_result["total_chunks"],
            "successful_chunks": step1_result["successful_chunks"],
            "success_rate": step1_result["success_rate"],
            "entity_counts": {
                k: len(v) for k, v in step1_result["entities"].items()
            }
        }

        if save_checkpoints:
            save_json_checkpoint(step1_result, review_dir / "phase1_entities.json")

        # Step 2: Discover canonical organizations
        print("Step 2: Discovering canonical organizations...")
        step2_result = discover_canonical_orgs(
            step1_result["entities"]["organizations"],
            self.llm_client,
            self.config,
            roles=step1_result["entities"].get("roles", []),
            person_name=person_name
        )

        results["steps"]["step2"] = {
            "canonical_orgs_count": len(step2_result["canonical_organizations"])
        }

        if save_checkpoints:
            save_json_checkpoint(step2_result, review_dir / "phase1_canonical_orgs.json")

        # Step 3: Assemble events
        print("Step 3: Assembling career events...")
        step3_result = assemble_events(
            step1_result["entities"],
            step2_result["canonical_organizations"],
            self.llm_client,
            self.config
        )

        results["steps"]["step3"] = {
            "events_count": len(step3_result["events"]),
            "deduplication_actions": len(step3_result["deduplication_log"])
        }

        if save_checkpoints:
            save_json_checkpoint(step3_result, review_dir / "phase1_events.json")

        # Step 4: Verify events
        print("Step 4: Verifying events...")
        step4_result = verify_events(
            step3_result["events"],
            step1_result["entities"],
            step3_result["deduplication_log"],
            self.llm_client,
            self.config
        )

        results["steps"]["step4"] = step4_result["summary"]

        if save_checkpoints:
            save_json_checkpoint(step4_result, review_dir / "phase1_verification.json")

        # Persist to database
        print("Persisting to database...")
        self._persist_to_db(
            person_id=person_id,
            canonical_orgs=step2_result["canonical_organizations"],
            events=step3_result["events"],
            verification=step4_result["verified_events"],
            source_url=source_url,
            chunk_ids=chunk_ids or []
        )

        # Update person status
        self.person_repo.update_status(person_id, "phase1_complete")

        results["status"] = "complete"
        print(f"Phase 1 complete for {person_name}")

        return results

    def _persist_to_db(
        self,
        person_id: int,
        canonical_orgs: List[Dict],
        events: List[Dict],
        verification: List[Dict],
        source_url: str,
        chunk_ids: List[int]
    ) -> None:
        """Persist Phase 1 results to database."""
        source_type = extract_source_type(source_url)

        # Build verification lookup
        verification_map = {v["event_id"]: v for v in verification}

        # Create canonical organizations
        org_id_map = {}  # canonical_id -> db org_id
        for org_data in canonical_orgs:
            org = CanonicalOrganization(
                person_id=person_id,
                canonical_id=org_data["canonical_id"],
                canonical_name=org_data["canonical_name"],
                org_type=org_data.get("org_type", "other"),
                metadata={
                    "variations_found": org_data.get("variations_found", []),
                    "reasoning": org_data.get("reasoning", "")
                }
            )
            db_org_id = self.org_repo.create(org)
            org_id_map[org_data["canonical_id"]] = db_org_id

            # Add aliases
            for variation in org_data.get("variations_found", []):
                self.org_repo.add_alias(db_org_id, variation)

        # Create events and evidence
        for event_data in events:
            # Get verification status
            ver = verification_map.get(event_data["event_id"], {})
            llm_status = ver.get("status", "valid")

            # Extract time info
            time_period = event_data.get("time_period", {})
            time_start, time_end = normalize_time_period(time_period.get("text"))
            if not time_start:
                time_start = time_period.get("start")
            if not time_end:
                time_end = time_period.get("end")

            # Get org_id
            canonical_org_id = event_data.get("canonical_org_id")
            db_org_id = org_id_map.get(canonical_org_id) if canonical_org_id else None

            # Create event
            event = CareerEvent(
                person_id=person_id,
                event_code=event_data["event_id"],
                event_type=event_data.get("event_type", "career_position"),
                org_id=db_org_id,
                time_start=time_start,
                time_end=time_end,
                time_text=time_period.get("text"),
                roles=event_data.get("roles", []),
                locations=event_data.get("locations", []),
                confidence=event_data.get("confidence", "medium"),
                llm_status=llm_status,
                validation_status="pending",
                created_source="phase1_extraction"
            )
            db_event_id = self.event_repo.create(event)

            # Create evidence for supporting quotes
            for quote in event_data.get("supporting_quotes", []):
                evidence = SourceEvidence(
                    event_id=db_event_id,
                    chunk_id=chunk_ids[0] if chunk_ids else None,
                    source_url=source_url,
                    source_type=source_type,
                    verbatim_quote=quote,
                    evidence_type="original",
                    extraction_phase="phase1",
                    model_used=self.config.get("model")
                )
                self.evidence_repo.create(evidence)

            # Create issues from verification
            for issue in ver.get("issues", []):
                vi = VerificationIssue(
                    event_id=db_event_id,
                    issue_type=issue.get("type", "completeness"),
                    severity=issue.get("severity", "warning"),
                    description=issue.get("description", "")
                )
                self.issue_repo.create(vi)

    def run_from_chunks(
        self,
        person_name: str,
        chunks_data: List[Dict[str, Any]],
        save_checkpoints: bool = True
    ) -> Dict[str, Any]:
        """Run pipeline from pre-loaded chunks from database.

        Args:
            person_name: Name of the person
            chunks_data: List of chunk dictionaries with text, chunk_id, source_url
            save_checkpoints: Whether to save JSON checkpoints

        Returns:
            Dictionary with pipeline results
        """
        if not chunks_data:
            raise ValueError("No chunks provided")

        # Concatenate text from all chunks
        chunks_data.sort(key=lambda x: x.get("chunk_index", 0))
        full_text = "\n".join(c.get("text", "") for c in chunks_data)

        # Get source URL (assume all from same source)
        source_url = chunks_data[0].get("source_url", "")
        chunk_ids = [c.get("chunk_id") for c in chunks_data if c.get("chunk_id")]

        return self.run(
            person_name=person_name,
            wikipedia_text=full_text,
            source_url=source_url,
            chunk_ids=chunk_ids,
            save_checkpoints=save_checkpoints
        )
