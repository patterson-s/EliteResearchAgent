"""Step 3: Assemble career events from entities and canonical organizations."""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from typing import Dict, Any, List, Optional
import json

from llm_client import LLMClient
from utils import load_prompt, load_config


def assemble_events(
    entities: Dict[str, List],
    canonical_orgs: List[Dict[str, Any]],
    llm_client: LLMClient,
    config: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Assemble career events from extracted entities.

    Args:
        entities: Dictionary with time_markers, organizations, roles, locations
        canonical_orgs: List of canonical organizations
        llm_client: LLM client instance
        config: Optional configuration

    Returns:
        Dictionary with events list and deduplication log
    """
    if config is None:
        config = load_config()

    system_prompt = load_prompt("phase1_assemble_events", config)

    # Prepare input data
    input_data = {
        "entities": {
            "time_markers": entities.get("time_markers", []),
            "organizations": entities.get("organizations", []),
            "roles": entities.get("roles", []),
            "locations": entities.get("locations", [])
        },
        "canonical_organizations": canonical_orgs
    }

    user_prompt = f"""Assemble career events from these extracted entities and canonical organizations:

INPUT DATA:
{json.dumps(input_data, indent=2)}

Create ONE event for EVERY role. Return ONLY valid JSON."""

    try:
        result = llm_client.generate_json(
            prompt=user_prompt,
            system_prompt=system_prompt
        )

        events = result.get("events", [])
        dedup_log = result.get("deduplication_log", [])
        unprocessed = result.get("unprocessed_roles", [])

        # Validate and clean up events
        validated_events = []
        for event in events:
            validated_event = {
                "event_id": event.get("event_id", f"E{len(validated_events)+1:03d}"),
                "event_type": event.get("event_type", "career_position"),
                "canonical_org_id": event.get("canonical_org_id"),
                "canonical_org_name": event.get("canonical_org_name", ""),
                "time_period": event.get("time_period", {}),
                "roles": event.get("roles", []),
                "locations": event.get("locations", []),
                "supporting_quotes": event.get("supporting_quotes", []),
                "source_entity_ids": event.get("source_entity_ids", {}),
                "confidence": event.get("confidence", "medium"),
                "merged_from": event.get("merged_from", []),
                "notes": event.get("notes", "")
            }
            validated_events.append(validated_event)

        return {
            "events": validated_events,
            "deduplication_log": dedup_log,
            "unprocessed_roles": unprocessed
        }

    except Exception as e:
        return {
            "events": [],
            "deduplication_log": [],
            "unprocessed_roles": [],
            "error": str(e)
        }


def extract_time_info(time_period: Dict[str, Any]) -> tuple:
    """Extract start and end from time period.

    Returns:
        Tuple of (start, end, text)
    """
    return (
        time_period.get("start"),
        time_period.get("end"),
        time_period.get("text", "")
    )
