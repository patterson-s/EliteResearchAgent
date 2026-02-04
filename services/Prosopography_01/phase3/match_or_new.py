"""Step 2: Determine if candidate matches existing event or is new."""

from typing import Dict, Any, List, Optional
import json

from ..llm_client import LLMClient
from ..utils import load_prompt, load_config
from ..db import CareerEvent


def match_or_new(
    candidate: Dict[str, Any],
    existing_events: List[CareerEvent],
    llm_client: LLMClient,
    config: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Determine if a candidate should merge with existing event or be created new.

    Args:
        candidate: The candidate event extracted from source
        existing_events: List of existing CareerEvent objects
        llm_client: LLM client instance
        config: Optional configuration

    Returns:
        Dictionary with decision, target_event_id (if merge), reasoning, confidence
    """
    if config is None:
        config = load_config()

    # If no existing events, it's automatically new
    if not existing_events:
        return {
            "decision": "new",
            "target_event_id": None,
            "reasoning": "No existing events to match against",
            "confidence": "high"
        }

    system_prompt = load_prompt("phase3_match_or_new", config)

    # Format existing events for the prompt
    existing_formatted = []
    for event in existing_events:
        existing_formatted.append({
            "event_id": event.event_code,
            "db_event_id": event.event_id,
            "organization": event.org_name or "Unknown",
            "time_period": {
                "start": event.time_start,
                "end": event.time_end,
                "text": event.time_text
            },
            "roles": event.roles,
            "locations": event.locations,
            "event_type": event.event_type
        })

    user_prompt = f"""Determine if this CANDIDATE event should be merged with an existing event or created as new.

CANDIDATE:
{json.dumps(candidate, indent=2)}

EXISTING EVENTS:
{json.dumps(existing_formatted, indent=2)}

Return your decision as JSON."""

    try:
        result = llm_client.generate_json(
            prompt=user_prompt,
            system_prompt=system_prompt
        )

        decision = result.get("decision", "new")
        target_event_code = result.get("target_event_id")

        # Map event_code to db event_id
        target_db_id = None
        if decision == "merge" and target_event_code:
            for event in existing_events:
                if event.event_code == target_event_code:
                    target_db_id = event.event_id
                    break

        return {
            "decision": decision,
            "target_event_id": target_db_id,
            "target_event_code": target_event_code,
            "reasoning": result.get("reasoning", ""),
            "confidence": result.get("confidence", "medium")
        }

    except Exception as e:
        # Default to new on error
        return {
            "decision": "new",
            "target_event_id": None,
            "reasoning": f"Error during matching: {str(e)}",
            "confidence": "low"
        }
