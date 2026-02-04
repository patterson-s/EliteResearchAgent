"""Step 3b: Create a new event from a candidate."""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from typing import Dict, Any, Optional
import json

from llm_client import LLMClient
from utils import load_prompt, load_config


def create_event(
    candidate: Dict[str, Any],
    next_event_number: int,
    llm_client: LLMClient,
    config: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Create a new event from a candidate.

    Args:
        candidate: The candidate event
        next_event_number: The number to use for E_NEW_XXX
        llm_client: LLM client instance
        config: Optional configuration

    Returns:
        Dictionary with new_event details
    """
    if config is None:
        config = load_config()

    system_prompt = load_prompt("phase3_create_event", config)

    user_prompt = f"""Create a new career event record from this candidate.

CANDIDATE:
{json.dumps(candidate, indent=2)}

Use event number: {next_event_number} (format as E_NEW_{next_event_number:03d})

Return the new event as JSON."""

    try:
        result = llm_client.generate_json(
            prompt=user_prompt,
            system_prompt=system_prompt
        )

        new_event = result.get("new_event", {})

        # Ensure required fields
        if "event_code" not in new_event:
            new_event["event_code"] = f"E_NEW_{next_event_number:03d}"

        if "event_type" not in new_event:
            new_event["event_type"] = candidate.get("event_type", "career_position")

        if "time_period" not in new_event:
            new_event["time_period"] = candidate.get("time_period", {})

        if "roles" not in new_event:
            new_event["roles"] = candidate.get("roles", [])

        if "locations" not in new_event:
            new_event["locations"] = candidate.get("locations", [])

        if "organization" not in new_event:
            new_event["organization"] = candidate.get("organization", "")

        if "supporting_quote" not in new_event:
            new_event["supporting_quote"] = candidate.get("supporting_quote", "")

        if "confidence" not in new_event:
            new_event["confidence"] = "medium"

        # Add source metadata
        new_event["source_url"] = candidate.get("source_url", "")
        new_event["chunk_id"] = candidate.get("chunk_id")

        return {"new_event": new_event}

    except Exception as e:
        # Create event from candidate data directly on error
        return {
            "new_event": {
                "event_code": f"E_NEW_{next_event_number:03d}",
                "organization": candidate.get("organization", ""),
                "event_type": candidate.get("event_type", "career_position"),
                "time_period": candidate.get("time_period", {}),
                "roles": candidate.get("roles", []),
                "locations": candidate.get("locations", []),
                "supporting_quote": candidate.get("supporting_quote", ""),
                "confidence": "low",
                "notes": f"Created with error: {str(e)}",
                "source_url": candidate.get("source_url", ""),
                "chunk_id": candidate.get("chunk_id")
            }
        }
