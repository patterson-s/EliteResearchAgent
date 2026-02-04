"""Step 3a: Enrich an existing event with new information."""

from typing import Dict, Any, Optional
import json

from ..llm_client import LLMClient
from ..utils import load_prompt, load_config
from ..db import CareerEvent


def enrich_event(
    existing_event: CareerEvent,
    candidate: Dict[str, Any],
    llm_client: LLMClient,
    config: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Enrich an existing event with new information from a candidate.

    Args:
        existing_event: The existing CareerEvent to enrich
        candidate: The candidate event with new information
        llm_client: LLM client instance
        config: Optional configuration

    Returns:
        Dictionary with updated_event, new_evidence, changes_made, changes_summary
    """
    if config is None:
        config = load_config()

    system_prompt = load_prompt("phase3_enrich_event", config)

    # Format existing event
    existing_formatted = {
        "event_id": existing_event.event_code,
        "db_event_id": existing_event.event_id,
        "org_id": existing_event.org_id,
        "organization_name": existing_event.org_name or "Unknown",
        "event_type": existing_event.event_type,
        "time_period": {
            "start": existing_event.time_start,
            "end": existing_event.time_end,
            "text": existing_event.time_text
        },
        "roles": existing_event.roles,
        "locations": existing_event.locations,
        "confidence": existing_event.confidence
    }

    user_prompt = f"""Enrich this existing event with new information.

EXISTING EVENT:
{json.dumps(existing_formatted, indent=2)}

NEW DETAILS FROM SOURCE:
{json.dumps(candidate, indent=2)}

Return the enriched event as JSON."""

    try:
        result = llm_client.generate_json(
            prompt=user_prompt,
            system_prompt=system_prompt
        )

        updated = result.get("updated_event", {})
        new_evidence = result.get("new_evidence", {})
        changes_made = result.get("changes_made", False)
        changes_summary = result.get("changes_summary", "")

        # Ensure we have the quote from the candidate if not provided
        if not new_evidence.get("quote") and candidate.get("supporting_quote"):
            new_evidence["quote"] = candidate["supporting_quote"]
            new_evidence["contribution"] = "validation"

        return {
            "updated_event": updated,
            "new_evidence": new_evidence,
            "changes_made": changes_made,
            "changes_summary": changes_summary
        }

    except Exception as e:
        # Return minimal update on error
        return {
            "updated_event": existing_formatted,
            "new_evidence": {
                "quote": candidate.get("supporting_quote", ""),
                "contribution": "validation"
            },
            "changes_made": False,
            "changes_summary": f"Error during enrichment: {str(e)}"
        }
