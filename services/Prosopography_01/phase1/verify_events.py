"""Step 4: Verify assembled events for quality and coherence."""

from typing import Dict, Any, List, Optional
import json

from ..llm_client import LLMClient
from ..utils import load_prompt, load_config


def verify_events(
    events: List[Dict[str, Any]],
    entities: Dict[str, List],
    deduplication_log: List[Dict[str, Any]],
    llm_client: LLMClient,
    config: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Verify assembled events for quality and coherence.

    Args:
        events: List of assembled events
        entities: Original extracted entities
        deduplication_log: Log of deduplication decisions
        llm_client: LLM client instance
        config: Optional configuration

    Returns:
        Dictionary with verified_events, summary, and deduplication_review
    """
    if config is None:
        config = load_config()

    if not events:
        return {
            "verified_events": [],
            "summary": {
                "total_events": 0,
                "valid": 0,
                "warnings": 0,
                "errors": 0,
                "career_positions": 0,
                "awards": 0
            },
            "deduplication_review": {
                "total_merges": 0,
                "questionable_merges": 0,
                "missed_merge_candidates": []
            }
        }

    system_prompt = load_prompt("phase1_verify_events", config)

    # Prepare input data
    input_data = {
        "events": events,
        "entities": entities,
        "deduplication_log": deduplication_log
    }

    user_prompt = f"""Verify these assembled career events:

INPUT DATA:
{json.dumps(input_data, indent=2)}

Check each event for temporal coherence, quote support, classification accuracy, and completeness.
Return ONLY valid JSON."""

    try:
        result = llm_client.generate_json(
            prompt=user_prompt,
            system_prompt=system_prompt
        )

        verified_events = result.get("verified_events", [])
        summary = result.get("summary", {})
        dedup_review = result.get("deduplication_review", {})

        # Ensure all events have verification status
        event_ids = {e["event_id"] for e in events}
        verified_ids = {v["event_id"] for v in verified_events}

        # Add missing verifications
        for event in events:
            if event["event_id"] not in verified_ids:
                verified_events.append({
                    "event_id": event["event_id"],
                    "status": "warning",
                    "issues": [{
                        "type": "completeness",
                        "severity": "low",
                        "description": "Not explicitly verified by LLM"
                    }]
                })

        # Recalculate summary if needed
        if not summary:
            summary = calculate_summary(events, verified_events)

        if not dedup_review:
            dedup_review = {
                "total_merges": len([d for d in deduplication_log if d.get("action") == "merged"]),
                "questionable_merges": 0,
                "missed_merge_candidates": []
            }

        return {
            "verified_events": verified_events,
            "summary": summary,
            "deduplication_review": dedup_review
        }

    except Exception as e:
        # Return basic verification on error
        verified_events = []
        for event in events:
            verified_events.append({
                "event_id": event["event_id"],
                "status": "warning",
                "issues": [{
                    "type": "completeness",
                    "severity": "low",
                    "description": f"Verification failed: {str(e)}"
                }]
            })

        return {
            "verified_events": verified_events,
            "summary": calculate_summary(events, verified_events),
            "deduplication_review": {
                "total_merges": len([d for d in deduplication_log if d.get("action") == "merged"]),
                "questionable_merges": 0,
                "missed_merge_candidates": []
            },
            "error": str(e)
        }


def calculate_summary(events: List[Dict], verified_events: List[Dict]) -> Dict[str, int]:
    """Calculate summary statistics from events and verification results."""
    status_counts = {"valid": 0, "warning": 0, "error": 0}
    for v in verified_events:
        status = v.get("status", "warning")
        if status in status_counts:
            status_counts[status] += 1

    type_counts = {"career_position": 0, "award": 0}
    for e in events:
        event_type = e.get("event_type", "career_position")
        if event_type in type_counts:
            type_counts[event_type] += 1

    return {
        "total_events": len(events),
        "valid": status_counts["valid"],
        "warnings": status_counts["warning"],
        "errors": status_counts["error"],
        "career_positions": type_counts["career_position"],
        "awards": type_counts["award"]
    }
