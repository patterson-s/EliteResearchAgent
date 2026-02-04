"""Step 1: Extract candidate career events from source chunks."""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from typing import Dict, Any, List, Optional

from llm_client import LLMClient
from utils import load_prompt, load_config


def extract_candidates(
    chunk_text: str,
    chunk_id: Optional[int],
    source_url: str,
    llm_client: LLMClient,
    config: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    """Extract candidate career events from a text chunk.

    Args:
        chunk_text: The text to extract from
        chunk_id: ID of the chunk in sources.chunks
        source_url: URL of the source
        llm_client: LLM client instance
        config: Optional configuration

    Returns:
        List of candidate event dictionaries
    """
    if config is None:
        config = load_config()

    system_prompt = load_prompt("phase3_extract_candidates", config)

    user_prompt = f"""Extract all career events from this text:

{chunk_text}

Return ONLY valid JSON with the career events."""

    try:
        result = llm_client.generate_json(
            prompt=user_prompt,
            system_prompt=system_prompt
        )

        candidates = result.get("career_events", [])

        # Add metadata to each candidate
        for candidate in candidates:
            candidate["chunk_id"] = chunk_id
            candidate["source_url"] = source_url

        return candidates

    except Exception as e:
        return [{
            "error": str(e),
            "chunk_id": chunk_id,
            "source_url": source_url
        }]
