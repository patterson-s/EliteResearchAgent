"""Step 1: Extract entities from text chunks."""

from typing import Dict, Any, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

from ..llm_client import LLMClient
from ..utils import load_prompt, load_config


def extract_entities_from_chunk(
    chunk_text: str,
    llm_client: LLMClient,
    config: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Extract entities from a single text chunk.

    Args:
        chunk_text: The text to extract entities from
        llm_client: LLM client instance
        config: Optional configuration

    Returns:
        Dictionary with time_markers, organizations, roles, locations
    """
    if config is None:
        config = load_config()

    system_prompt = load_prompt("phase1_extract_entities", config)

    user_prompt = f"""Extract all career-related entities from this biographical text:

{chunk_text}

Return ONLY valid JSON with the extracted entities."""

    try:
        result = llm_client.generate_json(
            prompt=user_prompt,
            system_prompt=system_prompt
        )

        # Ensure all required keys exist
        return {
            "time_markers": result.get("time_markers", []),
            "organizations": result.get("organizations", []),
            "roles": result.get("roles", []),
            "locations": result.get("locations", [])
        }
    except Exception as e:
        # Return empty result on error
        return {
            "time_markers": [],
            "organizations": [],
            "roles": [],
            "locations": [],
            "error": str(e)
        }


def extract_entities_parallel(
    chunks: List[str],
    llm_client: LLMClient,
    config: Optional[Dict[str, Any]] = None,
    max_workers: int = 4,
    max_retries: int = 3
) -> Dict[str, Any]:
    """Extract entities from multiple chunks in parallel.

    Args:
        chunks: List of text chunks
        llm_client: LLM client instance
        config: Optional configuration
        max_workers: Number of parallel workers
        max_retries: Maximum retries per chunk

    Returns:
        Dictionary with merged entities and chunk results
    """
    if config is None:
        config = load_config()

    phase1_config = config.get("phase1", {})
    max_workers = phase1_config.get("max_workers", max_workers)
    max_retries = phase1_config.get("max_retries", max_retries)

    chunk_results = []
    merged_entities = {
        "time_markers": [],
        "organizations": [],
        "roles": [],
        "locations": []
    }

    def process_chunk(chunk_idx: int, chunk_text: str) -> Dict[str, Any]:
        """Process a single chunk with retries."""
        last_error = None
        for attempt in range(max_retries):
            try:
                result = extract_entities_from_chunk(chunk_text, llm_client, config)
                if "error" not in result:
                    return {
                        "chunk_index": chunk_idx,
                        "success": True,
                        "entities": result
                    }
                last_error = result.get("error")
            except Exception as e:
                last_error = str(e)

            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff

        return {
            "chunk_index": chunk_idx,
            "success": False,
            "error": last_error,
            "entities": {
                "time_markers": [],
                "organizations": [],
                "roles": [],
                "locations": []
            }
        }

    # Process chunks in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_idx = {
            executor.submit(process_chunk, idx, chunk): idx
            for idx, chunk in enumerate(chunks)
        }

        for future in as_completed(future_to_idx):
            result = future.result()
            chunk_results.append(result)

            # Merge entities
            entities = result.get("entities", {})
            for key in merged_entities:
                merged_entities[key].extend(entities.get(key, []))

    # Sort by chunk index
    chunk_results.sort(key=lambda x: x["chunk_index"])

    # Calculate success rate
    successful = sum(1 for r in chunk_results if r["success"])
    success_rate = successful / len(chunks) if chunks else 0

    return {
        "entities": merged_entities,
        "chunk_results": chunk_results,
        "total_chunks": len(chunks),
        "successful_chunks": successful,
        "success_rate": success_rate
    }
