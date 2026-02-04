"""Utility functions for Prosopography Tool."""

import json
import os
from pathlib import Path
from typing import Dict, Any, Optional


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load configuration from JSON file."""
    if config_path is None:
        config_path = Path(__file__).parent / "config" / "config.json"
    else:
        config_path = Path(config_path)

    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_prompt(prompt_name: str, config: Optional[Dict[str, Any]] = None) -> str:
    """Load a prompt template by name."""
    if config is None:
        config = load_config()

    prompt_rel_path = config["prompts"].get(prompt_name)
    if not prompt_rel_path:
        raise ValueError(f"Unknown prompt: {prompt_name}")

    prompt_path = Path(__file__).parent / "config" / prompt_rel_path
    with open(prompt_path, "r", encoding="utf-8") as f:
        return f.read()


def parse_json_response(response_text: str) -> Dict[str, Any]:
    """Parse JSON from LLM response, handling common issues."""
    # Try to find JSON in the response
    text = response_text.strip()

    # Remove markdown code blocks if present
    if text.startswith("```json"):
        text = text[7:]
    elif text.startswith("```"):
        text = text[3:]
    if text.endswith("```"):
        text = text[:-3]

    text = text.strip()

    # Try to parse as JSON
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Try to find JSON object in the text
        start_idx = text.find("{")
        end_idx = text.rfind("}") + 1
        if start_idx != -1 and end_idx > start_idx:
            try:
                return json.loads(text[start_idx:end_idx])
            except json.JSONDecodeError:
                pass

        # Try to find JSON array
        start_idx = text.find("[")
        end_idx = text.rfind("]") + 1
        if start_idx != -1 and end_idx > start_idx:
            try:
                return json.loads(text[start_idx:end_idx])
            except json.JSONDecodeError:
                pass

        raise ValueError(f"Could not parse JSON from response: {text[:200]}...")


def save_json_checkpoint(data: Any, path: Path) -> None:
    """Save data to a JSON checkpoint file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, default=str)


def load_json_checkpoint(path: Path) -> Optional[Dict[str, Any]]:
    """Load data from a JSON checkpoint file if it exists."""
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def get_review_dir(person_name: str) -> Path:
    """Get the review directory for a person."""
    base_dir = Path(__file__).parent / "review" / person_name.replace(" ", "_")
    base_dir.mkdir(parents=True, exist_ok=True)
    return base_dir


def extract_source_type(url: str) -> str:
    """Extract source type from URL."""
    url_lower = url.lower()
    if "wikipedia.org" in url_lower:
        return "wikipedia"
    elif any(domain in url_lower for domain in [".gov", "government"]):
        return "official"
    elif any(domain in url_lower for domain in [".edu", "university", "college"]):
        return "academic"
    elif any(domain in url_lower for domain in ["nytimes", "bbc", "reuters", "guardian", "washingtonpost"]):
        return "news"
    else:
        return "other"


def normalize_time_period(time_text: Optional[str]) -> tuple:
    """Extract start and end years from time period text.

    Returns (start, end) tuple where each is a string or None.
    """
    if not time_text:
        return (None, None)

    import re

    # Find all 4-digit years
    years = re.findall(r'\b(1[89]\d{2}|20[0-2]\d)\b', time_text)

    if len(years) == 0:
        return (None, None)
    elif len(years) == 1:
        # Check if it's ongoing
        if any(word in time_text.lower() for word in ["present", "current", "ongoing", "since"]):
            return (years[0], "present")
        return (years[0], None)
    else:
        return (years[0], years[-1])


def chunk_text(text: str, chunk_size: int = 4000, overlap: int = 200) -> list:
    """Split text into overlapping chunks."""
    if len(text) <= chunk_size:
        return [text]

    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size

        # Try to break at a sentence boundary
        if end < len(text):
            # Look for sentence boundary
            for punct in [". ", ".\n", "? ", "?\n", "! ", "!\n"]:
                last_punct = text.rfind(punct, start, end)
                if last_punct > start + chunk_size // 2:
                    end = last_punct + len(punct)
                    break

        chunks.append(text[start:end])
        start = end - overlap

    return chunks
