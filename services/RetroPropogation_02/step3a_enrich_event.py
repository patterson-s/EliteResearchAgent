import json
import cohere
from pathlib import Path
from typing import Dict, Any

from utils import load_config, load_prompt, parse_json_response, get_api_key

def enrich_event(
    existing_event: Dict[str, Any],
    candidate: Dict[str, Any],
    config_path: Path
) -> Dict[str, Any]:
    config = load_config(config_path)
    api_key = get_api_key(config)
    
    script_dir = Path(__file__).parent
    system_prompt = load_prompt(script_dir / config["prompts"]["step3a_enrich_event"])
    
    input_data = {
        "existing_event": existing_event,
        "new_details": {
            "organization": candidate.get("organization"),
            "roles": candidate.get("roles", []),
            "time_period": candidate.get("time_period", {}),
            "locations": candidate.get("locations", []),
            "supporting_quote": candidate.get("supporting_quote"),
            "chunk_id": candidate.get("chunk_id"),
            "source_url": candidate.get("source_url")
        }
    }
    
    user_prompt = f"INPUT:\n{json.dumps(input_data, indent=2, ensure_ascii=False)}"
    
    co = cohere.Client(api_key)
    
    response = co.chat(
        model=config["model"],
        temperature=config["temperature"],
        preamble=system_prompt,
        message=user_prompt,
        max_tokens=config["max_tokens_enrich"]
    )
    
    raw_output = response.text.strip()
    result = parse_json_response(raw_output)
    
    result["raw_llm_output"] = raw_output
    
    return result

if __name__ == "__main__":
    script_dir = Path(__file__).parent
    config_path = script_dir / "config" / "config.json"
    
    existing = {
        "event_id": "E002",
        "canonical_org_id": "ORG_WHO",
        "roles": ["Director-General"],
        "time_period": {"start": "2007", "end": "present"},
        "supporting_evidence": [
            {
                "chunk_id": 42,
                "quote": "She became Director-General in 2007",
                "contribution": "original"
            }
        ]
    }
    
    candidate = {
        "organization": "World Health Organization",
        "roles": ["Director-General", "Chief Executive"],
        "time_period": {"start": "2007", "end": "2017"},
        "supporting_quote": "From 2007 to 2017, served as Director-General and Chief Executive of WHO",
        "chunk_id": 158,
        "source_url": "news.com/article"
    }
    
    result = enrich_event(existing, candidate, config_path)
    print(f"Changes made: {result.get('changes_made')}")
    print(f"Summary: {result.get('changes_summary')}")
