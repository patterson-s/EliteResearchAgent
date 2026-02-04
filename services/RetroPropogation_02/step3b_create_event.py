import json
import cohere
from pathlib import Path
from typing import Dict, Any, List

from utils import load_config, load_prompt, parse_json_response, get_api_key

def create_new_event(
    candidate: Dict[str, Any],
    existing_events: List[Dict[str, Any]],
    config_path: Path
) -> Dict[str, Any]:
    config = load_config(config_path)
    api_key = get_api_key(config)
    
    script_dir = Path(__file__).parent
    system_prompt = load_prompt(script_dir / config["prompts"]["step3b_create_event"])
    
    new_event_counter = len([e for e in existing_events if e.get("event_id", "").startswith("E_NEW")]) + 1
    
    input_data = {
        "candidate": candidate,
        "suggested_event_id": f"E_NEW_{new_event_counter:03d}",
        "existing_event_count": len(existing_events)
    }
    
    user_prompt = f"INPUT:\n{json.dumps(input_data, indent=2, ensure_ascii=False)}"
    
    co = cohere.Client(api_key)
    
    response = co.chat(
        model=config["model"],
        temperature=config["temperature"],
        preamble=system_prompt,
        message=user_prompt,
        max_tokens=config["max_tokens_create"]
    )
    
    raw_output = response.text.strip()
    result = parse_json_response(raw_output)
    
    result["raw_llm_output"] = raw_output
    
    return result

if __name__ == "__main__":
    script_dir = Path(__file__).parent
    config_path = script_dir / "config" / "config.json"
    
    candidate = {
        "organization": "International Monetary Fund",
        "roles": ["Economic Advisor"],
        "time_period": {"text": "2010-2015", "start": "2010", "end": "2015"},
        "locations": ["Washington DC"],
        "supporting_quote": "He served as Economic Advisor to the IMF from 2010 to 2015",
        "chunk_id": 201,
        "source_url": "https://news.com/bio",
        "event_type": "career_position"
    }
    
    existing = [
        {"event_id": "E001"},
        {"event_id": "E002"},
        {"event_id": "E_NEW_001"}
    ]
    
    result = create_new_event(candidate, existing, config_path)
    print(f"New event ID: {result['new_event']['event_id']}")
