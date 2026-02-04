import json
import cohere
from pathlib import Path
from typing import Dict, Any, List

from utils import load_config, load_prompt, parse_json_response, get_api_key

def match_or_new(
    candidate: Dict[str, Any],
    existing_events: List[Dict[str, Any]],
    config_path: Path
) -> Dict[str, Any]:
    config = load_config(config_path)
    api_key = get_api_key(config)
    
    script_dir = Path(__file__).parent
    system_prompt = load_prompt(script_dir / config["prompts"]["step2_match_or_new"])
    
    existing_summary = []
    for event in existing_events:
        summary = {
            "event_id": event.get("event_id"),
            "organization": event.get("canonical_org_id") or event.get("organization"),
            "roles": event.get("roles", []),
            "time_period": event.get("time_period", {})
        }
        existing_summary.append(summary)
    
    input_data = {
        "candidate": {
            "organization": candidate.get("organization"),
            "roles": candidate.get("roles", []),
            "time_period": candidate.get("time_period", {}),
            "locations": candidate.get("locations", [])
        },
        "existing_events": existing_summary
    }
    
    user_prompt = f"INPUT:\n{json.dumps(input_data, indent=2, ensure_ascii=False)}"
    
    co = cohere.Client(api_key)
    
    response = co.chat(
        model=config["model"],
        temperature=config["temperature"],
        preamble=system_prompt,
        message=user_prompt,
        max_tokens=config["max_tokens_match"]
    )
    
    raw_output = response.text.strip()
    decision = parse_json_response(raw_output)
    
    decision["raw_llm_output"] = raw_output
    
    return decision

if __name__ == "__main__":
    script_dir = Path(__file__).parent
    config_path = script_dir / "config" / "config.json"
    
    candidate = {
        "organization": "World Health Organization",
        "roles": ["Director-General"],
        "time_period": {"text": "2006-2017", "start": "2006", "end": "2017"}
    }
    
    existing = [
        {
            "event_id": "E001",
            "canonical_org_id": "ORG_UN",
            "roles": ["Secretary"],
            "time_period": {"start": "1990", "end": "1995"}
        },
        {
            "event_id": "E002",
            "canonical_org_id": "ORG_WHO",
            "roles": ["Director-General"],
            "time_period": {"start": "2007", "end": "2017"}
        }
    ]
    
    decision = match_or_new(candidate, existing, config_path)
    print(f"Decision: {decision['decision']}")
    print(f"Reasoning: {decision['reasoning']}")
