import os
import json
import re
from pathlib import Path
from typing import Dict, Any, List
import cohere
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parent.parent.parent / ".env"
load_dotenv(env_path)

def load_config(config_path: Path) -> Dict[str, Any]:
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

def load_prompt(prompt_path: Path) -> str:
    with open(prompt_path, "r", encoding="utf-8") as f:
        return f.read()

def fill_template(template: str, variables: Dict[str, str]) -> str:
    text = template
    for key, value in variables.items():
        text = text.replace(f"{{{{{key}}}}}", str(value))
    return text

def parse_extraction_output(text: str) -> List[Dict[str, Any]]:
    text = text.strip()
    text = re.sub(r"```json\s*", "", text)
    text = re.sub(r"```\s*$", "", text)
    
    try:
        data = json.loads(text)
        return data.get("events", [])
    except json.JSONDecodeError:
        return []

def extract_career_events(
    person_name: str,
    chunk_text: str,
    chunk_id: str,
    source_url: str,
    config_path: Path
) -> List[Dict[str, Any]]:
    config = load_config(config_path)
    api_key = os.getenv(config["api_key_env_var"])
    
    if not api_key:
        raise EnvironmentError(f"Missing {config['api_key_env_var']} environment variable")
    
    co = cohere.Client(api_key)
    
    script_dir = Path(__file__).parent
    system_prompt = load_prompt(script_dir / config["prompts"]["system_prompt_path"])
    user_template = load_prompt(script_dir / config["prompts"]["user_prompt_path"])
    
    user_prompt = fill_template(user_template, {
        "PERSON_NAME": person_name,
        "CHUNK_ID": chunk_id,
        "CHUNK_TEXT": chunk_text
    })
    
    response = co.chat(
        model=config["model"],
        temperature=config["temperature"],
        preamble=system_prompt,
        message=user_prompt,
        max_tokens=config["max_tokens"]
    )
    
    raw_output = response.text.strip()
    events = parse_extraction_output(raw_output)
    
    for event in events:
        event["chunk_id"] = chunk_id
        event["source_url"] = source_url
        event["raw_llm_output"] = raw_output
    
    return events

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Extract career events from a single chunk")
    parser.add_argument("--person", required=True)
    parser.add_argument("--chunk-id", required=True)
    parser.add_argument("--text", required=True)
    parser.add_argument("--url", default="unknown")
    parser.add_argument("--config", type=Path, default=Path("config/config.json"))
    args = parser.parse_args()
    
    events = extract_career_events(
        args.person, 
        args.text, 
        args.chunk_id, 
        args.url,
        args.config
    )
    
    print("\n" + "=" * 80)
    print(f"Extracted {len(events)} events from chunk {args.chunk_id}")
    print("=" * 80)
    for i, event in enumerate(events, 1):
        print(f"\n[{i}] {event.get('organization', 'N/A')} - {event.get('role', 'N/A')}")
        print(f"    Location: {event.get('location', 'N/A')}")
        print(f"    Dates: {event.get('start_date', '')} to {event.get('end_date', '')}")
        print(f"    Quotes: {len(event.get('supporting_quotes', []))}")