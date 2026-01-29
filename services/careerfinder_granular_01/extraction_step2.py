import os
import json
import re
from pathlib import Path
from typing import Dict, Any, List
import cohere
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(env_path)

def load_config(config_path: Path) -> Dict[str, Any]:
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

def load_prompt(prompt_path: Path) -> str:
    with open(prompt_path, "r", encoding="utf-8") as f:
        return f.read()

def parse_assembly_output(text: str) -> Dict[str, Any]:
    text = text.strip()
    text = re.sub(r"```json\s*", "", text)
    text = re.sub(r"```\s*$", "", text)
    
    try:
        data = json.loads(text)
        return data
    except json.JSONDecodeError:
        return {"events": []}

def assemble_events_step2(
    entities: Dict[str, List[Dict[str, Any]]],
    config_path: Path
) -> Dict[str, Any]:
    config = load_config(config_path)
    api_key = os.getenv(config["api_key_env_var"])
    
    if not api_key:
        raise EnvironmentError(f"Missing {config['api_key_env_var']} environment variable")
    
    co = cohere.Client(api_key)
    
    script_dir = Path(__file__).parent
    system_prompt = load_prompt(script_dir / config["prompts"]["step2_assembly"])
    
    user_prompt = f"ENTITIES:\n{json.dumps(entities, indent=2, ensure_ascii=False)}"
    
    response = co.chat(
        model=config["model"],
        temperature=config["temperature"],
        preamble=system_prompt,
        message=user_prompt,
        max_tokens=config["max_tokens_step2"]
    )
    
    raw_output = response.text.strip()
    assembled = parse_assembly_output(raw_output)
    
    return {
        "assembled_events": assembled.get("events", []),
        "raw_llm_output": raw_output
    }

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Assemble events from entities (Step 2)")
    parser.add_argument("--entities", required=True, help="Path to entities JSON file")
    parser.add_argument("--config", type=Path, default=Path("config/config.json"))
    args = parser.parse_args()
    
    with open(args.entities, "r", encoding="utf-8") as f:
        entities = json.load(f)
    
    result = assemble_events_step2(entities, args.config)
    
    print("\n" + "=" * 80)
    print(f"Assembled Events: {len(result['assembled_events'])}")
    print("=" * 80)
    
    for i, event in enumerate(result['assembled_events'], 1):
        print(f"\n[{i}] Confidence: {event.get('confidence', 'N/A')}")
        print(f"    Time markers: {event.get('time_marker_ids', [])}")
        print(f"    Organizations: {event.get('organization_ids', [])}")
        print(f"    Roles: {event.get('role_ids', [])}")
        print(f"    Locations: {event.get('location_ids', [])}")
        if event.get('notes'):
            print(f"    Notes: {event.get('notes')}")