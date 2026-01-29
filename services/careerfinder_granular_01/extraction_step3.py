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

def parse_verification_output(text: str) -> Dict[str, Any]:
    text = text.strip()
    text = re.sub(r"```json\s*", "", text)
    text = re.sub(r"```\s*$", "", text)
    
    try:
        data = json.loads(text)
        return data
    except json.JSONDecodeError:
        return {"verified_events": [], "summary": {}}

def verify_events_step3(
    events: List[Dict[str, Any]],
    entities: Dict[str, List[Dict[str, Any]]],
    config_path: Path
) -> Dict[str, Any]:
    config = load_config(config_path)
    api_key = os.getenv(config["api_key_env_var"])
    
    if not api_key:
        raise EnvironmentError(f"Missing {config['api_key_env_var']} environment variable")
    
    co = cohere.Client(api_key)
    
    script_dir = Path(__file__).parent
    system_prompt = load_prompt(script_dir / config["prompts"]["step3_verification"])
    
    user_prompt = f"EVENTS:\n{json.dumps(events, indent=2, ensure_ascii=False)}\n\nENTITIES:\n{json.dumps(entities, indent=2, ensure_ascii=False)}"
    
    response = co.chat(
        model=config["model"],
        temperature=config["temperature"],
        preamble=system_prompt,
        message=user_prompt,
        max_tokens=config["max_tokens_step3"]
    )
    
    raw_output = response.text.strip()
    verification = parse_verification_output(raw_output)
    
    return {
        "verified_events": verification.get("verified_events", []),
        "summary": verification.get("summary", {}),
        "raw_llm_output": raw_output
    }

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Verify assembled events (Step 3)")
    parser.add_argument("--events", required=True, help="Path to events JSON file")
    parser.add_argument("--entities", required=True, help="Path to entities JSON file")
    parser.add_argument("--config", type=Path, default=Path("config/config.json"))
    args = parser.parse_args()
    
    with open(args.events, "r", encoding="utf-8") as f:
        events = json.load(f)
    
    with open(args.entities, "r", encoding="utf-8") as f:
        entities = json.load(f)
    
    result = verify_events_step3(events, entities, args.config)
    
    print("\n" + "=" * 80)
    print("Verification Summary")
    print("=" * 80)
    
    summary = result.get("summary", {})
    print(f"Total events: {summary.get('total_events', 0)}")
    print(f"Valid: {summary.get('valid', 0)}")
    print(f"Warnings: {summary.get('warnings', 0)}")
    print(f"Errors: {summary.get('errors', 0)}")
    
    print("\n" + "Issues Found:")
    print("-" * 80)
    
    for verified in result.get("verified_events", []):
        if verified.get("issues"):
            print(f"\nEvent {verified.get('event_id')}: {verified.get('status')}")
            for issue in verified["issues"]:
                print(f"  - [{issue['severity']}] {issue['type']}: {issue['description']}")