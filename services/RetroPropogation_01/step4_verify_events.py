import json
import cohere
from pathlib import Path
from typing import Dict, Any, List

from utils import load_config, load_prompt, parse_json_response, get_api_key, save_json

def verify_events(
    events: List[Dict[str, Any]],
    entities: Dict[str, Any],
    dedup_log: List[Dict[str, Any]],
    config_path: Path
) -> Dict[str, Any]:
    print("\n" + "="*80)
    print("STEP 4: VERIFY EVENTS")
    print("="*80)
    
    config = load_config(config_path)
    api_key = get_api_key(config)
    
    script_dir = Path(__file__).parent
    system_prompt = load_prompt(script_dir / config["prompts"]["step4_verify_events"])
    
    input_data = {
        "events": events,
        "entities": entities,
        "deduplication_log": dedup_log
    }
    
    input_text = json.dumps(input_data, indent=2, ensure_ascii=False)
    user_prompt = f"INPUT:\n{input_text}"
    
    co = cohere.Client(api_key)
    
    print(f"Verifying {len(events)} events...")
    
    response = co.chat(
        model=config["model"],
        temperature=config["temperature"],
        preamble=system_prompt,
        message=user_prompt,
        max_tokens=config["max_tokens_step4"]
    )
    
    raw_output = response.text.strip()
    verification_data = parse_json_response(raw_output)
    
    verified_events = verification_data.get("verified_events", [])
    summary = verification_data.get("summary", {})
    dedup_review = verification_data.get("deduplication_review", {})
    
    print(f"\nVerification Summary:")
    print(f"  Total events: {summary.get('total_events', 0)}")
    print(f"  Valid: {summary.get('valid', 0)}")
    print(f"  Warnings: {summary.get('warnings', 0)}")
    print(f"  Errors: {summary.get('errors', 0)}")
    
    if summary.get('warnings', 0) > 0 or summary.get('errors', 0) > 0:
        print("\nIssues found:")
        for ve in verified_events:
            if ve.get("issues"):
                print(f"  Event {ve['event_id']}: {ve['status']}")
                for issue in ve["issues"]:
                    print(f"    [{issue['severity']}] {issue['type']}: {issue['description']}")
    
    result = {
        "verified_events": verified_events,
        "summary": summary,
        "deduplication_review": dedup_review,
        "raw_llm_output": raw_output
    }
    
    return result

if __name__ == "__main__":
    script_dir = Path(__file__).parent
    config_path = script_dir / "config" / "config.json"
    output_dir = script_dir / "outputs" / "Gro_Harlem_Brundtland"
    
    with open(output_dir / "step1_entities.json", "r", encoding="utf-8") as f:
        step1_data = json.load(f)
    
    with open(output_dir / "step3_events.json", "r", encoding="utf-8") as f:
        step3_data = json.load(f)
    
    result = verify_events(
        step3_data["events"],
        step1_data["entities"],
        step3_data["deduplication_log"],
        config_path
    )
    
    output_file = output_dir / "step4_verification.json"
    save_json(result, output_file)
    print(f"\nSaved to {output_file}")