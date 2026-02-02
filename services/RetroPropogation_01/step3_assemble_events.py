import json
import cohere
from pathlib import Path
from typing import Dict, Any, List

from utils import load_config, load_prompt, parse_json_response, get_api_key, save_json

def assemble_events(
    entities: Dict[str, Any],
    canonical_orgs: List[Dict[str, Any]],
    config_path: Path
) -> Dict[str, Any]:
    print("\n" + "="*80)
    print("STEP 3: ASSEMBLE EVENTS WITH DEDUPLICATION")
    print("="*80)
    
    config = load_config(config_path)
    api_key = get_api_key(config)
    
    script_dir = Path(__file__).parent
    system_prompt = load_prompt(script_dir / config["prompts"]["step3_assemble_events"])
    
    input_data = {
        "entities": entities,
        "canonical_organizations": canonical_orgs
    }
    
    input_text = json.dumps(input_data, indent=2, ensure_ascii=False)
    user_prompt = f"INPUT:\n{input_text}"
    
    co = cohere.Client(api_key)
    
    print("Assembling events with deduplication...")
    
    response = co.chat(
        model=config["model"],
        temperature=config["temperature"],
        preamble=system_prompt,
        message=user_prompt,
        max_tokens=config["max_tokens_step3"]
    )
    
    raw_output = response.text.strip()
    assembly_data = parse_json_response(raw_output)
    
    events = assembly_data.get("events", [])
    dedup_log = assembly_data.get("deduplication_log", [])
    
    career_count = sum(1 for e in events if e.get("event_type") == "career_position")
    award_count = sum(1 for e in events if e.get("event_type") == "award")
    
    print(f"\nAssembled {len(events)} events:")
    print(f"  Career positions: {career_count}")
    print(f"  Awards: {award_count}")
    print(f"  Deduplication actions: {len(dedup_log)}")
    
    result = {
        "events": events,
        "deduplication_log": dedup_log,
        "raw_llm_output": raw_output,
        "summary": {
            "total_events": len(events),
            "career_positions": career_count,
            "awards": award_count,
            "deduplication_actions": len(dedup_log)
        }
    }
    
    return result

if __name__ == "__main__":
    script_dir = Path(__file__).parent
    config_path = script_dir / "config" / "config.json"
    output_dir = script_dir / "outputs" / "Gro_Harlem_Brundtland"
    
    with open(output_dir / "step1_entities.json", "r", encoding="utf-8") as f:
        step1_data = json.load(f)
    
    with open(output_dir / "step2_canonical_orgs.json", "r", encoding="utf-8") as f:
        step2_data = json.load(f)
    
    result = assemble_events(
        step1_data["entities"],
        step2_data["canonical_organizations"],
        config_path
    )
    
    output_file = output_dir / "step3_events.json"
    save_json(result, output_file)
    print(f"\nSaved to {output_file}")