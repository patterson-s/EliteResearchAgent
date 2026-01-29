import json
import os
from pathlib import Path
from typing import Dict, Any, List
import cohere
from dotenv import load_dotenv

from load_events import load_events_from_chunks, format_event_for_display

load_dotenv(Path(__file__).parent.parent.parent / ".env")

def load_config(config_path: Path) -> Dict[str, Any]:
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

def load_prompt(prompt_path: Path) -> str:
    with open(prompt_path, "r", encoding="utf-8") as f:
        return f.read()

def discover_award_labels(
    award_events: List[Dict[str, Any]], 
    config_path: Path,
    output_dir: Path
) -> Dict[str, Any]:
    print("\n" + "="*80)
    print("PHASE 1B: DISCOVER AWARD LABELS")
    print("="*80)
    
    config = load_config(config_path)
    api_key = os.getenv(config["api_key_env_var"])
    
    if not api_key:
        raise EnvironmentError(f"{config['api_key_env_var']} not found")
    
    script_dir = Path(__file__).parent
    prompt_template = load_prompt(script_dir / config["prompts"]["discover_award_labels"])
    
    print(f"\nAnalyzing {len(award_events)} award events...")
    
    events_text = "\n\n".join([
        format_event_for_display(event) 
        for event in award_events
    ])
    
    prompt = prompt_template.format(events_text=events_text)
    
    co = cohere.Client(api_key)
    
    print("Calling LLM for label discovery...")
    response = co.chat(
        model=config["model"],
        temperature=config["temperature"],
        message=prompt,
        max_tokens=config["max_tokens_discover"]
    )
    
    raw_output = response.text.strip()
    raw_output = raw_output.replace("```json", "").replace("```", "").strip()
    
    try:
        result = json.loads(raw_output)
        award_labels = result.get("award_labels", [])
    except json.JSONDecodeError as e:
        print(f"ERROR: Failed to parse LLM output: {e}")
        print(f"Raw output: {raw_output[:500]}")
        return {"award_labels": []}
    
    print(f"\nDiscovered {len(award_labels)} award labels:")
    for label in award_labels:
        print(f"  {label['label_id']}: {label['label']}")
        print(f"    → {label['awarding_organization']}")
        print(f"    → Year: {label.get('year', 'N/A')}")
    
    output = {
        "award_labels": award_labels,
        "total_award_events": len(award_events),
        "raw_llm_output": raw_output
    }
    
    output_file = output_dir / "01b_award_labels.json"
    output_dir.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"\nSaved to {output_file}")
    
    return output

if __name__ == "__main__":
    script_dir = Path(__file__).parent
    data_dir = script_dir / "data" / "Abhijit_Banerjee"
    config_path = script_dir / "config" / "config.json"
    output_dir = script_dir / "outputs"
    
    events = load_events_from_chunks(data_dir)
    discover_award_labels(events["awards"], config_path, output_dir)