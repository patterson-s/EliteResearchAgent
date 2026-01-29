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

def format_labels_for_prompt(labels: List[Dict[str, Any]]) -> str:
    lines = []
    for label in labels:
        lines.append(f"{label['label_id']}: {label['label']}")
        lines.append(f"  Organization: {label['organization']}")
        lines.append(f"  Involvement: {label.get('involvement', 'N/A')}")
        lines.append(f"  Period: {label.get('time_period', 'N/A')}")
    return "\n".join(lines)

def classify_career_event(
    event: Dict[str, Any],
    labels: List[Dict[str, Any]],
    prompt_template: str,
    co: cohere.Client,
    model: str,
    temperature: float,
    max_tokens: int
) -> Dict[str, Any]:
    event_text = format_event_for_display(event)
    labels_text = format_labels_for_prompt(labels)
    
    prompt = prompt_template.format(
        event_text=event_text,
        labels_text=labels_text
    )
    
    response = co.chat(
        model=model,
        temperature=temperature,
        message=prompt,
        max_tokens=max_tokens
    )
    
    raw_output = response.text.strip()
    raw_output = raw_output.replace("```json", "").replace("```", "").strip()
    
    try:
        result = json.loads(raw_output)
        return {
            "event_index": event["event_index"],
            "assigned_label": result.get("assigned_label", "NONE"),
            "confidence": result.get("confidence", "low"),
            "reasoning": result.get("reasoning", "")
        }
    except json.JSONDecodeError:
        return {
            "event_index": event["event_index"],
            "assigned_label": "NONE",
            "confidence": "low",
            "reasoning": "Parse error"
        }

def classify_career_events(
    career_events: List[Dict[str, Any]],
    labels: List[Dict[str, Any]],
    config_path: Path,
    output_dir: Path
) -> Dict[str, Any]:
    print("\n" + "="*80)
    print("PHASE 2A: CLASSIFY CAREER EVENTS")
    print("="*80)
    
    config = load_config(config_path)
    api_key = os.getenv(config["api_key_env_var"])
    
    if not api_key:
        raise EnvironmentError(f"{config['api_key_env_var']} not found")
    
    script_dir = Path(__file__).parent
    prompt_template = load_prompt(script_dir / config["prompts"]["classify_career_event"])
    
    co = cohere.Client(api_key)
    
    print(f"\nClassifying {len(career_events)} career events into {len(labels)} labels...")
    
    classifications = []
    
    for i, event in enumerate(career_events, 1):
        print(f"  [{i}/{len(career_events)}] Classifying event {event['event_index']}...", end="")
        
        classification = classify_career_event(
            event, labels, prompt_template, co,
            config["model"], config["temperature"], config["max_tokens_classify"]
        )
        
        classifications.append(classification)
        print(f" → {classification['assigned_label']}")
    
    label_counts = {}
    for c in classifications:
        label = c["assigned_label"]
        label_counts[label] = label_counts.get(label, 0) + 1
    
    none_count = label_counts.get("NONE", 0)
    assigned_count = len(career_events) - none_count
    
    print(f"\nClassification complete:")
    print(f"  Assigned to labels: {assigned_count} ({assigned_count/len(career_events)*100:.1f}%)")
    print(f"  NONE: {none_count} ({none_count/len(career_events)*100:.1f}%)")
    
    print(f"\nDistribution by label:")
    for label_id, count in sorted(label_counts.items()):
        if label_id != "NONE":
            print(f"  {label_id}: {count} events")
    
    output = {
        "classifications": classifications,
        "summary": {
            "total_events": len(career_events),
            "assigned": assigned_count,
            "none": none_count,
            "coverage_percent": assigned_count/len(career_events)*100,
            "label_counts": label_counts
        }
    }
    
    output_file = output_dir / "02a_career_classifications.json"
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
    
    with open(output_dir / "01a_career_labels.json", "r", encoding="utf-8") as f:
        labels_data = json.load(f)
    
    classify_career_events(
        events["career"], 
        labels_data["career_labels"],
        config_path,
        output_dir
    )