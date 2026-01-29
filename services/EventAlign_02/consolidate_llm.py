import json
import os
from pathlib import Path
from typing import Dict, Any, List
import cohere
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent.parent / ".env")

def load_config(config_path: Path) -> Dict[str, Any]:
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

def load_prompt(prompt_path: Path) -> str:
    with open(prompt_path, "r", encoding="utf-8") as f:
        return f.read()

def format_event_for_llm(event: Dict[str, Any], index: int) -> str:
    lines = [f"EVENT {index}:"]
    lines.append(f"  Type: {event.get('event_type', 'N/A')}")
    
    if event.get("start_year"):
        end_str = f"-{event['end_year']}" if event.get("end_year") else "-present"
        lines.append(f"  Time: {event['start_year']}{end_str}")
    else:
        lines.append(f"  Time: Unknown")
    
    orgs = event.get("canonical_organizations", event.get("organizations", []))
    if orgs:
        lines.append(f"  Organizations: {', '.join(orgs)}")
    
    roles = event.get("canonical_roles", event.get("roles", []))
    if roles:
        lines.append(f"  Roles: {', '.join(roles)}")
    
    locs = event.get("locations", [])
    if locs:
        lines.append(f"  Locations: {', '.join(locs)}")
    
    if event.get("time_markers_raw"):
        lines.append(f"  Time markers: {', '.join(event['time_markers_raw'])}")
    
    lines.append(f"  Source: chunk {event.get('chunk_id')}")
    
    return "\n".join(lines)

def llm_consolidate_group(
    group: Dict[str, Any], 
    api_key: str,
    prompt_template: str,
    model: str,
    temperature: float,
    max_tokens: int
) -> Dict[str, Any]:
    if len(group["event_indices"]) == 1:
        event = group["events"][0]
        return {
            "decision": "singleton",
            "consolidated_event": {
                "event_type": event.get("event_type"),
                "organization": event.get("canonical_organizations", event.get("organizations", []))[0] if event.get("canonical_organizations", event.get("organizations", [])) else None,
                "role": event.get("canonical_roles", event.get("roles", []))[0] if event.get("canonical_roles", event.get("roles", [])) else None,
                "start_year": event.get("start_year"),
                "end_year": event.get("end_year"),
                "locations": event.get("locations", [])
            },
            "confidence": "high",
            "reasoning": "Single event, no consolidation needed"
        }
    
    co = cohere.Client(api_key)
    
    events_text = "\n\n".join([
        format_event_for_llm(event, i+1) 
        for i, event in enumerate(group["events"])
    ])
    
    prompt = prompt_template.format(events_text=events_text)
    
    response = co.chat(
        model=model,
        temperature=temperature,
        message=prompt,
        max_tokens=max_tokens
    )
    
    try:
        text = response.text.strip()
        text = text.replace("```json", "").replace("```", "").strip()
        result = json.loads(text)
        
        return {
            "decision": result.get("decision", "different_events"),
            "consolidated_event": result.get("consolidated", {}),
            "confidence": "high",
            "reasoning": result.get("reasoning", "")
        }
    except json.JSONDecodeError:
        return {
            "decision": "different_events",
            "consolidated_event": {},
            "confidence": "low",
            "reasoning": "LLM parse failed"
        }

def accumulate_provenance(group: Dict[str, Any]) -> Dict[str, Any]:
    provenance = {
        "source_event_count": len(group["event_indices"]),
        "source_chunks": [],
        "source_urls": set(),
        "organization_variants": set(),
        "role_variants": set(),
        "location_variants": set(),
        "time_representations": []
    }
    
    for event in group["events"]:
        provenance["source_chunks"].append(event.get("chunk_id"))
        
        if event.get("source_url"):
            provenance["source_urls"].add(event["source_url"])
        
        provenance["organization_variants"].update(event.get("organizations", []))
        provenance["role_variants"].update(event.get("roles", []))
        provenance["location_variants"].update(event.get("locations", []))
        
        if event.get("time_markers_raw"):
            for tm in event["time_markers_raw"]:
                provenance["time_representations"].append({
                    "text": tm,
                    "chunk": event.get("chunk_id")
                })
    
    provenance["source_urls"] = list(provenance["source_urls"])
    provenance["organization_variants"] = list(provenance["organization_variants"])
    provenance["role_variants"] = list(provenance["role_variants"])
    provenance["location_variants"] = list(provenance["location_variants"])
    
    return provenance

def consolidate_with_llm(groups: List[Dict[str, Any]], output_dir: Path, config_path: Path) -> List[Dict[str, Any]]:
    print("\n" + "="*80)
    print("PHASE 3: LLM CONSOLIDATION")
    print("="*80)
    
    config = load_config(config_path)
    
    api_key = os.getenv(config["api_key_env_var"])
    if not api_key:
        raise EnvironmentError(f"{config['api_key_env_var']} not found")
    
    script_dir = Path(__file__).parent
    prompt_template = load_prompt(script_dir / config["prompts"]["consolidate_group"])
    
    consolidated_events = []
    
    multi_event_groups = [g for g in groups if len(g["event_indices"]) > 1]
    singleton_groups = [g for g in groups if len(g["event_indices"]) == 1]
    
    print(f"\nProcessing {len(multi_event_groups)} multi-event groups with LLM...")
    
    for i, group in enumerate(multi_event_groups):
        print(f"  [{i+1}/{len(multi_event_groups)}] {group['group_id']}: {len(group['event_indices'])} events")
        
        consolidation = llm_consolidate_group(
            group, api_key, prompt_template,
            config["model"], config["temperature"], 
            config["max_tokens_consolidate"]
        )
        provenance = accumulate_provenance(group)
        
        consolidated = {
            "consolidated_event_id": group["group_id"],
            "decision": consolidation["decision"],
            "canonical_representation": consolidation["consolidated_event"],
            "accumulated_details": {
                "organization_variants": provenance["organization_variants"],
                "role_variants": provenance["role_variants"],
                "location_variants": provenance["location_variants"],
                "time_representations": provenance["time_representations"]
            },
            "provenance": {
                "source_event_count": provenance["source_event_count"],
                "source_chunks": provenance["source_chunks"],
                "source_urls": provenance["source_urls"]
            },
            "confidence": {
                "consolidation_confidence": consolidation["confidence"],
                "reasoning": consolidation["reasoning"]
            }
        }
        
        consolidated_events.append(consolidated)
        
        print(f"    Decision: {consolidation['decision']}")
    
    print(f"\nProcessing {len(singleton_groups)} singleton events...")
    for group in singleton_groups:
        consolidation = llm_consolidate_group(
            group, api_key, prompt_template,
            config["model"], config["temperature"],
            config["max_tokens_consolidate"]
        )
        provenance = accumulate_provenance(group)
        
        consolidated = {
            "consolidated_event_id": group["group_id"],
            "decision": "singleton",
            "canonical_representation": consolidation["consolidated_event"],
            "accumulated_details": {
                "organization_variants": provenance["organization_variants"],
                "role_variants": provenance["role_variants"],
                "location_variants": provenance["location_variants"],
                "time_representations": provenance["time_representations"]
            },
            "provenance": {
                "source_event_count": 1,
                "source_chunks": provenance["source_chunks"],
                "source_urls": provenance["source_urls"]
            },
            "confidence": {
                "consolidation_confidence": "high",
                "reasoning": "Single event"
            }
        }
        
        consolidated_events.append(consolidated)
    
    output_file = output_dir / "03_consolidated_events.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(consolidated_events, f, indent=2, ensure_ascii=False)
    
    print(f"\nConsolidation complete. Saved to {output_file}")
    print(f"Total consolidated events: {len(consolidated_events)}")
    
    same_event_count = sum(1 for e in consolidated_events if e["decision"] == "same_event")
    singleton_count = sum(1 for e in consolidated_events if e["decision"] == "singleton")
    different_count = len(consolidated_events) - same_event_count - singleton_count
    
    print(f"  - Same event (merged): {same_event_count}")
    print(f"  - Singletons: {singleton_count}")
    print(f"  - Different/Sequential: {different_count}")
    
    return consolidated_events

if __name__ == "__main__":
    script_dir = Path(__file__).parent
    output_dir = script_dir / "outputs"
    config_path = script_dir / "config" / "config.json"
    
    with open(output_dir / "02_candidate_groups.json", "r", encoding="utf-8") as f:
        groups = json.load(f)
    
    consolidate_with_llm(groups, output_dir, config_path)