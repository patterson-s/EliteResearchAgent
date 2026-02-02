import json
import os
from pathlib import Path
from typing import Dict, Any, List
import cohere
from dotenv import load_dotenv

from load_chunks import load_enriched_chunk

load_dotenv(Path(__file__).parent.parent.parent / ".env")

def load_config(config_path: Path) -> Dict[str, Any]:
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

def load_prompt(prompt_path: Path) -> str:
    with open(prompt_path, "r", encoding="utf-8") as f:
        return f.read()

def load_eventalign03_data(person_name: str, base_dir: Path) -> Dict[str, Any]:
    ea03_dir = base_dir.parent / "EventAlign_03" / "outputs" / person_name
    
    with open(ea03_dir / "02a_career_classifications.json", "r", encoding="utf-8") as f:
        career_class = json.load(f)
    
    data_dir = base_dir.parent / "EventAlign_03" / "data" / person_name
    
    all_events = []
    for chunk_file in data_dir.glob("chunk_*_results.json"):
        with open(chunk_file, "r", encoding="utf-8") as f:
            chunk_data = json.load(f)
        
        if chunk_data["status"] != "success":
            continue
        
        verified_events = chunk_data["step3"]["verified_events"]
        assembled_events = chunk_data["step2"]["assembled_events"]
        entities = chunk_data["step1"]["entities"]
        
        for i, verified in enumerate(verified_events):
            if verified.get("status") == "valid" and i < len(assembled_events):
                event = assembled_events[i].copy()
                event["chunk_id"] = chunk_data["chunk_id"]
                event["source_url"] = chunk_data["source_url"]
                event["entities"] = entities
                all_events.append(event)
    
    none_event_indices = [
        c["event_index"] for c in career_class["classifications"]
        if c["assigned_label"] == "NONE"
    ]
    
    none_events = [e for i, e in enumerate(all_events) if i in none_event_indices]
    
    return {
        "none_events": none_events,
        "total_events": len(all_events)
    }

def format_chunk_text(chunk: Dict[str, Any]) -> str:
    return f"Source: {chunk['source_url']}\n\n{chunk['raw_text']}"

def format_entities_summary(chunk: Dict[str, Any]) -> str:
    entities = chunk["entities"]
    
    lines = []
    
    time_markers = entities.get("time_markers", [])
    if time_markers:
        lines.append("Time markers:")
        for tm in time_markers[:10]:
            lines.append(f"  - {tm.get('text', 'N/A')}")
    
    organizations = entities.get("organizations", [])
    if organizations:
        lines.append("\nOrganizations:")
        for org in organizations[:10]:
            lines.append(f"  - {org.get('name', 'N/A')}")
    
    roles = entities.get("roles", [])
    if roles:
        lines.append("\nRoles:")
        for role in roles[:10]:
            lines.append(f"  - {role.get('title', 'N/A')}")
    
    locations = entities.get("locations", [])
    if locations:
        lines.append("\nLocations:")
        for loc in locations[:10]:
            lines.append(f"  - {loc.get('place', 'N/A')}")
    
    return "\n".join(lines)

def extract_none_event(
    person_name: str,
    event: Dict[str, Any],
    chunk: Dict[str, Any],
    config: Dict[str, Any],
    prompt_template: str,
    co: cohere.Client
) -> Dict[str, Any]:
    
    chunk_text = format_chunk_text(chunk)
    entities_summary = format_entities_summary(chunk)
    
    prompt = prompt_template.format(
        person_name=person_name,
        chunk_text=chunk_text,
        entities_summary=entities_summary
    )
    
    response = co.chat(
        model=config["model"],
        temperature=config["temperature"],
        message=prompt,
        max_tokens=config["max_tokens_extract"]
    )
    
    raw_output = response.text.strip()
    raw_output = raw_output.replace("```json", "").replace("```", "").strip()
    
    try:
        result = json.loads(raw_output)
    except json.JSONDecodeError as e:
        lines = raw_output.split('\n')
        brace_count = 0
        json_end_idx = -1
        
        for i, line in enumerate(lines):
            for char in line:
                if char == '{':
                    brace_count += 1
                elif char == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        json_end_idx = i
                        break
            if json_end_idx != -1:
                break
        
        if json_end_idx != -1:
            json_text = '\n'.join(lines[:json_end_idx + 1])
            try:
                result = json.loads(json_text)
                print(f"    WARNING: Extracted JSON from text with extra content")
            except json.JSONDecodeError:
                print(f"    ERROR: Failed to parse LLM output: {e}")
                return {
                    "chunk_id": event["chunk_id"],
                    "events": [],
                    "raw_output": raw_output,
                    "error": str(e)
                }
        else:
            print(f"    ERROR: Failed to parse LLM output: {e}")
            return {
                "chunk_id": event["chunk_id"],
                "events": [],
                "raw_output": raw_output,
                "error": str(e)
            }
    
    events = result.get("events", [])
    
    return {
        "chunk_id": event["chunk_id"],
        "events": events,
        "raw_output": raw_output
    }

def run_phase1b(person_name: str, config_path: Path, base_dir: Path, output_dir: Path):
    print("\n" + "="*80)
    print(f"PHASE 1B: EXTRACTION NONE EVENTS - {person_name}")
    print("="*80)
    
    config = load_config(config_path)
    api_key = os.getenv(config["api_key_env_var"])
    
    if not api_key:
        raise EnvironmentError(f"{config['api_key_env_var']} not found")
    
    prompt_template = load_prompt(base_dir / "config" / "prompts" / "extract_none_events.txt")
    
    ea03_data = load_eventalign03_data(person_name, base_dir)
    none_events = ea03_data["none_events"]
    
    print(f"\nFound {len(none_events)} NONE events out of {ea03_data['total_events']} total events")
    
    if not none_events:
        print("No NONE events to process")
        return
    
    ea03_dir = base_dir.parent / "EventAlign_03" / "outputs" / person_name
    with open(ea03_dir / "01a_career_labels.json", "r", encoding="utf-8") as f:
        career_labels_data = json.load(f)
    
    core_orgs = set()
    for label in career_labels_data["career_labels"]:
        core_org = label["organization"].lower()
        core_orgs.add(core_org)
    
    print(f"Core organizations to filter: {core_orgs}")
    
    co = cohere.Client(api_key)
    
    results = []
    processed_chunks = set()
    
    for i, event in enumerate(none_events, 1):
        chunk_id = event["chunk_id"]
        
        if chunk_id in processed_chunks:
            print(f"  [{i}/{len(none_events)}] Chunk {chunk_id} already processed, skipping")
            continue
        
        print(f"  [{i}/{len(none_events)}] Processing chunk {chunk_id}...", end="")
        
        try:
            chunk = load_enriched_chunk(chunk_id, person_name, base_dir)
        except FileNotFoundError as e:
            print(f" ERROR: {e}")
            continue
        
        extraction = extract_none_event(
            person_name, event, chunk, config, prompt_template, co
        )
        
        original_count = len(extraction['events'])
        filtered_events = []
        
        for evt in extraction['events']:
            evt_org = evt.get("organization", "").lower()
            if any(core in evt_org or evt_org in core for core in core_orgs):
                print(f"\n    FILTERED: Event for '{evt['organization']}' matches a core organization")
            else:
                filtered_events.append(evt)
        
        extraction['events'] = filtered_events
        extraction['events_filtered'] = original_count - len(filtered_events)
        
        print(f" {len(filtered_events)} events ({original_count - len(filtered_events)} filtered)")
        
        results.append(extraction)
        processed_chunks.add(chunk_id)
    
    output_file = output_dir / f"{person_name}_phase1b.json"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    total_extracted = sum(len(r["events"]) for r in results)
    
    output = {
        "person_name": person_name,
        "phase": "1b_extraction_none_events",
        "chunks_processed": len(results),
        "total_events_extracted": total_extracted,
        "extractions": results
    }
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"\n{'='*80}")
    print(f"PHASE 1B COMPLETE")
    print(f"Chunks processed: {len(results)}")
    print(f"Events extracted: {total_extracted}")
    print(f"Saved to: {output_file}")
    print(f"{'='*80}")
    
    return output

if __name__ == "__main__":
    script_dir = Path(__file__).parent
    config_path = script_dir / "config" / "config.json"
    output_dir = script_dir / "outputs"
    
    person_name = "Abhijit_Banerjee"
    
    run_phase1b(person_name, config_path, script_dir, output_dir)