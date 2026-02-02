import json
import os
from pathlib import Path
from typing import Dict, Any, List
import cohere
from dotenv import load_dotenv

from load_chunks import load_enriched_chunks_batch

load_dotenv(Path(__file__).parent.parent.parent / ".env")

def load_config(config_path: Path) -> Dict[str, Any]:
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

def load_prompt(prompt_path: Path) -> str:
    with open(prompt_path, "r", encoding="utf-8") as f:
        return f.read()

def load_eventalign03_data(person_name: str, base_dir: Path) -> Dict[str, Any]:
    ea03_dir = base_dir.parent / "EventAlign_03" / "outputs" / person_name
    
    with open(ea03_dir / "01a_career_labels.json", "r", encoding="utf-8") as f:
        career_labels = json.load(f)
    
    with open(ea03_dir / "03_cores_report.json", "r", encoding="utf-8") as f:
        cores_report = json.load(f)
    
    return {
        "career_labels": career_labels["career_labels"],
        "cores_report": cores_report
    }

def format_chunks_text(chunks: Dict[int, Dict[str, Any]]) -> str:
    lines = []
    for chunk_id in sorted(chunks.keys()):
        chunk = chunks[chunk_id]
        lines.append(f"=== CHUNK {chunk_id} ===")
        lines.append(f"Source: {chunk['source_url']}")
        lines.append(f"\n{chunk['raw_text']}\n")
    return "\n".join(lines)

def format_entities_summary(chunks: Dict[int, Dict[str, Any]]) -> str:
    all_time_markers = []
    all_organizations = []
    all_roles = []
    all_locations = []
    
    for chunk in chunks.values():
        entities = chunk["entities"]
        all_time_markers.extend(entities.get("time_markers", []))
        all_organizations.extend(entities.get("organizations", []))
        all_roles.extend(entities.get("roles", []))
        all_locations.extend(entities.get("locations", []))
    
    lines = []
    
    if all_time_markers:
        lines.append("Time markers found:")
        for tm in all_time_markers[:20]:
            lines.append(f"  - {tm.get('text', 'N/A')}: {tm.get('type', 'N/A')}")
    
    if all_organizations:
        lines.append("\nOrganizations found:")
        for org in all_organizations[:20]:
            lines.append(f"  - {org.get('name', 'N/A')}")
    
    if all_roles:
        lines.append("\nRoles found:")
        for role in all_roles[:20]:
            lines.append(f"  - {role.get('title', 'N/A')}")
    
    if all_locations:
        lines.append("\nLocations found:")
        for loc in all_locations[:20]:
            lines.append(f"  - {loc.get('place', 'N/A')}")
    
    return "\n".join(lines)

def extract_events_pass_a(
    person_name: str,
    core_id: str,
    core_data: Dict[str, Any],
    chunks: Dict[int, Dict[str, Any]],
    config: Dict[str, Any],
    prompt_template: str,
    co: cohere.Client
) -> Dict[str, Any]:
    
    core_label = core_data["label"]["label"]
    core_org = core_data["label"]["organization"]
    
    org_variations = set()
    for chunk in chunks.values():
        for org in chunk["entities"].get("organizations", []):
            org_name = org.get("name", "")
            if org_name:
                org_variations.add(org_name)
    
    chunks_text = format_chunks_text(chunks)
    entities_summary = format_entities_summary(chunks)
    
    prompt = prompt_template.format(
        person_name=person_name,
        core_org=core_org,
        core_org_standardized=core_label,
        org_variations=", ".join(sorted(org_variations)),
        chunks_text=chunks_text,
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
                print(f"  WARNING: Extracted JSON from text with extra content")
            except json.JSONDecodeError:
                print(f"  ERROR: Failed to parse LLM output for {core_id}: {e}")
                return {
                    "core_id": core_id,
                    "core_label": core_label,
                    "events": [],
                    "raw_output": raw_output,
                    "error": str(e)
                }
        else:
            print(f"  ERROR: Failed to parse LLM output for {core_id}: {e}")
            return {
                "core_id": core_id,
                "core_label": core_label,
                "events": [],
                "raw_output": raw_output,
                "error": str(e)
            }
    
    events = result.get("events", [])
    filtered_events = []
    
    for event in events:
        event_org = event.get("organization", "")
        if core_label.lower() in event_org.lower() or event_org.lower() in core_label.lower():
            filtered_events.append(event)
        else:
            print(f"    FILTERED: Event for '{event_org}' not matching core '{core_label}'")
    
    return {
        "core_id": core_id,
        "core_label": core_label,
        "events": filtered_events,
        "raw_output": raw_output,
        "events_filtered": len(events) - len(filtered_events)
    }

def run_phase1(person_name: str, config_path: Path, base_dir: Path, output_dir: Path):
    print("\n" + "="*80)
    print(f"PHASE 1A: EXTRACTION PASS A - {person_name}")
    print("="*80)
    
    config = load_config(config_path)
    api_key = os.getenv(config["api_key_env_var"])
    
    if not api_key:
        raise EnvironmentError(f"{config['api_key_env_var']} not found")
    
    prompt_template = load_prompt(base_dir / "config" / "prompts" / "extract_granular_events.txt")
    
    ea03_data = load_eventalign03_data(person_name, base_dir)
    cores_report = ea03_data["cores_report"]
    
    co = cohere.Client(api_key)
    
    results = []
    
    career_cores = cores_report.get("career_cores", {})
    
    print(f"\nProcessing {len(career_cores)} career cores...")
    
    for core_id, core_data in career_cores.items():
        print(f"\n  Processing {core_id}: {core_data['label']['label']}")
        
        chunk_ids = list(set(e["chunk_id"] for e in core_data["events"]))
        print(f"    Loading {len(chunk_ids)} chunks...")
        
        chunks = load_enriched_chunks_batch(chunk_ids, person_name, base_dir)
        
        if not chunks:
            print(f"    WARNING: No chunks loaded for {core_id}")
            continue
        
        print(f"    Extracting events (Pass A)...")
        extraction = extract_events_pass_a(
            person_name, core_id, core_data, chunks, config, prompt_template, co
        )
        
        print(f"    Extracted {len(extraction['events'])} events")
        
        results.append(extraction)
    
    output_file = output_dir / f"{person_name}_phase1a.json"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    output = {
        "person_name": person_name,
        "phase": "1a_extraction_pass_a",
        "cores_processed": len(results),
        "extractions": results
    }
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"\n{'='*80}")
    print(f"PHASE 1A COMPLETE")
    print(f"Saved to: {output_file}")
    print(f"{'='*80}")
    
    return output

if __name__ == "__main__":
    script_dir = Path(__file__).parent
    config_path = script_dir / "config" / "config.json"
    output_dir = script_dir / "outputs"
    
    person_name = "Abhijit_Banerjee"
    
    run_phase1(person_name, config_path, script_dir, output_dir)