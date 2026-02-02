import json
from pathlib import Path
from typing import Dict, Any, List

def inspect_eventalign03_outputs(base_dir: Path):
    print("="*80)
    print("INSPECTING EVENTALIGN_03 OUTPUTS")
    print("="*80)
    
    outputs_dir = base_dir.parent / "EventAlign_03" / "outputs"
    
    if not outputs_dir.exists():
        print(f"NOT FOUND: {outputs_dir}")
        return
    
    people = [d for d in outputs_dir.iterdir() if d.is_dir()]
    
    if not people:
        print("No people found in outputs")
        return
    
    person_dir = people[0]
    print(f"\nExamining: {person_dir.name}")
    
    files = list(person_dir.glob("*.json"))
    print(f"\nFiles found: {len(files)}")
    for f in files:
        print(f"  {f.name}")
    
    if (person_dir / "03_cores_report.json").exists():
        with open(person_dir / "03_cores_report.json", "r", encoding="utf-8") as f:
            report = json.load(f)
        
        print("\n" + "-"*80)
        print("CORES REPORT STRUCTURE")
        print("-"*80)
        print(f"Top-level keys: {list(report.keys())}")
        
        if "career_cores" in report and report["career_cores"]:
            first_core_id = list(report["career_cores"].keys())[0]
            first_core = report["career_cores"][first_core_id]
            print(f"\nFirst career core ID: {first_core_id}")
            print(f"Core structure keys: {list(first_core.keys())}")
            
            if first_core["events"]:
                first_event = first_core["events"][0]
                print(f"\nFirst event keys: {list(first_event.keys())}")
                print(f"Event sample:")
                for key in ["event_index", "chunk_id", "source_url", "event_type"]:
                    if key in first_event:
                        print(f"  {key}: {first_event[key]}")

def inspect_chunk_files(base_dir: Path):
    print("\n" + "="*80)
    print("INSPECTING CHUNK FILES")
    print("="*80)
    
    careerfinder_dir = base_dir.parent / "careerfinder_granular_01" / "review"
    
    if not careerfinder_dir.exists():
        print(f"NOT FOUND: {careerfinder_dir}")
        return
    
    people = [d for d in careerfinder_dir.iterdir() if d.is_dir()]
    
    if not people:
        print("No people found in review")
        return
    
    person_dir = people[0]
    print(f"\nExamining: {person_dir.name}")
    
    chunk_files = list(person_dir.glob("chunk_*_results.json"))
    print(f"\nChunk files found: {len(chunk_files)}")
    
    if chunk_files:
        first_chunk = chunk_files[0]
        print(f"First chunk file: {first_chunk.name}")
        
        with open(first_chunk, "r", encoding="utf-8") as f:
            chunk_data = json.load(f)
        
        print("\n" + "-"*80)
        print("CHUNK FILE STRUCTURE")
        print("-"*80)
        print(f"Top-level keys: {list(chunk_data.keys())}")
        
        for key in ["chunk_id", "chunk_index", "source_url", "status"]:
            if key in chunk_data:
                print(f"  {key}: {chunk_data[key]}")
        
        if "step1" in chunk_data:
            print(f"\nstep1 keys: {list(chunk_data['step1'].keys())}")
            if "entities" in chunk_data["step1"]:
                entities = chunk_data["step1"]["entities"]
                print(f"Entities keys: {list(entities.keys())}")
                for entity_type in entities:
                    print(f"  {entity_type}: {len(entities[entity_type])} items")
        
        if "step2" in chunk_data:
            print(f"\nstep2 keys: {list(chunk_data['step2'].keys())}")
            if "assembled_events" in chunk_data["step2"]:
                events = chunk_data["step2"]["assembled_events"]
                print(f"Assembled events: {len(events)}")
                if events:
                    print(f"First event keys: {list(events[0].keys())}")
        
        print("\n" + "-"*80)
        print("CHECKING FOR RAW TEXT")
        print("-"*80)
        
        text_fields = []
        for key in chunk_data.keys():
            if "text" in key.lower():
                text_fields.append(key)
        
        if text_fields:
            print(f"Found text fields: {text_fields}")
            for field in text_fields:
                value = chunk_data[field]
                if isinstance(value, str):
                    print(f"\n{field}: {len(value)} chars")
                    print(f"Preview: {value[:200]}...")
        else:
            print("No 'text' field found in chunk JSON")
            print("\nSearching in nested structures...")
            
            def find_text_recursive(obj, path=""):
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        new_path = f"{path}.{k}" if path else k
                        if "text" in k.lower() and isinstance(v, str) and len(v) > 50:
                            print(f"  Found at {new_path}: {len(v)} chars")
                        find_text_recursive(v, new_path)
                elif isinstance(obj, list) and obj:
                    find_text_recursive(obj[0], f"{path}[0]")
            
            find_text_recursive(chunk_data)

def check_id_mapping(base_dir: Path):
    print("\n" + "="*80)
    print("CHECKING ID MAPPING")
    print("="*80)
    
    ea03_dir = base_dir.parent / "EventAlign_03" / "outputs"
    cf_dir = base_dir.parent / "careerfinder_granular_01" / "review"
    
    if not ea03_dir.exists() or not cf_dir.exists():
        print("Required directories not found")
        return
    
    ea03_people = [d.name for d in ea03_dir.iterdir() if d.is_dir()]
    cf_people = [d.name for d in cf_dir.iterdir() if d.is_dir()]
    
    common_people = set(ea03_people) & set(cf_people)
    
    print(f"\nPeople in EventAlign_03: {len(ea03_people)}")
    print(f"People in careerfinder: {len(cf_people)}")
    print(f"Common people: {len(common_people)}")
    
    if common_people:
        person = list(common_people)[0]
        print(f"\nChecking {person}...")
        
        report_file = ea03_dir / person / "03_cores_report.json"
        if report_file.exists():
            with open(report_file, "r", encoding="utf-8") as f:
                report = json.load(f)
            
            chunk_ids_in_report = set()
            for core in report.get("career_cores", {}).values():
                for event in core.get("events", []):
                    if "chunk_id" in event:
                        chunk_ids_in_report.add(event["chunk_id"])
            
            print(f"Unique chunk_ids in report: {len(chunk_ids_in_report)}")
            if chunk_ids_in_report:
                sample_ids = list(chunk_ids_in_report)[:5]
                print(f"Sample IDs: {sample_ids}")
        
        chunk_dir = cf_dir / person
        chunk_files = list(chunk_dir.glob("chunk_*_results.json"))
        
        if chunk_files:
            chunk_ids_in_files = []
            for cf in chunk_files[:5]:
                with open(cf, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if "chunk_id" in data:
                        chunk_ids_in_files.append(data["chunk_id"])
            
            print(f"\nSample chunk_ids from files: {chunk_ids_in_files}")
            
            if chunk_ids_in_report and chunk_ids_in_files:
                if any(cid in chunk_ids_in_report for cid in chunk_ids_in_files):
                    print("\n✓ IDs MATCH - Can map events to chunk files by chunk_id")
                else:
                    print("\n✗ IDs DON'T MATCH - Need alternative mapping strategy")

def inspect_data_dir(base_dir: Path):
    print("\n" + "="*80)
    print("INSPECTING EVENTALIGN_03 DATA DIR")
    print("="*80)
    
    data_dir = base_dir.parent / "EventAlign_03" / "data"
    
    if not data_dir.exists():
        print(f"NOT FOUND: {data_dir}")
        return
    
    people = [d for d in data_dir.iterdir() if d.is_dir()]
    print(f"\nPeople in data dir: {len(people)}")
    
    if people:
        person_dir = people[0]
        print(f"Examining: {person_dir.name}")
        
        chunk_files = list(person_dir.glob("chunk_*_results.json"))
        print(f"Chunk files: {len(chunk_files)}")
        
        if chunk_files:
            print(f"Sample file: {chunk_files[0].name}")

if __name__ == "__main__":
    script_dir = Path(__file__).parent
    
    inspect_eventalign03_outputs(script_dir)
    inspect_chunk_files(script_dir)
    check_id_mapping(script_dir)
    inspect_data_dir(script_dir)
    
    print("\n" + "="*80)
    print("SUMMARY & RECOMMENDATIONS")
    print("="*80)
    print("\nBased on inspection above, determine:")
    print("1. Can we map event.chunk_id to chunk files?")
    print("2. Is raw text available in chunk JSON files?")
    print("3. If not, do we need to query DB using load_data.py?")
    print("4. What's the correct path structure for inputs?")