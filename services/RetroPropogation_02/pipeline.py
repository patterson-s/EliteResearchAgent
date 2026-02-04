from pathlib import Path
from typing import Dict, Any, List

from load_existing import load_existing_state
from load_data import get_next_source, load_chunks_for_source
from step1_extract_candidates import extract_candidates_from_chunk
from step2_match_or_new import match_or_new
from step3a_enrich_event import enrich_event
from step3b_create_event import create_new_event
from utils import save_json

def run_incremental_pipeline(
    person_name: str,
    config_path: Path,
    output_dir: Path,
    retroprop01_dir: Path,
    processed_sources: List[str] = None
) -> Dict[str, Any]:
    print("\n" + "="*80)
    print(f"RETROPROPAGATION_02 PIPELINE - {person_name}")
    print("="*80)
    
    print("\nLoading existing state from RetroPropogation_01...")
    state = load_existing_state(person_name, retroprop01_dir)
    
    events = state["events"]
    canonical_orgs = state["canonical_orgs"]
    
    print(f"Starting with {len(events)} existing events")
    
    if processed_sources is None:
        processed_sources = ["wikipedia.org"]
    
    print(f"\nFinding next source to process...")
    next_source = get_next_source(person_name, processed_sources)
    
    if not next_source:
        print("No new sources to process")
        return {
            "person_name": person_name,
            "events": events,
            "processed_sources": processed_sources,
            "status": "no_new_sources"
        }
    
    print(f"Processing source: {next_source}")
    
    chunks = load_chunks_for_source(person_name, next_source)
    print(f"Found {len(chunks)} chunks in this source")
    
    decision_log = []
    chunks_processed = 0
    candidates_found = 0
    events_merged = 0
    events_created = 0
    
    for i, chunk in enumerate(chunks, 1):
        print(f"\n[{i}/{len(chunks)}] Processing chunk {chunk['chunk_id']}...")
        
        extract_result = extract_candidates_from_chunk(chunk, config_path)
        candidates = extract_result["candidates"]
        
        if not candidates:
            print(f"  No career events found")
            continue
        
        print(f"  Found {len(candidates)} candidate events")
        candidates_found += len(candidates)
        
        for j, candidate in enumerate(candidates, 1):
            print(f"    [{j}/{len(candidates)}] Analyzing candidate...")
            
            decision = match_or_new(candidate, events, config_path)
            
            if decision["decision"] == "merge":
                target_id = decision["target_event_id"]
                target_idx = next((idx for idx, e in enumerate(events) if e.get("event_id") == target_id), None)
                
                if target_idx is not None:
                    print(f"      → MERGE with {target_id}: {decision['reasoning']}")
                    
                    enrich_result = enrich_event(events[target_idx], candidate, config_path)
                    events[target_idx] = enrich_result["updated_event"]
                    
                    events_merged += 1
                    
                    decision_log.append({
                        "chunk_id": chunk["chunk_id"],
                        "candidate": candidate,
                        "action": "merge",
                        "target_event_id": target_id,
                        "reasoning": decision["reasoning"],
                        "changes_made": enrich_result.get("changes_made", True),
                        "changes_summary": enrich_result.get("changes_summary", "")
                    })
                else:
                    print(f"      → ERROR: Target event {target_id} not found, creating new instead")
                    decision["decision"] = "new"
            
            if decision["decision"] == "new":
                print(f"      → CREATE NEW: {decision['reasoning']}")
                
                create_result = create_new_event(candidate, events, config_path)
                new_event = create_result["new_event"]
                events.append(new_event)
                
                events_created += 1
                
                decision_log.append({
                    "chunk_id": chunk["chunk_id"],
                    "candidate": candidate,
                    "action": "new",
                    "new_event_id": new_event["event_id"],
                    "reasoning": decision["reasoning"]
                })
        
        chunks_processed += 1
    
    processed_sources.append(next_source)
    
    print("\n" + "="*80)
    print("PIPELINE COMPLETE")
    print("="*80)
    print(f"\nSource processed: {next_source}")
    print(f"Chunks processed: {chunks_processed}/{len(chunks)}")
    print(f"Candidates found: {candidates_found}")
    print(f"Events merged: {events_merged}")
    print(f"Events created: {events_created}")
    print(f"Total events now: {len(events)}")
    
    result = {
        "person_name": person_name,
        "source_processed": next_source,
        "events": events,
        "canonical_orgs": canonical_orgs,
        "processed_sources": processed_sources,
        "decision_log": decision_log,
        "summary": {
            "chunks_processed": chunks_processed,
            "total_chunks": len(chunks),
            "candidates_found": candidates_found,
            "events_merged": events_merged,
            "events_created": events_created,
            "total_events": len(events)
        }
    }
    
    save_json(result["events"], output_dir / "events.json")
    save_json(result["decision_log"], output_dir / "decision_log.json")
    save_json(result["summary"], output_dir / "summary.json")
    save_json({"processed_sources": processed_sources}, output_dir / "processed_sources.json")
    
    print(f"\nOutput saved to: {output_dir}")
    
    return result

if __name__ == "__main__":
    script_dir = Path(__file__).parent
    config_path = script_dir / "config" / "config.json"
    retroprop01_dir = script_dir.parent / "RetroPropogation_01" / "outputs"
    
    person_name = "Amre Moussa"
    output_dir = script_dir / "outputs" / person_name.replace(" ", "_")
    
    result = run_incremental_pipeline(
        person_name,
        config_path,
        output_dir,
        retroprop01_dir
    )
