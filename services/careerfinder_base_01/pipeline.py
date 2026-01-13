import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional
import argparse

from load_data import load_chunks_for_person
from extraction import extract_career_events, load_config

def run_pipeline(
    person_name: str,
    config_path: Path,
    output_dir: Optional[Path] = None,
    from_file: Optional[Path] = None
) -> Dict[str, Any]:
    if output_dir is None:
        output_dir = Path(__file__).parent / "review"
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    config = load_config(config_path)
    timestamp = datetime.utcnow()
    
    print("=" * 100)
    print(f"CareerFinder Base v01: {person_name}")
    print("=" * 100)
    print(f"Started: {timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
    
    print("STEP 1: LOAD CHUNKS")
    print("-" * 100)
    if from_file:
        print(f"Loading from file: {from_file}")
    else:
        print(f"Loading from database...")
    
    person_chunks = load_chunks_for_person(person_name, from_file)
    print(f"Loaded {len(person_chunks)} chunks for {person_name}")
    
    if not person_chunks:
        print("No chunks found for person")
        return {
            "person_name": person_name,
            "timestamp": timestamp.isoformat(),
            "config": {
                "service_name": config["service_name"],
                "version": config["version"],
                "model": config["model"]
            },
            "raw_extractions": [],
            "chunks_processed": 0,
            "events_extracted": 0
        }
    
    print("\n" + "STEP 2: EXTRACT EVENTS")
    print("-" * 100)
    
    all_events = []
    checkpoint_dir = output_dir / "checkpoints"
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_file = checkpoint_dir / f"checkpoint_{person_name.replace(' ', '_')}_{timestamp.strftime('%Y%m%d_%H%M%S')}.json"
    
    for i, chunk in enumerate(person_chunks, 1):
        chunk_id = chunk.get("chunk_id", f"unknown_{i}")
        source_url = chunk.get("source_url", "unknown")
        chunk_text = chunk.get("text", "")
        
        print(f"[{i}/{len(person_chunks)}] Processing chunk: {chunk_id}")
        
        try:
            events = extract_career_events(
                person_name,
                chunk_text,
                chunk_id,
                source_url,
                config_path
            )
            
            if events:
                print(f"    -> Extracted {len(events)} event(s)")
                all_events.extend(events)
            else:
                print(f"    -> No events found")
            
            if i % 10 == 0 or events:
                checkpoint_data = {
                    "person_name": person_name,
                    "chunks_processed": i,
                    "events_extracted": len(all_events),
                    "events": all_events,
                    "last_chunk_id": chunk_id,
                    "timestamp": datetime.utcnow().isoformat()
                }
                with open(checkpoint_file, "w", encoding="utf-8") as f:
                    json.dump(checkpoint_data, f, indent=2, ensure_ascii=False)
        
        except Exception as e:
            print(f"    -> ERROR: {e}")
            checkpoint_data = {
                "person_name": person_name,
                "chunks_processed": i,
                "events_extracted": len(all_events),
                "events": all_events,
                "last_chunk_id": chunk_id,
                "last_error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }
            with open(checkpoint_file, "w", encoding="utf-8") as f:
                json.dump(checkpoint_data, f, indent=2, ensure_ascii=False)
            continue
    
    print("\n" + "STEP 3: SUMMARY")
    print("-" * 100)
    print(f"Total events extracted: {len(all_events)}")
    print(f"Chunks processed: {len(person_chunks)}")
    print(f"Events per chunk (avg): {len(all_events) / len(person_chunks):.2f}")
    
    result = {
        "person_name": person_name,
        "timestamp": timestamp.isoformat(),
        "config": {
            "service_name": config["service_name"],
            "version": config["version"],
            "model": config["model"]
        },
        "raw_extractions": all_events,
        "chunks_processed": len(person_chunks),
        "events_extracted": len(all_events)
    }
    
    output_file = output_dir / f"careerfinder_base_{person_name.replace(' ', '_')}_{timestamp.strftime('%Y%m%d_%H%M%S')}.json"
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    
    print(f"\nSaved to: {output_file.resolve()}")
    print("\n" + "=" * 100)
    print("Pipeline complete")
    print("=" * 100)
    
    return result

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="CareerFinder Base v01: Extract all career events"
    )
    parser.add_argument(
        "--person",
        required=True,
        help="Person name to process"
    )
    parser.add_argument(
        "--from-file",
        type=Path,
        help="Load chunks from cached JSON file instead of database"
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/config.json"),
        help="Path to config file"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output directory (default: review/)"
    )
    
    args = parser.parse_args()
    
    run_pipeline(args.person, args.config, args.output, args.from_file)