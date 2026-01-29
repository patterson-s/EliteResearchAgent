import json
import argparse
from pathlib import Path
from typing import Dict, Any, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from load_data import load_chunks_from_db, get_all_people
from classification import classify_chunk
from extraction_step1 import extract_entities_step1
from extraction_step2 import assemble_events_step2
from extraction_step3 import verify_events_step3

def process_chunk(
    chunk: Dict[str, Any],
    config_path: Path
) -> Dict[str, Any]:
    chunk_id = chunk.get("chunk_id")
    chunk_text = chunk.get("text", "")
    
    try:
        step1_result = extract_entities_step1(
            chunk_text,
            chunk,
            config_path,
            None
        )
        
        entities = step1_result["entities"]
        
        step2_result = assemble_events_step2(entities, config_path)
        
        events = step2_result["assembled_events"]
        
        step3_result = verify_events_step3(events, entities, config_path)
        
        return {
            "status": "success",
            "chunk_id": chunk_id,
            "chunk_index": chunk.get("chunk_index"),
            "source_url": chunk.get("source_url"),
            "title": chunk.get("title"),
            "document_type": step1_result["document_type"],
            "step1": {
                "entities": entities,
                "prompt_used": step1_result["prompt_used"]
            },
            "step2": {
                "assembled_events": events
            },
            "step3": {
                "verified_events": step3_result["verified_events"],
                "summary": step3_result["summary"]
            }
        }
        
    except Exception as e:
        return {
            "status": "error",
            "chunk_id": chunk_id,
            "chunk_index": chunk.get("chunk_index"),
            "source_url": chunk.get("source_url"),
            "error": str(e)
        }

def process_person(
    person_name: str,
    config_path: Path,
    output_dir: Path,
    workers: int = 4
) -> Dict[str, Any]:
    print(f"\nProcessing: {person_name}")
    
    chunks = load_chunks_from_db(person_name)
    print(f"Loaded {len(chunks)} chunks")
    
    person_dir = output_dir / person_name.replace(" ", "_")
    person_dir.mkdir(parents=True, exist_ok=True)
    
    results = []
    success_count = 0
    error_count = 0
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(process_chunk, chunk, config_path): chunk
            for chunk in chunks
        }
        
        for i, future in enumerate(as_completed(futures), 1):
            chunk = futures[future]
            result = future.result()
            results.append(result)
            
            chunk_file = person_dir / f"chunk_{result['chunk_id']}_results.json"
            with open(chunk_file, "w", encoding="utf-8") as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            
            if result["status"] == "success":
                success_count += 1
                events_count = len(result["step2"]["assembled_events"])
                valid_count = result["step3"]["summary"].get("valid", 0)
                print(f"  [{i}/{len(chunks)}] Chunk {result['chunk_id']}: {events_count} events, {valid_count} valid")
            else:
                error_count += 1
                print(f"  [{i}/{len(chunks)}] Chunk {result['chunk_id']}: ERROR - {result['error']}")
    
    summary = {
        "person_name": person_name,
        "timestamp": datetime.now().isoformat(),
        "total_chunks": len(chunks),
        "successful": success_count,
        "errors": error_count,
        "total_events": sum(
            len(r["step2"]["assembled_events"]) 
            for r in results if r["status"] == "success"
        ),
        "total_valid_events": sum(
            r["step3"]["summary"].get("valid", 0)
            for r in results if r["status"] == "success"
        ),
        "chunks": [
            {
                "chunk_id": r["chunk_id"],
                "status": r["status"],
                "source_url": r.get("source_url"),
                "events_count": len(r["step2"]["assembled_events"]) if r["status"] == "success" else 0,
                "valid_count": r["step3"]["summary"].get("valid", 0) if r["status"] == "success" else 0
            }
            for r in results
        ]
    }
    
    summary_file = person_dir / "summary.json"
    with open(summary_file, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    
    print(f"\nCompleted {person_name}: {success_count} success, {error_count} errors")
    print(f"Total events extracted: {summary['total_events']}")
    print(f"Total valid events: {summary['total_valid_events']}")
    print(f"Results saved to: {person_dir}")
    
    return summary

def main():
    parser = argparse.ArgumentParser(description="Batch process chunks for career extraction")
    parser.add_argument("--person", help="Process specific person by name")
    parser.add_argument("--first", action="store_true", help="Process first person in database")
    parser.add_argument("--all", action="store_true", help="Process all people (not implemented yet)")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel workers (default: 4)")
    args = parser.parse_args()
    
    script_dir = Path(__file__).parent
    config_path = script_dir / "config" / "config.json"
    output_dir = script_dir / "review"
    
    if args.all:
        print("--all option not yet implemented. Use --first or --person NAME")
        return
    
    if args.first:
        people = get_all_people()
        if not people:
            print("No people found in database")
            return
        person_name = people[0]
        print(f"Processing first person: {person_name}")
    elif args.person:
        person_name = args.person
    else:
        print("Please specify --person NAME or --first")
        return
    
    process_person(person_name, config_path, output_dir, args.workers)

if __name__ == "__main__":
    main()