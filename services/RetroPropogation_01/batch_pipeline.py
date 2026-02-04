import argparse
from pathlib import Path
from typing import List

from load_data import load_chunks_for_person, get_wikipedia_source, get_db_connection
from pipeline import run_pipeline

def get_all_people_from_db() -> List[str]:
    conn = get_db_connection()
    
    try:
        query = """
            SELECT DISTINCT person_name
            FROM sources.persons_searched
            ORDER BY person_name
        """
        
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
        
        people = [row[0] for row in rows]
        return people
    
    finally:
        conn.close()

def has_wikipedia_source(person_name: str) -> bool:
    chunks = load_chunks_for_person(person_name)
    wiki_chunks = get_wikipedia_source(chunks)
    return len(wiki_chunks) > 0

def is_already_processed(person_name: str, outputs_dir: Path) -> bool:
    person_dir = outputs_dir / person_name.replace(" ", "_")
    summary_file = person_dir / "pipeline_summary.json"
    return summary_file.exists()

def main():
    parser = argparse.ArgumentParser(description="Batch process multiple people")
    parser.add_argument("--all", action="store_true", help="Process all people with Wikipedia sources")
    parser.add_argument("--people", nargs="+", help="Specific people to process")
    parser.add_argument("--skip-existing", action="store_true", help="Skip already processed people")
    parser.add_argument("--limit", type=int, help="Limit number of people to process")
    args = parser.parse_args()
    
    script_dir = Path(__file__).parent
    config_path = script_dir / "config" / "config.json"
    outputs_dir = script_dir / "outputs"
    
    if args.people:
        people_to_process = args.people
    elif args.all:
        print("Fetching all people from database...")
        all_people = get_all_people_from_db()
        print(f"Found {len(all_people)} people in database")
        
        print("\nChecking for Wikipedia sources...")
        people_to_process = []
        for person in all_people:
            if has_wikipedia_source(person):
                people_to_process.append(person)
        
        print(f"Found {len(people_to_process)} people with Wikipedia sources")
    else:
        print("Please specify --all or --people NAME1 NAME2 ...")
        return
    
    if args.skip_existing:
        original_count = len(people_to_process)
        people_to_process = [
            p for p in people_to_process 
            if not is_already_processed(p, outputs_dir)
        ]
        skipped = original_count - len(people_to_process)
        if skipped > 0:
            print(f"Skipping {skipped} already processed people")
    
    if args.limit:
        people_to_process = people_to_process[:args.limit]
    
    if not people_to_process:
        print("No people to process")
        return
    
    print(f"\n{'='*80}")
    print(f"BATCH PROCESSING: {len(people_to_process)} people")
    print(f"{'='*80}\n")
    
    results = {
        "success": [],
        "failed": []
    }
    
    for i, person_name in enumerate(people_to_process, 1):
        print(f"\n[{i}/{len(people_to_process)}] Processing: {person_name}")
        print("-" * 80)
        
        try:
            output_dir = outputs_dir / person_name.replace(" ", "_")
            summary = run_pipeline(person_name, config_path, output_dir)
            results["success"].append(person_name)
            
            print(f"\nSUCCESS: {person_name}")
            print(f"  Events assembled: {summary['events_assembled']}")
            print(f"  Verification: {summary['verification_summary']['valid']} valid, "
                  f"{summary['verification_summary']['warnings']} warnings")
        
        except Exception as e:
            print(f"\nFAILED: {person_name}")
            print(f"  Error: {e}")
            results["failed"].append((person_name, str(e)))
    
    print(f"\n{'='*80}")
    print("BATCH PROCESSING COMPLETE")
    print(f"{'='*80}")
    print(f"\nSuccessful: {len(results['success'])}/{len(people_to_process)}")
    for person in results["success"]:
        print(f"  - {person}")
    
    if results["failed"]:
        print(f"\nFailed: {len(results['failed'])}/{len(people_to_process)}")
        for person, error in results["failed"]:
            print(f"  - {person}: {error}")

if __name__ == "__main__":
    main()