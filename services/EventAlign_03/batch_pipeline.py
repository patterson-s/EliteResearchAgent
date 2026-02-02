import argparse
from pathlib import Path
from typing import List

from load_events import load_events_from_chunks
from phase1a_discover_career_labels import discover_career_labels
from phase1b_discover_award_labels import discover_award_labels
from phase2a_classify_career_events import classify_career_events
from phase2b_classify_award_events import classify_award_events
from generate_report import generate_report

def get_all_people(data_dir: Path) -> List[str]:
    return [d.name for d in data_dir.iterdir() if d.is_dir()]

def is_already_processed(output_dir: Path) -> bool:
    report_file = output_dir / "03_cores_report.json"
    return report_file.exists()

def process_person(person: str, data_dir: Path, config_path: Path, outputs_dir: Path, force: bool = False):
    person_data_dir = data_dir / person
    person_output_dir = outputs_dir / person
    
    if is_already_processed(person_output_dir) and not force:
        print(f"\n{'='*80}")
        print(f"SKIPPING {person} - Already processed")
        print(f"{'='*80}")
        return "skipped"
    
    print(f"\n{'='*80}")
    print(f"PROCESSING {person}")
    print(f"{'='*80}")
    
    try:
        person_output_dir.mkdir(parents=True, exist_ok=True)
        
        events = load_events_from_chunks(person_data_dir)
        
        print(f"  Total events: {len(events['all'])}")
        print(f"  Career positions: {len(events['career'])}")
        print(f"  Awards: {len(events['awards'])}")
        
        if len(events['all']) == 0:
            print(f"  WARNING: No events found for {person}")
            return "no_events"
        
        discover_career_labels(events["career"], config_path, person_output_dir)
        discover_award_labels(events["awards"], config_path, person_output_dir)
        
        import json
        with open(person_output_dir / "01a_career_labels.json", "r", encoding="utf-8") as f:
            career_labels_data = json.load(f)
        
        with open(person_output_dir / "01b_award_labels.json", "r", encoding="utf-8") as f:
            award_labels_data = json.load(f)
        
        classify_career_events(
            events["career"],
            career_labels_data["career_labels"],
            config_path,
            person_output_dir
        )
        
        classify_award_events(
            events["awards"],
            award_labels_data["award_labels"],
            config_path,
            person_output_dir
        )
        
        generate_report(person_output_dir, person_data_dir)
        
        print(f"\n{'='*80}")
        print(f"COMPLETED {person}")
        print(f"{'='*80}")
        
        return "success"
        
    except Exception as e:
        print(f"\n{'='*80}")
        print(f"ERROR processing {person}: {e}")
        print(f"{'='*80}")
        return "error"

def main():
    parser = argparse.ArgumentParser(description="Batch process EventAlign_03 pipeline")
    parser.add_argument("--person", type=str, help="Process specific person")
    parser.add_argument("--all", action="store_true", help="Process all people")
    parser.add_argument("--force", action="store_true", help="Reprocess even if already done")
    args = parser.parse_args()
    
    if not args.person and not args.all:
        parser.error("Must specify either --person NAME or --all")
    
    script_dir = Path(__file__).parent
    data_dir = script_dir / "data"
    config_path = script_dir / "config" / "config.json"
    outputs_dir = script_dir / "outputs"
    
    if args.person:
        people = [args.person]
    else:
        people = get_all_people(data_dir)
    
    print(f"\n{'='*80}")
    print(f"BATCH PROCESSING - {len(people)} people")
    print(f"{'='*80}")
    
    results = {
        "success": [],
        "skipped": [],
        "error": [],
        "no_events": []
    }
    
    for person in people:
        status = process_person(person, data_dir, config_path, outputs_dir, args.force)
        results[status].append(person)
    
    print(f"\n{'='*80}")
    print("BATCH PROCESSING COMPLETE")
    print(f"{'='*80}")
    print(f"\nSuccessfully processed: {len(results['success'])}")
    for person in results['success']:
        print(f"  ✓ {person}")
    
    if results['skipped']:
        print(f"\nSkipped (already processed): {len(results['skipped'])}")
        for person in results['skipped']:
            print(f"  - {person}")
    
    if results['no_events']:
        print(f"\nNo events found: {len(results['no_events'])}")
        for person in results['no_events']:
            print(f"  ! {person}")
    
    if results['error']:
        print(f"\nErrors: {len(results['error'])}")
        for person in results['error']:
            print(f"  ✗ {person}")

if __name__ == "__main__":
    main()