import json
import argparse
from pathlib import Path
from typing import List
from datetime import datetime

from load_data import load_dataset
from pipeline import run_pipeline

def run_batch(
    person_names: List[str],
    df,
    config_path: Path,
    output_dir: Path
):
    print("=" * 100)
    print(f"Birth Year Verification Batch Processing")
    print("=" * 100)
    print(f"Total people: {len(person_names)}")
    print(f"Dataset: {len(df)} chunks for {df['person_name'].nunique()} people")
    print(f"Config: {config_path}")
    print(f"Output: {output_dir}")
    print()
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    results = {
        "batch_started": datetime.utcnow().isoformat(),
        "total_people": len(person_names),
        "completed": 0,
        "failed": 0,
        "results": []
    }
    
    for i, person in enumerate(person_names, 1):
        print(f"\n[{i}/{len(person_names)}] Processing: {person}")
        print("-" * 100)
        
        try:
            run_pipeline(person, df, config_path, output_dir)
            
            results["completed"] += 1
            results["results"].append({
                "person_name": person,
                "status": "success"
            })
            
            print(f"✓ Success: {person}")
        
        except Exception as e:
            results["failed"] += 1
            results["results"].append({
                "person_name": person,
                "status": "failed",
                "error": str(e)
            })
            
            print(f"✗ Failed: {person}")
            print(f"Error: {e}")
    
    results["batch_completed"] = datetime.utcnow().isoformat()
    
    summary_file = output_dir / f"batch_summary_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    with open(summary_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print("\n" + "=" * 100)
    print("Batch Processing Complete")
    print("=" * 100)
    print(f"Completed: {results['completed']}/{results['total_people']}")
    print(f"Failed: {results['failed']}/{results['total_people']}")
    print(f"\nSummary saved to: {summary_file}")
    print("=" * 100)

def load_person_names(input_file: Path) -> List[str]:
    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    if isinstance(data, list):
        return data
    elif isinstance(data, dict) and "names" in data:
        return data["names"]
    else:
        raise ValueError("Input file must contain a list of names or a dict with 'names' key")

def main():
    parser = argparse.ArgumentParser(
        description="Batch birth year verification for multiple people"
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="JSON file with list of person names"
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/config.json"),
        help="Path to config file"
    )
    parser.add_argument(
        "--data",
        type=Path,
        default=Path("data/chunks_dataset.pkl"),
        help="Path to dataset pickle file"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("review"),
        help="Output directory for review files"
    )
    
    args = parser.parse_args()
    
    print("Loading dataset...")
    df = load_dataset(args.data)
    print(f"Loaded {len(df)} chunks\n")
    
    person_names = load_person_names(args.input_file)
    
    run_batch(person_names, df, args.config, args.output)

if __name__ == "__main__":
    main()