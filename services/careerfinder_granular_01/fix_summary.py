import json
from pathlib import Path

def update_summary_with_valid_counts(person_dir: Path):
    summary_file = person_dir / "summary.json"
    
    if not summary_file.exists():
        print(f"No summary found in {person_dir}")
        return
    
    with open(summary_file, "r", encoding="utf-8") as f:
        summary = json.load(f)
    
    total_valid = 0
    
    for chunk_info in summary["chunks"]:
        chunk_id = chunk_info["chunk_id"]
        chunk_file = person_dir / f"chunk_{chunk_id}_results.json"
        
        if chunk_file.exists():
            with open(chunk_file, "r", encoding="utf-8") as f:
                chunk_data = json.load(f)
            
            if chunk_data["status"] == "success":
                valid_count = chunk_data["step3"]["summary"].get("valid", 0)
                chunk_info["valid_count"] = valid_count
                total_valid += valid_count
            else:
                chunk_info["valid_count"] = 0
        else:
            chunk_info["valid_count"] = 0
    
    summary["total_valid_events"] = total_valid
    
    with open(summary_file, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    
    print(f"Updated {summary_file}")
    print(f"Total events: {summary['total_events']}")
    print(f"Total valid events: {total_valid}")

if __name__ == "__main__":
    review_dir = Path(__file__).parent / "review"
    
    for person_dir in review_dir.iterdir():
        if person_dir.is_dir():
            print(f"\nProcessing {person_dir.name}...")
            update_summary_with_valid_counts(person_dir)