import json
from pathlib import Path
from typing import List, Dict, Any

from load_events import load_events_from_chunks, format_event_for_display

def load_classifications(output_dir: Path) -> Dict[str, Any]:
    with open(output_dir / "02a_career_classifications.json", "r", encoding="utf-8") as f:
        career_class = json.load(f)
    
    with open(output_dir / "02b_award_classifications.json", "r", encoding="utf-8") as f:
        award_class = json.load(f)
    
    return {
        "career": career_class["classifications"],
        "awards": award_class["classifications"]
    }

def get_none_events(
    all_events: List[Dict[str, Any]], 
    classifications: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    none_indices = [
        c["event_index"] 
        for c in classifications 
        if c["assigned_label"] == "NONE"
    ]
    
    return [e for e in all_events if e["event_index"] in none_indices]

def print_event_details(event: Dict[str, Any], index: int, total: int):
    print("\n" + "="*80)
    print(f"NONE EVENT {index}/{total}")
    print("="*80)
    
    print(format_event_for_display(event))
    
    print("\n" + "-"*80)

def main():
    script_dir = Path(__file__).parent
    data_dir = script_dir / "data" / "Abhijit_Banerjee"
    output_dir = script_dir / "outputs"
    
    print("Loading events and classifications...")
    events = load_events_from_chunks(data_dir)
    classifications = load_classifications(output_dir)
    
    career_none = get_none_events(events["career"], classifications["career"])
    award_none = get_none_events(events["awards"], classifications["awards"])
    
    print("\n" + "="*80)
    print(f"CAREER EVENTS CLASSIFIED AS NONE: {len(career_none)}")
    print("="*80)
    
    for i, event in enumerate(career_none[:10], 1):
        print_event_details(event, i, min(10, len(career_none)))
    
    if len(career_none) > 10:
        print(f"\n... and {len(career_none) - 10} more career NONE events")
    
    print("\n\n" + "="*80)
    print(f"AWARD EVENTS CLASSIFIED AS NONE: {len(award_none)}")
    print("="*80)
    
    for i, event in enumerate(award_none[:10], 1):
        print_event_details(event, i, min(10, len(award_none)))
    
    if len(award_none) > 10:
        print(f"\n... and {len(award_none) - 10} more award NONE events")
    
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"Total career NONE: {len(career_none)}")
    print(f"Total award NONE: {len(award_none)}")
    print(f"Total NONE: {len(career_none) + len(award_none)}")

if __name__ == "__main__":
    main()