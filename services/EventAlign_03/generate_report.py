import json
from pathlib import Path
from typing import Dict, Any, List
from collections import defaultdict

from load_events import load_events_from_chunks

def generate_report(output_dir: Path, data_dir: Path) -> Dict[str, Any]:
    print("\n" + "="*80)
    print("GENERATING CORE DISCOVERY REPORT")
    print("="*80)
    
    with open(output_dir / "01a_career_labels.json", "r", encoding="utf-8") as f:
        career_labels_data = json.load(f)
    
    with open(output_dir / "01b_award_labels.json", "r", encoding="utf-8") as f:
        award_labels_data = json.load(f)
    
    with open(output_dir / "02a_career_classifications.json", "r", encoding="utf-8") as f:
        career_class_data = json.load(f)
    
    with open(output_dir / "02b_award_classifications.json", "r", encoding="utf-8") as f:
        award_class_data = json.load(f)
    
    events = load_events_from_chunks(data_dir)
    
    career_labels = career_labels_data["career_labels"]
    award_labels = award_labels_data["award_labels"]
    
    career_cores = {}
    for label in career_labels:
        label_id = label["label_id"]
        assigned_events = [
            c for c in career_class_data["classifications"]
            if c["assigned_label"] == label_id
        ]
        
        event_indices = [c["event_index"] for c in assigned_events]
        full_events = [e for e in events["career"] if e["event_index"] in event_indices]
        
        career_cores[label_id] = {
            "label": label,
            "assigned_count": len(assigned_events),
            "event_indices": event_indices,
            "events": full_events
        }
    
    award_cores = {}
    for label in award_labels:
        label_id = label["label_id"]
        assigned_events = [
            c for c in award_class_data["classifications"]
            if c["assigned_label"] == label_id
        ]
        
        event_indices = [c["event_index"] for c in assigned_events]
        full_events = [e for e in events["awards"] if e["event_index"] in event_indices]
        
        award_cores[label_id] = {
            "label": label,
            "assigned_count": len(assigned_events),
            "event_indices": event_indices,
            "events": full_events
        }
    
    total_events = len(events["all"])
    career_assigned = career_class_data["summary"]["assigned"]
    award_assigned = award_class_data["summary"]["assigned"]
    total_assigned = career_assigned + award_assigned
    
    coverage_percent = (total_assigned / total_events) * 100
    
    report = {
        "summary": {
            "total_events": total_events,
            "career_events": len(events["career"]),
            "award_events": len(events["awards"]),
            "total_assigned_to_cores": total_assigned,
            "coverage_percent": coverage_percent,
            "career_cores_count": len(career_labels),
            "award_cores_count": len(award_labels),
            "career_none_count": career_class_data["summary"]["none"],
            "award_none_count": award_class_data["summary"]["none"]
        },
        "career_cores": career_cores,
        "award_cores": award_cores
    }
    
    output_file = output_dir / "03_cores_report.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    print(f"\n{'='*80}")
    print("CORE DISCOVERY SUMMARY")
    print(f"{'='*80}")
    
    print(f"\nTotal Events: {total_events}")
    print(f"  Career: {len(events['career'])}")
    print(f"  Awards: {len(events['awards'])}")
    
    print(f"\nCore Labels Discovered:")
    print(f"  Career cores: {len(career_labels)}")
    print(f"  Award cores: {len(award_labels)}")
    
    print(f"\nCoverage:")
    print(f"  Assigned to cores: {total_assigned} ({coverage_percent:.1f}%)")
    print(f"  Career NONE: {career_class_data['summary']['none']}")
    print(f"  Award NONE: {award_class_data['summary']['none']}")
    
    print(f"\nCareer Cores:")
    for label_id, core in sorted(career_cores.items()):
        print(f"  {label_id}: {core['assigned_count']} events")
        print(f"    {core['label']['label']}")
    
    print(f"\nAward Cores:")
    for label_id, core in sorted(award_cores.items()):
        print(f"  {label_id}: {core['assigned_count']} events")
        print(f"    {core['label']['label']}")
    
    print(f"\nReport saved to {output_file}")
    
    return report

if __name__ == "__main__":
    script_dir = Path(__file__).parent
    output_dir = script_dir / "outputs"
    data_dir = script_dir / "data" / "Abhijit_Banerjee"
    
    generate_report(output_dir, data_dir)