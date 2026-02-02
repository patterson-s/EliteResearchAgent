import json
from pathlib import Path
from typing import Dict, Any, List
import statistics

def load_all_reports(outputs_dir: Path) -> Dict[str, Dict[str, Any]]:
    reports = {}
    
    for person_dir in outputs_dir.iterdir():
        if not person_dir.is_dir():
            continue
        
        report_file = person_dir / "03_cores_report.json"
        if not report_file.exists():
            continue
        
        with open(report_file, "r", encoding="utf-8") as f:
            reports[person_dir.name] = json.load(f)
    
    return reports

def analyze_coverage(reports: Dict[str, Dict[str, Any]]) -> None:
    print("="*80)
    print("EVENTALIGN_03 COVERAGE ANALYSIS")
    print("="*80)
    
    coverages = []
    total_events = []
    career_cores_counts = []
    award_cores_counts = []
    career_none_counts = []
    award_none_counts = []
    
    for person, report in reports.items():
        summary = report["summary"]
        coverages.append(summary["coverage_percent"])
        total_events.append(summary["total_events"])
        career_cores_counts.append(summary["career_cores_count"])
        award_cores_counts.append(summary["award_cores_count"])
        career_none_counts.append(summary["career_none_count"])
        award_none_counts.append(summary["award_none_count"])
    
    print(f"\nTotal actors processed: {len(reports)}")
    
    print("\n" + "-"*80)
    print("COVERAGE STATISTICS")
    print("-"*80)
    print(f"Mean coverage: {statistics.mean(coverages):.1f}%")
    print(f"Median coverage: {statistics.median(coverages):.1f}%")
    print(f"Std deviation: {statistics.stdev(coverages):.1f}%")
    print(f"Min coverage: {min(coverages):.1f}%")
    print(f"Max coverage: {max(coverages):.1f}%")
    
    print("\n" + "-"*80)
    print("EVENT COUNTS")
    print("-"*80)
    print(f"Mean total events: {statistics.mean(total_events):.1f}")
    print(f"Median total events: {statistics.median(total_events):.1f}")
    print(f"Total events (all actors): {sum(total_events)}")
    
    print("\n" + "-"*80)
    print("CORES DISCOVERED")
    print("-"*80)
    print(f"Mean career cores: {statistics.mean(career_cores_counts):.1f}")
    print(f"Mean award cores: {statistics.mean(award_cores_counts):.1f}")
    print(f"Total career cores: {sum(career_cores_counts)}")
    print(f"Total award cores: {sum(award_cores_counts)}")
    
    print("\n" + "-"*80)
    print("NONE ASSIGNMENTS")
    print("-"*80)
    print(f"Mean career NONE: {statistics.mean(career_none_counts):.1f}")
    print(f"Mean award NONE: {statistics.mean(award_none_counts):.1f}")
    print(f"Total career NONE: {sum(career_none_counts)}")
    print(f"Total award NONE: {sum(award_none_counts)}")
    
    print("\n" + "-"*80)
    print("COVERAGE DISTRIBUTION")
    print("-"*80)
    
    bins = [(0, 50), (50, 70), (70, 85), (85, 95), (95, 100)]
    for low, high in bins:
        count = sum(1 for c in coverages if low <= c < high)
        print(f"{low}-{high}%: {count} actors ({count/len(coverages)*100:.1f}%)")
    count_100 = sum(1 for c in coverages if c == 100)
    print(f"100%: {count_100} actors ({count_100/len(coverages)*100:.1f}%)")
    
    print("\n" + "-"*80)
    print("LOWEST COVERAGE ACTORS")
    print("-"*80)
    
    sorted_actors = sorted(reports.items(), key=lambda x: x[1]["summary"]["coverage_percent"])
    
    for person, report in sorted_actors[:10]:
        s = report["summary"]
        print(f"{person}: {s['coverage_percent']:.1f}% ({s['total_assigned_to_cores']}/{s['total_events']} events)")
        print(f"  Career cores: {s['career_cores_count']}, Award cores: {s['award_cores_count']}")
        print(f"  NONE: Career {s['career_none_count']}, Award {s['award_none_count']}")
    
    print("\n" + "-"*80)
    print("HIGHEST COVERAGE ACTORS")
    print("-"*80)
    
    for person, report in sorted_actors[-10:]:
        s = report["summary"]
        print(f"{person}: {s['coverage_percent']:.1f}% ({s['total_assigned_to_cores']}/{s['total_events']} events)")
        print(f"  Career cores: {s['career_cores_count']}, Award cores: {s['award_cores_count']}")

if __name__ == "__main__":
    script_dir = Path(__file__).parent
    outputs_dir = script_dir / "outputs"
    
    reports = load_all_reports(outputs_dir)
    
    if not reports:
        print("No reports found in outputs directory")
    else:
        analyze_coverage(reports)