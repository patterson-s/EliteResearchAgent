import json
from pathlib import Path
from typing import Dict, Any, List, Optional

def sort_key(event: Dict[str, Any]) -> tuple:
    rep = event.get("canonical_representation")
    
    if rep is None:
        return (9999, 0)
    
    start = rep.get("start_year")
    end = rep.get("end_year")
    
    if start is None:
        return (9999, 0)
    
    return (start, end if end is not None else 9999)

def format_timeline_entry(event: Dict[str, Any], index: int) -> Dict[str, Any]:
    rep = event.get("canonical_representation") or {}
    details = event.get("accumulated_details") or {}
    prov = event.get("provenance") or {}
    
    time_str = ""
    if rep.get("start_year"):
        if rep.get("end_year"):
            time_str = f"{rep['start_year']}-{rep['end_year']}"
        else:
            time_str = f"{rep['start_year']}-present"
    else:
        time_str = "Unknown"
    
    entry = {
        "sequence_number": index,
        "event_id": event.get("consolidated_event_id"),
        "time_period": time_str,
        "start_year": rep.get("start_year"),
        "end_year": rep.get("end_year"),
        "event_type": rep.get("event_type"),
        "organization": rep.get("organization"),
        "role": rep.get("role"),
        "locations": rep.get("locations", []),
        "additional_details": rep.get("additional_details", []),
        "consolidation_status": event.get("decision"),
        "variants": {
            "organizations": details.get("organization_variants", []),
            "roles": details.get("role_variants", []),
            "locations": details.get("location_variants", [])
        },
        "sources": {
            "chunk_count": prov.get("source_event_count", 0),
            "chunk_ids": prov.get("source_chunks", []),
            "urls": prov.get("source_urls", [])
        },
        "confidence": event.get("confidence", {})
    }
    
    return entry

def generate_summary_stats(timeline: List[Dict[str, Any]]) -> Dict[str, Any]:
    total = len(timeline)
    
    with_time = sum(1 for e in timeline if e.get("start_year") is not None)
    without_time = total - with_time
    
    career_positions = sum(1 for e in timeline if e.get("event_type") == "career_position")
    awards = sum(1 for e in timeline if e.get("event_type") == "award")
    
    merged = sum(1 for e in timeline if e.get("consolidation_status") == "same_event")
    singletons = sum(1 for e in timeline if e.get("consolidation_status") == "singleton")
    
    years = [e["start_year"] for e in timeline if e.get("start_year")]
    year_range = [min(years), max(years)] if years else None
    
    return {
        "total_events": total,
        "with_time": with_time,
        "without_time": without_time,
        "career_positions": career_positions,
        "awards": awards,
        "merged_events": merged,
        "singleton_events": singletons,
        "year_range": year_range
    }

def build_timeline(consolidated_events: List[Dict[str, Any]], output_dir: Path) -> Dict[str, Any]:
    print("\n" + "="*80)
    print("PHASE 4: TIMELINE CONSTRUCTION")
    print("="*80)
    
    sorted_events = sorted(consolidated_events, key=sort_key)
    
    timeline = []
    for i, event in enumerate(sorted_events, 1):
        timeline.append(format_timeline_entry(event, i))
    
    summary = generate_summary_stats(timeline)
    
    output = {
        "person_name": "Abhijit Banerjee",
        "timeline_summary": summary,
        "timeline": timeline
    }
    
    output_file = output_dir / "04_final_timeline.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"\nTimeline construction complete. Saved to {output_file}")
    print("\nSUMMARY:")
    print(f"  Total events in timeline: {summary['total_events']}")
    print(f"  Events with dates: {summary['with_time']}")
    print(f"  Events without dates: {summary['without_time']}")
    print(f"  Career positions: {summary['career_positions']}")
    print(f"  Awards: {summary['awards']}")
    print(f"  Merged from duplicates: {summary['merged_events']}")
    print(f"  Year range: {summary['year_range']}")
    
    print("\nFIRST 10 TIMELINE ENTRIES:")
    for entry in timeline[:10]:
        role = entry.get('role') or 'N/A'
        org = entry.get('organization') or 'N/A'
        time_period = entry.get('time_period', 'Unknown')
        seq = entry.get('sequence_number', 0)
        print(f"  {seq}. [{time_period}] {role} at {org}")
    
    return output

if __name__ == "__main__":
    script_dir = Path(__file__).parent
    output_dir = script_dir / "outputs"
    
    with open(output_dir / "03_consolidated_events.json", "r", encoding="utf-8") as f:
        consolidated_events = json.load(f)
    
    build_timeline(consolidated_events, output_dir)