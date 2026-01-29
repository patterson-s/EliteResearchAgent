import json
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
from collections import defaultdict
import re

def load_all_events(data_dir: Path) -> List[Dict[str, Any]]:
    events = []
    
    for chunk_file in data_dir.glob("chunk_*_results.json"):
        with open(chunk_file, "r", encoding="utf-8") as f:
            chunk_data = json.load(f)
        
        if chunk_data["status"] != "success":
            continue
        
        verified_events = chunk_data["step3"]["verified_events"]
        assembled_events = chunk_data["step2"]["assembled_events"]
        entities = chunk_data["step1"]["entities"]
        
        for i, verified in enumerate(verified_events):
            if verified.get("status") == "valid" and i < len(assembled_events):
                event = assembled_events[i].copy()
                event["chunk_id"] = chunk_data["chunk_id"]
                event["source_url"] = chunk_data["source_url"]
                event["entities"] = entities
                events.append(event)
    
    return events

def parse_time_marker(text: str, marker_type: str) -> Tuple[Optional[int], Optional[int]]:
    text = text.strip()
    
    year_pattern = r'\b(19\d{2}|20\d{2})\b'
    years = re.findall(year_pattern, text)
    
    if not years:
        return None, None
    
    years = [int(y) for y in years]
    
    if marker_type == "range":
        if len(years) >= 2:
            return min(years), max(years)
        elif len(years) == 1:
            return years[0], years[0]
    elif marker_type == "point":
        return years[0], years[0]
    elif marker_type == "open":
        if len(years) >= 1:
            return years[0], None
    
    return None, None

def resolve_event_details(event: Dict[str, Any]) -> Dict[str, Any]:
    entities = event.get("entities", {})
    
    time_ids = event.get("time_marker_ids", [])
    org_ids = event.get("organization_ids", [])
    role_ids = event.get("role_ids", [])
    loc_ids = event.get("location_ids", [])
    
    time_markers = [
        entities["time_markers"][i] 
        for i in time_ids if i < len(entities.get("time_markers", []))
    ]
    
    organizations = [
        entities["organizations"][i].get("name", "") 
        for i in org_ids if i < len(entities.get("organizations", []))
    ]
    
    roles = [
        entities["roles"][i].get("title", "") 
        for i in role_ids if i < len(entities.get("roles", []))
    ]
    
    locations = [
        entities["locations"][i].get("place", "") 
        for i in loc_ids if i < len(entities.get("locations", []))
    ]
    
    start_year, end_year = None, None
    if time_markers:
        for tm in time_markers:
            s, e = parse_time_marker(tm.get("text", ""), tm.get("type", ""))
            if s is not None:
                start_year = s if start_year is None else min(start_year, s)
            if e is not None:
                end_year = e if end_year is None else max(end_year, e)
    
    return {
        "event_type": event.get("event_type"),
        "start_year": start_year,
        "end_year": end_year,
        "organizations": organizations,
        "roles": roles,
        "locations": locations,
        "chunk_id": event.get("chunk_id"),
        "source_url": event.get("source_url"),
        "time_markers_raw": [tm.get("text") for tm in time_markers]
    }

def events_overlap(e1: Dict[str, Any], e2: Dict[str, Any]) -> bool:
    s1, e1_year = e1["start_year"], e1["end_year"]
    s2, e2_year = e2["start_year"], e2["end_year"]
    
    if s1 is None or s2 is None:
        return False
    
    end1 = e1_year if e1_year is not None else s1
    end2 = e2_year if e2_year is not None else s2
    
    return not (end1 < s2 or end2 < s1)

def org_role_similarity(e1: Dict[str, Any], e2: Dict[str, Any]) -> Dict[str, Any]:
    orgs1 = set(e1["organizations"])
    orgs2 = set(e2["organizations"])
    roles1 = set(e1["roles"])
    roles2 = set(e2["roles"])
    
    org_overlap = orgs1 & orgs2
    role_overlap = roles1 & roles2
    
    return {
        "same_org": len(org_overlap) > 0,
        "same_role": len(role_overlap) > 0,
        "org_overlap": list(org_overlap),
        "role_overlap": list(role_overlap)
    }

def find_temporal_clusters(events: List[Dict[str, Any]]) -> List[List[int]]:
    events_with_time = [
        (i, e) for i, e in enumerate(events) 
        if e["start_year"] is not None
    ]
    
    clusters = []
    visited = set()
    
    for i, e1 in events_with_time:
        if i in visited:
            continue
        
        cluster = [i]
        visited.add(i)
        
        for j, e2 in events_with_time:
            if j in visited:
                continue
            
            if events_overlap(e1, e2):
                cluster.append(j)
                visited.add(j)
        
        if len(cluster) > 1:
            clusters.append(cluster)
    
    return clusters

def analyze_timeline(data_dir: Path, output_dir: Path):
    print("Loading events...")
    events = load_all_events(data_dir)
    print(f"Loaded {len(events)} valid events")
    
    print("\nResolving event details...")
    resolved_events = [resolve_event_details(e) for e in events]
    
    with_time = [e for e in resolved_events if e["start_year"] is not None]
    without_time = [e for e in resolved_events if e["start_year"] is None]
    
    with_range = [e for e in with_time if e["end_year"] is not None]
    with_point = [e for e in with_time if e["end_year"] is None]
    
    print("\n" + "="*80)
    print("TIMELINE COVERAGE")
    print("="*80)
    print(f"Total valid events: {len(resolved_events)}")
    print(f"With time markers: {len(with_time)} ({len(with_time)/len(resolved_events)*100:.1f}%)")
    print(f"  - With range (start+end): {len(with_range)}")
    print(f"  - With point only: {len(with_point)}")
    print(f"Without time markers: {len(without_time)} ({len(without_time)/len(resolved_events)*100:.1f}%)")
    
    if with_time:
        years = []
        for e in with_time:
            if e["start_year"]:
                years.append(e["start_year"])
            if e["end_year"]:
                years.append(e["end_year"])
        print(f"\nYear range: {min(years)} - {max(years)}")
    
    coverage_stats = {
        "total_events": len(resolved_events),
        "with_time": len(with_time),
        "without_time": len(without_time),
        "with_range": len(with_range),
        "with_point": len(with_point),
        "year_range": [min(years), max(years)] if with_time and years else None
    }
    
    print("\n" + "="*80)
    print("TEMPORAL CLUSTERS (overlapping events)")
    print("="*80)
    
    clusters = find_temporal_clusters(resolved_events)
    print(f"Found {len(clusters)} clusters of overlapping events")
    
    cluster_details = []
    
    for cluster_idx, cluster in enumerate(clusters):
        cluster_events = [resolved_events[i] for i in cluster]
        
        print(f"\n--- Cluster {cluster_idx + 1} ({len(cluster)} events) ---")
        
        years = set()
        for e in cluster_events:
            if e["start_year"]:
                years.add(e["start_year"])
            if e["end_year"]:
                years.add(e["end_year"])
        print(f"Year span: {min(years) if years else 'N/A'} - {max(years) if years else 'N/A'}")
        
        all_orgs = set()
        all_roles = set()
        for e in cluster_events:
            all_orgs.update(e["organizations"])
            all_roles.update(e["roles"])
        
        print(f"Organizations in cluster: {', '.join(all_orgs) if all_orgs else 'None'}")
        print(f"Roles in cluster: {', '.join(all_roles) if all_roles else 'None'}")
        
        similarities = []
        for i in range(len(cluster_events)):
            for j in range(i+1, len(cluster_events)):
                sim = org_role_similarity(cluster_events[i], cluster_events[j])
                if sim["same_org"] or sim["same_role"]:
                    similarities.append({
                        "event1_idx": cluster[i],
                        "event2_idx": cluster[j],
                        "similarity": sim
                    })
                    if sim["same_org"] and sim["same_role"]:
                        print(f"  POTENTIAL DUPLICATE: Events {cluster[i]} and {cluster[j]}")
                        print(f"    Same org: {sim['org_overlap']}")
                        print(f"    Same role: {sim['role_overlap']}")
        
        cluster_details.append({
            "cluster_id": cluster_idx,
            "event_indices": cluster,
            "size": len(cluster),
            "year_span": [min(years), max(years)] if years else None,
            "organizations": list(all_orgs),
            "roles": list(all_roles),
            "potential_duplicates": similarities
        })
    
    print("\n" + "="*80)
    print("EVENTS WITHOUT TIME - Potential Matches")
    print("="*80)
    
    unmapped_matches = []
    
    for i, no_time_event in enumerate(without_time):
        matches = []
        for j, with_time_event in enumerate(with_time):
            sim = org_role_similarity(no_time_event, with_time_event)
            if sim["same_org"] or sim["same_role"]:
                matches.append({
                    "with_time_event_idx": j,
                    "similarity": sim,
                    "years": f"{with_time_event['start_year']}-{with_time_event['end_year']}"
                })
        
        if matches:
            print(f"\nEvent without time (index {i}):")
            print(f"  Orgs: {', '.join(no_time_event['organizations'])}")
            print(f"  Roles: {', '.join(no_time_event['roles'])}")
            print(f"  Potential matches: {len(matches)}")
            for m in matches[:3]:
                print(f"    - Event {m['with_time_event_idx']} ({m['years']})")
        
        unmapped_matches.append({
            "no_time_event_idx": i,
            "event": no_time_event,
            "matches": matches
        })
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    with open(output_dir / "timeline_coverage.json", "w", encoding="utf-8") as f:
        json.dump(coverage_stats, f, indent=2, ensure_ascii=False)
    
    with open(output_dir / "temporal_clusters.json", "w", encoding="utf-8") as f:
        json.dump(cluster_details, f, indent=2, ensure_ascii=False)
    
    with open(output_dir / "unmapped_events.json", "w", encoding="utf-8") as f:
        json.dump(unmapped_matches, f, indent=2, ensure_ascii=False)
    
    with open(output_dir / "all_resolved_events.json", "w", encoding="utf-8") as f:
        json.dump(resolved_events, f, indent=2, ensure_ascii=False)
    
    print("\n" + "="*80)
    print(f"Analysis complete. Results saved to {output_dir}")
    print("="*80)

if __name__ == "__main__":
    script_dir = Path(__file__).parent
    data_dir = script_dir / "data" / "Abhijit_Banerjee"
    output_dir = script_dir / "outputs"
    
    analyze_timeline(data_dir, output_dir)