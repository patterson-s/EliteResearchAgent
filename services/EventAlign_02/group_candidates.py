import json
from pathlib import Path
from typing import Dict, Any, List, Set, Tuple
from collections import defaultdict

def apply_normalization(events: List[Dict[str, Any]], normalization_map: Dict[str, Any]) -> List[Dict[str, Any]]:
    normalized_events = []
    
    for event in events:
        normalized = event.copy()
        
        canonical_orgs = []
        for org in event.get("organizations", []):
            if org in normalization_map["organizations"]:
                canonical_orgs.append(normalization_map["organizations"][org]["canonical_name"])
        normalized["canonical_organizations"] = canonical_orgs
        
        canonical_roles = []
        for role in event.get("roles", []):
            if role in normalization_map["roles"]:
                canonical_roles.append(normalization_map["roles"][role]["canonical_name"])
        normalized["canonical_roles"] = canonical_roles
        
        normalized_events.append(normalized)
    
    return normalized_events

def events_overlap_time(e1: Dict[str, Any], e2: Dict[str, Any]) -> bool:
    s1, end1 = e1.get("start_year"), e1.get("end_year")
    s2, end2 = e2.get("start_year"), e2.get("end_year")
    
    if s1 is None or s2 is None:
        return False
    
    e1_end = end1 if end1 is not None else s1
    e2_end = end2 if end2 is not None else s2
    
    return not (e1_end < s2 or e2_end < s1)

def same_canonical_entity(list1: List[str], list2: List[str]) -> bool:
    if not list1 or not list2:
        return False
    return bool(set(list1) & set(list2))

def group_candidates(events: List[Dict[str, Any]], output_dir: Path, config_path: Path = None) -> List[Dict[str, Any]]:
    print("\n" + "="*80)
    print("PHASE 2: CANDIDATE GROUPING")
    print("="*80)
    
    groups = []
    grouped_indices = set()
    
    for i, event_i in enumerate(events):
        if i in grouped_indices:
            continue
        
        group = {
            "group_id": f"G{i+1:03d}",
            "event_indices": [i],
            "events": [event_i],
            "grouping_criteria": []
        }
        
        for j, event_j in enumerate(events[i+1:], i+1):
            if j in grouped_indices:
                continue
            
            criteria_met = []
            
            temporal_overlap = events_overlap_time(event_i, event_j)
            same_org = same_canonical_entity(
                event_i.get("canonical_organizations", []),
                event_j.get("canonical_organizations", [])
            )
            same_role = same_canonical_entity(
                event_i.get("canonical_roles", []),
                event_j.get("canonical_roles", [])
            )
            
            if temporal_overlap and same_org and same_role:
                criteria_met = ["temporal_overlap", "same_org", "same_role"]
            elif temporal_overlap and same_org:
                criteria_met = ["temporal_overlap", "same_org"]
            elif temporal_overlap and same_role:
                criteria_met = ["temporal_overlap", "same_role"]
            elif same_org and same_role and (event_i.get("start_year") is None or event_j.get("start_year") is None):
                criteria_met = ["same_org", "same_role", "missing_time"]
            
            if criteria_met:
                group["event_indices"].append(j)
                group["events"].append(event_j)
                group["grouping_criteria"].append({
                    "event_pair": [i, j],
                    "criteria": criteria_met
                })
                grouped_indices.add(j)
        
        if len(group["event_indices"]) > 1:
            grouped_indices.add(i)
            groups.append(group)
    
    singleton_events = [
        {
            "group_id": f"S{i+1:03d}",
            "event_indices": [i],
            "events": [events[i]],
            "grouping_criteria": []
        }
        for i in range(len(events)) if i not in grouped_indices
    ]
    
    print(f"\nFound {len(groups)} multi-event groups")
    print(f"Found {len(singleton_events)} singleton events")
    
    for group in groups:
        print(f"\n{group['group_id']}: {len(group['event_indices'])} events")
        
        years = set()
        orgs = set()
        roles = set()
        
        for event in group["events"]:
            if event.get("start_year"):
                years.add(event["start_year"])
            if event.get("end_year"):
                years.add(event["end_year"])
            orgs.update(event.get("canonical_organizations", []))
            roles.update(event.get("canonical_roles", []))
        
        print(f"  Years: {sorted(years) if years else 'None'}")
        print(f"  Orgs: {', '.join(orgs) if orgs else 'None'}")
        print(f"  Roles: {', '.join(roles) if roles else 'None'}")
    
    all_groups = groups + singleton_events
    
    output_file = output_dir / "02_candidate_groups.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_groups, f, indent=2, ensure_ascii=False)
    
    print(f"\nGrouping complete. Saved to {output_file}")
    print(f"Total groups: {len(all_groups)} ({len(groups)} multi-event, {len(singleton_events)} singleton)")
    
    return all_groups

if __name__ == "__main__":
    script_dir = Path(__file__).parent
    output_dir = script_dir / "outputs"
    
    with open(output_dir / "all_resolved_events.json", "r", encoding="utf-8") as f:
        events = json.load(f)
    
    with open(output_dir / "01_normalized_entities.json", "r", encoding="utf-8") as f:
        normalization_map = json.load(f)
    
    normalized_events = apply_normalization(events, normalization_map)
    
    with open(output_dir / "all_normalized_events.json", "w", encoding="utf-8") as f:
        json.dump(normalized_events, f, indent=2, ensure_ascii=False)
    
    group_candidates(normalized_events, output_dir)