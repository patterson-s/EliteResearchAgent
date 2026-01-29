import json
from pathlib import Path
from typing import Dict, Any, List

def load_events_from_chunks(data_dir: Path) -> Dict[str, List[Dict[str, Any]]]:
    all_events = []
    
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
                event["event_index"] = len(all_events)
                all_events.append(event)
    
    career_events = [e for e in all_events if e.get("event_type") == "career_position"]
    award_events = [e for e in all_events if e.get("event_type") == "award"]
    
    return {
        "all": all_events,
        "career": career_events,
        "awards": award_events
    }

def format_event_for_display(event: Dict[str, Any]) -> str:
    entities = event.get("entities", {})
    
    time_ids = event.get("time_marker_ids", [])
    org_ids = event.get("organization_ids", [])
    role_ids = event.get("role_ids", [])
    
    time_markers = [
        entities["time_markers"][i].get("text", "") 
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
    
    lines = []
    lines.append(f"Event {event['event_index']}")
    lines.append(f"  Type: {event.get('event_type', 'N/A')}")
    
    if time_markers:
        lines.append(f"  Time: {', '.join(time_markers)}")
    
    if organizations:
        lines.append(f"  Organizations: {', '.join(organizations)}")
    
    if roles:
        lines.append(f"  Roles: {', '.join(roles)}")
    
    lines.append(f"  Source: chunk {event.get('chunk_id')}")
    
    return "\n".join(lines)

if __name__ == "__main__":
    script_dir = Path(__file__).parent
    data_dir = script_dir / "data" / "Abhijit_Banerjee"
    
    events = load_events_from_chunks(data_dir)
    
    print(f"Loaded {len(events['all'])} total events")
    print(f"  Career positions: {len(events['career'])}")
    print(f"  Awards: {len(events['awards'])}")