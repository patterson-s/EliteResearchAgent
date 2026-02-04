import json
from pathlib import Path
from typing import Dict, Any, List

def load_existing_state(person_name: str, retroprop01_dir: Path) -> Dict[str, Any]:
    person_dir = retroprop01_dir / person_name.replace(" ", "_")
    
    if not person_dir.exists():
        raise FileNotFoundError(f"No RetroPropogation_01 output found for {person_name}")
    
    with open(person_dir / "step3_events.json", "r", encoding="utf-8") as f:
        step3 = json.load(f)
    
    with open(person_dir / "step2_canonical_orgs.json", "r", encoding="utf-8") as f:
        step2 = json.load(f)
    
    events = step3["events"]
    canonical_orgs = step2["canonical_organizations"]
    
    for event in events:
        if "supporting_evidence" not in event:
            event["supporting_evidence"] = []
            if "supporting_quotes" in event:
                for quote in event["supporting_quotes"]:
                    event["supporting_evidence"].append({
                        "quote": quote,
                        "source": "wikipedia",
                        "contribution": "original"
                    })
    
    return {
        "events": events,
        "canonical_orgs": canonical_orgs,
        "person_name": person_name
    }

if __name__ == "__main__":
    script_dir = Path(__file__).parent
    retroprop01_dir = script_dir.parent / "RetroPropogation_01" / "outputs"
    
    person_name = "Amre Moussa"
    state = load_existing_state(person_name, retroprop01_dir)
    
    print(f"Loaded state for {person_name}")
    print(f"Events: {len(state['events'])}")
    print(f"Canonical orgs: {len(state['canonical_orgs'])}")
