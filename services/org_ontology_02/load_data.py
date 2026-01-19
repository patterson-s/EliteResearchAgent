import json
from collections import Counter
from typing import Dict, List, Tuple

def load_config() -> dict:
    with open('config.json', 'r') as f:
        return json.load(f)

def extract_organizations_from_file(file_content: str) -> Tuple[Counter, Dict[str, List[dict]], str]:
    org_counter = Counter()
    org_examples = {}
    
    try:
        data = json.loads(file_content)
        
        person_name = data.get('person_name', 'Unknown')
        raw_extractions = data.get('raw_extractions', [])
        
        for event in raw_extractions:
            org = event.get('organization')
            
            if org and org.strip():
                org_clean = org.strip()
                org_counter[org_clean] += 1
                
                if org_clean not in org_examples:
                    org_examples[org_clean] = []
                
                org_examples[org_clean].append({
                    'person': person_name,
                    'role': event.get('role'),
                    'location': event.get('location'),
                    'dates': f"{event.get('start_date', '')} - {event.get('end_date', '')}"
                })
        
        return org_counter, org_examples, person_name
    
    except Exception as e:
        print(f"Error processing file: {e}")
        return Counter(), {}, "Unknown"

def get_organization_data(file_content: str) -> Tuple[List[Tuple[str, int]], Dict[str, List[dict]], str]:
    org_counter, org_examples, person_name = extract_organizations_from_file(file_content)
    org_list = sorted(org_counter.items(), key=lambda x: x[1], reverse=True)
    
    return org_list, org_examples, person_name