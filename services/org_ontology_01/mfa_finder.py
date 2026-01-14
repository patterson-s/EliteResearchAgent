import json
from collections import defaultdict

def is_mfa_org(org_name):
    mfa_patterns = [
        'ministry of foreign affairs',
        'foreign ministry',
        'foreign service',
        'embassy',
        'permanent mission',
        'consulate',
        'diplomatic',
        'ambassador'
    ]
    org_lower = org_name.lower()
    return any(pattern in org_lower for pattern in mfa_patterns)

def extract_mfa_orgs(jsonl_path):
    mfa_orgs = defaultdict(lambda: {'count': 0, 'people': set(), 'roles': set()})
    
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line in f:
            person_data = json.loads(line)
            person_name = person_data.get('person_name', 'Unknown')
            
            for event in person_data.get('career_events', []):
                org = event.get('organization', '')
                if org and is_mfa_org(org):
                    mfa_orgs[org]['count'] += 1
                    mfa_orgs[org]['people'].add(person_name)
                    mfa_orgs[org]['roles'].add(event.get('role', 'Unknown'))
    
    sorted_orgs = sorted(mfa_orgs.items(), key=lambda x: x[1]['count'], reverse=True)
    
    print(f"Found {len(sorted_orgs)} MFA-related organizations\n")
    print("="*100)
    
    for org, data in sorted_orgs:
        print(f"\n{org}")
        print(f"  Occurrences: {data['count']}")
        print(f"  People: {len(data['people'])}")
        print(f"  Sample roles: {', '.join(list(data['roles'])[:3])}")

if __name__ == '__main__':
    extract_mfa_orgs('data/careerfinder_results.jsonl')