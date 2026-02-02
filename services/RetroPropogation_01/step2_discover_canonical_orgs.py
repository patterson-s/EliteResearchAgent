import json
import cohere
from pathlib import Path
from typing import Dict, Any, List

from utils import load_config, load_prompt, parse_json_response, get_api_key, save_json

def discover_canonical_orgs(entities: Dict[str, Any], config_path: Path) -> Dict[str, Any]:
    print("\n" + "="*80)
    print("STEP 2: DISCOVER CANONICAL ORGANIZATIONS")
    print("="*80)
    
    config = load_config(config_path)
    api_key = get_api_key(config)
    
    organizations = entities.get("organizations", [])
    
    if not organizations:
        print("No organizations found")
        return {
            "canonical_organizations": [],
            "raw_llm_output": ""
        }
    
    print(f"Analyzing {len(organizations)} organization entities...")
    
    script_dir = Path(__file__).parent
    system_prompt = load_prompt(script_dir / config["prompts"]["step2_discover_canonical_orgs"])
    
    orgs_text = json.dumps(organizations, indent=2, ensure_ascii=False)
    user_prompt = f"ORGANIZATIONS:\n{orgs_text}"
    
    co = cohere.Client(api_key)
    
    response = co.chat(
        model=config["model"],
        temperature=config["temperature"],
        preamble=system_prompt,
        message=user_prompt,
        max_tokens=config["max_tokens_step2"]
    )
    
    raw_output = response.text.strip()
    canonical_data = parse_json_response(raw_output)
    
    canonical_orgs = canonical_data.get("canonical_organizations", [])
    
    print(f"\nDiscovered {len(canonical_orgs)} canonical organizations:")
    for org in canonical_orgs:
        print(f"  {org['canonical_id']}: {org['canonical_name']}")
        print(f"    Type: {org['org_type']}")
        print(f"    Covers entity indices: {org['entity_indices']}")
    
    result = {
        "canonical_organizations": canonical_orgs,
        "raw_llm_output": raw_output,
        "total_org_entities": len(organizations)
    }
    
    return result

if __name__ == "__main__":
    script_dir = Path(__file__).parent
    config_path = script_dir / "config" / "config.json"
    output_dir = script_dir / "outputs" / "Gro_Harlem_Brundtland"
    
    with open(output_dir / "step1_entities.json", "r", encoding="utf-8") as f:
        step1_data = json.load(f)
    
    result = discover_canonical_orgs(step1_data["entities"], config_path)
    
    output_file = output_dir / "step2_canonical_orgs.json"
    save_json(result, output_file)
    print(f"\nSaved to {output_file}")