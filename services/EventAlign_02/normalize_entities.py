import json
import os
from pathlib import Path
from typing import Dict, Any, List, Set
from collections import defaultdict
from fuzzywuzzy import fuzz
import cohere
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent.parent / ".env")

def load_config(config_path: Path) -> Dict[str, Any]:
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

def load_prompt(prompt_path: Path) -> str:
    with open(prompt_path, "r", encoding="utf-8") as f:
        return f.read()

def extract_unique_entities(events: List[Dict[str, Any]]) -> Dict[str, Set[str]]:
    orgs = set()
    roles = set()
    
    for event in events:
        orgs.update(event.get("organizations", []))
        roles.update(event.get("roles", []))
    
    return {
        "organizations": orgs,
        "roles": roles
    }

def fuzzy_cluster_entities(entities: Set[str], threshold: int = 85) -> List[List[str]]:
    entities_list = sorted(list(entities))
    clusters = []
    used = set()
    
    for i, entity in enumerate(entities_list):
        if entity in used:
            continue
        
        cluster = [entity]
        used.add(entity)
        
        for j, other in enumerate(entities_list[i+1:], i+1):
            if other in used:
                continue
            
            ratio = fuzz.ratio(entity.lower(), other.lower())
            if ratio >= threshold:
                cluster.append(other)
                used.add(other)
        
        clusters.append(cluster)
    
    return clusters

def llm_decide_canonical(
    cluster: List[str], 
    entity_type: str, 
    api_key: str,
    prompt_template: str,
    model: str,
    temperature: float,
    max_tokens: int
) -> Dict[str, Any]:
    if len(cluster) == 1:
        return {
            "canonical_name": cluster[0],
            "variants": cluster,
            "confidence": "high",
            "reasoning": "single variant"
        }
    
    co = cohere.Client(api_key)
    
    variants_list = '\n'.join(f'{i+1}. {v}' for i, v in enumerate(cluster))
    
    prompt = prompt_template.format(
        entity_type=entity_type,
        variants_list=variants_list
    )
    
    response = co.chat(
        model=model,
        temperature=temperature,
        message=prompt,
        max_tokens=max_tokens
    )
    
    try:
        text = response.text.strip()
        text = text.replace("```json", "").replace("```", "").strip()
        result = json.loads(text)
        
        if not result.get("same_entity", False):
            return {
                "canonical_name": cluster[0],
                "variants": [cluster[0]],
                "confidence": "low",
                "reasoning": "LLM determined not same entity"
            }
        return {
            "canonical_name": result.get("canonical_name", cluster[0]),
            "variants": result.get("variants", cluster),
            "confidence": "high",
            "reasoning": result.get("reasoning", "")
        }
    except json.JSONDecodeError:
        return {
            "canonical_name": cluster[0],
            "variants": cluster,
            "confidence": "medium",
            "reasoning": "LLM parse failed, using first variant"
        }

def normalize_entities(events: List[Dict[str, Any]], output_dir: Path, config_path: Path) -> Dict[str, Any]:
    print("\n" + "="*80)
    print("PHASE 1: ENTITY NORMALIZATION")
    print("="*80)
    
    config = load_config(config_path)
    
    api_key = os.getenv(config["api_key_env_var"])
    if not api_key:
        raise EnvironmentError(f"{config['api_key_env_var']} not found")
    
    script_dir = Path(__file__).parent
    prompt_template = load_prompt(script_dir / config["prompts"]["normalize_entity"])
    
    print("\nExtracting unique entities...")
    unique_entities = extract_unique_entities(events)
    
    print(f"Found {len(unique_entities['organizations'])} unique organizations")
    print(f"Found {len(unique_entities['roles'])} unique roles")
    
    org_threshold = config["similarity_thresholds"]["organization_fuzzy"]
    role_threshold = config["similarity_thresholds"]["role_fuzzy"]
    
    print(f"\nClustering organizations by similarity (threshold: {org_threshold}%)...")
    org_clusters = fuzzy_cluster_entities(unique_entities['organizations'], threshold=org_threshold)
    print(f"Clustered into {len(org_clusters)} organization groups")
    
    print(f"\nClustering roles by similarity (threshold: {role_threshold}%)...")
    role_clusters = fuzzy_cluster_entities(unique_entities['roles'], threshold=role_threshold)
    print(f"Clustered into {len(role_clusters)} role groups")
    
    org_mappings = {}
    role_mappings = {}
    
    print("\nProcessing organization clusters with LLM...")
    for i, cluster in enumerate(org_clusters):
        if len(cluster) > 1:
            print(f"  [{i+1}/{len(org_clusters)}] Cluster: {', '.join(cluster)}")
            result = llm_decide_canonical(
                cluster, "organization", api_key,
                prompt_template, config["model"], 
                config["temperature"], config["max_tokens_normalize"]
            )
            print(f"    → Canonical: {result['canonical_name']}")
        else:
            result = {
                "canonical_name": cluster[0],
                "variants": cluster,
                "confidence": "high",
                "reasoning": "single variant"
            }
        
        for variant in result["variants"]:
            org_mappings[variant] = result
    
    print("\nProcessing role clusters with LLM...")
    for i, cluster in enumerate(role_clusters):
        if len(cluster) > 1:
            print(f"  [{i+1}/{len(role_clusters)}] Cluster: {', '.join(cluster)}")
            result = llm_decide_canonical(
                cluster, "role", api_key,
                prompt_template, config["model"],
                config["temperature"], config["max_tokens_normalize"]
            )
            print(f"    → Canonical: {result['canonical_name']}")
        else:
            result = {
                "canonical_name": cluster[0],
                "variants": cluster,
                "confidence": "high",
                "reasoning": "single variant"
            }
        
        for variant in result["variants"]:
            role_mappings[variant] = result
    
    normalization_map = {
        "organizations": org_mappings,
        "roles": role_mappings,
        "stats": {
            "unique_organizations": len(unique_entities['organizations']),
            "unique_roles": len(unique_entities['roles']),
            "org_canonical_forms": len(set(m["canonical_name"] for m in org_mappings.values())),
            "role_canonical_forms": len(set(m["canonical_name"] for m in role_mappings.values()))
        }
    }
    
    output_file = output_dir / "01_normalized_entities.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(normalization_map, f, indent=2, ensure_ascii=False)
    
    print(f"\nNormalization complete. Saved to {output_file}")
    print(f"Organizations: {len(unique_entities['organizations'])} → {normalization_map['stats']['org_canonical_forms']} canonical forms")
    print(f"Roles: {len(unique_entities['roles'])} → {normalization_map['stats']['role_canonical_forms']} canonical forms")
    
    return normalization_map

if __name__ == "__main__":
    script_dir = Path(__file__).parent
    events_file = script_dir / "outputs" / "all_resolved_events.json"
    output_dir = script_dir / "outputs"
    config_path = script_dir / "config" / "config.json"
    
    with open(events_file, "r", encoding="utf-8") as f:
        events = json.load(f)
    
    normalize_entities(events, output_dir, config_path)