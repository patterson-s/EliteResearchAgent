import os
import json
import re
from pathlib import Path
from typing import Dict, Any, List
import cohere
from dotenv import load_dotenv

from classification import classify_chunk

env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(env_path)

def load_config(config_path: Path) -> Dict[str, Any]:
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

def load_prompt(prompt_path: Path) -> str:
    with open(prompt_path, "r", encoding="utf-8") as f:
        return f.read()

def parse_entities_output(text: str) -> Dict[str, List[Dict[str, Any]]]:
    text = text.strip()
    text = re.sub(r"```json\s*", "", text)
    text = re.sub(r"```\s*$", "", text)
    
    try:
        data = json.loads(text)
        return {
            "time_markers": data.get("time_markers", []),
            "organizations": data.get("organizations", []),
            "roles": data.get("roles", []),
            "locations": data.get("locations", [])
        }
    except json.JSONDecodeError:
        return {
            "time_markers": [],
            "organizations": [],
            "roles": [],
            "locations": []
        }

def extract_entities_step1(
    chunk_text: str,
    chunk_metadata: Dict[str, Any],
    config_path: Path,
    prompt_variant: str = None
) -> Dict[str, Any]:
    config = load_config(config_path)
    api_key = os.getenv(config["api_key_env_var"])
    
    if not api_key:
        raise EnvironmentError(f"Missing {config['api_key_env_var']} environment variable")
    
    co = cohere.Client(api_key)
    
    doc_type = classify_chunk(chunk_metadata)
    
    if prompt_variant:
        prompt_key = f"step1_{prompt_variant}"
    else:
        if doc_type == "cv_structured":
            prompt_key = "step1_cv_structured"
        else:
            prompt_key = "step1_narrative"
    
    script_dir = Path(__file__).parent
    system_prompt = load_prompt(script_dir / config["prompts"][prompt_key])
    
    user_prompt = f"TEXT:\n{chunk_text}"
    
    response = co.chat(
        model=config["model"],
        temperature=config["temperature"],
        preamble=system_prompt,
        message=user_prompt,
        max_tokens=config["max_tokens_step1"]
    )
    
    raw_output = response.text.strip()
    entities = parse_entities_output(raw_output)
    
    return {
        "document_type": doc_type,
        "prompt_used": prompt_key,
        "entities": entities,
        "raw_llm_output": raw_output
    }

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Extract entities from chunk (Step 1)")
    parser.add_argument("--text", required=True, help="Chunk text")
    parser.add_argument("--url", default="unknown", help="Source URL")
    parser.add_argument("--title", default="unknown", help="Document title")
    parser.add_argument("--method", default="html", help="Extraction method")
    parser.add_argument("--config", type=Path, default=Path("config/config.json"))
    parser.add_argument("--prompt", choices=["cv_structured", "narrative"], help="Force specific prompt")
    args = parser.parse_args()
    
    metadata = {
        "source_url": args.url,
        "title": args.title,
        "extraction_method": args.method
    }
    
    result = extract_entities_step1(args.text, metadata, args.config, args.prompt)
    
    print("\n" + "=" * 80)
    print(f"Document Type: {result['document_type']}")
    print(f"Prompt Used: {result['prompt_used']}")
    print("=" * 80)
    
    entities = result["entities"]
    print(f"\nTime Markers: {len(entities['time_markers'])}")
    for tm in entities['time_markers'][:3]:
        print(f"  - {tm.get('text')}")
    
    print(f"\nOrganizations: {len(entities['organizations'])}")
    for org in entities['organizations'][:3]:
        print(f"  - {org.get('name')}")
    
    print(f"\nRoles: {len(entities['roles'])}")
    for role in entities['roles'][:3]:
        print(f"  - {role.get('title')}")
    
    print(f"\nLocations: {len(entities['locations'])}")
    for loc in entities['locations'][:3]:
        print(f"  - {loc.get('place')}")