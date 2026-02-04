import cohere
from pathlib import Path
from typing import Dict, Any

from utils import load_config, load_prompt, parse_json_response, get_api_key

def extract_candidates_from_chunk(
    chunk: Dict[str, Any],
    config_path: Path
) -> Dict[str, Any]:
    config = load_config(config_path)
    api_key = get_api_key(config)
    
    script_dir = Path(__file__).parent
    system_prompt = load_prompt(script_dir / config["prompts"]["step1_extract_candidates"])
    
    user_prompt = f"TEXT:\n{chunk['text']}"
    
    co = cohere.Client(api_key)
    
    response = co.chat(
        model=config["model"],
        temperature=config["temperature"],
        preamble=system_prompt,
        message=user_prompt,
        max_tokens=config["max_tokens_extract"]
    )
    
    raw_output = response.text.strip()
    result = parse_json_response(raw_output)
    
    candidates = result.get("career_events", [])
    
    for candidate in candidates:
        candidate["chunk_id"] = chunk["chunk_id"]
        candidate["source_url"] = chunk["source_url"]
    
    return {
        "candidates": candidates,
        "chunk_id": chunk["chunk_id"],
        "raw_llm_output": raw_output
    }

if __name__ == "__main__":
    from load_data import load_chunks_for_source
    
    script_dir = Path(__file__).parent
    config_path = script_dir / "config" / "config.json"
    
    person_name = "Amre Moussa"
    source_url = "https://www.un.org/sg/en/content/sg/personnel-appointments/2016-07-18/mr-amre-moussa-egypt-member-high-level-panel"
    
    chunks = load_chunks_for_source(person_name, source_url)
    
    if chunks:
        result = extract_candidates_from_chunk(chunks[0], config_path)
        print(f"Candidates found: {len(result['candidates'])}")
        for c in result["candidates"]:
            print(f"  - {c.get('organization')}: {c.get('roles')}")
