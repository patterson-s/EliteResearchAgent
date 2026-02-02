import cohere
from pathlib import Path
from typing import Dict, Any, List
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

from utils import load_config, load_prompt, parse_json_response, get_api_key, save_json

def extract_entities_from_chunk(
    text: str, 
    chunk_index: int, 
    config: Dict[str, Any], 
    api_key: str,
    system_prompt: str,
    max_retries: int = 3
) -> Dict[str, Any]:
    co = cohere.Client(api_key)
    
    for attempt in range(max_retries):
        try:
            user_prompt = f"TEXT:\n{text}"
            
            response = co.chat(
                model=config["model"],
                temperature=config["temperature"],
                preamble=system_prompt,
                message=user_prompt,
                max_tokens=config["max_tokens_step1"]
            )
            
            raw_output = response.text.strip()
            entities = parse_json_response(raw_output)
            
            return {
                "chunk_index": chunk_index,
                "entities": entities,
                "raw_llm_output": raw_output,
                "text_length": len(text),
                "status": "success"
            }
            
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
            else:
                return {
                    "chunk_index": chunk_index,
                    "entities": {
                        "time_markers": [],
                        "organizations": [],
                        "roles": [],
                        "locations": []
                    },
                    "raw_llm_output": "",
                    "text_length": len(text),
                    "status": "failed",
                    "error": str(e)
                }

def merge_entities(chunk_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    merged = {
        "time_markers": [],
        "organizations": [],
        "roles": [],
        "locations": []
    }
    
    for chunk_result in chunk_results:
        if chunk_result.get("status") == "success":
            entities = chunk_result["entities"]
            merged["time_markers"].extend(entities.get("time_markers", []))
            merged["organizations"].extend(entities.get("organizations", []))
            merged["roles"].extend(entities.get("roles", []))
            merged["locations"].extend(entities.get("locations", []))
    
    return merged

def extract_entities_from_chunks(
    chunks: List[Dict[str, Any]], 
    config_path: Path,
    max_workers: int = 4
) -> Dict[str, Any]:
    print("\n" + "="*80)
    print("STEP 1: EXTRACT ENTITIES")
    print("="*80)
    
    config = load_config(config_path)
    api_key = get_api_key(config)
    
    script_dir = Path(__file__).parent
    system_prompt = load_prompt(script_dir / config["prompts"]["step1_extract_entities"])
    
    print(f"Processing {len(chunks)} chunks in parallel (workers={max_workers})...")
    
    chunk_results = []
    failed_chunks = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                extract_entities_from_chunk,
                chunk['text'],
                chunk['chunk_index'],
                config,
                api_key,
                system_prompt
            ): chunk for chunk in chunks
        }
        
        for i, future in enumerate(as_completed(futures), 1):
            chunk = futures[future]
            result = future.result()
            chunk_results.append(result)
            
            if result.get("status") == "success":
                entities = result["entities"]
                total = (len(entities.get('time_markers', [])) + 
                        len(entities.get('organizations', [])) + 
                        len(entities.get('roles', [])) + 
                        len(entities.get('locations', [])))
                print(f"  [{i}/{len(chunks)}] Chunk {result['chunk_index']}: {total} entities")
            else:
                failed_chunks.append(result['chunk_index'])
                print(f"  [{i}/{len(chunks)}] Chunk {result['chunk_index']}: FAILED - {result.get('error', 'unknown')}")
    
    chunk_results.sort(key=lambda x: x['chunk_index'])
    
    print("\nMerging entities from all chunks...")
    merged_entities = merge_entities(chunk_results)
    
    print(f"\nTotal extracted:")
    print(f"  Time markers: {len(merged_entities['time_markers'])}")
    print(f"  Organizations: {len(merged_entities['organizations'])}")
    print(f"  Roles: {len(merged_entities['roles'])}")
    print(f"  Locations: {len(merged_entities['locations'])}")
    
    if failed_chunks:
        print(f"\nWARNING: {len(failed_chunks)} chunks failed: {failed_chunks}")
    
    result = {
        "entities": merged_entities,
        "chunk_results": chunk_results,
        "total_chunks": len(chunks),
        "failed_chunks": failed_chunks,
        "success_rate": (len(chunks) - len(failed_chunks)) / len(chunks) * 100
    }
    
    return result

if __name__ == "__main__":
    from load_data import load_chunks_for_person, get_wikipedia_source
    
    script_dir = Path(__file__).parent
    config_path = script_dir / "config" / "config.json"
    output_dir = script_dir / "outputs" / "Gro_Harlem_Brundtland"
    
    person_name = "Gro Harlem Brundtland"
    chunks = load_chunks_for_person(person_name)
    wiki_chunks = get_wikipedia_source(chunks)
    
    if not wiki_chunks:
        print("No Wikipedia source found")
        exit(1)
    
    result = extract_entities_from_chunks(wiki_chunks, config_path)
    
    output_file = output_dir / "step1_entities.json"
    save_json(result, output_file)
    print(f"\nSaved to {output_file}")