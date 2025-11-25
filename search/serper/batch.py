import json
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
from search.serper.client import search
from search.serper.fetcher import fetch_url_text

SEARCH_TEMPLATE = '{name} biography OR CV OR career OR education OR appointed OR minister OR ambassador OR director'
MAX_RESULTS = 20
OUTPUT_DIR = Path(__file__).parent / "outputs"

def read_names_from_json(filepath: str) -> List[str]:
    with open(filepath, 'r', encoding='utf-8') as f:
        names = json.load(f)
    if not isinstance(names, list):
        raise ValueError("JSON file must contain a list of names")
    return [str(name).strip() for name in names if str(name).strip()]

def build_search_query(name: str) -> str:
    return SEARCH_TEMPLATE.format(name=name)

def process_person(name: str, max_results: int) -> List[Dict[str, Any]]:
    query = build_search_query(name)
    print(f"  Searching: {query}")
    
    try:
        resp = search(query, num_results=max_results)
    except Exception as e:
        print(f"  Search failed: {e}")
        return []
    
    organic = resp.get("organic", []) or resp.get("results", []) or []
    print(f"  Found {len(organic)} results")
    results = organic[:max_results]
    
    all_results = []
    searched_at = datetime.utcnow().isoformat()
    
    for i, r in enumerate(results):
        url = r.get("link") or r.get("url") or r.get("snippet")
        title = r.get("title") or ""
        
        result_entry = {
            "person": name,
            "search_query": query,
            "searched_at": searched_at,
            "rank": i + 1,
            "url": url,
            "title": title,
            "fetch_status": "pending",
            "fetch_error": None,
            "full_text": None,
            "fetched_at": None,
            "extraction_method": None
        }
        
        try:
            print(f"  Fetching [{i+1}/{len(results)}]: {url}")
            fetched_title, text, extraction_method = fetch_url_text(url)
            if fetched_title:
                result_entry["title"] = fetched_title
            
            result_entry["full_text"] = text
            result_entry["fetch_status"] = "success"
            result_entry["fetched_at"] = datetime.utcnow().isoformat()
            result_entry["extraction_method"] = extraction_method
            
        except Exception as e:
            print(f"  Fetch failed: {e}")
            result_entry["fetch_status"] = "failed"
            result_entry["fetch_error"] = str(e)
        
        all_results.append(result_entry)
    
    return all_results

def main():
    if len(sys.argv) < 2:
        print("Usage: python -m search.serper.batch <input_json_file>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    
    OUTPUT_DIR.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = OUTPUT_DIR / f"search_results_{timestamp}.json"
    temp_file = OUTPUT_DIR / f"search_results_{timestamp}_temp.json"
    
    print(f"Reading names from: {input_file}")
    names = read_names_from_json(input_file)
    print(f"Found {len(names)} names to process\n")
    
    all_results = []
    
    for idx, name in enumerate(names, 1):
        print(f"[{idx}/{len(names)}] Processing: {name}")
        person_results = process_person(name, MAX_RESULTS)
        all_results.extend(person_results)
        print(f"  Collected {len(person_results)} results")
        
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(all_results, f, indent=2, ensure_ascii=False)
        print(f"  Saved to temp file\n")
    
    print(f"Writing final results to: {output_file}")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)
    
    if temp_file.exists():
        temp_file.unlink()
    
    print(f"\nComplete! Processed {len(names)} people, collected {len(all_results)} total results")
    print(f"Output saved to: {output_file}")

if __name__ == "__main__":
    main()