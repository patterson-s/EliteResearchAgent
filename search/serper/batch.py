import json
import sys
from datetime import datetime
from typing import List
from database.connection import get_connection, release_connection
from search.serper.client import search
from search.serper.fetcher import fetch_url_text

SEARCH_TEMPLATE = '{name} biography OR CV OR career OR education OR appointed OR minister OR ambassador OR director'
MAX_RESULTS = 20

def read_names_from_json(filepath: str) -> List[str]:
    with open(filepath, 'r', encoding='utf-8') as f:
        names = json.load(f)
    if not isinstance(names, list):
        raise ValueError("JSON file must contain a list of names")
    return [str(name).strip() for name in names if str(name).strip()]

def build_search_query(name: str) -> str:
    return SEARCH_TEMPLATE.format(name=name)

def save_person_search(conn, person_name: str, search_query: str) -> int:
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO sources.persons_searched (person_name, search_query, searched_at)
        VALUES (%s, %s, %s)
        RETURNING id
    """, (person_name, search_query, datetime.utcnow()))
    person_search_id = cur.fetchone()[0]
    conn.commit()
    return person_search_id

def save_search_result(conn, person_search_id: int, rank: int, url: str, 
                       title: str, fetch_status: str, fetch_error: str = None, 
                       full_text: str = None):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO sources.search_results 
        (person_search_id, rank, url, title, fetch_status, fetch_error, full_text, fetched_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (person_search_id, url) DO NOTHING
    """, (person_search_id, rank, url, title, fetch_status, fetch_error, 
          full_text, datetime.utcnow() if fetch_status == 'success' else None))
    conn.commit()

def process_person(conn, name: str, max_results: int):
    query = build_search_query(name)
    print(f"  Searching: {query}")
    
    try:
        resp = search(query, num_results=max_results)
    except Exception as e:
        print(f"  Search failed: {e}")
        return
    
    person_search_id = save_person_search(conn, name, query)
    
    organic = resp.get("organic", []) or resp.get("results", []) or []
    print(f"  Found {len(organic)} results")
    results = organic[:max_results]
    
    for i, r in enumerate(results):
        url = r.get("link") or r.get("url") or r.get("snippet")
        title = r.get("title") or ""
        
        try:
            print(f"  Fetching [{i+1}/{len(results)}]: {url}")
            fetched_title, text = fetch_url_text(url)
            if fetched_title:
                title = fetched_title
            
            save_search_result(conn, person_search_id, i + 1, url, title, 
                             'success', full_text=text)
            
        except Exception as e:
            print(f"  Fetch failed: {e}")
            save_search_result(conn, person_search_id, i + 1, url, title, 
                             'failed', fetch_error=str(e))

def main():
    if len(sys.argv) < 2:
        print("Usage: python batch.py <input_json_file>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    
    print(f"Reading names from: {input_file}")
    names = read_names_from_json(input_file)
    print(f"Found {len(names)} names to process\n")
    
    conn = get_connection()
    
    try:
        for idx, name in enumerate(names, 1):
            print(f"[{idx}/{len(names)}] Processing: {name}")
            process_person(conn, name, MAX_RESULTS)
            print()
    finally:
        release_connection(conn)
    
    print(f"\nComplete! Processed {len(names)} people")

if __name__ == "__main__":
    main()
