import json
import sys
from pathlib import Path
from datetime import datetime
from database.connection import get_connection, release_connection

def load_json_file(filepath: str) -> list:
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def get_or_create_person_search(conn, person_name: str, search_query: str, searched_at: str) -> int:
    cur = conn.cursor()
    
    cur.execute("""
        SELECT id FROM sources.persons_searched 
        WHERE person_name = %s AND search_query = %s AND searched_at = %s
    """, (person_name, search_query, searched_at))
    
    row = cur.fetchone()
    if row:
        return row[0]
    
    cur.execute("""
        INSERT INTO sources.persons_searched (person_name, search_query, searched_at)
        VALUES (%s, %s, %s)
        RETURNING id
    """, (person_name, search_query, searched_at))
    
    person_search_id = cur.fetchone()[0]
    conn.commit()
    return person_search_id

def insert_search_result(conn, person_search_id: int, result: dict):
    cur = conn.cursor()
    
    cur.execute("""
        INSERT INTO sources.search_results 
        (person_search_id, rank, url, title, fetch_status, fetch_error, full_text, fetched_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (person_search_id, url) DO NOTHING
    """, (
        person_search_id,
        result['rank'],
        result['url'],
        result['title'],
        result['fetch_status'],
        result.get('fetch_error'),
        result.get('full_text'),
        result.get('fetched_at')
    ))
    conn.commit()

def load_results_to_db(conn, results: list):
    person_search_cache = {}
    
    for result in results:
        cache_key = (result['person'], result['search_query'], result['searched_at'])
        
        if cache_key not in person_search_cache:
            person_search_id = get_or_create_person_search(
                conn,
                result['person'],
                result['search_query'],
                result['searched_at']
            )
            person_search_cache[cache_key] = person_search_id
        else:
            person_search_id = person_search_cache[cache_key]
        
        insert_search_result(conn, person_search_id, result)

def main():
    if len(sys.argv) < 2:
        print("Usage: python -m search.serper.load_to_db <search_results_json_file>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    
    if not Path(input_file).exists():
        print(f"Error: File not found: {input_file}")
        sys.exit(1)
    
    print(f"Loading results from: {input_file}")
    results = load_json_file(input_file)
    print(f"Found {len(results)} results to load")
    
    conn = get_connection()
    
    try:
        load_results_to_db(conn, results)
        print(f"Successfully loaded {len(results)} results to database")
    except Exception as e:
        print(f"Error loading to database: {e}")
        conn.rollback()
        raise
    finally:
        release_connection(conn)

if __name__ == "__main__":
    main()