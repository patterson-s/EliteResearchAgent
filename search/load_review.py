import json
import argparse
from pathlib import Path
from datetime import datetime
from database.connection import get_connection, release_connection

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

def insert_search_result(conn, person_search_id: int, result: dict) -> int:
    cur = conn.cursor()
    
    full_text = result.get('full_text')
    if full_text:
        full_text = full_text.replace('\x00', '')
    
    title = result.get('title', '')
    if title:
        title = title.replace('\x00', '')
    
    provenance_narrative = result.get('provenance_narrative', '')
    
    cur.execute("""
        INSERT INTO sources.search_results 
        (person_search_id, rank, url, title, fetch_status, fetch_error, full_text, fetched_at, 
         extraction_method, extraction_quality, needs_ocr, provenance_narrative)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (person_search_id, url) DO UPDATE SET
            full_text = EXCLUDED.full_text,
            extraction_method = EXCLUDED.extraction_method,
            extraction_quality = EXCLUDED.extraction_quality,
            provenance_narrative = EXCLUDED.provenance_narrative
        RETURNING id
    """, (
        person_search_id,
        result['rank'],
        result['url'],
        title,
        result['fetch_status'],
        result.get('fetch_error'),
        full_text,
        result.get('fetched_at'),
        result.get('extraction_method'),
        result.get('extraction_quality'),
        result.get('needs_ocr', False),
        provenance_narrative
    ))
    
    search_result_id = cur.fetchone()[0]
    conn.commit()
    return search_result_id

def insert_chunks(conn, search_result_id: int, chunks: list):
    cur = conn.cursor()
    
    for chunk in chunks:
        chunk_text = chunk.get('text', '')
        if chunk_text:
            chunk_text = chunk_text.replace('\x00', '')
        
        cur.execute("""
            INSERT INTO sources.chunks 
            (search_result_id, chunk_index, start_token, end_token, char_start, char_end, token_count, text)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (search_result_id, chunk_index) DO NOTHING
            RETURNING id
        """, (
            search_result_id,
            chunk['chunk_index'],
            chunk['start_token'],
            chunk['end_token'],
            chunk['char_start'],
            chunk['char_end'],
            chunk['token_count'],
            chunk_text
        ))
        
        chunk_row = cur.fetchone()
        if chunk_row:
            chunk_id = chunk_row[0]
            
            if chunk.get('embedding'):
                cur.execute("""
                    INSERT INTO sources.embeddings (chunk_id, model, embedding)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (chunk_id) DO NOTHING
                """, (
                    chunk_id,
                    chunk.get('embedding_model', 'embed-v4.0'),
                    chunk['embedding']
                ))
    
    conn.commit()

def load_review_to_db(input_file: Path):
    with open(input_file, 'r', encoding='utf-8') as f:
        results = json.load(f)
    
    print(f"Loading {len(results)} search results to database")
    
    conn = get_connection()
    
    try:
        person_search_cache = {}
        
        for idx, result in enumerate(results, 1):
            print(f"[{idx}/{len(results)}] Loading: {result.get('person')} - {result.get('title', 'Untitled')[:50]}")
            
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
            
            search_result_id = insert_search_result(conn, person_search_id, result)
            
            if result.get('chunks'):
                insert_chunks(conn, search_result_id, result['chunks'])
                print(f"  Loaded {len(result['chunks'])} chunks with embeddings")
        
        print(f"\nComplete! Loaded {len(results)} results to database")
        
    except Exception as e:
        print(f"Error loading to database: {e}")
        conn.rollback()
        raise
    finally:
        release_connection(conn)

def main():
    parser = argparse.ArgumentParser(description="Load review data to PostgreSQL database")
    parser.add_argument("input_file", type=Path, help="JSON file from review folder")
    
    args = parser.parse_args()
    
    if not args.input_file.exists():
        print(f"Error: File not found: {args.input_file}")
        return 1
    
    load_review_to_db(args.input_file)

if __name__ == "__main__":
    main()