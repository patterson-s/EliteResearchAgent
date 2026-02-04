import os
import psycopg2
from psycopg2.extras import RealDictCursor
from pathlib import Path
from typing import List, Dict, Any
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(env_path)

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        database=os.getenv("DB_NAME", "eliteresearch"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD")
    )

def get_sources_for_person(person_name: str) -> List[str]:
    conn = get_db_connection()
    
    try:
        query = """
            SELECT DISTINCT sr.url
            FROM sources.search_results sr
            JOIN sources.persons_searched ps ON sr.person_search_id = ps.id
            WHERE ps.person_name = %s
            ORDER BY sr.url
        """
        
        with conn.cursor() as cur:
            cur.execute(query, (person_name,))
            rows = cur.fetchall()
        
        return [row[0] for row in rows]
    
    finally:
        conn.close()

def get_next_source(person_name: str, exclude_sources: List[str]) -> str:
    all_sources = get_sources_for_person(person_name)
    
    for source in all_sources:
        if source not in exclude_sources:
            return source
    
    return None

def load_chunks_for_source(person_name: str, source_url: str) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    
    try:
        query = """
            SELECT 
                c.id as chunk_id,
                c.text,
                c.chunk_index,
                sr.url as source_url,
                sr.title,
                sr.extraction_method,
                ps.person_name
            FROM sources.chunks c
            JOIN sources.search_results sr ON c.search_result_id = sr.id
            JOIN sources.persons_searched ps ON sr.person_search_id = ps.id
            WHERE ps.person_name = %s AND sr.url = %s
            ORDER BY c.chunk_index
        """
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (person_name, source_url))
            rows = cur.fetchall()
        
        chunks = [dict(row) for row in rows]
        return chunks
    
    finally:
        conn.close()

if __name__ == "__main__":
    person_name = "Amre Moussa"
    
    sources = get_sources_for_person(person_name)
    print(f"Total sources for {person_name}: {len(sources)}")
    
    for i, source in enumerate(sources, 1):
        print(f"{i}. {source}")
    
    next_source = get_next_source(person_name, ["wikipedia.org"])
    if next_source:
        print(f"\nNext source to process: {next_source}")
        chunks = load_chunks_for_source(person_name, next_source)
        print(f"Chunks in this source: {len(chunks)}")
