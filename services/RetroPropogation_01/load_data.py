import os
import psycopg2
from psycopg2.extras import RealDictCursor
from pathlib import Path
from typing import List, Dict, Any
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parent.parent.parent / ".env"
load_dotenv(env_path)

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        database=os.getenv("DB_NAME", "eliteresearch"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD")
    )

def load_chunks_for_person(person_name: str) -> List[Dict[str, Any]]:
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
            WHERE ps.person_name = %s
            ORDER BY sr.url, c.chunk_index
        """
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (person_name,))
            rows = cur.fetchall()
        
        chunks = [dict(row) for row in rows]
        return chunks
    
    finally:
        conn.close()

def group_chunks_by_source(chunks: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    sources = {}
    for chunk in chunks:
        url = chunk["source_url"]
        if url not in sources:
            sources[url] = []
        sources[url].append(chunk)
    return sources

def get_wikipedia_source(chunks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    for chunk in chunks:
        if "wikipedia.org" in chunk["source_url"].lower():
            return [c for c in chunks if c["source_url"] == chunk["source_url"]]
    return []

def concatenate_chunks(chunks: List[Dict[str, Any]]) -> str:
    sorted_chunks = sorted(chunks, key=lambda c: c["chunk_index"])
    return "\n\n".join([c["text"] for c in sorted_chunks])

if __name__ == "__main__":
    person_name = "Gro Harlem Brundtland"
    
    print(f"Loading chunks for {person_name}...")
    chunks = load_chunks_for_person(person_name)
    print(f"Found {len(chunks)} total chunks")
    
    sources = group_chunks_by_source(chunks)
    print(f"\nSources found: {len(sources)}")
    for url, source_chunks in sources.items():
        print(f"  {url}: {len(source_chunks)} chunks")
    
    wiki_chunks = get_wikipedia_source(chunks)
    if wiki_chunks:
        print(f"\nWikipedia source: {len(wiki_chunks)} chunks")
        full_text = concatenate_chunks(wiki_chunks)
        print(f"Full text length: {len(full_text)} characters")
    else:
        print("\nNo Wikipedia source found")