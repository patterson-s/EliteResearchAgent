import json
import os
from pathlib import Path
from typing import Dict, Any, List
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent.parent / ".env")

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        database=os.getenv("DB_NAME", "eliteresearch"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD")
    )

def load_chunk_text_from_db(chunk_id: int) -> str:
    conn = get_db_connection()
    
    try:
        query = """
            SELECT text
            FROM sources.chunks
            WHERE id = %s
        """
        
        with conn.cursor() as cur:
            cur.execute(query, (chunk_id,))
            row = cur.fetchone()
        
        if row:
            return row[0]
        else:
            return ""
    
    finally:
        conn.close()

def load_chunk_texts_from_db(chunk_ids: List[int]) -> Dict[int, str]:
    if not chunk_ids:
        return {}
    
    conn = get_db_connection()
    
    try:
        query = """
            SELECT id, text
            FROM sources.chunks
            WHERE id = ANY(%s)
        """
        
        with conn.cursor() as cur:
            cur.execute(query, (chunk_ids,))
            rows = cur.fetchall()
        
        return {row[0]: row[1] for row in rows}
    
    finally:
        conn.close()

def load_chunk_file(chunk_file: Path) -> Dict[str, Any]:
    with open(chunk_file, "r", encoding="utf-8") as f:
        return json.load(f)

def find_chunk_file(chunk_id: int, person_name: str, base_dir: Path) -> Path:
    chunk_dir = base_dir.parent / "careerfinder_granular_01" / "review" / person_name
    chunk_file = chunk_dir / f"chunk_{chunk_id}_results.json"
    
    if not chunk_file.exists():
        chunk_dir_alt = base_dir.parent / "EventAlign_03" / "data" / person_name
        chunk_file = chunk_dir_alt / f"chunk_{chunk_id}_results.json"
    
    return chunk_file

def load_enriched_chunk(chunk_id: int, person_name: str, base_dir: Path) -> Dict[str, Any]:
    chunk_file = find_chunk_file(chunk_id, person_name, base_dir)
    
    if not chunk_file.exists():
        raise FileNotFoundError(f"Chunk file not found: chunk_{chunk_id}_results.json for {person_name}")
    
    chunk_data = load_chunk_file(chunk_file)
    
    raw_text = load_chunk_text_from_db(chunk_id)
    
    return {
        "chunk_id": chunk_id,
        "raw_text": raw_text,
        "source_url": chunk_data.get("source_url", ""),
        "title": chunk_data.get("title", ""),
        "entities": chunk_data.get("step1", {}).get("entities", {}),
        "assembled_events": chunk_data.get("step2", {}).get("assembled_events", [])
    }

def load_enriched_chunks_batch(chunk_ids: List[int], person_name: str, base_dir: Path) -> Dict[int, Dict[str, Any]]:
    texts = load_chunk_texts_from_db(chunk_ids)
    
    enriched_chunks = {}
    
    for chunk_id in chunk_ids:
        chunk_file = find_chunk_file(chunk_id, person_name, base_dir)
        
        if not chunk_file.exists():
            print(f"Warning: Chunk file not found for chunk_id {chunk_id}")
            continue
        
        chunk_data = load_chunk_file(chunk_file)
        
        enriched_chunks[chunk_id] = {
            "chunk_id": chunk_id,
            "raw_text": texts.get(chunk_id, ""),
            "source_url": chunk_data.get("source_url", ""),
            "title": chunk_data.get("title", ""),
            "entities": chunk_data.get("step1", {}).get("entities", {}),
            "assembled_events": chunk_data.get("step2", {}).get("assembled_events", [])
        }
    
    return enriched_chunks

if __name__ == "__main__":
    script_dir = Path(__file__).parent
    
    chunk = load_enriched_chunk(3115, "Abhijit_Banerjee", script_dir)
    
    print(f"Chunk ID: {chunk['chunk_id']}")
    print(f"Source: {chunk['source_url']}")
    print(f"Raw text length: {len(chunk['raw_text'])} chars")
    print(f"Entities: {list(chunk['entities'].keys())}")
    print(f"Assembled events: {len(chunk['assembled_events'])}")
    print(f"\nRaw text preview:\n{chunk['raw_text'][:300]}...")