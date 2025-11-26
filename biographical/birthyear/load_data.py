import os
import json
from pathlib import Path
from typing import Dict, List, Any
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parent.parent.parent / ".env"
print(f"DEBUG: Loading .env from: {env_path}")
print(f"DEBUG: .env exists: {env_path.exists()}")
load_dotenv(env_path)
print(f"DEBUG: DB_PASSWORD after load: {'***' if os.getenv('DB_PASSWORD') else 'NOT SET'}")

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        database=os.getenv("DB_NAME", "eliteresearch"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD")
    )

def load_all_chunks_with_embeddings() -> pd.DataFrame:
    conn = get_db_connection()
    
    try:
        query = """
            SELECT 
                c.id as chunk_id,
                c.text,
                c.chunk_index,
                sr.url,
                sr.title,
                sr.extraction_method,
                ps.person_name,
                e.embedding
            FROM sources.chunks c
            JOIN sources.search_results sr ON c.search_result_id = sr.id
            JOIN sources.persons_searched ps ON sr.person_search_id = ps.id
            JOIN sources.embeddings e ON c.id = e.chunk_id
            WHERE e.model = 'embed-v4.0'
            ORDER BY ps.person_name, sr.url, c.chunk_index
        """
        
        df = pd.read_sql_query(query, conn)
        
        return df
    
    finally:
        conn.close()

def save_dataset(df: pd.DataFrame, output_path: Path) -> None:
    df.to_pickle(output_path)

def load_dataset(input_path: Path) -> pd.DataFrame:
    return pd.read_pickle(input_path)

def get_dataset_stats(df: pd.DataFrame) -> Dict[str, Any]:
    from urllib.parse import urlparse
    
    df['domain'] = df['url'].apply(lambda x: urlparse(x).netloc.lower().replace('www.', ''))
    
    stats = {
        "total_chunks": len(df),
        "total_people": df['person_name'].nunique(),
        "total_sources": df['url'].nunique(),
        "total_domains": df['domain'].nunique(),
        "chunks_per_person": df.groupby('person_name').size().describe().to_dict(),
        "people_list": sorted(df['person_name'].unique().tolist())
    }
    
    return stats

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Load all chunks and embeddings from database")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/chunks_dataset.pkl"),
        help="Output pickle file"
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Print dataset statistics"
    )
    
    args = parser.parse_args()
    
    print("=" * 80)
    print("Loading chunks and embeddings from database")
    print("=" * 80)
    print()
    
    df = load_all_chunks_with_embeddings()
    
    print(f"Loaded {len(df)} chunks")
    print()
    
    if args.stats:
        stats = get_dataset_stats(df)
        print("Dataset Statistics:")
        print("-" * 80)
        print(f"Total chunks: {stats['total_chunks']}")
        print(f"Total people: {stats['total_people']}")
        print(f"Total sources: {stats['total_sources']}")
        print(f"Total domains: {stats['total_domains']}")
        print()
        print("Chunks per person:")
        for key, val in stats['chunks_per_person'].items():
            print(f"  {key}: {val:.1f}")
        print()
        print(f"First 10 people: {', '.join(stats['people_list'][:10])}")
        print()
    
    args.output.parent.mkdir(parents=True, exist_ok=True)
    save_dataset(df, args.output)
    
    print(f"Saved dataset to: {args.output.resolve()}")
    print("=" * 80)