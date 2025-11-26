import os
import json
from pathlib import Path
from typing import List, Dict, Any
from urllib.parse import urlparse
import numpy as np
import pandas as pd
import cohere
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parent.parent.parent / ".env"
load_dotenv(env_path)

def load_config(config_path: Path) -> Dict[str, Any]:
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

def extract_domain(url: str) -> str:
    try:
        host = urlparse(url).netloc.lower()
        return host[4:] if host.startswith("www.") else host
    except Exception:
        return ""

def cosine_similarity(a: List[float], b: List[float]) -> float:
    a_arr, b_arr = np.array(a), np.array(b)
    return float(np.dot(a_arr, b_arr) / (np.linalg.norm(a_arr) * np.linalg.norm(b_arr)))

def retrieve_candidate_chunks(
    person_name: str,
    df: pd.DataFrame,
    query_embedding: List[float],
    initial_k: int,
    min_sim: float
) -> List[Dict[str, Any]]:
    person_df = df[df['person_name'] == person_name].copy()
    
    if len(person_df) == 0:
        return []
    
    person_df['similarity'] = person_df['embedding'].apply(
        lambda emb: cosine_similarity(query_embedding, emb)
    )
    
    person_df = person_df[person_df['similarity'] >= min_sim]
    person_df = person_df.sort_values('similarity', ascending=False).head(initial_k)
    
    person_df['domain'] = person_df['url'].apply(extract_domain)
    
    candidates = person_df.to_dict('records')
    
    return candidates

def rerank_chunks(
    query: str,
    candidates: List[Dict[str, Any]],
    top_k: int,
    co: cohere.Client
) -> List[Dict[str, Any]]:
    if not candidates:
        return []
    
    top_k = min(top_k, len(candidates))
    documents = [c["text"] for c in candidates]
    
    rerank_result = co.rerank(
        model="rerank-v3.5",
        query=query,
        documents=documents,
        top_n=top_k
    )
    
    reranked = []
    for result in rerank_result.results:
        idx = result.index
        candidate = candidates[idx].copy()
        candidate["rerank_score"] = result.relevance_score
        reranked.append(candidate)
    
    return reranked

def retrieve_birth_chunks(
    person_name: str,
    df: pd.DataFrame,
    config_path: Path
) -> List[Dict[str, Any]]:
    config = load_config(config_path)
    api_key = os.getenv(config["api_keys"]["cohere"])
    
    if not api_key:
        raise EnvironmentError(f"Missing {config['api_keys']['cohere']} environment variable")
    
    co = cohere.Client(api_key)
    
    query = config["retrieval"]["query_template"].format(person_name=person_name)
    
    query_embedding = co.embed(
        model="embed-v4.0",
        texts=[query],
        input_type="search_query"
    ).embeddings[0]
    
    candidates = retrieve_candidate_chunks(
        person_name,
        df,
        query_embedding,
        config["retrieval"]["initial_candidates"],
        config["retrieval"]["min_similarity"]
    )
    
    if not candidates:
        return []
    
    reranked = rerank_chunks(
        query,
        candidates,
        config["retrieval"]["rerank_top_k"],
        co
    )
    
    return reranked

if __name__ == "__main__":
    import argparse
    from load_data import load_dataset
    
    parser = argparse.ArgumentParser(description="Retrieve birth-relevant chunks with re-ranking")
    parser.add_argument("--person", required=True, help="Person name")
    parser.add_argument("--config", type=Path, default=Path("config/config.json"))
    parser.add_argument("--data", type=Path, default=Path("data/chunks_dataset.pkl"))
    args = parser.parse_args()
    
    print(f"Loading dataset from {args.data}...")
    df = load_dataset(args.data)
    print(f"Loaded {len(df)} chunks for {df['person_name'].nunique()} people")
    print()
    
    results = retrieve_birth_chunks(args.person, df, args.config)
    
    print(f"\nRetrieved {len(results)} re-ranked chunks for {args.person}\n")
    print("=" * 80)
    for i, r in enumerate(results, 1):
        print(f"[{i}] sim={r['similarity']:.3f} rerank={r['rerank_score']:.3f}")
        print(f"    domain: {r['domain']}")
        print(f"    url: {r['url']}")
        print(f"    chunk_id: {r['chunk_id']}")
        print()