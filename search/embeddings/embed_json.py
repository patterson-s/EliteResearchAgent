import os
import json
import time
from pathlib import Path
from datetime import datetime
from typing import List
import cohere

def chunkify(lst: List, size: int) -> List[List]:
    return [lst[i:i+size] for i in range(0, len(lst), size)]

def embed_json_results(input_file: Path, output_file: Path, batch_size: int = 64):
    api_key = os.getenv("COHERE_API_KEY")
    if not api_key:
        raise EnvironmentError("Environment variable COHERE_API_KEY is missing")
    
    co = cohere.Client(api_key)
    model_name = "embed-v4.0"
    
    with open(input_file, 'r', encoding='utf-8') as f:
        results = json.load(f)
    
    print(f"Loaded {len(results)} search results")
    
    all_chunks_to_embed = []
    chunk_metadata = []
    
    for result_idx, result in enumerate(results):
        if not result.get('chunks'):
            continue
        
        for chunk in result['chunks']:
            all_chunks_to_embed.append(chunk['text'][:3000])
            chunk_metadata.append({
                'result_idx': result_idx,
                'chunk_idx': chunk['chunk_index']
            })
    
    if not all_chunks_to_embed:
        print("No chunks to embed")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        return
    
    print(f"Found {len(all_chunks_to_embed)} chunks to embed")
    
    embedded_at = datetime.utcnow().isoformat()
    
    batches = list(zip(
        chunkify(all_chunks_to_embed, batch_size),
        chunkify(chunk_metadata, batch_size)
    ))
    
    for batch_idx, (text_batch, meta_batch) in enumerate(batches, 1):
        print(f"Embedding batch {batch_idx}/{len(batches)} ({len(text_batch)} chunks)")
        
        resp = co.embed(model=model_name, texts=text_batch, input_type="search_document")
        embeddings = resp.embeddings
        
        for embedding, meta in zip(embeddings, meta_batch):
            result_idx = meta['result_idx']
            chunk_idx = meta['chunk_idx']
            
            chunk = results[result_idx]['chunks'][chunk_idx]
            chunk['embedding'] = embedding
            chunk['embedded_at'] = embedded_at
            chunk['embedding_model'] = model_name
        
        print(f"  Saved {len(text_batch)} embeddings")
        
        if batch_idx < len(batches):
            time.sleep(1)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"\nComplete!")
    print(f"  Embedded {len(all_chunks_to_embed)} chunks")
    print(f"Saved to: {output_file}")

def main():
    import sys
    import argparse
    
    parser = argparse.ArgumentParser(description="Embed chunks in JSON file using Cohere")
    parser.add_argument("input_file", type=Path, help="Input JSON file")
    parser.add_argument("--batch-size", type=int, default=64, help="Chunks per API call")
    
    args = parser.parse_args()
    
    output_file = args.input_file.parent / f"{args.input_file.stem}_embedded.json"
    
    embed_json_results(args.input_file, output_file, args.batch_size)

if __name__ == "__main__":
    main()