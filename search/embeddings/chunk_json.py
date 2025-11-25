import json
import re
from pathlib import Path
from datetime import datetime
from typing import List, Tuple, Dict, Any

def find_whitespace_tokens_with_spans(text: str) -> List[Tuple[int, int]]:
    return [(m.start(), m.end()) for m in re.finditer(r"\S+", text, flags=re.UNICODE)]

def chunk_by_size(spans: List[Tuple[int, int]], size: int) -> List[Tuple[int, int]]:
    chunks = []
    n = len(spans)
    for start in range(0, n, size):
        end = min(start + size, n)
        chunks.append((start, end))
    return chunks

def chunk_text(text: str, chunk_size: int) -> List[Dict[str, Any]]:
    if not text or not text.strip():
        return []
    
    token_spans = find_whitespace_tokens_with_spans(text)
    if not token_spans:
        return []
    
    token_chunks = chunk_by_size(token_spans, chunk_size)
    
    chunks = []
    for ci, (t_start, t_end) in enumerate(token_chunks):
        char_start = token_spans[t_start][0]
        char_end = token_spans[t_end - 1][1]
        chunk_text = text[char_start:char_end]
        
        chunks.append({
            "chunk_index": ci,
            "start_token": t_start,
            "end_token": t_end,
            "char_start": char_start,
            "char_end": char_end,
            "token_count": t_end - t_start,
            "text": chunk_text
        })
    
    return chunks

def chunk_json_results(input_file: Path, output_file: Path, chunk_size: int = 400):
    with open(input_file, 'r', encoding='utf-8') as f:
        results = json.load(f)
    
    print(f"Loaded {len(results)} search results")
    
    chunked_at = datetime.utcnow().isoformat()
    total_chunks = 0
    
    for idx, result in enumerate(results, 1):
        if result.get('fetch_status') != 'success':
            continue
        
        if not result.get('full_text'):
            continue
        
        print(f"[{idx}/{len(results)}] Chunking: {result.get('person')} - {result.get('title', 'Untitled')[:50]}")
        
        chunks = chunk_text(result['full_text'], chunk_size)
        result['chunks'] = chunks
        result['chunked_at'] = chunked_at
        result['chunk_size'] = chunk_size
        result['num_chunks'] = len(chunks)
        
        total_chunks += len(chunks)
        print(f"  Generated {len(chunks)} chunks")
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"\nComplete!")
    print(f"  Total chunks: {total_chunks}")
    print(f"Saved to: {output_file}")

def main():
    import sys
    import argparse
    
    parser = argparse.ArgumentParser(description="Chunk full_text in JSON file")
    parser.add_argument("input_file", type=Path, help="Input JSON file")
    parser.add_argument("--chunk-size", type=int, default=400, help="Token count per chunk")
    
    args = parser.parse_args()
    
    output_file = args.input_file.parent / f"{args.input_file.stem}_chunked.json"
    
    chunk_json_results(args.input_file, output_file, args.chunk_size)

if __name__ == "__main__":
    main()