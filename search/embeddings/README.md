# Embeddings Module

## Purpose

Chunks full text from search results and generates vector embeddings for semantic search. This prepares source material for downstream services to efficiently find relevant passages.

## Structure

```
embeddings/
├── chunk.py    # Split full_text into passages
└── embed.py    # Generate Cohere embeddings
```

## Workflow

### 1. Chunk Source Texts

After loading search results to database, chunk them:

```bash
python -m search.embeddings.chunk --chunk-size 400
```

Options:
- `--chunk-size` - tokens per chunk (default 400)
- `--limit` - process only N documents (for testing)

This creates entries in `sources.chunks` table.

### 2. Generate Embeddings

```bash
python -m search.embeddings.embed --batch-size 64
```

Options:
- `--batch-size` - chunks per API call (default 64)
- `--limit` - embed only N chunks (for testing)

This creates entries in `sources.embeddings` table.

## What Gets Created

**Chunks:**
- Text segments (~400 tokens each)
- Token and character offsets
- Links back to source document

**Embeddings:**
- 1024-dimensional vectors (Cohere embed-v4.0)
- Model name for versioning
- Links to chunks

## Database Tables

`sources.chunks`
- search_result_id, chunk_index, text
- Token positions: start_token, end_token, token_count
- Character positions: char_start, char_end

`sources.embeddings`
- chunk_id, model, embedding (FLOAT8[])

## Notes

- Scripts skip already-processed items (idempotent)
- Only processes successfully fetched results
- Embeddings use Cohere embed-v4.0 model
- 1 second pause between API batches for rate limiting
- Text truncated to 3000 chars before embedding

## Downstream Usage

Services use semantic search to find relevant chunks:

```python
# Pseudo-code for service
query_embedding = embed("birth year")
similar_chunks = find_nearest(query_embedding, person_name)
# Process chunks with LLM
```