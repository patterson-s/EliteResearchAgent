# Search Module

## Overview

The search module collects, processes, and prepares source material for prosopographical research. It handles the complete pipeline from web search to embedded text chunks ready for downstream analysis.

## Quick Start

**Single command to process everything:**

```bash
python -m search.pipeline names\person_names.json
```

This runs all steps: search → PDF inspection → OCR → chunking → embedding → provenance generation

**Review and load to database:**

```bash
# Review the output in search\review\
python -m search.load_review search\review\search_complete_TIMESTAMP.json
```

## Architecture

```
search/
├── serper/          # Web search and URL fetching
├── ocr/             # PDF quality assessment and OCR refinement
├── embeddings/      # Text chunking and vector generation
├── provenance/      # Narrative provenance tracking
├── review/          # Staging area before database commit
├── pipeline.py      # Orchestrates all steps
└── load_review.py   # Loads reviewed data to PostgreSQL
```

## Complete Workflow

### 1. Prepare Input

Create a JSON file with person names:

```json
["Federica Mogherini", "Kofi Annan", "Amina Mohammed"]
```

### 2. Run Pipeline

```bash
python -m search.pipeline names\person_names.json
```

**What happens:**
- **Step 1:** Searches Serper for each person (up to 20 URLs per person)
- **Step 2:** Fetches and extracts text from each URL (HTML with BeautifulSoup, PDF with PyPDF2)
- **Step 3:** Inspects PDF extractions for quality (flags short or garbled text)
- **Step 4:** Re-extracts poor PDFs using Mistral OCR
- **Step 5:** Chunks all text into ~400 token segments
- **Step 6:** Generates Cohere embeddings for each chunk
- **Step 7:** Creates provenance narratives documenting the full lifecycle

**Output:** `search\review\search_complete_TIMESTAMP.json`

### 3. Review Output

The JSON file in `search\review\` contains everything:
- Search metadata (person, query, timestamp, rank)
- URL and fetch status
- Extracted text (HTML or OCR)
- Text chunks with token positions
- Vector embeddings
- Complete provenance narrative

**Inspect before loading to database** - you can edit or filter results if needed.

### 4. Load to Database

```bash
python -m search.load_review search\review\search_complete_TIMESTAMP.json
```

This inserts into PostgreSQL:
- `sources.persons_searched` - search operations
- `sources.search_results` - URLs and full text with provenance
- `sources.chunks` - text segments
- `sources.embeddings` - vector representations

## Configuration

### Environment Variables

Required in `.env`:
```
SERPER_API_KEY=your_serper_key
COHERE_API_KEY=your_cohere_key
MISTRAL_API_KEY=your_mistral_key
DB_HOST=localhost
DB_PORT=5432
DB_NAME=eliteresearch
DB_USER=postgres
DB_PASSWORD=your_password
```

### Pipeline Options

```bash
python -m search.pipeline names.json --chunk-size 400 --ocr-limit 10
```

Options:
- `--chunk-size` - tokens per chunk (default: 400)
- `--ocr-limit` - max PDFs to OCR (useful for testing, default: no limit)

## Data Flow

```
Input JSON (person names)
    ↓
Serper API → URLs
    ↓
Fetch → Raw HTML/PDF
    ↓
Extract → Full text (PyPDF2 for PDFs)
    ↓
Inspect → Quality assessment
    ↓
OCR → Mistral refinement (if needed)
    ↓
Chunk → ~400 token segments
    ↓
Embed → Cohere vectors
    ↓
Provenance → Narrative generation
    ↓
Review folder → JSON staging
    ↓
Load → PostgreSQL database
```

## Key Features

### Extraction Methods

- **html** - Standard web pages extracted with BeautifulSoup
- **pdf_basic** - PDFs extracted with PyPDF2
- **pdf_ocr** - PDFs re-extracted with Mistral OCR after quality check

### Quality Assessment

PDFs flagged for OCR if:
- Text length < 100 characters
- High ratio of garbled/non-printable characters (>30%)

### Provenance Narratives

Each source gets a human-readable audit trail:

```
This source was collected on 2025-11-25 18:30:15 UTC as part of a search for "Federica Mogherini".
The search query was: Federica Mogherini biography OR CV OR career...
This URL ranked #3 in the search results.
The document was fetched successfully on 2025-11-25 18:30:22 UTC.
Initial extraction method: pdf_basic
Extraction quality assessment: poor (text length: 87 characters)
OCR processing applied on 2025-11-25 18:35:10 UTC using Mistral OCR
Final extraction method: pdf_ocr
This source was chunked into 6 text segments on 2025-11-25 18:40:05 UTC.
Embeddings generated on 2025-11-25 18:41:15 UTC using Cohere embed-v4.0.
```

## Modular Components

Each step can be run independently if needed:

### Search Only
```bash
python -m search.serper.batch names.json
# Output: search\serper\outputs\search_results_TIMESTAMP.json
```

### Inspect PDFs
```bash
python -m search.ocr.inspect_json search\serper\outputs\search_results_TIMESTAMP.json
# Output: search\serper\outputs\search_results_TIMESTAMP_inspected.json
```

### Process with OCR
```bash
python -m search.ocr.process_json search\serper\outputs\search_results_TIMESTAMP_inspected.json --limit 5
# Output: search\serper\outputs\search_results_TIMESTAMP_inspected_ocr.json
```

### Chunk Texts
```bash
python -m search.embeddings.chunk_json input.json --chunk-size 400
# Output: input_chunked.json
```

### Generate Embeddings
```bash
python -m search.embeddings.embed_json input.json --batch-size 64
# Output: input_embedded.json
```

## Database Schema

### sources.persons_searched
- person_name, search_query, searched_at

### sources.search_results
- url, title, fetch_status, full_text
- extraction_method, extraction_quality, needs_ocr
- provenance_narrative

### sources.chunks
- search_result_id, chunk_index, text
- start_token, end_token, token_count
- char_start, char_end

### sources.embeddings
- chunk_id, model, embedding (FLOAT8[])

## Troubleshooting

**Import errors:** Ensure all `__init__.py` files exist in module folders

**OCR fails:** Check MISTRAL_API_KEY in `.env`

**Embedding fails:** Check COHERE_API_KEY and ensure sufficient API credits

**Database connection fails:** Verify PostgreSQL is running and credentials in `.env` are correct

**Null byte errors:** Should be handled automatically by sanitization in load_review.py

## Cost Estimates

- **Serper API:** ~$0.001 per search
- **Mistral OCR:** ~$0.001-0.01 per PDF page
- **Cohere Embeddings:** ~$0.10 per million tokens

For 50 people with 20 URLs each (1000 URLs total):
- Serper: ~$0.05
- OCR (assume 20% need it): ~$2-20
- Embeddings (assume 500K tokens): ~$0.05
- **Total: ~$2-20** (mostly OCR)

## Best Practices

1. **Test with small batches first** - use `--ocr-limit 5` to verify pipeline
2. **Review before loading** - inspect JSON in review folder for quality
3. **Keep review files** - they're your backup before database commit
4. **Monitor OCR usage** - most expensive step, only used when needed
5. **Check provenance narratives** - quick way to audit data quality# Search Module

## Purpose

Collects raw source material from the web for prosopographical research. This is the **collection layer** - it gathers evidence that downstream services will process and extract facts from.

## Structure

```
search/
└── serper/
    ├── batch.py          # Main search script (outputs JSON)
    ├── load_to_db.py     # Loads JSON to PostgreSQL
    ├── client.py         # Serper API wrapper
    ├── fetcher.py        # URL fetching + PDF extraction
    └── outputs/          # JSON files saved here
```

## Workflow

### 1. Search and Fetch

Create a JSON file with person names:
```json
["Federica Mogherini", "Kofi Annan"]
```

Run batch search:
```bash
python -m search.serper.batch names.json
```

This outputs to `search/serper/outputs/search_results_TIMESTAMP.json`

### 2. Inspect Output

Check the JSON file for:
- Encoding errors
- Failed fetches
- Malformed data

Edit if needed before loading to database.

### 3. Load to Database

```bash
python -m search.serper.load_to_db search\serper\outputs\search_results_TIMESTAMP.json
```

## What Gets Collected

For each person:
- Search query used
- Timestamp of search
- Up to 20 URLs from Serper results
- URL rank in search results
- Page title
- Full text (HTML or PDF)
- Fetch status (success/failed)
- Error messages if failed

## Data Flow

```
Person names → Serper API → URLs → Fetch pages → Extract text → JSON → PostgreSQL sources schema
```

## Configuration

Edit constants in `batch.py`:
- `SEARCH_TEMPLATE` - query template
- `MAX_RESULTS` - URLs per person (default 20)

API keys in `.env`:
- `SERPER_API_KEY`

## Notes

- PDFs are automatically extracted to text
- Failed fetches are logged but don't stop the batch
- JSON files are your backup - don't delete them
- Same person can be searched multiple times (timestamped)