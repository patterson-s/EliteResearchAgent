# CareerFinder Base v01

## Purpose

Extract ALL career events from biographical text in standardized format.

**Goal: Maximum recall, not precision.**

This is the first layer in the career extraction pipeline. Deduplication, verification, and enrichment happen in separate downstream services.

## Career Event Definition

A discrete career event is defined by:
- **Organization**: which entity (government, company, university, NGO, etc.)
- **Role/Function**: what the person was doing (position, responsibility)
- **Location**: where this took place (city, country, embassy)

If ANY of these three change → new career event.

## Architecture

Single-stage extraction pipeline:
1. Load all chunks for person
2. Extract events from each chunk independently
3. Save all raw events to review folder

No filtering, no deduplication, no verification at this stage.

## What Gets Extracted

Per event:
- `organization` (string or null)
- `role` (string or null)
- `location` (string or null)
- `start_date` (YYYY format or null)
- `end_date` (YYYY format or null)
- `description` (string or null)
- `supporting_quotes` (array of verbatim quotes from source)
- `chunk_id` (provenance)
- `source_url` (provenance)

**Partial events allowed** - not all fields required.
**Supporting quotes required** - at least one per event.

## Usage

### Single Person

```bash
python pipeline.py \
  --person "Kofi Annan" \
  --chunks /path/to/chunks.json
```

### With Custom Config

```bash
python pipeline.py \
  --person "Kofi Annan" \
  --chunks /path/to/chunks.json \
  --config config/config.json \
  --output custom_review/
```

### Test Extraction Module

```bash
python extraction.py \
  --person "Test Person" \
  --chunk-id "test_001" \
  --text "He served as Ambassador to Thailand from 1985 to 1990." \
  --url "http://example.com"
```

## Output Format

Saved to: `review/careerfinder_base_PersonName_TIMESTAMP.json`

```json
{
  "person_name": "Kofi Annan",
  "timestamp": "2025-12-17T10:30:00",
  "config": {
    "service_name": "careerfinder_base_01",
    "version": "1.0.0",
    "model": "command-a-03-2025"
  },
  "raw_extractions": [
    {
      "organization": "United Nations",
      "role": "Secretary-General",
      "location": "New York",
      "start_date": "1997",
      "end_date": "2006",
      "description": "Led the UN during post-Cold War period",
      "supporting_quotes": [
        "served as Secretary-General from 1997 to 2006"
      ],
      "chunk_id": "chunk_123",
      "source_url": "https://en.wikipedia.org/wiki/Kofi_Annan"
    }
  ],
  "chunks_processed": 45,
  "events_extracted": 87
}
```

## Configuration

Edit `config/config.json`:

```json
{
  "service_name": "careerfinder_base_01",
  "version": "1.0.0",
  "model": "command-a-03-2025",
  "temperature": 0.2,
  "max_tokens": 2000,
  "api_key_env_var": "COHERE_API_KEY"
}
```

## File Structure

```
services/careerfinder_base_01/
├── config/
│   ├── config.json
│   └── prompts/
│       ├── system.txt
│       └── user.txt
├── review/                    # Output folder (gitignored)
│   └── careerfinder_base_*.json
├── load_data.py               # Load chunks for person
├── extraction.py              # LLM extraction
├── pipeline.py                # Main orchestrator
└── README.md
```

## Design Principles Applied

- ✓ Review folder pattern (no direct DB writes)
- ✓ Complete provenance (chunk_id + source_url)
- ✓ Configuration-driven
- ✓ Modular components
- ✓ Type hints throughout
- ✓ Error handling per chunk

## Future Steps

1. **Deduplication service** - Merge overlapping events
2. **Verification service** - Cross-source validation
3. **Enrichment service** - Add metadata (metatype, type, tags)
4. **Database loading** - `load_review.py` to commit verified events
5. **Batch processing** - Process multiple people

## Cost Estimates

Per person (100 chunks):
- 100 extraction calls × ~2000 tokens output
- Approximately $0.05-0.10 per person

## Notes

- Designed for recall, not precision
- Duplicates are expected and desired
- Filtering happens in downstream services
- Quotes provide traceability to source text