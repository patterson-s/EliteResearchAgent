# Search Module

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