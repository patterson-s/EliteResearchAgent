# Birth Year Verification Service - User Guide

## Overview

This service extracts and verifies birth years from prosopographical sources using:
- Semantic search with Cohere embedding and re-ranking
- LLM-based extraction (Cohere Command-A)
- Vote-based triangulation requiring 2+ independent sources
- Complete provenance tracking

## Quick Start

### One-Time Setup

1. **Load dataset from database:**
   ```bash
   python load_data.py --output data/chunks_dataset.pkl --stats
   ```
   This queries PostgreSQL once and caches all chunks + embeddings (~2700 chunks for 75 people).
   Takes ~10-15 seconds.

2. **Verify dataset loaded:**
   Check that `data/chunks_dataset.pkl` exists and stats look correct.

### Running Verification

#### Full Dataset (All 75 People)

```bash
# 1. Get list of all people
python get_all_people.py

# 2. Run batch verification
python batch.py all_people.json --output review

# 3. Generate summary report
python summarize_results.py
```

**Expected time:** ~20-30 minutes for 75 people
**Output:** 
- `review/birthyear_PERSON_TIMESTAMP.json` (one per person)
- `review/batch_summary_TIMESTAMP.json`
- `review/summary_report_TIMESTAMP.txt`

#### Subset of People

Create a custom JSON file:
```json
["Kofi Annan", "Amina J. Mohammed", "Federica Mogherini"]
```

Run batch:
```bash
python batch.py my_people.json --output review
python summarize_results.py
```

#### Single Person

```bash
python pipeline.py --person "Kofi Annan" --data data/chunks_dataset.pkl
```

## Understanding Results

### Verification Status

- **verified** - Found birth year in 2+ independent sources (by domain), all agree
- **conflict_resolved** - Found in 2+ sources but some disagreement, resolved by vote count or quality
- **no_corroboration** - Found birth year but only in 1 source
- **conflict_inconclusive** - Multiple conflicting years, couldn't resolve
- **no_evidence** - No birth information found in any scanned chunks

### Success Metrics

**Target:** 60-80% verification rate (verified + conflict_resolved)

From test run (5 people):
- Verification achieved: 4/5 (80%)
- Birth year found: 4/5 (80%)

### Output Files

#### Individual Results (`review/birthyear_PERSON_TIMESTAMP.json`)

Contains:
```json
{
  "person_name": "Abhijit Banerjee",
  "timestamp": "2025-11-26T00:26:47",
  "retrieval": {
    "candidates_retrieved": 10,
    "top_candidates": [...]
  },
  "extraction": {
    "chunks_scanned": 2,
    "extractions": [...]
  },
  "verification": {
    "verification_status": "verified",
    "birth_year": 1961,
    "independent_source_count": 2,
    "year_ledgers": {...}
  },
  "provenance_narrative": "..."
}
```

**Key fields:**
- `verification.birth_year` - The verified year (or null)
- `verification.verification_status` - Outcome classification
- `verification.year_ledgers` - Details on all years found with source counts
- `provenance_narrative` - Human-readable audit trail

#### Summary Report (`review/summary_report_TIMESTAMP.txt`)

Text file with:
- Individual results for each person
- Overall statistics
- Status breakdown
- Verification rate

**TODO:** Expand summary to include:
- Verification rate by evidence type
- Average chunks scanned per person
- Most common sources (domains)
- Distribution of birth years found
- Early stop effectiveness metrics

## Configuration

Edit `config/config.json`:

```json
{
  "retrieval": {
    "initial_candidates": 30,    // Initial semantic search results
    "rerank_top_k": 10,           // Final re-ranked candidates
    "min_similarity": 0.2         // Cosine similarity threshold
  },
  "extraction": {
    "model": "command-a-03-2025",
    "temperature": 0.2,           // Low for consistency
    "max_tokens": 400
  },
  "verification": {
    "min_independent_sources": 2, // Verification threshold
    "max_chunks_to_scan": 10,     // Cost control
    "early_stop_on_verified": true // Stop when verified
  }
}
```

**Tuning guidance:**
- Increase `initial_candidates` if verification rate is low (more coverage)
- Increase `max_chunks_to_scan` if many "no_corroboration" results (needs more sources)
- Decrease `min_similarity` if retrieval returns too few candidates (lower quality bar)
- Set `early_stop_on_verified: false` to always scan all chunks (higher cost, more thorough)

## Workflow Details

### 1. Load Dataset (One Time)

```bash
python load_data.py --output data/chunks_dataset.pkl --stats
```

**What it does:**
- Queries PostgreSQL for all chunks + embeddings
- Saves to pickle file for fast re-use
- Prints dataset statistics

**When to re-run:**
- After adding new people to search data
- After re-processing embeddings
- Dataset file corrupted/deleted

### 2. Retrieval (Per Person)

**Semantic search:**
- Query: "date of birth or birth information of {person_name}"
- Cohere Embed v4.0 generates query embedding
- Cosine similarity against all chunks for that person
- Top 30 candidates by similarity

**Re-ranking:**
- Cohere Rerank v3.5 on the 30 candidates
- Top 10 by relevance score
- Ensures diversity across domains

### 3. Extraction (Per Chunk)

**LLM prompt:**
- System prompt: Extraction instructions
- User prompt: Person name + chunk text
- Model: Cohere Command-A (temperature 0.2)

**Output parsing:**
- `contains_birthdate: true|false`
- `birth_year: YYYY or null`
- Evidence type classification

**Early stopping:**
- After scanning 2+ chunks, check if verification achieved
- Stops immediately if 2+ independent sources agree
- Typical: 2-5 chunks scanned vs 10 max

### 4. Verification (After Extraction)

**Vote counting:**
- Group extractions by year
- Count independent sources (by domain)
- Track evidence quality (born-field > born-narrative > other > category)

**Decision logic:**
1. If any year has 2+ sources → verified (or conflict_resolved if multiple years)
2. If only 1 year found with <2 sources → no_corroboration
3. If multiple years with ties → conflict_inconclusive
4. If no years found → no_evidence

**Conflict resolution:**
- Primary: Vote count (more sources wins)
- Tiebreaker: Evidence quality (born-field beats category)

### 5. Provenance Generation

Creates human-readable narrative documenting:
- Retrieval scores and domains
- Extraction results and evidence types
- Verification logic and conflict resolution
- Traceability back to chunk IDs → sources → search operations

## Cost Estimates

Per person (with early stopping):
- Query embedding: ~$0.00001
- Re-ranking: ~$0.0001
- Extractions (avg 3): ~$0.0003
- **Total: ~$0.0004 per person**

Full dataset (75 people): ~$0.03

Without early stopping (all 10 chunks):
- Per person: ~$0.001
- Full dataset: ~$0.075

## Troubleshooting

### Low Verification Rate (<50%)

**Possible causes:**
1. Source data lacks birth information
2. Retrieval not finding relevant chunks
3. Extraction prompt needs refinement

**Diagnosis:**
- Manually check a few "no_evidence" cases
- Look at retrieved chunk text - does it contain birth info?
- If yes → extraction prompt issue
- If no → retrieval issue or data gap

**Fixes:**
- Increase `initial_candidates` and `rerank_top_k`
- Lower `min_similarity` threshold
- Refine extraction prompt

### Many "no_corroboration" Results

Birth year found but only in 1 source.

**Fix:**
- Increase `max_chunks_to_scan` (scan more chunks)
- Disable `early_stop_on_verified` (scan all chunks even after finding one)

### High API Costs

**Reduce costs:**
- Enable `early_stop_on_verified` (stops at 2 sources)
- Decrease `max_chunks_to_scan` (fewer extractions)
- Decrease `rerank_top_k` (fewer candidates)

### Dataset Out of Date

After adding new people to search data:

```bash
rm data/chunks_dataset.pkl
python load_data.py --output data/chunks_dataset.pkl --stats
```

## File Structure

```
biographical/birthyear/
├── config/
│   ├── config.json           # Service configuration
│   └── prompts/
│       ├── system.txt        # LLM system prompt
│       └── user.txt          # LLM user prompt template
├── data/
│   └── chunks_dataset.pkl    # Cached dataset from DB
├── review/                   # Output folder (gitignored)
│   ├── birthyear_*.json      # Individual results
│   ├── batch_summary_*.json  # Batch metadata
│   └── summary_report_*.txt  # Summary statistics
├── archive/                  # Test files (optional)
│   ├── get_test_people.py
│   ├── run_test.py
│   └── TEST.md
├── load_data.py              # Load dataset from DB
├── get_all_people.py         # Get all person names
├── retrieval.py              # Semantic search + rerank
├── extraction.py             # LLM extraction
├── verification.py           # Vote-based verification
├── provenance.py             # Narrative generation
├── pipeline.py               # Main orchestrator
├── batch.py                  # Batch processing
├── summarize_results.py      # Generate summary report
├── load_review.py            # Load to PostgreSQL (future)
├── requirements.txt          # Dependencies
├── README.md                 # Technical documentation
├── INSTALL.md                # Setup guide
└── schema.sql                # PostgreSQL schema (future)
```

## Next Steps

### Immediate
1. Run on full dataset (75 people)
2. Review verification rate and quality
3. Adjust configuration if needed

### Future Enhancements
- **Expand summary report** with detailed metrics
- **Database loading** - Load verified results to PostgreSQL
- **Batch parallelization** - Process multiple people simultaneously
- **Birth location extraction** - Extend to extract place of birth
- **Confidence scoring** - Add confidence estimates per extraction
- **Evaluation interface** - Streamlit app for human review

## Support

**Common issues:**
- Environment variable errors → Check `.env` file in project root
- Import errors → Ensure all `__init__.py` files exist
- API errors → Check Cohere API key and credits
- Database errors → Verify PostgreSQL running and credentials correct

**Debug mode:**
Add `--stats` flag to `load_data.py` for detailed dataset information.

**Logs:**
Pipeline prints detailed progress. Redirect to file:
```bash
python batch.py all_people.json > batch_run.log 2>&1
```