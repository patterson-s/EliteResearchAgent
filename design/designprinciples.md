# Design Principles for Prosopographical Research Services

## Core Philosophy

Build modular, auditable extraction pipelines that prioritize data quality and provenance over speed. Every analytical output must be traceable back to original sources through complete documentation chains.

## 1. Review-First Pattern

**Never write directly to the database from extraction pipelines.**

- All outputs save to `review/` folder as JSON first
- Human inspection checkpoint before database commit
- Separate `load_review.py` script for manual database loading
- Prevents bad extractions from polluting the database
- Enables iterative refinement of prompts and logic
```python
# Good
output_file = output_dir / f"result_{person}_{timestamp}.json"
with open(output_file, "w") as f:
    json.dump(result, f, indent=2)

# Bad
cursor.execute("INSERT INTO results ...")
conn.commit()
```

## 2. Complete Provenance Tracking

**Every result must link back through the full processing chain.**

Chain structure:
```
verified_result → extraction → chunk → search_result → search_operation
```

Required metadata at each step:
- Timestamps
- Model versions and parameters
- Similarity/relevance scores
- Source identifiers (URLs, domains)
- Processing versions

Implementation:
- Store chunk_id in extraction results
- Maintain search_result_id in chunks table
- Link search results to original search operations
- Generate human-readable provenance narratives

## 3. Source Independence Verification

**Require multiple independent sources to verify facts.**

- Group findings by domain (not just URL)
- Set minimum threshold (typically 2+ sources)
- Track source diversity explicitly
- Use source count for conflict resolution
```python
# Good: Count independent domains
domains = set()
for extraction in extractions:
    domains.add(extraction['domain'])
verified = len(domains) >= 2

# Bad: Accept single source
verified = any(e['contains_info'] for e in extractions)
```

## 4. Vote-Based Triangulation

**Resolve conflicts through voting with quality tiebreakers.**

Decision hierarchy:
1. Vote count (most independent sources wins)
2. Evidence quality ranking (explicit fields > narrative > categories)
3. Mark as inconclusive if tied
```python
QUALITY_RANKS = {
    "explicit-field": 0,
    "narrative-mention": 1,
    "inferred": 2,
    "category": 3
}
```

Status taxonomy:
- `verified` - Single value, 2+ sources, all agree
- `conflict_resolved` - Multiple values, winner has 2+ sources
- `no_corroboration` - Found value but only 1 source
- `conflict_inconclusive` - Multiple values, no clear winner
- `no_evidence` - Nothing found

## 5. Modular Pipeline Architecture

**Build services as composable stages with clear interfaces.**

Standard pipeline stages:
1. **Load** - Query database once, cache locally
2. **Retrieve** - Semantic search + rerank for relevant chunks
3. **Extract** - LLM-based structured extraction per chunk
4. **Verify** - Cross-source triangulation and conflict resolution
5. **Provenance** - Generate audit narrative
6. **Review** - Save JSON outputs for inspection
7. **Load** - Manual database commit after review

Each stage:
- Standalone script with test harness
- Clear input/output contracts
- Can be tested independently
- Receives configuration object
```python
# Each stage has this structure
def stage_function(input_data, config_path):
    config = load_config(config_path)
    # Process
    return output_data

if __name__ == "__main__":
    # Test harness
    parser = argparse.ArgumentParser()
    # ...
```

## 6. Configuration-Driven Design

**All tunable parameters in external config files.**

Config file structure:
```json
{
  "service_name": "birth_year_verification",
  "version": "1.0.0",
  "retrieval": {
    "initial_candidates": 30,
    "rerank_top_k": 10,
    "min_similarity": 0.2,
    "query_template": "..."
  },
  "extraction": {
    "model": "command-a-03-2025",
    "temperature": 0.2,
    "max_tokens": 400
  },
  "verification": {
    "min_independent_sources": 2,
    "max_chunks_to_scan": 10,
    "early_stop_on_verified": true
  }
}
```

Prompts in separate text files:
- `config/prompts/system.txt`
- `config/prompts/user.txt`

Benefits:
- Tune without code changes
- Track configurations in version control
- Easy A/B testing
- Document prompt evolution

## 7. Early Stopping for Cost Control

**Stop processing when verification achieved.**

Implementation:
- After each extraction, check if verification criteria met
- Stop immediately if 2+ independent sources agree
- Track chunks_scanned vs total_available in results
```python
for chunk in candidates[:max_chunks]:
    extraction = extract(chunk)
    extractions.append(extraction)
    
    if early_stop and len(extractions) >= 2:
        verification = verify(extractions)
        if verification['status'] in ['verified', 'conflict_resolved']:
            break
```

Typical savings: 60-70% reduction in API calls

## 8. Checkpoint-Based Data Loading

**Query database once, cache for repeated use.**

Pattern:
1. Run `load_data.py` once to query PostgreSQL
2. Save to pickle file (chunks + embeddings)
3. All subsequent processing loads from pickle
4. Regenerate only when source data changes
```python
# One-time load
df = load_all_chunks_with_embeddings()  # ~10 seconds
df.to_pickle("data/chunks_dataset.pkl")

# Fast reuse
df = pd.read_pickle("data/chunks_dataset.pkl")  # <1 second
```

## 9. Semantic Retrieval + Reranking

**Use two-stage retrieval for precision.**

Stage 1 - Semantic Search:
- Embed query with Cohere Embed v4.0
- Cosine similarity against all person's chunks
- Top N candidates (typically 30)
- Fast, broad coverage

Stage 2 - Reranking:
- Cohere Rerank v3.5 on candidates
- Top K final results (typically 10)
- Slower, higher precision
- Ensures domain diversity

Query design:
- Service-specific (e.g., "date of birth information")
- Person name included
- Stored in config template

## 10. Explicit Error Handling

**Process should continue even when individual items fail.**

Batch processing pattern:
```python
results = {"completed": 0, "failed": 0, "results": []}

for person in people:
    try:
        result = run_pipeline(person, ...)
        results["completed"] += 1
        results["results"].append({"person": person, "status": "success"})
    except Exception as e:
        results["failed"] += 1
        results["results"].append({"person": person, "status": "failed", "error": str(e)})
        print(f"Failed: {person} - {e}")

# Save batch summary
with open(f"batch_summary_{timestamp}.json", "w") as f:
    json.dump(results, f, indent=2)
```

## 11. Database Schema Separation

**Separate raw source data from analytical outputs.**

Schema structure:
- `sources` schema - Raw search/scraping data
  - `persons_searched`
  - `search_results`
  - `chunks`
  - `embeddings`
  
- `services` schema - Verified analytical results
  - `{service}_verifications` (e.g., birth_verifications)
  - `{service}_extractions` (individual attempts)
  - `{service}_verification_evidence` (linking table)

Benefits:
- Clear data lineage
- Can rebuild services without touching sources
- Prevents analytical code from corrupting source data

## 12. Incremental Result Files

**Save one result file per person, not one big file.**

File naming:
```
review/birthyear_Person_Name_20251217_143022.json
review/education_Person_Name_20251217_143145.json
```

Benefits:
- Easy to find specific person's results
- Can process subsets without reprocessing all
- Git-friendly (small diffs)
- Natural checkpoint for resuming batches

## 13. Evidence Quality Stratification

**Rank evidence types by reliability.**

Quality hierarchy (best to worst):
1. Explicit structured fields (born: 1950)
2. Narrative mentions (was born in 1950)
3. Inferred from context
4. Category tags (1950 births)

Use for:
- Tiebreaking in conflict resolution
- Confidence scoring
- Filtering low-quality extractions

## 14. Summary Reports for Stakeholders

**Generate human-readable summaries after batch runs.**

Include:
- Per-person results with status symbols (✓/○)
- Overall statistics (verification rate, conflicts)
- Status breakdown counts
- Methodology notes
- Links to detailed JSON files

Save to timestamped text file for easy sharing.

## 15. Type Hints Throughout

**Use explicit type hints for clarity.**
```python
def retrieve_chunks(
    person_name: str,
    df: pd.DataFrame,
    config_path: Path
) -> List[Dict[str, Any]]:
    pass

def verify_result(
    extractions: List[Dict[str, Any]],
    min_sources: int = 2
) -> Dict[str, Any]:
    pass
```

## Anti-Patterns to Avoid

1. **Monolithic Scripts** - Don't put everything in one file
2. **Direct DB Writes** - Always use review folder first
3. **Magic Numbers** - Put thresholds in config
4. **Lost Provenance** - Never lose link to source chunks
5. **Single Source Trust** - Always verify across sources
6. **Silent Failures** - Log and save error details
7. **Hardcoded Prompts** - Use external template files
8. **All-or-Nothing Batches** - Save progress incrementally

## Service Template Checklist

When building a new biographical service:

- [ ] Create service-specific config.json
- [ ] Write system and user prompt templates
- [ ] Implement retrieval with service query
- [ ] Build extraction with structured output parsing
- [ ] Implement vote-based verification
- [ ] Generate provenance narratives
- [ ] Save to review/ folder
- [ ] Create load_review.py script
- [ ] Add tables to services schema
- [ ] Write batch processing script
- [ ] Create summary report generator
- [ ] Document in MARKDOWN.md