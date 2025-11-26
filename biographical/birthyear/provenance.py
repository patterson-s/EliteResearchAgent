from datetime import datetime
from typing import Dict, List, Any

def generate_provenance_narrative(
    person_name: str,
    retrieval_results: List[Dict[str, Any]],
    extractions: List[Dict[str, Any]],
    verification: Dict[str, Any],
    timestamp: datetime
) -> str:
    lines = []
    
    lines.append(f"Birth year verification for {person_name}")
    lines.append(f"Verification completed: {timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    lines.append("")
    
    lines.append("RETRIEVAL PHASE")
    lines.append(f"Retrieved {len(retrieval_results)} candidate chunks using semantic search with Cohere Embed v4.0")
    lines.append(f"Re-ranked candidates using Cohere Rerank v3.5")
    
    if retrieval_results:
        lines.append(f"Top candidate domains: {', '.join(set(r['domain'] for r in retrieval_results[:5]))}")
        lines.append(f"Similarity scores ranged from {min(r['similarity'] for r in retrieval_results):.3f} to {max(r['similarity'] for r in retrieval_results):.3f}")
        lines.append(f"Rerank scores ranged from {min(r['rerank_score'] for r in retrieval_results):.3f} to {max(r['rerank_score'] for r in retrieval_results):.3f}")
    
    lines.append("")
    lines.append("EXTRACTION PHASE")
    lines.append(f"Scanned {len(extractions)} chunks for birth information")
    
    found_count = sum(1 for e in extractions if e.get("contains_birth_info"))
    lines.append(f"Found birth information in {found_count} chunks")
    
    if found_count > 0:
        years_found = [e["extracted_year"] for e in extractions if e.get("extracted_year")]
        if years_found:
            lines.append(f"Years extracted: {', '.join(str(y) for y in sorted(set(years_found)))}")
    
    lines.append("")
    lines.append("VERIFICATION PHASE")
    lines.append(f"Verification status: {verification['verification_status']}")
    lines.append(f"Independent source count: {verification['independent_source_count']}")
    
    if verification.get("birth_year"):
        lines.append(f"Verified birth year: {verification['birth_year']}")
        
        year_ledgers = verification.get("year_ledgers", {})
        winner_ledger = year_ledgers.get(verification["birth_year"])
        
        if winner_ledger:
            lines.append(f"Supporting domains: {', '.join(winner_ledger['domains'])}")
            lines.append(f"Supporting extractions: {len(winner_ledger['extractions'])}")
            
            evidence_types = [e.get("evidence_type") for e in winner_ledger["extractions"] if e.get("evidence_type")]
            if evidence_types:
                lines.append(f"Evidence types: {', '.join(set(evidence_types))}")
        
        if len(year_ledgers) > 1:
            lines.append("")
            lines.append("CONFLICT DETAILS")
            for year, ledger in year_ledgers.items():
                if year != verification["birth_year"]:
                    lines.append(f"Alternative year {year} found in {ledger['count']} source(s): {', '.join(ledger['domains'])}")
    
    else:
        lines.append("No birth year could be verified")
        
        year_ledgers = verification.get("year_ledgers", {})
        if year_ledgers:
            lines.append("")
            lines.append("PARTIAL EVIDENCE")
            for year, ledger in year_ledgers.items():
                lines.append(f"Year {year} found in {ledger['count']} source(s): {', '.join(ledger['domains'])}")
    
    lines.append("")
    lines.append("TRACEABILITY")
    lines.append(f"All extractions link to chunks in sources.chunks table via chunk_id")
    lines.append(f"All chunks link to search results in sources.search_results table")
    lines.append(f"All search results link to search operations in sources.persons_searched table")
    lines.append(f"Complete provenance chain maintained from verification back to original search")
    
    return "\n".join(lines)

def generate_extraction_provenance(
    extraction: Dict[str, Any],
    chunk_info: Dict[str, Any],
    timestamp: datetime
) -> str:
    lines = []
    
    lines.append(f"Birth year extraction from chunk {extraction['chunk_id']}")
    lines.append(f"Extraction timestamp: {timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    lines.append("")
    
    lines.append(f"Source URL: {chunk_info.get('url', 'unknown')}")
    lines.append(f"Source domain: {chunk_info.get('domain', 'unknown')}")
    lines.append(f"Chunk index: {chunk_info.get('chunk_index', 'unknown')}")
    lines.append(f"Extraction method: {chunk_info.get('extraction_method', 'unknown')}")
    lines.append("")
    
    lines.append(f"Person: {extraction['person_name']}")
    lines.append(f"Contains birth info: {extraction['contains_birth_info']}")
    lines.append(f"Extracted year: {extraction.get('extracted_year', 'None')}")
    lines.append(f"Evidence type: {extraction.get('evidence_type', 'None')}")
    lines.append("")
    
    lines.append("LLM REASONING:")
    lines.append(extraction.get('reasoning', 'No reasoning provided'))
    lines.append("")
    
    lines.append("This extraction links to:")
    lines.append(f"- Chunk ID: {extraction['chunk_id']} in sources.chunks")
    lines.append("- Through chunk, traces back to search_result_id in sources.search_results")
    lines.append("- Through search result, traces back to person_searched_id in sources.persons_searched")
    
    return "\n".join(lines)

if __name__ == "__main__":
    from datetime import datetime
    
    test_retrieval = [
        {"chunk_id": 1, "domain": "wikipedia.org", "similarity": 0.85, "rerank_score": 0.92},
        {"chunk_id": 2, "domain": "britannica.com", "similarity": 0.78, "rerank_score": 0.88}
    ]
    
    test_extractions = [
        {
            "chunk_id": 1,
            "person_name": "Test Person",
            "contains_birth_info": True,
            "extracted_year": 1950,
            "evidence_type": "born-field",
            "reasoning": "Found explicit date of birth field"
        }
    ]
    
    test_verification = {
        "verification_status": "verified",
        "birth_year": 1950,
        "independent_source_count": 2,
        "total_extractions": 2,
        "year_ledgers": {
            1950: {
                "year": 1950,
                "count": 2,
                "domains": ["wikipedia.org", "britannica.com"],
                "extractions": []
            }
        }
    }
    
    narrative = generate_provenance_narrative(
        "Test Person",
        test_retrieval,
        test_extractions,
        test_verification,
        datetime.utcnow()
    )
    
    print(narrative)