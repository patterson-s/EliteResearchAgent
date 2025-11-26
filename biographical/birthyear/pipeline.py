import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional
import argparse
import pandas as pd

from load_data import load_dataset
from retrieval import retrieve_birth_chunks, load_config
from extraction import extract_birth_year
from verification import verify_birth_year
from provenance import generate_provenance_narrative

def run_pipeline(
    person_name: str,
    df: pd.DataFrame,
    config_path: Path,
    output_dir: Optional[Path] = None
) -> Dict[str, Any]:
    if output_dir is None:
        output_dir = Path(__file__).parent / "review"
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    config = load_config(config_path)
    timestamp = datetime.utcnow()
    
    print("=" * 100)
    print(f"Birth Year Verification Pipeline: {person_name}")
    print("=" * 100)
    print(f"Started: {timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
    
    print("STEP 1: RETRIEVAL")
    print("-" * 100)
    retrieval_results = retrieve_birth_chunks(person_name, df, config_path)
    print(f"Retrieved {len(retrieval_results)} candidate chunks")
    
    if retrieval_results:
        print(f"Top 3 candidates:")
        for i, r in enumerate(retrieval_results[:3], 1):
            print(f"  [{i}] sim={r['similarity']:.3f} rerank={r['rerank_score']:.3f} | {r['domain']}")
    else:
        print("No candidates found")
    
    print("\n" + "STEP 2: EXTRACTION")
    print("-" * 100)
    
    max_scan = config["verification"]["max_chunks_to_scan"]
    early_stop = config["verification"]["early_stop_on_verified"]
    
    extractions = []
    verified = False
    
    for i, chunk in enumerate(retrieval_results[:max_scan], 1):
        print(f"[{i}/{min(len(retrieval_results), max_scan)}] Extracting from chunk {chunk['chunk_id']} ({chunk['domain']})")
        
        extraction = extract_birth_year(
            person_name,
            chunk["text"],
            chunk["chunk_id"],
            config_path
        )
        
        extraction["url"] = chunk["url"]
        extraction["domain"] = chunk["domain"]
        extraction["chunk_index"] = chunk["chunk_index"]
        extraction["similarity"] = chunk["similarity"]
        extraction["rerank_score"] = chunk["rerank_score"]
        
        extractions.append(extraction)
        
        if extraction["contains_birth_info"] and extraction["extracted_year"]:
            print(f"     -> Found: year={extraction['extracted_year']} (type={extraction['evidence_type']})")
        else:
            print(f"     -> No birth info")
        
        if early_stop and len(extractions) >= 2:
            temp_verification = verify_birth_year(
                extractions,
                config["verification"]["min_independent_sources"]
            )
            if temp_verification["verification_status"] in ["verified", "conflict_resolved"]:
                print(f"\nEarly stop: verification achieved with {len(extractions)} extractions")
                verified = True
                break
    
    print("\n" + "STEP 3: VERIFICATION")
    print("-" * 100)
    
    verification = verify_birth_year(
        extractions,
        config["verification"]["min_independent_sources"]
    )
    
    print(f"Status: {verification['verification_status']}")
    print(f"Birth year: {verification.get('birth_year', 'None')}")
    print(f"Independent sources: {verification['independent_source_count']}")
    print(f"Total extractions: {verification['total_extractions']}")
    
    if verification.get("year_ledgers"):
        print("\nYear breakdown:")
        for year, ledger in verification["year_ledgers"].items():
            print(f"  {year}: {ledger['count']} sources from {', '.join(ledger['domains'][:3])}")
    
    print("\n" + "STEP 4: PROVENANCE")
    print("-" * 100)
    
    provenance_narrative = generate_provenance_narrative(
        person_name,
        retrieval_results,
        extractions,
        verification,
        timestamp
    )
    
    print("Generated provenance narrative")
    
    result = {
        "person_name": person_name,
        "timestamp": timestamp.isoformat(),
        "config": {
            "service_name": config["service_name"],
            "version": config["version"],
            "model": config["extraction"]["model"]
        },
        "retrieval": {
            "candidates_retrieved": len(retrieval_results),
            "top_candidates": [
                {
                    "chunk_id": r["chunk_id"],
                    "domain": r["domain"],
                    "url": r["url"],
                    "similarity": r["similarity"],
                    "rerank_score": r["rerank_score"]
                }
                for r in retrieval_results[:5]
            ]
        },
        "extraction": {
            "chunks_scanned": len(extractions),
            "extractions": extractions
        },
        "verification": verification,
        "provenance_narrative": provenance_narrative
    }
    
    output_file = output_dir / f"birthyear_{person_name.replace(' ', '_')}_{timestamp.strftime('%Y%m%d_%H%M%S')}.json"
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    
    print(f"\nSaved to: {output_file.resolve()}")
    print("\n" + "=" * 100)
    print("Pipeline complete")
    print("=" * 100)
    
    return result

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Birth year verification pipeline"
    )
    parser.add_argument(
        "--person",
        required=True,
        help="Person name to verify"
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/config.json"),
        help="Path to config file"
    )
    parser.add_argument(
        "--data",
        type=Path,
        default=Path("data/chunks_dataset.pkl"),
        help="Path to dataset pickle file"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output directory (default: review/)"
    )
    
    args = parser.parse_args()
    
    print("Loading dataset...")
    df = load_dataset(args.data)
    print(f"Loaded {len(df)} chunks for {df['person_name'].nunique()} people\n")
    
    run_pipeline(args.person, df, args.config, args.output)