import os
import json
import argparse
from pathlib import Path
from typing import Dict, Any
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parent.parent.parent / ".env"
load_dotenv(env_path)

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        database=os.getenv("DB_NAME", "eliteresearch"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD")
    )

def load_verification_to_db(data: Dict[str, Any]) -> None:
    conn = get_db_connection()
    
    try:
        with conn.cursor() as cur:
            person_name = data["person_name"]
            verification = data["verification"]
            extractions = data["extraction"]["extractions"]
            
            cur.execute("""
                DELETE FROM services.birth_verifications
                WHERE person_name = %s
            """, (person_name,))
            
            cur.execute("""
                INSERT INTO services.birth_verifications (
                    person_name,
                    birth_year,
                    verification_status,
                    independent_source_count,
                    total_extractions_attempted,
                    provenance_narrative,
                    verified_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING verification_id
            """, (
                person_name,
                verification.get("birth_year"),
                verification["verification_status"],
                verification["independent_source_count"],
                verification["total_extractions"],
                data["provenance_narrative"],
                data["timestamp"]
            ))
            
            verification_id = cur.fetchone()[0]
            
            extraction_ids = []
            for extraction in extractions:
                cur.execute("""
                    INSERT INTO services.birth_extractions (
                        chunk_id,
                        person_name,
                        extracted_year,
                        contains_birth_info,
                        evidence_type,
                        extraction_timestamp,
                        model_used,
                        raw_llm_output,
                        reasoning
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING extraction_id
                """, (
                    extraction["chunk_id"],
                    extraction["person_name"],
                    extraction.get("extracted_year"),
                    extraction["contains_birth_info"],
                    extraction.get("evidence_type"),
                    data["timestamp"],
                    data["config"]["model"],
                    extraction.get("raw_llm_output"),
                    extraction.get("reasoning")
                ))
                
                extraction_id = cur.fetchone()[0]
                extraction_ids.append(extraction_id)
            
            if extraction_ids:
                evidence_rows = [
                    (verification_id, ext_id, 1)
                    for ext_id in extraction_ids
                ]
                
                execute_values(
                    cur,
                    """
                    INSERT INTO services.birth_verification_evidence 
                    (verification_id, extraction_id, evidence_weight)
                    VALUES %s
                    """,
                    evidence_rows
                )
            
            conn.commit()
            
            print(f"✓ Loaded verification for {person_name}")
            print(f"  - Verification ID: {verification_id}")
            print(f"  - Status: {verification['verification_status']}")
            print(f"  - Birth year: {verification.get('birth_year', 'None')}")
            print(f"  - Extractions: {len(extraction_ids)}")
    
    except Exception as e:
        conn.rollback()
        raise e
    
    finally:
        conn.close()

def main():
    parser = argparse.ArgumentParser(
        description="Load birth year verification results to PostgreSQL"
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="Path to review JSON file"
    )
    
    args = parser.parse_args()
    
    if not args.input_file.exists():
        print(f"Error: File not found: {args.input_file}")
        return
    
    print("=" * 80)
    print("Loading Birth Year Verification to Database")
    print("=" * 80)
    print(f"Input file: {args.input_file}")
    print()
    
    with open(args.input_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    load_verification_to_db(data)
    
    print()
    print("=" * 80)
    print("Load complete")
    print("=" * 80)

if __name__ == "__main__":
    main()