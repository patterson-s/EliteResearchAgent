import os
import json
import argparse
from pathlib import Path
from typing import Dict, Any, List
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

def create_schema_if_not_exists(conn) -> None:
    schema_path = Path(__file__).parent / "schema.sql"
    
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'services' 
                AND table_name = 'birth_verifications'
            )
        """)
        
        exists = cur.fetchone()[0]
        
        if not exists:
            print("Creating schema tables...")
            with open(schema_path, "r", encoding="utf-8") as f:
                schema_sql = f.read()
            cur.execute(schema_sql)
            conn.commit()
            print("Schema created successfully\n")
        else:
            print("Schema tables already exist\n")

def load_verification_to_db(data: Dict[str, Any], conn) -> None:
    with conn.cursor() as cur:
        person_name = data["person_name"]
        verification = data["verification"]
        timestamp = data["timestamp"]
        config = data["config"]
        
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
            timestamp
        ))
        
        verification_id = cur.fetchone()[0]
        
        extractions = data["extraction"]["extractions"]
        positive_extractions = [e for e in extractions if e.get("contains_birth_info")]
        
        extraction_ids = []
        for extraction in positive_extractions:
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
                timestamp,
                config["model"],
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
        
        return verification_id, len(extraction_ids)

def load_directory(review_dir: Path) -> None:
    json_files = sorted(review_dir.glob("birthyear_*.json"))
    
    if not json_files:
        print(f"No birthyear_*.json files found in {review_dir}")
        return
    
    print(f"Found {len(json_files)} files to process")
    print("=" * 80)
    
    conn = get_db_connection()
    
    try:
        create_schema_if_not_exists(conn)
        
        success_count = 0
        failed_count = 0
        total_extractions = 0
        
        for i, json_file in enumerate(json_files, 1):
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                
                verification_id, extraction_count = load_verification_to_db(data, conn)
                
                person_name = data["person_name"]
                status = data["verification"]["verification_status"]
                birth_year = data["verification"].get("birth_year")
                
                print(f"[{i}/{len(json_files)}] {person_name}")
                print(f"  Status: {status}")
                print(f"  Birth year: {birth_year if birth_year else 'None'}")
                print(f"  Extractions stored: {extraction_count}")
                print(f"  Verification ID: {verification_id}")
                
                success_count += 1
                total_extractions += extraction_count
                
            except Exception as e:
                failed_count += 1
                print(f"[{i}/{len(json_files)}] FAILED: {json_file.name}")
                print(f"  Error: {e}")
                conn.rollback()
                continue
        
        conn.commit()
        
        print("\n" + "=" * 80)
        print("Batch Load Complete")
        print("=" * 80)
        print(f"Successfully loaded: {success_count}/{len(json_files)}")
        print(f"Failed: {failed_count}/{len(json_files)}")
        print(f"Total extractions stored: {total_extractions}")
        
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
        "review_dir",
        type=Path,
        help="Directory containing birthyear_*.json files"
    )
    
    args = parser.parse_args()
    
    if not args.review_dir.exists():
        print(f"Error: Directory not found: {args.review_dir}")
        return
    
    if not args.review_dir.is_dir():
        print(f"Error: Not a directory: {args.review_dir}")
        return
    
    print("=" * 80)
    print("Loading Birth Year Verifications to Database")
    print("=" * 80)
    print(f"Review directory: {args.review_dir}")
    print()
    
    load_directory(args.review_dir)

if __name__ == "__main__":
    main()