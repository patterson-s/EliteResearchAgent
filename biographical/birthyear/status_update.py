import os
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

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

def get_status_summary():
    conn = get_db_connection()
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT 
                    verification_status,
                    COUNT(*) as count,
                    COUNT(CASE WHEN birth_year IS NOT NULL THEN 1 END) as with_year
                FROM services.birth_verifications
                GROUP BY verification_status
                ORDER BY verification_status
            """)
            
            status_counts = cur.fetchall()
            
            cur.execute("""
                SELECT COUNT(*) as total
                FROM services.birth_verifications
            """)
            
            total = cur.fetchone()["total"]
            
            return status_counts, total
    
    finally:
        conn.close()

def main():
    print("=" * 80)
    print("Birth Year Verification Status Report")
    print("=" * 80)
    print()
    
    status_counts, total = get_status_summary()
    
    if total == 0:
        print("No verification records found in database")
        return
    
    validated = 0
    single_source = 0
    no_birthdate = 0
    other = 0
    
    print("Status Breakdown:")
    print("-" * 80)
    
    for row in status_counts:
        status = row["verification_status"]
        count = row["count"]
        with_year = row["with_year"]
        
        pct = (count / total) * 100
        
        print(f"{status:25} {count:3} ({pct:5.1f}%)  [{with_year} with year]")
        
        if status in ["verified", "conflict_resolved"]:
            validated += count
        elif status == "no_corroboration":
            single_source += count
        elif status == "no_evidence":
            no_birthdate += count
        else:
            other += count
    
    print()
    print("=" * 80)
    print("Summary")
    print("=" * 80)
    print(f"Total people processed:    {total}")
    print()
    print(f"Validated (2+ sources):    {validated:3} ({(validated/total)*100:5.1f}%)")
    print(f"Single source only:        {single_source:3} ({(single_source/total)*100:5.1f}%)")
    print(f"No birthdate found:        {no_birthdate:3} ({(no_birthdate/total)*100:5.1f}%)")
    if other > 0:
        print(f"Other (inconclusive):      {other:3} ({(other/total)*100:5.1f}%)")
    print()
    print(f"Birth year found:          {validated + single_source:3} ({((validated + single_source)/total)*100:5.1f}%)")
    print("=" * 80)

if __name__ == "__main__":
    main()