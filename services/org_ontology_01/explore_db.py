import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

from database.connection import get_connection, release_connection

def check_provenance():
    conn = get_connection()
    try:
        cur = conn.cursor()
        
        print("CHECKING PROVENANCE DATA")
        print("=" * 80)
        
        print("\n1. Sources.search_results provenance fields:")
        cur.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(extraction_method) as has_extraction_method,
                COUNT(extraction_quality) as has_extraction_quality,
                COUNT(CASE WHEN needs_ocr = true THEN 1 END) as needs_ocr_true,
                COUNT(provenance_narrative) as has_provenance_narrative
            FROM sources.search_results
        """)
        row = cur.fetchone()
        print(f"  Total rows: {row[0]:,}")
        print(f"  Has extraction_method: {row[1]:,}")
        print(f"  Has extraction_quality: {row[2]:,}")
        print(f"  Needs OCR: {row[3]:,}")
        print(f"  Has provenance_narrative: {row[4]:,}")
        
        if row[4] > 0:
            print("\n2. Sample provenance narrative:")
            cur.execute("""
                SELECT provenance_narrative 
                FROM sources.search_results 
                WHERE provenance_narrative IS NOT NULL 
                LIMIT 1
            """)
            narrative = cur.fetchone()[0]
            print(f"  {narrative[:500]}...")
        
        print("\n3. Sources.chunks - any provenance metadata?")
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'sources' 
            AND table_name = 'chunks'
            ORDER BY ordinal_position
        """)
        columns = cur.fetchall()
        print("  Columns:")
        for col_name, col_type in columns:
            print(f"    {col_name}: {col_type}")
        
        print("\n4. Sources.embeddings - any provenance metadata?")
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'sources' 
            AND table_name = 'embeddings'
            ORDER BY ordinal_position
        """)
        columns = cur.fetchall()
        print("  Columns:")
        for col_name, col_type in columns:
            print(f"    {col_name}: {col_type}")
        
        cur.execute("SELECT DISTINCT model FROM sources.embeddings")
        models = [row[0] for row in cur.fetchall()]
        print(f"\n  Embedding models used: {', '.join(models)}")
        
    finally:
        release_connection(conn)

if __name__ == "__main__":
    check_provenance()