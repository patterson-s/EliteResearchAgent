"""Setup script to create the prosopography database schema."""

import sys
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from db.connection import get_connection, release_connection


def setup_database():
    """Create the prosopography schema and tables."""

    schema_path = Path(__file__).parent / "db" / "schema.sql"

    if not schema_path.exists():
        print(f"Error: Schema file not found at {schema_path}")
        return False

    print(f"Reading schema from {schema_path}...")
    with open(schema_path, "r", encoding="utf-8") as f:
        schema_sql = f.read()

    print("Connecting to database...")
    conn = get_connection()

    try:
        print("Creating schema and tables...")
        with conn.cursor() as cur:
            cur.execute(schema_sql)
        conn.commit()
        print("Schema created successfully!")

        # Verify tables were created
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'prosopography'
                ORDER BY table_name
            """)
            tables = [row[0] for row in cur.fetchall()]

        print(f"\nCreated {len(tables)} tables in prosopography schema:")
        for table in tables:
            print(f"  - {table}")

        return True

    except Exception as e:
        print(f"Error creating schema: {e}")
        conn.rollback()
        return False

    finally:
        release_connection(conn)


if __name__ == "__main__":
    success = setup_database()
    sys.exit(0 if success else 1)
