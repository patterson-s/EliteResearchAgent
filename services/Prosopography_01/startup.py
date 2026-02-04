"""Startup script for Prosopography Tool.

Ensures database schema is created before running the app.
Used by Render deployment.
"""

import os
import sys
from pathlib import Path

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))


def check_and_create_schema():
    """Check if schema exists, create if not."""
    from db.connection import get_connection, release_connection

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Check if prosopography schema exists
            cur.execute("""
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.schemata
                    WHERE schema_name = 'prosopography'
                )
            """)
            schema_exists = cur.fetchone()[0]

            if not schema_exists:
                print("Creating prosopography schema...")
                schema_path = Path(__file__).parent / "db" / "schema.sql"

                with open(schema_path, "r", encoding="utf-8") as f:
                    schema_sql = f.read()

                cur.execute(schema_sql)
                conn.commit()
                print("Schema created successfully!")
            else:
                print("Prosopography schema already exists.")

    except Exception as e:
        print(f"Schema check/creation error: {e}")
        conn.rollback()
    finally:
        release_connection(conn)


def main():
    """Run startup checks and launch Streamlit."""
    print("Running startup checks...")

    # Check database connection and schema
    try:
        check_and_create_schema()
    except Exception as e:
        print(f"Warning: Could not check database schema: {e}")
        print("The app will start but may have database errors.")

    # Launch Streamlit
    import subprocess
    port = os.getenv("PORT", "8501")

    cmd = [
        "streamlit", "run", "ui/app.py",
        "--server.port", port,
        "--server.address", "0.0.0.0",
        "--server.headless", "true"
    ]

    print(f"Starting Streamlit on port {port}...")
    subprocess.run(cmd)


if __name__ == "__main__":
    main()
