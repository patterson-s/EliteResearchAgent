"""Database connection pool management for Prosopography Tool."""

import os
import psycopg2
from psycopg2 import pool
from urllib.parse import urlparse
from dotenv import load_dotenv
from contextlib import contextmanager

load_dotenv()

connection_pool = None


def parse_database_url(url: str) -> dict:
    """Parse a DATABASE_URL into connection parameters."""
    parsed = urlparse(url)
    return {
        'host': parsed.hostname,
        'port': parsed.port or 5432,
        'database': parsed.path[1:],  # Remove leading /
        'user': parsed.username,
        'password': parsed.password,
    }


def init_connection_pool():
    """Initialize the database connection pool.

    Supports both:
    - DATABASE_URL (for cloud deployments like Render/Supabase)
    - Individual DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD env vars
    """
    global connection_pool
    if connection_pool is None:
        # Check for DATABASE_URL first (cloud deployment)
        database_url = os.getenv('DATABASE_URL')

        if database_url:
            # Parse the URL and create connection
            params = parse_database_url(database_url)

            # Render uses 'postgres://' but psycopg2 needs 'postgresql://'
            # Also add sslmode for cloud databases
            connection_pool = psycopg2.pool.SimpleConnectionPool(
                1, 10,  # Smaller pool for cloud (free tier limits)
                host=params['host'],
                port=params['port'],
                database=params['database'],
                user=params['user'],
                password=params['password'],
                sslmode='require'  # Required for most cloud databases
            )
        else:
            # Fall back to individual env vars (local development)
            connection_pool = psycopg2.pool.SimpleConnectionPool(
                1, 20,
                host=os.getenv('DB_HOST', 'localhost'),
                port=os.getenv('DB_PORT', '5432'),
                database=os.getenv('DB_NAME', 'eliteresearch'),
                user=os.getenv('DB_USER', 'postgres'),
                password=os.getenv('DB_PASSWORD')
            )
    return connection_pool


def get_connection():
    """Get a connection from the pool."""
    p = init_connection_pool()
    return p.getconn()


def release_connection(conn):
    """Return a connection to the pool."""
    if conn is None:
        return
    try:
        p = init_connection_pool()
        p.putconn(conn)
    except Exception:
        # If pool is closed or connection is bad, just close it
        try:
            conn.close()
        except Exception:
            pass


@contextmanager
def get_db_connection():
    """Context manager for database connections - ensures proper release."""
    conn = None
    try:
        conn = get_connection()
        yield conn
    finally:
        if conn:
            release_connection(conn)


def close_all_connections():
    """Close all connections in the pool."""
    global connection_pool
    if connection_pool is not None:
        try:
            connection_pool.closeall()
        except Exception:
            pass
        connection_pool = None


def reset_pool():
    """Reset the connection pool - useful when connections get exhausted."""
    close_all_connections()
    init_connection_pool()
