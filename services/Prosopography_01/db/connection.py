"""Database connection pool management for Prosopography Tool."""

import os
import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv

load_dotenv()

connection_pool = None


def init_connection_pool():
    """Initialize the database connection pool."""
    global connection_pool
    if connection_pool is None:
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
    pool = init_connection_pool()
    return pool.getconn()


def release_connection(conn):
    """Return a connection to the pool."""
    pool = init_connection_pool()
    pool.putconn(conn)


def close_all_connections():
    """Close all connections in the pool."""
    global connection_pool
    if connection_pool is not None:
        connection_pool.closeall()
        connection_pool = None
