"""
Database Query Utility

Provides a simple interface to execute SQL queries against the production database.
Automatically handles connection lifecycle (open → query → close).

Note: Connection credentials are loaded from .env via db_connection.py
"""

import sys
import os

# Add root folder (Pypeline) to sys.path
# Required when running scripts from subdirectories (e.g., utils/)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db_connection import get_db_connection
import pandas as pd

def fetch_data(query):
    """Fetches data from the database and closes the connection after execution."""
    engine = get_db_connection()
    try:
        df = pd.read_sql(query, engine)
        return df
    finally:
        engine.dispose()
        print("✅ Database connection closed.")