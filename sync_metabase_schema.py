"""
Metabase Schema Sync Trigger

Triggers a schema refresh in Metabase after DuckDB updates.
This ensures Metabase reflects the latest table structures and columns.
"""

import requests
import os
from dotenv import load_dotenv

# Load .env variables
load_dotenv()

METABASE_SITE_URL = os.getenv("METABASE_URL")
USERNAME = os.getenv("METABASE_USERNAME")
PASSWORD = os.getenv("METABASE_PASSWORD")
DATABASE_ID = os.getenv("METABASE_DB_ID")

def sync_schema():
    try:
        # Authenticate with Metabase to get session token
        session = requests.post(f"{METABASE_SITE_URL}/api/session", json={
            "username": USERNAME,
            "password": PASSWORD
        })
        session.raise_for_status()
        token = session.json()["id"]

        # Trigger schema sync to refresh table/column metadata
        headers = {"X-Metabase-Session": token}
        res = requests.post(f"{METABASE_SITE_URL}/api/database/{DATABASE_ID}/sync_schema", headers=headers)
        res.raise_for_status()
        print("✅ Metabase schema sync triggered successfully.")

    except requests.exceptions.RequestException as e:
        # Common failures: wrong credentials, network issues, invalid database ID
        print(f"❌ Failed to sync Metabase schema: {e}")
    except KeyError as e:
        # Session token not found in response (authentication failed)
        print(f"❌ Authentication failed - check credentials: {e}")
    except Exception as e:
        print(f"❌ Unexpected error during schema sync: {e}")

if __name__ == "__main__":
    sync_schema()