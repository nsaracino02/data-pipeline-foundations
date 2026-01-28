"""
DuckDB Data Warehouse Builder

Creates/updates the DuckDB database from parquet files:
1. Backs up existing database
2. Cleans up old backups
3. Connects with retry logic (handles locks from BI tools)
4. Loads parquet files as tables

Output: db/empower_mx_dwh.duckdb
"""

import duckdb
import shutil
import time
from pathlib import Path
from datetime import datetime

# Define paths
DATA_DIR = Path(__file__).parent / "data"
DB_DIR = Path(__file__).parent / "db"
DB_PATH = DB_DIR / "empower_mx_dwh.duckdb"

# Ensure the db folder exists
DB_DIR.mkdir(parents=True, exist_ok=True)

# STEP 1: Snapshot old DB before overwriting
if DB_PATH.exists():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = DB_DIR / f"empower_mx_dwh_backup_{timestamp}.duckdb"
    shutil.copy(DB_PATH, backup_path)
    print(f"📦 Backup created at: {backup_path}")

# STEP 2: Cleanup old backups (keep only latest)
backups = sorted(DB_DIR.glob("empower_mx_dwh_backup_*.duckdb"), reverse=True)
for old_backup in backups[1:]:
    old_backup.unlink()
    print(f"🧹 Deleted old backup: {old_backup.name}")
    
# STEP 3: Try connecting with retry logic in case of lock
MAX_RETRIES = 5
WAIT_SECONDS = 2
con = None

for attempt in range(MAX_RETRIES):
    try:
        con = duckdb.connect(DB_PATH.as_posix())
        print("✅ Connected to DuckDB")
        break
    except duckdb.IOException as e:
        if "Conflicting lock" in str(e):
            print(f"⏳ DuckDB file is locked (attempt {attempt + 1}/{MAX_RETRIES}), retrying in {WAIT_SECONDS}s...")
            time.sleep(WAIT_SECONDS)
        else:
            raise e

if con is None:
    raise RuntimeError("❌ Could not connect to DuckDB due to a persistent lock.")

# STEP 4: Load parquet files into DuckDB
# Map parquet files to their corresponding table names
# Fact tables: transactional data (loans, collections)
# Dim tables: reference/lookup data (calendar, experiments, users)
# Analytics tables: raw data from external systems (Arcus)
parquet_table_map = {
    "loan.parquet": "fact_loan",
    "collections_strategies.parquet": "fact_collections_strategies",
    "dim_calendar.parquet": "dim_calendar",
    "arcus_payments_raw.parquet": "analytics_arcus_payments",
    "arcus_transactions_raw.parquet": "analytics_arcus_transactions",
    "arcus_transactions.parquet": "dim_arcus_transactions",
    "experiments.parquet": "dim_user_experiment",
    "dispute.parquet": "dim_loan_dispute",
    "referrals_transactions.parquet": "dim_referral_transactions",
    "offers.parquet": "dim_user_analytics",
    "referrals_arcus_payouts.parquet": "dim_referral_arcus_payouts",
    "arcus_disbursements.parquet": "analytics_arcus_disbursements",
    "growth_data.parquet": "dim_growth_data"
}

# Drop existing tables that are not in the new map
existing_tables = [row[0] for row in con.execute("SHOW TABLES").fetchall()]
desired_tables = list(parquet_table_map.values())
tables_to_drop = set(existing_tables) - set(desired_tables)

for table in tables_to_drop:
    con.execute(f"DROP TABLE {table}")
    print(f"🗑️ Dropped outdated table: {table}")

# Load and replace each table
for parquet_file, table_name in parquet_table_map.items():
    parquet_path = DATA_DIR / parquet_file
    print(f"Loading {parquet_path} into table '{table_name}'...")
    
    # Create or replace table from Parquet
    con.execute(f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT * FROM '{parquet_path.as_posix()}'
    """)

con.close()
print(f"\n✅ DuckDB created at: {DB_PATH}")