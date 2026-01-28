# Pypeline

A Python-based ETL pipeline that extracts data from multiple sources, loads it into DuckDB, and exposes it through Metabase for analytics.

## What This Is

This is a personal data engineering project built to consolidate financial and operational data from disparate sources into a single queryable data warehouse. It runs scheduled ETL jobs, maintains a DuckDB database, and serves dashboards through a locally-hosted Metabase instance.

The pipeline handles:
- SQL Server database extractions (loan data, user events, collections strategies)
- Google Sheets exports (manual data entry, reconciliation workflows)
- Google Drive file processing (CSV/Excel files from third-party systems)
- Parquet-based intermediate storage
- DuckDB as the analytical database
- Automated Metabase schema synchronization

## Why This Exists

I built this to solve a real problem: fragmented data across multiple systems with no unified view. The pipeline automates daily data pulls, standardizes formats, and makes the data accessible to non-technical stakeholders through Metabase dashboards.

This is a learning project that demonstrates:
- ETL design patterns (extract → transform → load)
- Working with multiple data sources (SQL Server, Google APIs, flat files)
- Database schema design (fact/dimension tables)
- Automation (cron jobs, retry logic, error handling)
- Documentation and code organization

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      DATA SOURCES                            │
├─────────────────────────────────────────────────────────────┤
│  SQL Server  │  Google Sheets  │  Google Drive (CSV/Excel)  │
└──────┬───────┴────────┬─────────┴────────────┬──────────────┘
       │                │                      │
       ▼                ▼                      ▼
┌─────────────────────────────────────────────────────────────┐
│                    EXTRACT SCRIPTS                           │
│  extract_loan_detail.py                                      │
│  extract_collections_strategies.py                           │
│  extract_manual_arcus_*.py                                   │
│  extract_growth_data.py                                      │
│  ... (15+ extraction scripts)                                │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                  PARQUET FILES (data/)                       │
│  loan.parquet                                                │
│  collections_strategies.parquet                              │
│  arcus_payments_raw.parquet                                  │
│  dim_calendar.parquet                                        │
│  ... (15+ parquet files)                                     │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              create_duckdb.py                                │
│  • Backs up existing database                                │
│  • Loads parquet files as tables                             │
│  • Handles connection locks (retry logic)                    │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│           DuckDB (db/empower_mx_dwh.duckdb)                  │
│  fact_loan                                                   │
│  fact_collections_strategies                                 │
│  dim_calendar                                                │
│  analytics_arcus_payments                                    │
│  ... (15+ tables)                                            │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              sync_metabase_schema.py                         │
│  Triggers schema refresh via Metabase API                    │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│         METABASE (Docker, localhost:3000)                    │
│  Dashboards, queries, visualizations                         │
└─────────────────────────────────────────────────────────────┘
```

**Orchestration:**
- Cron jobs run daily ETL scripts (`cron_jobs/run_etl.sh`)
- Scripts execute sequentially with error handling
- Logs written to `cron_jobs/etl_log.txt`

## Repository Structure

```
Pypeline/
├── extract_*.py              # Data extraction scripts (SQL, Google APIs, files)
├── create_duckdb.py          # Builds DuckDB from parquet files
├── sync_metabase_schema.py   # Triggers Metabase schema refresh
├── load_*.py                 # Export scripts (DuckDB → Google Sheets)
├── analytics_*.py            # Ad-hoc analysis scripts
├── cron_jobs/
│   ├── run_etl.sh            # Main ETL orchestration script
│   └── run_etl_complete.sh   # Full pipeline (includes accounting data)
├── utils/
│   ├── fetch_data_utils.py   # SQL Server query wrapper
│   ├── fetch_parquet_utils.py # Parquet file loader
│   └── gsheets_utils.py      # Google Sheets/Drive API helpers
├── db/
│   └── empower_mx_dwh.duckdb # DuckDB database (gitignored)
├── data/                     # Parquet files (gitignored)
├── db_connection.py          # SQL Server connection config
└── .env                      # Credentials (gitignored)
```

## How to Run Locally

**Prerequisites:**
- Python 3.9+
- Virtual environment (`etl_env/`)
- SQL Server ODBC Driver 18
- Docker (for Metabase)
- Google Cloud service account credentials

**Setup:**

1. **Install dependencies:**
   ```bash
   python -m venv etl_env
   source etl_env/bin/activate
   pip install sqlalchemy pyodbc pandas duckdb gspread oauth2client python-dotenv
   ```

2. **Configure environment variables:**
   Create `.env` with:
   ```bash
   # SQL Server
   DB_SERVER=your-server.database.windows.net
   DB_DATABASE=your_database
   DB_UID=your_username
   DB_PASSWORD=your_password

   # Google Sheets/Drive
   GOOGLE_SHEETS_CREDENTIALS=/path/to/service-account.json

   # Metabase
   METABASE_URL=http://localhost:3000
   METABASE_USERNAME=admin@example.com
   METABASE_PASSWORD=your_password
   METABASE_DB_ID=1
   ```

3. **Run ETL pipeline:**
   ```bash
   ./cron_jobs/run_etl.sh
   ```

4. **Start Metabase:**
   ```bash
   docker run -d -p 3000:3000 \
     -v $(pwd)/db:/data \
     -e MB_DB_TYPE=duckdb \
     -e MB_DB_FILE=/data/empower_mx_dwh.duckdb \
     metabase/metabase
   ```

5. **Access dashboards:**
   Open `http://localhost:3000`

## Notes & Limitations

**This is a learning project, not production software.**

### What's Excluded
- **Real credentials:** All `.env` files, service account JSON, and connection strings are gitignored
- **Real data:** Parquet files and DuckDB databases are gitignored
- **Personal paths:** Some scripts reference `/Users/nicolesaracino/...` paths (being cleaned up)

### Known Issues
- Hardcoded absolute paths in some utility files (being migrated to relative paths)
- No automated testing (manual validation only)
- No CI/CD pipeline
- Error handling is basic (print statements, no structured logging)
- No data quality checks or validation framework
- Cron job scheduling is manual (no Airflow/Prefect)

### What Works Well
- Modular extraction scripts (easy to add new sources)
- Parquet-based intermediate storage (fast, portable)
- DuckDB handles analytical queries efficiently
- Retry logic for database locks (handles Metabase connections)
- Backup system prevents data loss during rebuilds

### Future Improvements
- Migrate to environment variables for all paths
- Add structured logging (replace print statements)
- Implement data quality checks (row counts, null checks, schema validation)
- Add unit tests for transformation logic
- Containerize the entire pipeline (Docker Compose)
- Replace cron with a proper orchestrator (Airflow, Prefect, or Dagster)

## Tech Stack

- **Language:** Python 3.9+
- **Database:** DuckDB (analytical), SQL Server (source)
- **Storage:** Parquet files (intermediate)
- **APIs:** Google Sheets API, Google Drive API, Metabase API
- **Orchestration:** Bash scripts + cron
- **BI Tool:** Metabase (Docker)
- **Libraries:** pandas, SQLAlchemy, gspread, pyodbc

## License

This is a personal project. Code is provided as-is for portfolio review purposes.