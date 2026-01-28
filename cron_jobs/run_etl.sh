#!/bin/bash

cd "$(dirname "$0")/.."  # Go to project root (one level up from cron_jobs)

echo "===== ETL START: $(date) ====="

# Activate the virtual environment
source "$(pwd)/etl_env/bin/activate"

# Run the Python ETL script
python extract_collections_strategies.py && echo "✓ extract_collections_strategies.py complete"
python extract_loan_detail.py && echo "✓ extract_loan_detail.py complete"
python create_calendar.py && echo "✓ create_calendar.py complete"
python extract_user_experiment.py && echo "✓ extract_user_experiment.py complete"
python extract_dispute_transaction.py && echo "✓ extract_dispute_transaction.py complete"
# python extract_user_detail.py && echo "✓ extract_user_detail.py complete"
# python extract_arcus_transactions.py && echo "✓ extract_arcus_transactions.py complete"
python extract_referrals_transactions.py && echo "✓ extract_referrals_transactions.py complete"
python extract_growth_data.py && echo "✓ extract_growth_data.py complete"
# python extract_referral_arcus_payout.py && echo "✓ extract_referral_arcus_payout.py complete"
# Python extract_arcus_disbursement.py && echo "✓ extract_arcus_disbursement.py complete"
# python extract_manual_arcus.py && echo "✓ extract_manual_arcus.py complete"
# python extract_arcus_statement_pdf.py && echo "✓ extract_arcus_statement_pdf.py complete"
# python extract_moonflow_data.py && echo "✓ extract_moonflow_data.py complete"
# python load_moonflow_data.py && echo "✓ load_moonflow_data.py complete"
# python load_agencies_data.py && echo "✓ load_agencies_data.py complete"

# 💾 Build the DuckDB database
python create_duckdb.py && echo "✓ create_duckdb.py complete"

# Update metabase schema
python sync_metabase_schema.py && echo "✓ sync_metabase_schema.py complete"

echo "===== ETL END: $(date) ====="