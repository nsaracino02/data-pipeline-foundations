from utils.gsheets_utils import load_drive_file_as_dataframe, list_files_in_folder
import pandas as pd
from datetime import datetime
import os
import numpy as np
from utils.fetch_parquet_utils import fetch_parquet
from dotenv import load_dotenv

load_dotenv()

GROWTH_DATA_FOLDER_ID = os.getenv("GROWTH_DATA_FOLDER_ID")

# Output path
PARQUET_SAVE_PATH = "data/growth_data.parquet"

pd.set_option('display.max_columns', None)
pd.set_option('display.float_format', '{:.2f}'.format)

def transform_facebook_raw(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform raw Facebook export to standardized format.
    
    - Cleans numeric columns (removes $, commas)
    - Converts dates
    - Maps Facebook column names to internal conventions
    - FOG = First Offer Generated, FLA = First Loan Accepted
    """
    # 1. Date
    df["Install Day"] = pd.to_datetime(df["Install Day"], format="%b %d, %Y")

    # 2. Clean numeric-like columns
    numeric_like_cols = [
        col for col in df.columns
        if any(keyword in col.lower()
               for keyword in ["sum", "cost", "click", "impression", "sales", "users"])
    ]

    def clean_numeric(col):
        return (
            col.astype(str)
               .str.replace(",", "", regex=False)
               .str.replace("$", "", regex=False)
               .str.strip()
               .replace("", np.nan)
               .astype(float)
        )

    for col in numeric_like_cols:
        df[col] = clean_numeric(df[col])

    # 3. Keep only rows with Ad ID (filters out summary rows)
    df = df[df["Ad"].notna()].copy()

    # 4. Rename columns
    rename_map = {
        "Install Day": "install_day",
        "Media Source": "media_source",
        "Campaign ID": "campaign_id",
        "Campaign": "campaign_name",
        "Adset ID": "adset_id",
        "Adset": "adset_name",
        "Ad ID": "ad_id",
        "Ad": "ad_name",
        "Impressions (sum)": "impressions",
        "Clicks (sum)": "clicks",
        "Installs (sum)": "installs",
        "Cost (sum)": "cost",
        "Event Counter - firstoffergenerated (sum)": "fog_event_counter",
        "Unique Users - firstoffergenerated (sum)": "fog_unique_users",
        "Event Counter - serverfirstloanacceptedgp (sum)": "fla_event_counter",
        "Unique Users - serverfirstloanacceptedgp (sum)": "fla_unique_users",
        "Sales in USD - serverfirstloanacceptedgp (sum)": "fla_sales_usd",
    }
    df = df.rename(columns=rename_map)

    return df

def process_monthly_files(
    folder_id: str,
    parquet_file: str = "growth_data.parquet",
    months_to_refresh=None,          # e.g. ["2025_09", "2025_11"]
    process_missing: bool = True     # also load months not in parquet yet
) -> pd.DataFrame:
    """
    Process monthly Facebook Ads CSV files from Google Drive.
    
    Two modes:
    1. Incremental: Auto-process new months not in parquet (process_missing=True)
    2. Refresh: Re-process specific months (months_to_refresh=["2025_11"])
    
    CSV naming convention: YYYY_MM.csv (e.g., 2025_11.csv)
    """

    # Normalize months_to_refresh
    if isinstance(months_to_refresh, str):
        months_to_refresh = [months_to_refresh]
    months_to_refresh = set(months_to_refresh or [])

    # 1. Load existing parquet
    try:
        df_existing = fetch_parquet(parquet_file=parquet_file)
        df_existing["install_day"] = pd.to_datetime(df_existing["install_day"])
        existing_months = set(df_existing["install_day"].dt.strftime("%Y_%m").unique())
        print("Existing months in parquet:", sorted(existing_months))
    except Exception as e:
        print(f"Could not load existing parquet ({parquet_file}): {e}")
        df_existing = None
        existing_months = set()
        print("Starting with empty history.")

    # 2. List files in Google Drive folder
    files = list_files_in_folder(folder_id)
    if not files:
        print(f"No files found in folder {folder_id}")
        return df_existing

    new_dfs = []
    months_to_drop_from_existing = set()

    # Process CSV files
    for f in files:
        name = f.get("name", "")
        file_id = f["id"]

        if not name.lower().endswith(".csv"):
            print(f"Skipping non-CSV file: {name}")
            continue

        # Expect "2025_11.csv" → month_tag = "2025_11"
        month_tag = name.rsplit(".", 1)[0]

        # CASE A — Explicit refresh (manual selection)
        if month_tag in months_to_refresh:
            print(f"Refreshing month {month_tag} using {name}")
            months_to_drop_from_existing.add(month_tag)

        # CASE B — Missing month and we want to auto-process new ones
        elif process_missing and (month_tag not in existing_months):
            print(f"Processing NEW month {month_tag} from {name}")

        # CASE C — Skip anything else
        else:
            print(f"Skipping {name} (month {month_tag} not selected for refresh or already exists)")
            continue

        raw_df = load_drive_file_as_dataframe(file_id)
        proc_df = transform_facebook_raw(raw_df)
        new_dfs.append(proc_df)

    # 3. No new data to process?
    if not new_dfs:
        print("No new or refreshed months to process.")
        return df_existing

    df_new = pd.concat(new_dfs, ignore_index=True)

    # 4. Drop explicitly refreshed months from existing parquet
    if df_existing is not None and months_to_drop_from_existing:
        mask = df_existing["install_day"].dt.strftime("%Y_%m").isin(months_to_drop_from_existing)
        print(f"Dropping {mask.sum()} rows for refreshed months: {months_to_drop_from_existing}")
        df_existing = df_existing[~mask].copy()

    # 5. Append new processed data
    if df_existing is not None:
        df_final = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_final = df_new

    # 6. Save updated parquet
    os.makedirs("data", exist_ok=True)
    df_final.to_parquet(PARQUET_SAVE_PATH, index=False)

    print(f"Updated parquet saved to {PARQUET_SAVE_PATH}")

    return df_final

# ========================================
# EXECUTION CASES
# ========================================

# Case 1: normal monthly behavior (only new months)
# growth_data = process_monthly_files(
#     folder_id=GROWTH_DATA_FOLDER_ID,
#     parquet_file="growth_data.parquet",
#     months_to_refresh=None,       # or just omit
#     process_missing=True,
# )

# Case 2: refresh just November 2025 (e.g. partial → full month)
growth_data = process_monthly_files(
    folder_id=GROWTH_DATA_FOLDER_ID,
    parquet_file="growth_data.parquet",
    months_to_refresh=["2026_01"],   # manually pick month(s)
    process_missing=True,            # still append any other new months if they appear
)

# Case 3: only refresh specific months, ignore other missing ones
# growth_data = process_monthly_files(
#     folder_id=GROWTH_DATA_FOLDER_ID,
#     parquet_file="growth_data.parquet",
#     months_to_refresh=["2025_11"],
#     process_missing=False,           # don't auto-append missing months
# )

print("Growth data parquet stored locally.")