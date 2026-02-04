import os
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from utils.gsheets_utils import list_files_in_folder, load_drive_file_as_dataframe

# Load environment variables
load_dotenv()

# ============================================================================
# TRANSACTIONS PROCESSING
# ============================================================================

TRANSACTIONS_FOLDER_ID = os.getenv("ARCUS_TRANSACTIONS_FOLDER_ID")
if not TRANSACTIONS_FOLDER_ID:
    raise ValueError("ARCUS_TRANSACTIONS_FOLDER_ID not set in .env file")

processed_log = Path("data/arcus_processed_transactions_folders.txt")
output_parquet = Path("data/arcus_transactions_raw.parquet")

# Ensure the log file exists
processed_log.parent.mkdir(parents=True, exist_ok=True)
processed_log.touch(exist_ok=True)

# Read already processed folder IDs
with open(processed_log, "r") as f:
    processed_folders = set(line.strip() for line in f.readlines())

# List all subfolders in the main Transactions folder
all_folders = list_files_in_folder(TRANSACTIONS_FOLDER_ID)
transaction_subfolders = [f for f in all_folders if f.get("mimeType") == "application/vnd.google-apps.folder" and f["name"].startswith("transactions_")]

if not transaction_subfolders:
    print("❌ No transactions subfolders found.")
    exit()

# Sort by folder name
transaction_subfolders_sorted = sorted(transaction_subfolders, key=lambda x: x["name"])

# Track processed in this run
processed_this_run = []
all_dfs = []

for folder in transaction_subfolders_sorted:
    folder_id = folder["id"]
    folder_name = folder["name"]
    
    if folder_id in processed_folders:
        print(f"✅ Skipping already processed folder: {folder_name}")
        continue

    print(f"📂 Processing folder: {folder_name}")

    # List CSV files inside this subfolder
    csv_files = list_files_in_folder(folder_id)
    csv_files = [f for f in csv_files if f["name"].lower().endswith(".csv")]

    for csv_file in csv_files:
        file_id = csv_file["id"]
        file_name = csv_file["name"]
        
        try:
            df = load_drive_file_as_dataframe(file_id)

            if df.shape[0] <= 1:
                print(f"⚠️ Skipping {file_name} (no transactions).")
                continue

            # Drop final row (totals)
            df = df.iloc[:-1]

            if df.empty:
                print(f"⚠️ Skipping {file_name} (empty after dropping totals).")
                continue

            all_dfs.append(df)
        except Exception as e:
            print(f"❌ Error processing {file_name}: {e}")

    processed_this_run.append(folder_id)

if not all_dfs:
    print("⚠️ No new valid data to process.")
    exit()

# Combine all new data
new_df = pd.concat(all_dfs, ignore_index=True)

# Convert from cents to currency units
new_df["amount"] = new_df["amount"] / 100

new_df['date'] = pd.to_datetime(new_df['date'], utc=True)

# Append to existing parquet file if it exists
output_parquet.parent.mkdir(parents=True, exist_ok=True)

if output_parquet.exists():
    existing_df = pd.read_parquet(output_parquet)
    final_df = pd.concat([existing_df, new_df], ignore_index=True)
    print(f"📎 Appending {len(new_df)} new rows to {len(existing_df)} existing rows")
else:
    final_df = new_df
    print(f"📄 Creating new file with {len(new_df)} rows")

final_df.to_parquet(output_parquet, index=False)
print(f"✅ Data exported to {output_parquet} (total: {len(final_df)} rows)")

# Update log with folder IDs
with open(processed_log, "a") as f:
    for folder_id in processed_this_run:
        f.write(f"{folder_id}\n")

print(f"📝 Logged {len(processed_this_run)} folders as processed.")