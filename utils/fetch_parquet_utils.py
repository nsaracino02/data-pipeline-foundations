"""
Parquet File Loader Utility

Loads parquet files from the project's data directory.
Provides a default path relative to the project root, with option to override.
"""

import pandas as pd
from pathlib import Path

def fetch_parquet(parquet_file, prefix_path=None):
    # Load a parquet file from the data directory.
    if prefix_path is None:
        # Use default path relative to project root (data/ folder)
        project_root = Path(__file__).parent.parent
        prefix_path = project_root / "data"
    
    file_path = f"{prefix_path}/{parquet_file}"
    return pd.read_parquet(file_path)