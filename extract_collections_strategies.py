import os
import pandas as pd
from utils.fetch_data_utils import fetch_data

# Output configuration
OUTPUT_DIR = os.getenv("DATA_DIR", "data")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "collections_strategies.parquet")

# ========================================
# EXTRACT STRATEGY ASSIGNMENTS
# ========================================
# Filters for active collection strategies only (excludes deprecated/test strategies)

strategies_df = fetch_data("""
select
    UserLoanId,
    CreatedAt,
    Strategy,
    case
        when Strategy = 3 then 'CMD'
        when Strategy = 4 then 'Integra'
        when Strategy = 5 then 'IvrPreventativeAndReminderCollectionCallV2'
        when Strategy = 8 then 'AgencyReminderCallV1'
        when Strategy = 7 then 'Vozy'
        when Strategy = 10 then 'MoonflowVariationV1'
        when Strategy = 11 then 'MoonflowControlGroupV1'
        when Strategy = 12 then 'MoonflowPaymentCommitmentV1'
        when Strategy = 13 then 'Pypper'
        when Strategy = 14 then 'Pypper_late_20'
    end as StrategyName,
    case when Strategy in (5,8) then 'PreDD' else 'PostDD' end as StrategyType,
    IsDeleted
from LoanCollectionStrategies lcs
where
    Strategy in (3,4,5,7,8,10,11,12,13,14)
"""
)

print("Data extracted successfully.")

# ========================================
# TIMEZONE CONVERSION
# ========================================
# Convert UTC timestamps to Mexico City timezone for business reporting

strategies_df["CreatedAt"] = pd.to_datetime(strategies_df["CreatedAt"], errors="coerce")
strategies_df['CreatedAt'] = strategies_df['CreatedAt'].dt.tz_localize('UTC')
strategies_df['CreatedAtCDMX'] = strategies_df['CreatedAt'].dt.tz_convert('America/Mexico_City')

# Strip timezone info for DuckDB compatibility (stores as naive datetime)
for col in strategies_df.select_dtypes(include=['datetimetz']).columns:
    strategies_df[col] = strategies_df[col].dt.tz_localize(None)

# ========================================
# DATA TYPE STANDARDIZATION
# ========================================
# Convert UserLoanId to string for consistent joins with other datasets
strategies_df['UserLoanId'] = strategies_df['UserLoanId'].astype(str)

print("Final data set created successfully.")

# ========================================
# DATA TYPE STANDARDIZATION
# ========================================
# Convert UserLoanId to string for consistent joins with other datasets
strategies_df.to_parquet(OUTPUT_FILE, index=False)
print("Collections strategies parquet stored locally.")