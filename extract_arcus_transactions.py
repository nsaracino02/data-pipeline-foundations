from utils.fetch_data_utils import fetch_data
import pandas as pd
import numpy as np
from datetime import datetime


print("Start pulling data from db:")

arcus = fetch_data("""
select
    ar.ArcusTransactionId,
    ar.ExternalId,
    ar.Reference,
    ar.ArcusCustomerId,
    ulat.UserLoanId,
    ar.Description,
    ar.Amount,
    ar.CreatedAt,
    ar.ModifiedAt,
    ar.CompletedAt,
    ulat.IsDistribution,
    case when ulat.IsDistribution = 1 then 'Out' else 'In' end as TransactionType,
    ar.Status,
    case
        when ar.Status = 0 then 'Pending'
        when ar.Status = 1 then 'Succeeded'
        when ar.Status = 2 then 'Failed'
        when ar.Status = 3 then 'Refunded'
        when ar.Status = 4 then 'Returned' -- returned by the banking system
    end as StatusDescription,
    ar.TransactionDirection,
    case when ar.TransactionDirection = 0 then 'Credit' else 'Debit' end as TransactionDirectionDescription,
    ar.ExternalAccountNumber,
    ar.ExternalAccountIdentifier,
    ar.ExternalAccountName,
    ar.TrackingId,
    case when ua.ArcusTransactionId is not null then 1 else 0 end as IsUnallocated,
    ar.FailureCode
from ArcusTransactions ar   
    left join UserLoanArcusTransactions ulat  on ar.ArcusTransactionId = ulat.ArcusTransactionId
    left join UnallocatedPaymentArcusTransactions ua on ua.ArcusTransactionId = ar.ArcusTransactionId
where ar.CreatedAt >= '2025-06-01'
""")

print("✅ arcus db transactions")

# Convert UTC timestamps to Mexico City timezone
arcus['CreatedAt'] = arcus['CreatedAt'].dt.tz_localize('UTC')
arcus['CreatedAtCDMX'] = arcus['CreatedAt'].dt.tz_convert('America/Mexico_City')

arcus['ModifiedAt'] = arcus['ModifiedAt'].dt.tz_localize('UTC')
arcus['ModifiedAtCDMX'] = arcus['ModifiedAt'].dt.tz_convert('America/Mexico_City')

arcus["CompletedAt"] = pd.to_datetime(arcus["CompletedAt"], errors="coerce")
arcus['CompletedAt'] = arcus['CompletedAt'].dt.tz_localize('UTC')
arcus['CompletedAtCDMX'] = arcus['CompletedAt'].dt.tz_convert('America/Mexico_City')

# Remove timezone info for Parquet compatibility (stores as naive datetime)
for col in arcus.select_dtypes(include=['datetimetz']).columns:
    arcus[col] = arcus[col].dt.tz_localize(None)

# Convert UserLoanId to string for consistent joining with other datasets
arcus['UserLoanId'] = arcus['UserLoanId'].apply(
    lambda x: str(int(x)) if pd.notnull(x) else None
)

arcus['UserLoanId'] = arcus['UserLoanId'].astype(str)

# decimal format and remove tz type

arcus.to_parquet("data/arcus_transactions.parquet", index=False)
print("Arcus transactions parquet stored locally.")