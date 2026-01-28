import os
from utils.fetch_data_utils import fetch_data
from utils.fetch_parquet_utils import fetch_parquet
import pandas as pd
import numpy as np
from datetime import datetime

OUTPUT_DIR = os.getenv("DATA_DIR", "data")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "loan.parquet")

pd.set_option('display.max_columns', None)

print("Start pulling data from db:")

loans = fetch_data("""
select
    uls.UserId,
    l.UserLoanId,
    l.CreatedAt as IssueDate,
    l.ModifiedAt as ModifiedAt,
    l.DueDate,
    l.Amount as PrincipalAmount,
    l.Fee,
    l.Fee * 0.16 as TaxOnFee,
    case when IsLate = 1 then l.LateFee else 0 end as LateFee,
    case when IsLate = 1 then l.LateFee * 0.16 else 0 end as TaxOnLateFee,
    l.LoanStatus,
    l.IsLate,
    case
        when l.LoanStatus = 0 then 'Created'
        when l.LoanStatus = 1 then 'Active'
        when l.LoanStatus = 2 then 'Repaid'
        when l.LoanStatus = 3  then 'Defaulted'
        when l.LoanStatus = 5  then 'Repaying'
        when l.LoanStatus = 6  then 'DisbursementFailed'
        when l.LoanStatus = 7  then 'Disbursing'
        when l.LoanStatus = 8  then 'CollectionFailed'
    end as LoanStatusDescription,
    row_number() over(partition by uls.UserId order by l.CreatedAt) as LoanNumber,
    l.FeeRatio,
    jlo.OfferPolicy as JitOfferPolicy,
    CASE jlo.OfferPolicy
        WHEN 0 THEN 'TenPercentFee'
        WHEN 1 THEN 'FifteenPercentFee'
        WHEN 2 THEN 'MultiAmountsV1'
        WHEN 3 THEN 'MultiTermsV1'
    END as JitOfferPolicyName,
    jlo.CreditPolicy,
    CASE jlo.CreditPolicy
        WHEN 1 THEN 'Belvo'
        WHEN 2 THEN 'Nubarium'
        WHEN 3 THEN 'Statements'
        WHEN 4 THEN 'RepeatBelvo'
        WHEN 5 THEN 'RepeatStatements'
        WHEN 6 THEN 'RepeatControl'
        WHEN 7 THEN 'Avocado'
        WHEN 8 THEN 'AvocadoV2'
        WHEN 9 THEN 'BadAvocadoV2'
        WHEN 10 THEN 'Random'
        WHEN 14 THEN 'BajaV1'
        WHEN 15 THEN 'BajaV2'
        WHEN 16 THEN 'CaboV1'
        WHEN 17 THEN 'CaboGraduation'
        WHEN 18 THEN 'DurangoV1'
        WHEN 19 THEN 'DurangoGraduation'
        WHEN 20 THEN 'DurangoAncho'
        WHEN 21 THEN 'DurangoV2Conservative'
        WHEN 22 THEN 'DurangoV2Aggressive'
        ELSE null
    END AS CreditPolicyName,
    jlo.MlScore
from UserLoans l
join UserLoanSubscriptions uls on l.UserLoanSubscriptionId = uls.UserLoanSubscriptionId
left join LoanOffers jlo ON l.JitLoanOfferId = jlo.LoanOfferId
where
    l.LoanStatus not in (6)
    -- and convert(date, l.CreatedAt) >= '2024-01-01'
""")

print("✅ loans")

arcus = fetch_data("""
select
    ulat.UserLoanId,
    sum(ar.Amount) as AmountPaidArcus,
    max(ar.CompletedAt) as LastPaidAtArcus
from UserLoanArcusTransactions ulat
    join ArcusTransactions ar on ar.ArcusTransactionId = ulat.ArcusTransactionId
where
    ulat.IsDistribution = 0 -- only credit/in transactions
    and ar.Status != 2
group by ulat.UserLoanId
""")

print("✅ arcus")

stripe = fetch_data("""
select
    ulst.UserLoanId,
    sum(st.Amount) as AmountPaidStripe,
    max(st.CreatedAt) as LastPaidAtStripe
from UserLoanStripeTransactions ulst
    join StripeTransactions st ON ulst.StripeTransactionId = ST.StripeTransactionId
where ST.Status = 1 -- Succeded
group by ulst.UserLoanId
""")

print("✅ stripe")

dispute = fetch_data("""
select
    ulst.UserLoanId,
    sum(case when sd.StripeDisputeId is not null then st.Amount else 0 end) as DisputeAmount
from UserLoanStripeTransactions ulst
    join StripeTransactions st ON ulst.StripeTransactionId = ST.StripeTransactionId
    join StripeDispute sd on sd.StripeTransactionId = st.StripeTransactionId
where ST.Status = 1 -- Succeded
and sd.DisputeStatus = 2 -- remediatedlost
group by ulst.UserLoanId
""")

print("✅ dispute")

cash = fetch_data("""
select
    ulot.UserLoanId,
    sum(ot.Amount) as AmountPaidCash,
    max(ot.CreatedAt) as LastPaidAtCash
from UserLoanOpenpayTransactions ulot
    join OpenpayTransactions ot on ulot.OpenpayTransactionId = ot.OpenpayTransactionId
where ulot.IsDistribution = 0
and ot.Status = 2
group by ulot.UserLoanId
""")

print("✅ cash")

# Transform UTC to CDMX dates
loans['IssueDate'] = loans['IssueDate'].dt.tz_localize('UTC')
loans['IssueDateCDMX'] = loans['IssueDate'].dt.tz_convert('America/Mexico_City')

loans['ModifiedAt'] = loans['ModifiedAt'].dt.tz_localize('UTC')
loans['ModifiedAtCDMX'] = loans['ModifiedAt'].dt.tz_convert('America/Mexico_City')

arcus["LastPaidAtArcus"] = pd.to_datetime(arcus["LastPaidAtArcus"], errors="coerce")
arcus['LastPaidAtArcus'] = arcus['LastPaidAtArcus'].dt.tz_localize('UTC')
arcus['LastPaidAtArcusCDMX'] = arcus['LastPaidAtArcus'].dt.tz_convert('America/Mexico_City')

stripe["LastPaidAtStripe"] = pd.to_datetime(stripe["LastPaidAtStripe"], errors="coerce")
stripe['LastPaidAtStripe'] = stripe['LastPaidAtStripe'].dt.tz_localize('UTC')
stripe['LastPaidAtStripeCDMX'] = stripe['LastPaidAtStripe'].dt.tz_convert('America/Mexico_City')

cash["LastPaidAtCash"] = pd.to_datetime(cash["LastPaidAtCash"], errors="coerce")
cash['LastPaidAtCash'] = cash['LastPaidAtCash'].dt.tz_localize('UTC')
cash['LastPaidAtCashCDMX'] = cash['LastPaidAtCash'].dt.tz_convert('America/Mexico_City')

repayment = loans.merge(arcus, on="UserLoanId", how="left").merge(
    stripe, on="UserLoanId", how="left"
).merge(dispute, on="UserLoanId", how="left").merge(cash, on="UserLoanId", how="left")

# Fill NaN values with 0 for payment amounts
repayment["AmountPaidArcus"] = repayment["AmountPaidArcus"].fillna(0)
repayment["AmountPaidStripe"] = repayment["AmountPaidStripe"].fillna(0)
repayment["AmountPaidCash"] = repayment["AmountPaidCash"].fillna(0)
repayment["DisputeAmount"] = repayment["DisputeAmount"].fillna(0)
# repayment["LastAmountPaid"] = repayment["LastAmountPaid"].fillna(0)

# Compute total amount due
repayment["TotalAmountDue"] = (
    repayment["PrincipalAmount"]
    + repayment["Fee"]
    + repayment["TaxOnFee"]
    + repayment["LateFee"]
    + repayment["TaxOnLateFee"]
)

# Initialize columns for apportioned amounts
repayment['LateFeePaid'] = 0.0
repayment['TaxOnLateFeePaid'] = 0.0
repayment['FeePaid'] = 0.0
repayment['TaxOnFeePaid'] = 0.0
repayment['PrincipalPaid'] = 0.0

# Compute total amount paid
repayment["TotalAmountPaid"] = (
    repayment["AmountPaidArcus"] + repayment["AmountPaidStripe"] + repayment["AmountPaidCash"] - repayment["DisputeAmount"]
)
repayment["TotalOriginalAmountPaid"] = repayment["TotalAmountPaid"]

# Adjust LoanStatus = 2 and TotalAmountPaid < TotalAmountDue for underpayments adjustment
repayment['TotalAmountPaid'] = np.where(
    (repayment['TotalAmountPaid'] < repayment['TotalAmountDue']) &  (repayment['LoanStatus'] == 2),
    repayment['TotalAmountDue'],
    repayment['TotalAmountPaid']
)

# Apportion payments including taxes
def apportion_payments(row):
    # Use the lower of what the user paid or what they owed
    amount_to_apportion = min(row['TotalAmountPaid'], row['TotalAmountDue'])
    remaining = amount_to_apportion
    # remaining = row['TotalAmountPaid']

    # Step 1: Late Fee + Tax (total 92.8)
    total_late_fee_due = row['LateFee'] + row['TaxOnLateFee']
    if remaining >= total_late_fee_due:
        late_fee_paid = row['LateFee']
        tax_on_late_fee_paid = row['TaxOnLateFee']
        remaining -= total_late_fee_due
    else:
        late_fee_paid = round(remaining / 1.16, 2)
        tax_on_late_fee_paid = round(remaining - late_fee_paid, 2)
        remaining = 0

    # Step 2: Fee + Tax (total 34.8)
    total_fee_due = row['Fee'] + row['TaxOnFee']
    if remaining >= total_fee_due:
        fee_paid = row['Fee']
        tax_on_fee_paid = row['TaxOnFee']
        remaining -= total_fee_due
    else:
        fee_paid = round(remaining / 1.16, 2)
        tax_on_fee_paid = round(remaining - fee_paid, 2)
        remaining = 0

    # Step 3: Principal (no tax)
    principal_paid = min(remaining, row['PrincipalAmount'])

    return principal_paid, fee_paid, tax_on_fee_paid, late_fee_paid, tax_on_late_fee_paid

# Apply the function to calculate apportioned payments
repayment[['PrincipalPaid', 'FeePaid', 'TaxOnFeePaid', 'LateFeePaid', 'TaxOnLateFeePaid']] = repayment.apply(
    lambda row: apportion_payments(row), axis=1, result_type='expand'
)

print("Finished apportioning.")

repayment['LastPaidDate'] = repayment[['LastPaidAtArcus', 'LastPaidAtStripe', 'LastPaidAtCash']].max(axis=1)
repayment['LastPaidDateCDMX'] = repayment['LastPaidDate'].dt.tz_convert('America/Mexico_City')

# ========================================
# SETTLEMENT DATE CALCULATION
# ========================================
# SettledAt: Timestamp when loan was fully repaid
# - For repaid loans WITH payments: use latest payment date across all channels
# - For repaid loans WITHOUT payments: assume settled on due date (edge case)
# - For outstanding loans: NULL

repayment["SettledAt"] = np.where(
    (repayment['LoanStatus'] == 2) & repayment['LastPaidDate'].notnull(),
    repayment['LastPaidDate'],
    pd.NaT
)

repayment['SettledAtCDMX'] = repayment['SettledAt'].dt.tz_convert('America/Mexico_City')

repayment["SettledAt"] = np.where(
    (repayment['LoanStatus'] == 2) & repayment['LastPaidDate'].isnull(),
    pd.to_datetime(repayment["DueDate"], errors="coerce").dt.tz_localize('UTC'),
    repayment["SettledAt"]
)

repayment["SettledAtCDMX"] = np.where(
    (repayment['LoanStatus'] == 2) & repayment['LastPaidDate'].isnull(),
    pd.to_datetime(repayment["DueDate"], errors="coerce").dt.tz_localize('America/Mexico_City'),
    repayment["SettledAtCDMX"]
)

repayment["LoanCohort"] = np.where(
    repayment["LoanNumber"] == 1,
    "First",
    "Repeat"
)

for col in repayment.select_dtypes(include=['datetimetz']).columns:
    repayment[col] = repayment[col].dt.tz_localize(None)

# ========================================
# DAYS LATE CALCULATION (DPD)
# ========================================
# DaysLate: Calendar days between due date and settlement (or today if unsettled)
# - Settled loans: SettledAtCDMX - DueDate
# - Outstanding loans: today - DueDate
# - Clipped to 0 minimum (early payments = 0 days late)

today = pd.to_datetime(datetime.now().date())

repayment["DaysLate"] = np.where(
    repayment["SettledAt"].notnull(),
    (repayment["SettledAtCDMX"] - repayment["DueDate"]).dt.days,
    (today - repayment["DueDate"]).dt.days
)

# No negative DPD
repayment["DaysLate"] = repayment["DaysLate"].clip(lower=0)

# Convert UserId and UserLoanId to string
repayment['UserId'] = repayment['UserId'].astype(str)
repayment['UserLoanId'] = repayment['UserLoanId'].astype(str)

print("Loans data set created successfully.")

# INCLUDE STRATEGIES
print("Started adding collections strategies.")

stgy_df = fetch_parquet(parquet_file="collections_strategies.parquet")

stgy_postdd = stgy_df[stgy_df['Strategy'].isin([3, 4, 10, 11, 12, 13])]

loans_df = repayment.merge(stgy_postdd, on="UserLoanId", how="left")

# Make sure your datetime columns are proper datetimes (keeps tz if present)
loans_df["DueDate"] = pd.to_datetime(loans_df["DueDate"], errors="coerce")
loans_df["SettledAtCDMX"] = pd.to_datetime(loans_df["SettledAtCDMX"], errors="coerce")

# ========================================
# POST-DUE-DATE FLAG CALCULATION
# ========================================
# IsPostDD: Indicates loan entered post-due-date collections workflow
# A loan is post-DD if ANY of:
# 1. Explicitly assigned to post-DD strategy (3, 4, 13)
# 2. Past due AND settled after 30-hour grace period
# 3. Past due AND still unsettled after 30-hour grace period
#
# Grace period: DueDate (midnight) + 30 hours = ~6am next day

# Floor DueDate to start of day and add 30 hours

threshold = loans_df["DueDate"].dt.normalize() + pd.Timedelta(hours=30)

now_cdmx = pd.Timestamp.now(tz="America/Mexico_City").tz_localize(None)

due = pd.to_datetime(loans_df["DueDate"])
settled = pd.to_datetime(loans_df["SettledAtCDMX"])

past_due = due < now_cdmx
settled_after_threshold = settled > threshold
over_30h_without_settlement = ((now_cdmx - due) > pd.Timedelta(hours=30)) & settled.isna()

loans_df["IsPostDD"] = (
    loans_df["Strategy"].isin([3, 4, 13])
    | (past_due & (settled_after_threshold | over_30h_without_settlement))
)

# Make sure CreatedAt is a datetime
loans_df["CreatedAt"] = pd.to_datetime(loans_df["CreatedAt"], errors="coerce")

# Sort by UserLoanId + CreatedAt DESC
loans_sorted = loans_df.sort_values(["UserLoanId", "CreatedAt"], ascending=[True, False])

# Drop duplicates keeping the first (which is the latest CreatedAt per UserLoanId)
loans_clean = loans_sorted.drop_duplicates(subset=["UserLoanId"], keep="first")

loans_clean["StrategyCreatedAt"] = loans_clean.apply(
    lambda row: threshold[row.name]
    if (
        (row["IsPostDD"] and pd.isna(row["CreatedAt"]))
        or (row["IsPostDD"] and row["Strategy"] in [10, 11, 12])
    )
    else row["CreatedAt"],
    axis=1
)

loans_clean["StrategyCreatedAtCDMX"] = loans_clean.apply(
    lambda row: threshold[row.name]
    if (
        (row["IsPostDD"] and pd.isna(row["CreatedAt"]))
        or (row["IsPostDD"] and row["Strategy"] in [10, 11, 12])
    )
    else row["CreatedAtCDMX"],
    axis=1
)

loans_clean["StrategyName"] = loans_clean["StrategyName"].fillna("Twilio")

# Remove no needed columns
loans_clean = loans_clean.drop(columns=["CreatedAt", "CreatedAtCDMX", "IsDeleted", "StrategyType"])

# ADD PYPPER 20+ TEST
pypper = fetch_parquet(parquet_file="collections_strategies.parquet")

pypper = stgy_df[stgy_df['Strategy'] == 14]
pypper = pypper[['UserLoanId', 'Strategy', 'StrategyName', 'CreatedAt', 'CreatedAtCDMX']]
pypper = pypper.rename(columns={'CreatedAt': 'LateStrategyCreatedAt', 'CreatedAtCDMX': 'LateStrategyCreatedAtCDMX', 'StrategyName': 'LateStrategyName', 'Strategy': 'LateStrategy'})

loans_clean = loans_clean.merge(pypper, on="UserLoanId", how="left")

print("Final data set created successfully.")

loans_clean.to_parquet(OUTPUT_FILE, index=False)
print("Loan repayment parquet stored locally.")