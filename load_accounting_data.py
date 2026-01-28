"""
Load Accounting Data and Export to Google Drive

Processes loan and repayment data to generate accounting reports:
- Accounting summaries (CDMX and UTC timezones)
- Settled loans by month
- Loan origination/repayment details (3-month rolling window)
- Referral payout summaries and details

Outputs: Excel files uploaded to Google Drive folders
"""

import pandas as pd
import os
from dotenv import load_dotenv
from utils.fetch_parquet_utils import fetch_parquet
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
import numpy as np
from utils.gsheets_utils import export_dataframe_to_sheet, export_dataframe_to_drive

# Load environment variables
load_dotenv()

# Google Drive folder IDs from environment
ACCOUNTING_FOLDER_ID = os.getenv("ACCOUNTING_FOLDER_ID")
SETTLED_CDMX_FOLDER_ID = os.getenv("SETTLED_CDMX_FOLDER_ID")
LOAN_DETAIL_FOLDER_ID = os.getenv("LOAN_DETAIL_FOLDER_ID")
REFERRALS_FOLDER_ID = os.getenv("REFERRALS_FOLDER_ID")
REFERRALS_DETAIL_FOLDER_ID = os.getenv("REFERRALS_DETAIL_FOLDER_ID")

# display all columns
pd.set_option('display.max_columns', None)
pd.set_option('display.float_format', '{:.2f}'.format)

loans_data = fetch_parquet(parquet_file="loan.parquet")
loans = loans_data[loans_data['LoanStatus'] != 6].copy()

# Flag loans that are settled but underpaid (paid less than due)
loans['UnderpaidFlag'] = np.where(
    (loans['TotalAmountPaid'] < loans['TotalAmountDue']) &  (loans['LoanStatus'] == 2),
    True,
    False
)

# Calculate Overpaid amounts
loans['OverpaidAmount'] = np.where(
    loans['TotalAmountPaid'] > loans['TotalAmountDue'] ,
    round(loans["TotalAmountPaid"] - loans['TotalAmountDue'], 2),
    0
)

# Adjustment for overpaid amounts
loans['ApportionedAmountPaid'] = np.where(
    loans['TotalAmountPaid'] > loans['TotalAmountDue'] ,
    round(loans["TotalAmountDue"], 2),
    round(loans["TotalAmountPaid"], 2)
)

loans['IssueMonth'] = loans['IssueDate'].values.astype('datetime64[M]')
loans['IssueMonthCDMX'] = loans['IssueDateCDMX'].values.astype('datetime64[M]')
loans['SettledAtMonth'] = loans['SettledAt'].values.astype('datetime64[M]')
loans['SettledAtMonthCDMX'] = loans['SettledAtCDMX'].values.astype('datetime64[M]')
loans['DueDateMonth'] = loans['DueDate'].values.astype('datetime64[M]')

selected_columns = [
    'UserId',
    'UserLoanId',
    'IssueMonth',
    'IssueMonthCDMX',
    'IssueDate',
    'IssueDateCDMX',
    'DueDate',
    'DueDateMonth',
    'LoanStatus',
    'LoanNumber',
    'IsLate',
    'PrincipalAmount',
    'Fee',
    'TaxOnFee',
    'LateFee',
    'TaxOnLateFee',
    'TotalAmountDue',
    'LateFeePaid',
    'TaxOnLateFeePaid',
    'FeePaid',
    'TaxOnFeePaid',
    'PrincipalPaid',
    'ApportionedAmountPaid',
    'TotalAmountPaid',
    'OverpaidAmount',
    'JitOfferPolicy',
    'JitOfferPolicyName',
    'LastPaidDate',
    'LastPaidDateCDMX',
    'SettledAt',
    'SettledAtCDMX',
    'SettledAtMonth',
    'SettledAtMonthCDMX',
    'UnderpaidFlag',
    'DisputeAmount'
]

loan_repayment_detail = loans[selected_columns].copy()

loan_repayment_detail_2025 = loan_repayment_detail[loan_repayment_detail['IssueMonthCDMX'] >= '205-01-01'].copy()
loan_repayment_detail_2025['FeeRatio'] = loan_repayment_detail_2025['Fee'] / loan_repayment_detail_2025['PrincipalAmount']

# Flag loans that are settled but underpaid (paid less than due)
last_day_prev_month = (datetime.today().replace(day=1) - pd.Timedelta(days=1)).date()

accounting_cdmx = loan_repayment_detail.groupby('IssueMonthCDMX')[
    ['PrincipalAmount', 'Fee', 'TaxOnFee', 'LateFee', 'TaxOnLateFee', 'TotalAmountDue',
     'PrincipalPaid', 'FeePaid', 'TaxOnFeePaid', 'LateFeePaid', 'TaxOnLateFeePaid', 'ApportionedAmountPaid']
].sum().reset_index().round(2)

accounting_cdmx['IssueMonthCDMX'] = accounting_cdmx['IssueMonthCDMX'].dt.date
accounting_cdmx = accounting_cdmx[accounting_cdmx['IssueMonthCDMX'] < last_day_prev_month]

settled_cdmx = loan_repayment_detail.groupby('SettledAtMonthCDMX')[
    ['PrincipalPaid', 'FeePaid', 'TaxOnFeePaid', 'LateFeePaid', 'TaxOnLateFeePaid', 'ApportionedAmountPaid', 'DisputeAmount']
].sum().reset_index().round(2)

# Filter settled loans up to end of previous month
settled_cdmx['SettledAtMonthCDMX'] = pd.to_datetime(settled_cdmx['SettledAtMonthCDMX'], errors='coerce')
settled_cdmx['SettledAtMonthCDMX'] = settled_cdmx['SettledAtMonthCDMX'].dt.date
settled_cdmx = settled_cdmx[settled_cdmx['SettledAtMonthCDMX'] <= last_day_prev_month]

now = datetime.now()
timestamp = now.strftime("%Y%m%d_%H%M%S")

# Upload accounting summary (CDMX timezone only)
export_dataframe_to_drive(
    df=accounting_cdmx,
    folder_id=ACCOUNTING_FOLDER_ID,
    filename=f"accounting_cdmx_{timestamp}.xlsx"
)

print("Finished uploading accounting_cdmx to folder")

# Upload settled loans (CDMX timezone)
export_dataframe_to_drive(
    df=settled_cdmx,
    folder_id=SETTLED_CDMX_FOLDER_ID,
    filename=f"settled_cdmx_{timestamp}.xlsx"
)          

print("Finished uploading settled_cdmx to folder")

# Calculate 3-month rolling window (current month - 2 months to current month - 1 month)
first_day_3_months_ago = (last_day_prev_month.replace(day=1) - relativedelta(months=2)).replace(day=1)
first_day_last_month = last_day_prev_month.replace(day=1)

loan_repayment_detail_2025['IssueMonthCDMX'] = loan_repayment_detail_2025['IssueMonthCDMX'].dt.date

loan_repayment_detail_p3 = loan_repayment_detail_2025[loan_repayment_detail_2025['IssueMonthCDMX'] >= first_day_3_months_ago].copy()
loan_repayment_detail_p3 = loan_repayment_detail_p3[loan_repayment_detail_p3['IssueMonthCDMX']<= first_day_last_month]

export_dataframe_to_drive(
    df=loan_repayment_detail_p3,
    folder_id=LOAN_DETAIL_FOLDER_ID,
    filename=f"loan_origination_repayment_detail_{first_day_3_months_ago}_to_{first_day_last_month}.xlsx"
)

print("Finished uploading loan_origination_repayment_detail to folder")


# ============================================================================
# REFERRAL PAYOUTS PROCESSING
# ============================================================================

from utils.bootstrap import configure_project_root
project_root = configure_project_root()

import pandas as pd
from utils.fetch_data_utils import fetch_data

# Aggregate referral payouts by month
refferrals_data = fetch_data("""
SELECT
    DATEPART(YEAR, RP.ModifiedAt AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time (Mexico)') AS Year,
    DATEPART(MONTH, RP.ModifiedAt AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time (Mexico)') AS Month,
    COUNT(*) AS TotalTransactions,
    SUM(RP.Amount) AS TotalAmount
FROM ReferralPayouts RP
INNER JOIN Referrals R ON RP.ReferralId = R.ReferralId
INNER JOIN ReferralLinks RL ON R.ReferralLinkId = RL.ReferralLinkId
WHERE R.[Status] = 3 AND RP.Status = 2
GROUP BY
    DATEPART(YEAR, RP.ModifiedAt AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time (Mexico)'),
    DATEPART(MONTH, RP.ModifiedAt AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time (Mexico)')
ORDER BY Year, Month
"""
)

print("Data extracted successfully.")

# Get previous month and year for filename
prev_month_date = datetime.now().replace(day=1) - pd.Timedelta(days=1)
prev_month = prev_month_date.month
prev_year = prev_month_date.year

export_dataframe_to_drive(
    df=refferrals_data,
    folder_id=REFERRALS_FOLDER_ID,
    filename=f"referidos_{prev_year}_{prev_month}.xlsx"
)   

# Detailed referral transactions with referrer information
refferrals_detail = fetch_data("""
SELECT
    -- Referrer information (who got the money)
    referrer.PublicToken AS ReferrerPublicToken,

    -- Transaction details
    RP.Amount AS TransactionAmount,
    RP.ModifiedAt AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time (Mexico)' AS TransactionDate,

    -- Date parts for grouping
    DATEPART(YEAR, RP.ModifiedAt AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time (Mexico)') AS TransactionYear,
    DATEPART(MONTH, RP.ModifiedAt AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time (Mexico)') AS TransactionMonth

FROM Referrals R
INNER JOIN ReferralLinks RL ON R.ReferralLinkId = RL.ReferralLinkId
LEFT JOIN ReferralPayouts RP ON RP.ReferralId = R.ReferralId
INNER JOIN [User] referrer ON RL.UserId = referrer.UserId

WHERE
    R.[Status] = 3 -- CriteriaMet
    AND RP.Status = 2 -- Paid
"""
)

print("Data extracted successfully.")

# Format datetime columns for Excel compatibility (remove timezone info)
datetime_cols = refferrals_detail.select_dtypes(include=['datetimetz', 'datetime']).columns

refferrals_detail[datetime_cols] = refferrals_detail[datetime_cols].apply(
    lambda col: col.dt.strftime('%-m/%-d/%Y')  # Note: works on Unix/Linux/macOS
)
    
export_dataframe_to_drive(
    df=refferrals_detail,
    folder_id=REFERRALS_DETAIL_FOLDER_ID,
    filename=f"referidos_detalle_{prev_year}_{prev_month}.xlsx"
)