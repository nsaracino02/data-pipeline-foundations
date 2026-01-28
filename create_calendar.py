"""
Create Calendar Dimension Table

Generates a date dimension table with Mexico-specific business calendar features:
- Quincenas: Bi-monthly payroll periods (15th and end-of-month)
- Weekend adjustments: Quincena dates moved to Friday if they fall on weekends
- Relative day calculations: Days before/after each quincena for cohort analysis

Date Range: August 2022 - Present (filtered to September 2022+ for data availability)
Output: data/dim_calendar.parquet
"""

import os
import pandas as pd
from datetime import timedelta

# Output configuration
OUTPUT_DIR = os.getenv("DATA_DIR", "data")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "dim_calendar.parquet")

# ========================================
# DATE RANGE SETUP
# ========================================
# Start from August 2022 to capture full quincena cycles
# End at today to include current period
start_date = pd.to_datetime("2022-08-01")
end_date = pd.to_datetime("today").normalize()

# ========================================
# GENERATE CALENDAR WITH QUINCENAS
# ========================================
# Quincena: Mexico's bi-monthly payroll period
# - Q1 (Quincena 1): 1st-15th of month, paid on 15th
# - Q2 (Quincena 2): 16th-end of month, paid on last day

data = []
current = start_date
prev_q2 = None

while current <= end_date:
    month_start = current.replace(day=1)
    month_end = (month_start + pd.offsets.MonthEnd(0)).date()
    days_in_month = pd.date_range(start=month_start, end=month_end, freq='D')

    # Define quincena payment dates (15th and end-of-month)
    q1 = pd.Timestamp(month_start.year, month_start.month, 15)
    q2 = pd.Timestamp(month_end)

    # Adjust quincena dates if they fall on weekends
    # Saturday → Friday, Sunday → Friday (ensures business day payment)
    def adjust(date):
        if date.weekday() == 5:  # Saturday
            return date - timedelta(days=1)
        elif date.weekday() == 6:  # Sunday
            return date - timedelta(days=2)
        return date

    q1_adj = adjust(q1)
    q2_adj = adjust(q2)

    for day in days_in_month:
        quincena = q1_adj if day <= q1_adj else q2_adj
        prev_quincena = prev_q2 if day <= q1_adj else q1_adj

        data.append({
            'DateMonth': month_start.date(),
            'DateDay': day.date(),
            'Quincena': quincena.date(),
            'IsQuincena': day.date() == quincena.date(),
            'PrevQuincena': prev_quincena.date() if prev_quincena else None,
            'DayOfWeek': day.strftime('%A'),
            # Days relative to quincena: negative = before, positive = after
            # Used for cohort analysis (e.g., "loans due 3 days after quincena")
            'DayRelativeToQuincena': (day.date() - quincena.date()).days
        })

    prev_q2 = q2_adj  # Update for next loop
    current += pd.offsets.MonthBegin(1)

# Create DataFrame
df = pd.DataFrame(data)

# Filter to September 2022+ (aligns with loan data availability)
df = df[df['DateDay'] >= pd.to_datetime("2022-09-01").date()]

print(f"Calendar dimension created: {len(df)} days from {df['DateDay'].min()} to {df['DateDay'].max()}")

# ========================================
# SAVE OUTPUT
# ========================================
df.to_parquet(OUTPUT_FILE, index=False)
print(f"Calendar dimension stored at: {OUTPUT_FILE}")

# ========================================
# TODO: ADD HOLIDAYS
# ========================================
# Future enhancement: Add Mexican federal holidays column
# Reference: https://www.gob.mx/cms/uploads/attachment/file/156203/1044_Ley_Federal_del_Trabajo.pdf
