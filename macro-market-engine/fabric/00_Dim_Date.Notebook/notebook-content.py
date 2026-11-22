# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "MacroMarketLH",
# META       "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 00 · Dim_Date
# Builds the master calendar that every fact table joins to. Flags **US trading days**,
# month-ends and quarter-ends, and precomputes `NextTradingDay` so macro releases landing
# on a holiday/weekend can be aligned forward in the Silver layer.

# CELL ********************

import pandas as pd
import numpy as np
from pandas.tseries.holiday import USFederalHolidayCalendar, GoodFriday

START, END = "2000-01-01", "2030-12-31"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def build_dim_date(start, end):
    dates = pd.date_range(start, end, freq="D")
    cal = USFederalHolidayCalendar()
    holidays = set(cal.holidays(start=start, end=end)) | set(GoodFriday.dates(start, end))

    df = pd.DataFrame({"Date": dates})
    df["DateKey"]      = df["Date"].dt.strftime("%Y%m%d").astype(int)
    df["Year"]         = df["Date"].dt.year
    df["Quarter"]      = df["Date"].dt.quarter
    df["Month"]        = df["Date"].dt.month
    df["MonthName"]    = df["Date"].dt.strftime("%b")
    df["Day"]          = df["Date"].dt.day
    df["DayOfWeek"]    = df["Date"].dt.dayofweek
    df["IsWeekday"]    = df["DayOfWeek"] < 5
    df["IsHoliday"]    = df["Date"].isin(holidays)
    df["IsTradingDay"] = df["IsWeekday"] & ~df["IsHoliday"]
    df["IsMonthEnd"]   = df["Date"].dt.is_month_end
    df["IsQuarterEnd"] = df["Date"].dt.is_quarter_end

    trading = df.loc[df["IsTradingDay"], "Date"].reset_index(drop=True)
    idx = np.clip(trading.searchsorted(df["Date"].values, side="left"), 0, len(trading) - 1)
    df["NextTradingDay"] = trading.iloc[idx].values
    return df

pdf = build_dim_date(START, END)
print(pdf.shape, "rows;", int(pdf["IsTradingDay"].sum()), "trading days")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Persist as a Gold dimension. Dim_Date is static-ish, so a full overwrite is fine.
sdf = spark.createDataFrame(pdf)
(sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("Dim_Date"))

spark.sql("OPTIMIZE Dim_Date ZORDER BY (DateKey)")
display(spark.table("Dim_Date").limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
