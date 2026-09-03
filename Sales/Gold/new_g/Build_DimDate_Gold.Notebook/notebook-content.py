# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "086407fd-b321-4254-b11b-0f2c1677636c",
# META       "default_lakehouse_name": "LH_Param_Demo",
# META       "default_lakehouse_workspace_id": "8b9e9590-f406-49ee-a8fa-43321bf8d391",
# META       "known_lakehouses": [
# META         {
# META           "id": "086407fd-b321-4254-b11b-0f2c1677636c"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 📅 Build gold.dim_date — Report-Ready Date Dimension (2018–2028)
# **Lakehouse:** LH_Param_Demo | **Target:** `gold.dim_date`
# 
# Static, continuous Date dimension (one row per day) 2018-01-01 -> 2028-12-31.
# - No DateKey (Date column is the join key)
# - MonthStartDate (bridges monthly Budget grain)
# - FiscalYear / FiscalQuarter
# - MonthYearLabel ("Jan 2024")
# - Columns arranged in logical order

# MARKDOWN ********************

# ## Step 1 — Imports

# CELL ********************

from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2 — Set the fixed range & fiscal start month

# CELL ********************

start_date = "2018-01-01"
end_date   = "2028-12-31"
FISCAL_START_MONTH = 4   # change to 1 if fiscal year = calendar year

print(f"Calendar: {start_date} -> {end_date} | Fiscal starts month {FISCAL_START_MONTH}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3 — Generate the calendar + attributes

# CELL ********************

date_df = spark.sql(f"""
    SELECT explode(sequence(
        to_date('{start_date}'),
        to_date('{end_date}'),
        interval 1 day)) AS Date
""")

dim_date = (date_df
    .withColumn("Year",           F.year("Date"))
    .withColumn("Quarter",        F.quarter("Date"))
    .withColumn("QuarterName",    F.concat(F.lit("Q"), F.quarter("Date")))
    .withColumn("Month",          F.month("Date"))
    .withColumn("MonthName",      F.date_format("Date", "MMMM"))
    .withColumn("MonthShort",     F.date_format("Date", "MMM"))
    .withColumn("MonthYearLabel", F.date_format("Date", "MMM yyyy"))
    .withColumn("YearMonth",      F.date_format("Date", "yyyy-MM"))
    .withColumn("MonthStartDate", F.trunc("Date", "month"))
    .withColumn("Day",            F.dayofmonth("Date"))
    .withColumn("DayOfWeek",      F.dayofweek("Date"))
    .withColumn("DayName",        F.date_format("Date", "EEEE"))
    .withColumn("WeekOfYear",     F.weekofyear("Date"))
    .withColumn("IsWeekend",      F.when(F.dayofweek("Date").isin(1, 7), True).otherwise(False))
    .withColumn("FiscalYear",
        F.when(F.month("Date") >= FISCAL_START_MONTH, F.year("Date") + 1).otherwise(F.year("Date")))
    .withColumn("FiscalQuarter",
        F.quarter(F.add_months("Date", -(FISCAL_START_MONTH - 1))))
)

dim_date = dim_date.select(
    "Date",
    "Year", "Quarter", "QuarterName",
    "Month", "MonthName", "MonthShort", "MonthYearLabel", "YearMonth", "MonthStartDate",
    "Day", "DayOfWeek", "DayName", "WeekOfYear", "IsWeekend",
    "FiscalYear", "FiscalQuarter"
)

print("Total days generated:", dim_date.count())
dim_date.show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 4 — 💾 Save to the GOLD schema

# CELL ********************

(dim_date.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("gold.dim_date"))

print("✅ gold.dim_date created successfully!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 5 — Verify

# CELL ********************

dd = spark.table("gold.dim_date")
print("Row count:", dd.count())
print("Min:", dd.agg(F.min('Date')).collect()[0][0], " Max:", dd.agg(F.max('Date')).collect()[0][0])
dd.orderBy("Date").show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📌 After loading — Semantic Model setup
# **A) Mark as Date Table** → choose the `Date` column.
# **B) Sort by column:** MonthName->Month, MonthShort->Month, QuarterName->Quarter, MonthYearLabel->YearMonth.
# **Relationships:** Invoice fact InvoiceDate -> DimDate[Date]; Budget fact BudgetMonth -> DimDate[MonthStartDate].
