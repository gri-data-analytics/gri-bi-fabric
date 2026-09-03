# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Budget Consolidation – PySpark (converted from Power Query M)
# 
# This notebook reproduces the original Power Query logic in **PySpark** for Microsoft Fabric.
# Each transformation from the M script is isolated into its own cell with an explanation.
# 
# **Assumptions**
# - The five source tables already exist as tables in the attached **Lakehouse** (adjust names if they are files).
# - Table names containing special characters (e.g. `ST3_Budget(Acu)`) are read via `spark.read.table` using backticks.
# - `unionByName(..., allowMissingColumns=True)` is used to mimic Power Query's `Table.Combine`, which matches columns **by name** (not position).


# MARKDOWN ********************

# ## 0. Imports & setup
# 
# Import the functions and types used throughout the notebook.

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, DateType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. `Source` — combine the five budget tables
# 
# Equivalent of:
# ```m
# Source = Table.Combine({#"ST3_Budget(Acu)", #"ST1_Budget(Acu)",
#                         #"ST1_Budget_NonEx(Acu)", #"ST3_Budget_NonEx(Acu)",
#                         #"IFS Budget 2025"})
# ```
# `Table.Combine` stacks the tables and aligns columns **by name**. In Spark we replicate
# this with `unionByName(allowMissingColumns=True)` so that any table missing a column
# still unions cleanly (missing values become `null`).

# CELL ********************

# Read each source table (adjust to spark.read.parquet/format if they are files)
df_st3_budget        = spark.read.table("`ST3_Budget(Acu)`")
df_st1_budget        = spark.read.table("`ST1_Budget(Acu)`")
df_st1_budget_nonex  = spark.read.table("`ST1_Budget_NonEx(Acu)`")
df_st3_budget_nonex  = spark.read.table("`ST3_Budget_NonEx(Acu)`")
df_ifs_budget_2025   = spark.read.table("`IFS Budget 2025`")

source_tables = [
    df_st3_budget,
    df_st1_budget,
    df_st1_budget_nonex,
    df_st3_budget_nonex,
    df_ifs_budget_2025,
]

# Combine by column name (Power Query Table.Combine semantics)
source = source_tables[0]
for tbl in source_tables[1:]:
    source = source.unionByName(tbl, allowMissingColumns=True)

df = source

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Add custom column `Sales_BudAmt($k)`
# 
# Equivalent of:
# ```m
# #"Added custom" = Table.AddColumn(Source, "Sales_BudAmt($k)", each [SalesBudAmt] / 1000)
# #"Changed column type" = Table.TransformColumnTypes(..., {{"Sales_BudAmt($k)", type number}})
# ```
# Creates the sales budget amount expressed in thousands and casts it to a numeric
# (`double`) type, matching the `type number` change in M.

# CELL ********************

df = df.withColumn(
    "Sales_BudAmt($k)",
    (F.col("SalesBudAmt") / F.lit(1000)).cast(DoubleType())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Add custom column `Weight(MT)`
# 
# Equivalent of:
# ```m
# #"Added custom 1" = Table.AddColumn(..., "Weight(MT)", each [SalesBud] / 1000)
# #"Changed column type 1" = Table.TransformColumnTypes(..., {{"Weight(MT)", type number}})
# ```
# Derives the weight in metric tonnes from `SalesBud` (divided by 1000) and casts to
# numeric. Note this is created **before** `SalesBud` is renamed, exactly as in the M order.

# CELL ********************

df = df.withColumn(
    "Weight(MT)",
    (F.col("SalesBud") / F.lit(1000)).cast(DoubleType())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Rename `SalesBud` → `Weight`
# 
# Equivalent of:
# ```m
# #"Renamed columns" = Table.RenameColumns(..., {{"SalesBud", "Weight"}})
# ```

# CELL ********************

df = df.withColumnRenamed("SalesBud", "Weight")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Remove columns `Refid` and `FinBudAmount`
# 
# Equivalent of:
# ```m
# #"Removed columns" = Table.RemoveColumns(..., {"Refid", "FinBudAmount"})
# ```

# CELL ********************

df = df.drop("Refid", "FinBudAmount")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Change `Month` column type to `date`
# 
# Equivalent of:
# ```m
# #"Changed column type 2" = Table.TransformColumnTypes(..., {{"Month", type date}})
# ```
# Casts `Month` to a `date` type. If `Month` is a string with a specific format,
# use `F.to_date(F.col("Month"), "<format>")` instead of a plain cast.

# CELL ********************

df = df.withColumn("Month", F.col("Month").cast(DateType()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Result — preview & (optional) save
# 
# The DataFrame `df` now matches the final `#"Changed column type 2"` step of the
# Power Query script. Preview it and optionally persist it as a Lakehouse table.

# CELL ********************

display(df)

# Optional: write the consolidated budget to the Lakehouse
# df.write.mode("overwrite").saveAsTable("Budget_Consolidated")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
