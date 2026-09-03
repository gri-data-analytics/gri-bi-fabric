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

# # Budget_All (Refined) — Silver to Silver Transformation
# 
# **Source table:** `silver.silver_budget_all`  
# **Target table:** `silver.silver_budget_all_refined_pbi`
# 
# This notebook replicates the Power Query M logic for this downstream **Budget_All** query, which reads from a Power Platform Dataflow. That dataflow is fed by the same underlying data as our `silver.silver_budget_all` table, so we read directly from there instead of reconnecting to the dataflow — matching the same pattern used for the Invoice_All refinement notebook.
# 
# > **Column name mapping:** the M script references `[Sales_BudAmt($k)]` and `[Weight(MT)]` — these correspond to `Sales_BudAmt_k` and `Weight_MT` in our silver table (renamed earlier to satisfy Delta's column-naming restrictions). Run `printSchema()` in cell 2 to confirm these exact names before proceeding.
# 
# > **Target table name:** following the same `_refined_pbi` convention used for `silver_invoice_all_refined_pbi` — rename cell 8's target if you'd prefer something else.


# MARKDOWN ********************

# ## 1. Imports
# Load the PySpark functions and types needed for this transformation.

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Load Source Table
# Reads the already-built `silver_budget_all` table. Run `printSchema()` first to confirm the exact names of `Sales_BudAmt_k` and `Weight_MT`.

# CELL ********************

df = spark.table("silver.silver_budget_all")
df.printSchema()
display(df.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Add Column `Zero Logic`
# Equivalent to the **Added Custom** step: sums `Sales_BudAmt_k` and `Weight_MT` to create a combined check value used to filter out fully-zero rows.
# 
# *(Renamed to `Zero_Logic` here — Delta doesn't allow spaces in column names.)*

# CELL ********************

df = df.withColumn("Zero_Logic", F.col("Sales_BudAmt_k") + F.col("Weight_MT"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Change Data Type of `Zero_Logic`
# Equivalent to the **Changed Type** step: ensures `Zero_Logic` is numeric (double).

# CELL ********************

df = df.withColumn("Zero_Logic", F.col("Zero_Logic").cast(DoubleType()))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Filter Out Rows Where `Zero_Logic` Equals Zero
# Equivalent to the **Filtered Rows** step: removes rows where both `Sales_BudAmt_k` and `Weight_MT` are zero (or cancel out to zero).

# CELL ********************

df = df.filter(F.col("Zero_Logic") != 0)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Add Column `Z`
# Equivalent to the **Added Custom1** step: a binary flag that's `0` when *both* `Weight_MT` and `Sales_BudAmt_k` are zero or null, and `1` otherwise.
# 
# `Z = if (Weight_MT = 0 or Weight_MT is null) and (Sales_BudAmt_k = 0 or Sales_BudAmt_k is null) then 0 else 1`

# CELL ********************

weight_is_zero_or_null = (F.col("Weight_MT") == 0) | (F.col("Weight_MT").isNull())
budamt_is_zero_or_null = (F.col("Sales_BudAmt_k") == 0) | (F.col("Sales_BudAmt_k").isNull())

df = df.withColumn(
    "Z",
    F.when(weight_is_zero_or_null & budamt_is_zero_or_null, F.lit(0)).otherwise(F.lit(1))
)
df = df.withColumn("Z", F.col("Z").cast(IntegerType()))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Preview Final Result
# Quick sanity check of the transformed dataframe before writing it out.

# CELL ********************

display(df.limit(20))
print(f"Row count: {df.count()}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. Write to Silver Schema
# Persist the final refined table as a managed Delta table in the **silver** schema, overwriting any previous version.

# CELL ********************

df.write.mode("overwrite").format("delta").saveAsTable("silver.silver_budget_all_refined_pbi")
print("Write complete: silver.silver_budget_all_refined_pbi")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
