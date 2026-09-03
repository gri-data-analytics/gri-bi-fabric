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

# # Budget_All — Silver Combine Transformation
# 
# **Source tables:**
# - `silver.silver_acu_st3_budget` (ST3_Budget(Acu))
# - `silver.silver_acu_st1_budget` (ST1_Budget(Acu))
# - `silver.silver_acu_st1_budget_nonex` (ST1_Budget_NonEx(Acu))
# - `silver.silver_acu_st3_budget_nonex` (ST3_Budget_NonEx(Acu))
# - `silver.silver_ifs_ifs_budget_2025` (IFS Budget 2025)
# 
# **Target table:** `silver.silver_budget_all`
# 
# This notebook replicates the Power Query M logic for **Budget_All**, which combines (unions) five already-built silver tables, then adds derived columns.
# 
# > **Important:** as with Invoice_All, `Table.Combine` unions by matching column names. These five source tables don't all have identical schemas (e.g. `ST3_Budget_NonEx(Acu)` carries extra columns like `FinBudAmount` that the budget-only tables don't). **Run the `printSchema()` calls in cell 2 and compare all five schemas before proceeding.**


# MARKDOWN ********************

# ## 1. Imports
# Load the PySpark functions and types needed for this transformation.

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, DateType


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Load and Inspect the Five Source Tables
# Equivalent to the `Source` step's inputs. Load each silver table individually and compare schemas before combining, to catch any column-naming mismatches early.

# CELL ********************

df_st3_budget       = spark.table("silver.silver_acu_st3_budget")
df_st1_budget       = spark.table("silver.silver_acu_st1_budget")
df_st1_budget_nonex = spark.table("silver.silver_acu_st1_budget_nonex")
df_st3_budget_nonex = spark.table("silver.silver_acu_st3_budget_nonex")
df_ifs_budget       = spark.table("silver.silver_ifs_ifs_budget_2025")

print("--- ST3_Budget(Acu) schema ---")
df_st3_budget.printSchema()
print("--- ST1_Budget(Acu) schema ---")
df_st1_budget.printSchema()
print("--- ST1_Budget_NonEx(Acu) schema ---")
df_st1_budget_nonex.printSchema()
print("--- ST3_Budget_NonEx(Acu) schema ---")
df_st3_budget_nonex.printSchema()
print("--- IFS Budget 2025 schema ---")
df_ifs_budget.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Combine (Union) the Five Tables
# Equivalent to the **Source = Table.Combine(...)** step: stacks all five tables into one, aligning by column name and filling missing columns with `null` where a source doesn't have them.

# CELL ********************

df = df_st3_budget.unionByName(df_st1_budget, allowMissingColumns=True)
df = df.unionByName(df_st1_budget_nonex, allowMissingColumns=True)
df = df.unionByName(df_st3_budget_nonex, allowMissingColumns=True)
df = df.unionByName(df_ifs_budget, allowMissingColumns=True)

display(df.limit(20))
print(f"Combined row count: {df.count()}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Add Column `Sales_BudAmt($k)`
# Equivalent to the first **Added custom** step: converts `SalesBudAmt` to thousands.
# 
# *(Renamed to `Sales_BudAmt_k` here — Delta doesn't allow `$`, `(`, or `)` in column names.)*

# CELL ********************

df = df.withColumn("Sales_BudAmt_k", F.col("SalesBudAmt") / 1000)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Change Data Type of `Sales_BudAmt_k`
# Equivalent to the **Changed column type** step: ensures `Sales_BudAmt_k` is numeric (double).

# CELL ********************

df = df.withColumn("Sales_BudAmt_k", F.col("Sales_BudAmt_k").cast(DoubleType()))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Add Column `Weight(MT)`
# Equivalent to the second **Added custom** step: converts `SalesBud` to metric tons.
# 
# *(Renamed to `Weight_MT` here — Delta doesn't allow parentheses in column names.)*

# CELL ********************

df = df.withColumn("Weight_MT", F.col("SalesBud") / 1000)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Change Data Type of `Weight_MT`
# Equivalent to the **Changed column type** step: ensures `Weight_MT` is numeric (double).

# CELL ********************

df = df.withColumn("Weight_MT", F.col("Weight_MT").cast(DoubleType()))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. Rename `SalesBud` to `Weight`
# Equivalent to the **Renamed columns** step.

# CELL ********************

df = df.withColumnRenamed("SalesBud", "Weight")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 9. Remove Columns `Refid` and `FinBudAmount`
# Equivalent to the **Removed columns** step: drops columns not needed downstream. Using `.drop()` here rather than an explicit column reference, since PySpark's `.drop()` is safe to call even if a column doesn't exist in the combined schema (unlike `F.col()` or `withColumnRenamed`, which would throw `UNRESOLVED_COLUMN`). This matters here because not all five source tables necessarily carry these columns.

# CELL ********************

df = df.drop("Refid", "FinBudAmount")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 10. Change Data Type of `Month`
# Equivalent to the final **Changed column type** step: ensures `Month` is a proper date type.

# CELL ********************

df = df.withColumn("Month", F.col("Month").cast(DateType()))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 11. Preview Final Result
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

# ## 12. Write to Silver Schema
# Persist the final combined table as a managed Delta table in the **silver** schema, overwriting any previous version.

# CELL ********************

df.write.mode("overwrite").format("delta").saveAsTable("silver.silver_budget_all")
print("Write complete: silver.silver_budget_all")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
