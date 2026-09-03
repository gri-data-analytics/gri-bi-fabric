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

# # PBI Invoice Data (Acu) — Bronze to Silver Transformation
# 
# **Source table:** `bronze.bronze_acu_PBI_Sales_Test`  
# **Target table:** `silver.silver_acu_PBI_Sales_Test`
# 
# This notebook replicates, step by step, the Power Query M logic used to build the **PBI Invoice Data(Acu)** table from the **PBI Sales Test** Acumatica OData source, so it can run natively as a PySpark transformation inside Microsoft Fabric.


# MARKDOWN ********************

# ## 1. Imports
# Load the PySpark functions and types needed for casting, conditional logic, and literals.

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Load Source Table
# Equivalent to the `Source` + `Navigation` steps in Power Query — here the OData table has already been landed as-is in the **bronze** schema, so we simply read it.

# CELL ********************

df = spark.table("bronze.bronze_acu_PBI_Sales_Test")
display(df.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Replace Nulls in `CurrencyRate`
# Equivalent to the **Replaced value** step: any `null` in `CurrencyRate` is replaced with `1`.

# CELL ********************

df = df.withColumn(
    "CurrencyRate",
    F.when(F.col("CurrencyRate").isNull(), F.lit(1)).otherwise(F.col("CurrencyRate"))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Change `LineWeight` Data Type
# Equivalent to the **Changed column type** step: cast `LineWeight` to a numeric (double) type.

# CELL ********************

df = df.withColumn("LineWeight", F.col("LineWeight").cast(DoubleType()))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Replace Nulls in `RateReciprocal`
# Equivalent to the second **Replaced value** step: any `null` in `RateReciprocal` is replaced with `1`.

# CELL ********************

df = df.withColumn(
    "RateReciprocal",
    F.when(F.col("RateReciprocal").isNull(), F.lit(1)).otherwise(F.col("RateReciprocal"))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Add Column `LineAmount2_USD`
# Equivalent to the **Added custom** step: flips the sign of `LineAmount` for Credit Memos so that credits reduce the total, and keeps invoices as-is.
# 
# `LineAmount2_USD = if Type = "Credit Memo" then -LineAmount else LineAmount`

# CELL ********************

df = df.withColumn(
    "LineAmount2_USD",
    F.when(F.col("Type") == "Credit Memo", -F.col("LineAmount")).otherwise(F.col("LineAmount"))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Add Column `Weight(Kg)`
# Equivalent to the second **Added custom** step: same sign-flip logic applied to `LineWeight` for Credit Memos.
# 
# `Weight(Kg) = if Type = "Credit Memo" then -LineWeight else LineWeight`

# CELL ********************

df = df.withColumn(
    "Weight(Kg)",
    F.when(F.col("Type") == "Credit Memo", -F.col("LineWeight")).otherwise(F.col("LineWeight"))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. Change Data Types of New Columns
# Equivalent to the **Changed column type** step: ensure `LineAmount2_USD` and `Weight(Kg)` are numeric (double) after being computed.

# CELL ********************

df = df.withColumn("LineAmount2_USD", F.col("LineAmount2_USD").cast(DoubleType()))
df = df.withColumn("Weight(Kg)", F.col("Weight(Kg)").cast(DoubleType()))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 9. Add Column `USD Value`
# Equivalent to the **Added custom** step: converts the line amount to USD by multiplying by the currency rate reciprocal.
# 
# `USD Value = LineAmount2_USD * RateReciprocal`

# CELL ********************

df = df.withColumn("USD Value", F.col("LineAmount2_USD") * F.col("RateReciprocal"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 10. Change Data Type of `USD Value`
# Equivalent to the **Changed column type** step: cast the newly computed `USD Value` to a numeric (double) type.

# CELL ********************

df = df.withColumn("USD Value", F.col("USD Value").cast(DoubleType()))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 11. Rename `ItemClass` to `BU`
# Equivalent to the **Renamed columns** step.

# CELL ********************

df = df.withColumnRenamed("ItemClass", "BU")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 12. Replace `"ST2"` with `"ST1"` in `BU`
# Equivalent to the **Replaced value** step: standardizes the business unit code so `ST2` rows are reclassified as `ST1`.

# CELL ********************

df = df.withColumn(
    "BU",
    F.when(F.col("BU") == "ST2", F.lit("ST1")).otherwise(F.col("BU"))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 13. Rename Remaining Columns
# Equivalent to the second **Renamed columns** step:
# - `Weight(Kg)` → `Weight`
# - `USD Value` → `Invoice Value`
# - `Customer` → `Customer_ID`
# - `CustomerName` → `Customer_Name`

# CELL ********************

df = (
    df.withColumnRenamed("Weight(Kg)", "Weight")
      .withColumnRenamed("USD Value", "Invoice_Value")
      .withColumnRenamed("Customer", "Customer_ID")
      .withColumnRenamed("CustomerName", "Customer_Name")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# ## 14. Filter Rows
# Equivalent to the **Filtered rows** step: keep only `Invoice` type transactions, excluding any that have been `Canceled`.

# CELL ********************

df = df.filter((F.col("Type") == "Invoice") & (F.col("Status") != "Canceled"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 15. Add Column `DB`
# Equivalent to the **Added custom** step: tags every row with a constant source-system identifier.

# CELL ********************

df = df.withColumn("DB", F.lit("ACU"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 16. Change Data Type of `DB`
# Equivalent to the **Changed column type** step: ensures `DB` is stored as a string/text type.

# CELL ********************

df = df.withColumn("DB", F.col("DB").cast(StringType()))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 17. Preview Final Result
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

# ## 18. Write to Silver Schema
# Persist the final transformed table as a managed Delta table in the **silver** schema, overwriting any previous version.

# CELL ********************

df.write.mode("overwrite").format("delta").saveAsTable("silver.silver_acu_PBI_Sales_Test")
print("Write complete: silver.silver_acu_PBI_Sales_Test")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
