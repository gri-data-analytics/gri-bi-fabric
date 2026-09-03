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

# # PBI OG Value (Acu) — Bronze to Silver Transformation
# 
# **Source table:** `bronze.bronze_acu_Sales_Data`  
# **Target table:** `silver.silver_acu_PBI_OG_Value`
# 
# This notebook replicates, step by step, the Power Query M logic used to build the **PBI OG Value(Acu)** table from the **Sales Data** Acumatica OData source.


# MARKDOWN ********************

# ## 1. Imports
# Load the PySpark functions and types needed for casting, conditional logic, and filtering.

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Load Source Table
# Equivalent to the `Source` + `Navigation` steps — reads the Sales Data already landed in bronze.

# CELL ********************

df = spark.table("bronze.bronze_acu_Sales_Data")
display(df.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Replace Nulls in `RateReciprocal`
# Equivalent to the first **Replaced value** step: any `null` in `RateReciprocal` is replaced with `1`.

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

# ## 4. Replace Nulls in `CurrencyRate`
# Equivalent to the second **Replaced value** step: any `null` in `CurrencyRate` is replaced with `1`.

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

# ## 5. Add Column `USD Value`
# Equivalent to the **Added custom** step: converts the order total to USD by multiplying by the currency rate reciprocal.
# 
# `USD Value = OrderTotal * RateReciprocal`

# CELL ********************

df = df.withColumn("USD Value", F.col("OrderTotal") * F.col("RateReciprocal"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Transform / Replace Errors on `USD Value`
# The original query temporarily casts `USD Value` to text and replaces any conversion errors with `null`, before casting it back to a number a few steps later (steps 5-6 in the M script). This round-trip has no lasting effect once the column is re-cast to numeric, so in Spark we skip the intermediate text conversion and go straight to ensuring `USD Value` is numeric, with nulls preserved as nulls (matching the net behavior of the original steps).

# CELL ********************

df = df.withColumn("USD Value", F.col("USD Value").cast(DoubleType()))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Rename Columns
# Equivalent to the **Renamed columns** step:
# - `Customer` → `Customer_ID`
# - `CustomerName` → `Customer_Name`
# - `itemclass_Formula1259fbf58c3049b39e9474e4e1b76799` → `BU`
# - `OrderWeight` → `Weight`

# CELL ********************

df = (
    df.withColumnRenamed("Customer", "Customer_ID")
      .withColumnRenamed("CustomerName", "Customer_Name")
      .withColumnRenamed("itemclass_Formula1259fbf58c3049b39e9474e4e1b76799", "BU")
      .withColumnRenamed("OrderWeight", "Weight")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

df = (
    df.withColumnRenamed("Customer", "Customer_ID")
      .withColumnRenamed("CustomerName", "Customer_Name")
      .withColumnRenamed("itemclass_Formula1259fbf58c3049b39e9474e4e1b76799", "BU")
      .withColumnRenamed("OrderWeight", "Weight")
      .withColumnRenamed("USD Value", "USD_Value")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. Change Data Types of `Weight` and `USD Value`
# Equivalent to the **Changed column type** step: ensures both columns are numeric (double).

# CELL ********************

df = df.withColumn("Weight", F.col("Weight").cast(DoubleType()))
df = df.withColumn("USD Value", F.col("USD Value").cast(DoubleType()))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

df = df.withColumn("Weight", F.col("Weight").cast(DoubleType()))
df = df.withColumn("USD_Value", F.col("USD_Value").cast(DoubleType()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 9. Rename `CustomerClass` to `Region`
# Equivalent to the second **Renamed columns** step.

# CELL ********************

df = df.withColumnRenamed("CustomerClass", "Region")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 10. Replace `"ST2"` with `"ST1"` in `BU`
# Equivalent to the third **Replaced value** step: standardizes the business unit code so `ST2` rows are reclassified as `ST1`.

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

# ## 11. Filter Out Canceled Rows
# Equivalent to the first **Filtered rows** step: excludes any records with `Status = "Canceled"`.

# CELL ********************

df = df.filter(F.col("Status") != "Canceled")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 12. Filter to Dates After 2023-08-01
# Equivalent to the second **Filtered rows** step: keeps only records with a `Date` strictly after `2023-08-01 00:00:00`.

# CELL ********************

df = df.filter(F.col("Date") > F.lit("2023-08-01 00:00:00"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.withColumn("Source_System", F.lit("ACU"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 13. Preview Final Result
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

# ## 14. Write to Silver Schema
# Persist the final transformed table as a managed Delta table in the **silver** schema, overwriting any previous version.
# 
# **Note:** `USD Value` contains a space, which Delta allows, but if you'd prefer a SQL-friendly name (as we did for the earlier PBI Invoice Details table), rename it to `USD_Value` or `Invoice_Value` before this write step.

# CELL ********************

df.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable("silver.silver_acu_PBI_OG_Value")
print("Write complete: silver.silver_acu_PBI_OG_Value")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
