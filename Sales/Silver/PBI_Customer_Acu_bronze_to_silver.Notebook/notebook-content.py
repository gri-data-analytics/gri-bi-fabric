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

# # PBI Customer (Acu) — Bronze to Silver Transformation
# 
# **Source table:** `bronze.bronze_acu_AR_Customers`  
# **Target table:** `silver.silver_acu_PBI_Customer`
# 
# This notebook replicates, step by step, the Power Query M logic used to build the **PBI Customer(Acu)** table from the **AR-Customers** Acumatica OData source.


# MARKDOWN ********************

# ## 1. Imports
# Load the PySpark functions needed for renaming, filtering, and type casting.

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import StringType


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Load Source Table
# Equivalent to the `Source` + `Navigation 1` steps — reads the AR-Customers data already landed in bronze.

# CELL ********************

df = spark.table("bronze.bronze_acu_AR_Customers")
display(df.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Rename Columns
# Equivalent to the **Renamed columns** step:
# - `CustomerID` → `Customer_ID`
# - `CustomerName` → `Customer_Name`
# - `CustomerRegion` → `Region`
# - `Country` → `CountryC`
# - `CountryName` → `Country`
# - `CustomerSegment` → `Customer_Segment`
# 
# *(Note: `Country` and `CountryName` swap in a chained rename — Spark's `withColumnRenamed` handles this safely since it doesn't rename in place until each call is applied.)*

# CELL ********************

df = (
    df.withColumnRenamed("CustomerID", "Customer_ID")
      .withColumnRenamed("CustomerName", "Customer_Name")
      .withColumnRenamed("CustomerRegion", "Region")
      .withColumnRenamed("Country", "CountryC")
      .withColumnRenamed("CountryName", "Country")
      .withColumnRenamed("CustomerSegment", "Customer_Segment")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Remove Duplicates
# Equivalent to the **Removed duplicates** step: keeps only the first occurrence of each distinct `Customer_ID`.

# CELL ********************

df = df.dropDuplicates(["Customer_ID"])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Add Column `Export_Category`
# Equivalent to the **Added custom** step: tags every row with a constant category label.

# CELL ********************

df = df.withColumn("Export_Category", F.lit("Export Sales"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Change Data Type of `Export_Category`
# Equivalent to the **Transform columns** step: ensures `Export_Category` is stored as a string/text type.

# CELL ********************

df = df.withColumn("Export_Category", F.col("Export_Category").cast(StringType()))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Replace Error Values in `Export_Category`
# Equivalent to the **Replace errors** step, which replaces any evaluation errors in `Export_Category` with `null`. Since this column is a constant literal with no expression that can error, there are no error values to replace in Spark — this step is a no-op here and is included only for parity with the original query.

# CELL ********************

# No-op: Export_Category is a constant literal and cannot produce error values in Spark.
# Included to mirror the Power Query step for traceability.


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. Filter Out Specific Customer IDs
# Equivalent to the **Custom** step: excludes two specific customer records (`"CS0193    "` and `"CS0371    "`, including their trailing spaces exactly as they appear in the source data).

# CELL ********************

df = df.filter(
    (F.col("Customer_ID") != "CS0193    ") & (F.col("Customer_ID") != "CS0371    ")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 9. Filter Out Customer IDs Containing "CS0381"
# Equivalent to the **Filtered rows** step: excludes any customer whose ID contains the substring `CS0381`.

# CELL ********************

df = df.filter(~F.col("Customer_ID").contains("CS0381"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 10. Preview Final Result
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

# ## 11. Write to Silver Schema
# Persist the final transformed table as a managed Delta table in the **silver** schema, overwriting any previous version.

# CELL ********************

df.write.mode("overwrite").format("delta").saveAsTable("silver.silver_acu_PBI_Customer")
print("Write complete: silver.silver_acu_PBI_Customer")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
