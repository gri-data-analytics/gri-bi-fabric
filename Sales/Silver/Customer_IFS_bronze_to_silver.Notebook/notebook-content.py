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

# # Customer (IFS) — Bronze to Silver Transformation
# 
# **Source table:** `bronze.bronze_ifs_Customer`  
# **Target table:** `silver.silver_ifs_Customer`
# 
# This notebook replicates, step by step, the Power Query M logic used to build the **Customer(IFS)** table. As with the other IFS tables, the script provided starts directly from `Source`, so we read straight from the bronze table.


# MARKDOWN ********************

# ## 1. Imports
# Load the PySpark functions and types needed for renaming and casting.

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
# Reads the IFS Customer data already landed in bronze. Run `printSchema()` first to confirm actual column names before trusting the rename step below.

# CELL ********************

df = spark.table("bronze.bronze_ifs_Customer")
df.printSchema()
display(df.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Rename Columns
# Equivalent to the **Renamed columns** step:
# - `CUSTOMETID` → `Customer_ID` *(note: kept the exact source spelling — this appears to be a typo for `CUSTOMERID` in the original system, but the source column is genuinely named `CUSTOMETID`, so it's preserved as-is here)*
# - `CUSTOMERNAME` → `Customer_Name`
# - `REGION` → `Region`
# - `COUNTRYNAME` → `Country`
# - `CUSTOMERSEGMENT` → `Customer_Segment`

# CELL ********************

df = (
    df.withColumnRenamed("CUSTOMETID", "Customer_ID")
      .withColumnRenamed("CUSTOMERNAME", "Customer_Name")
      .withColumnRenamed("REGION", "Region")
      .withColumnRenamed("COUNTRYNAME", "Country")
      .withColumnRenamed("CUSTOMERSEGMENT", "Customer_Segment")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Remove Duplicates on `Customer_ID`
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

df = df.withColumn("Export_Category", F.lit("Local Sales"))


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
# Equivalent to the **Replace errors** step. Since this column is a constant literal, there are no error values to replace in Spark — this step is a no-op here, included only for parity with the original query.

# CELL ********************

# No-op: Export_Category is a constant literal and cannot produce error values in Spark.
# Included to mirror the Power Query step for traceability.


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. Preview Final Result
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

# ## 9. Write to Silver Schema
# Persist the final transformed table as a managed Delta table in the **silver** schema, overwriting any previous version.

# CELL ********************

df.write.mode("overwrite").format("delta").saveAsTable("silver.silver_ifs_Customer")
print("Write complete: silver.silver_ifs_Customer")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
