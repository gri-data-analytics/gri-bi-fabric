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

# # IFS Budget 2025 — Bronze to Silver Transformation
# 
# **Source table:** `bronze.bronze_ifs_IFS_Budget_2025`  
# **Target table:** `silver.silver_ifs_IFS_Budget_2025`
# 
# This notebook replicates, step by step, the Power Query M logic used to build the **IFS Budget 2025** table. As with the previous IFS table, the M script provided starts partway through (at `Changed column type`, referencing `Source` directly), so we read straight from the bronze table.


# MARKDOWN ********************

# ## 1. Imports
# Load the PySpark functions and types needed for casting and renaming.

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DateType


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Load Source Table
# Reads the IFS Budget 2025 data already landed in bronze. Run `printSchema()` first to confirm actual column names before trusting the rename step below.

# CELL ********************

df = spark.table("bronze.bronze_ifs_IFS_Budget_2025")
df.printSchema()
display(df.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Change Data Type of `MONTH`
# Equivalent to the **Changed column type** step: casts `MONTH` to a proper date type.

# CELL ********************

df = df.withColumn("MONTH", F.col("MONTH").cast(DateType()))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Rename Columns
# Equivalent to the **Renamed columns** step:
# - `CUSTOMER_ID` → `Customer_ID`
# - `CUSTOMER_NAME` → `Customer_Name`
# - `MONTH` → `Month`
# - `SALESBUD` → `SalesBud`
# - `SALESBUDAMT` → `SalesBudAmt`

# CELL ********************

df = (
    df.withColumnRenamed("CUSTOMER_ID", "Customer_ID")
      .withColumnRenamed("CUSTOMER_NAME", "Customer_Name")
      .withColumnRenamed("MONTH", "Month")
      .withColumnRenamed("SALESBUD", "SalesBud")
      .withColumnRenamed("SALESBUDAMT", "SalesBudAmt")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Preview Final Result
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

# ## 6. Write to Silver Schema
# Persist the final transformed table as a managed Delta table in the **silver** schema, overwriting any previous version.

# CELL ********************

df.write.mode("overwrite").format("delta").saveAsTable("silver.silver_ifs_IFS_Budget_2025")
print("Write complete: silver.silver_ifs_IFS_Budget_2025")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
