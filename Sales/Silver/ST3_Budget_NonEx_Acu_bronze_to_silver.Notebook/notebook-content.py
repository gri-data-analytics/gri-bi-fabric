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

# # ST3_Budget_NonEx (Acu) — Bronze to Silver Transformation
# 
# **Source table:** `bronze.bronze_acu_PBI_Budget_newPneu`  
# **Target table:** `silver.silver_acu_ST3_Budget_NonEx`
# 
# This notebook replicates, step by step, the Power Query M logic used to build the **ST3_Budget_NonEx(Acu)** table from the **PBI_Budget_newPneu** Acumatica OData source.


# MARKDOWN ********************

# ## 1. Imports
# Load the PySpark functions and types needed for renaming, deduplication, and date casting.

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
# Equivalent to the `Source` + `Navigation 1` steps — reads the PBI_Budget_newPneu data already landed in bronze.

# CELL ********************

df = spark.table("bronze.bronze_acu_PBI_Budget_newPneu")
display(df.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Rename Columns
# Equivalent to the **Renamed columns** step:
# - `CustomerCode` → `Customer_ID`
# - `CustomerName` → `Customer_Name`
# - `BudgetMonth` → `Month`
# - `SalesBudAmount` → `SalesBudAmt`
# - `ST3_Formula1bea30309a5747c696827198a3d72671` → `BU`

# CELL ********************

df = (
    df.withColumnRenamed("CustomerCode", "Customer_ID")
      .withColumnRenamed("CustomerName", "Customer_Name")
      .withColumnRenamed("BudgetMonth", "Month")
      .withColumnRenamed("SalesBudAmount", "SalesBudAmt")
      .withColumnRenamed("ST3_Formula1bea30309a5747c696827198a3d72671", "BU")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Remove Full-Row Duplicates
# Equivalent to the **Removed duplicates** step (`Table.Distinct` with no column list checks every column): removes any rows that are entirely identical across all columns.

# CELL ********************

df = df.dropDuplicates()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Remove Columns
# Equivalent to the **Removed columns** step: drops `ST3_createdDateTime` and `Refid`, which aren't needed downstream.

# CELL ********************

df = df.drop("ST3_createdDateTime", "Refid")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Change Data Type of `Month`
# Equivalent to the **Changed column type** step: casts `Month` to a proper date type.

# CELL ********************

df = df.withColumn("Month", F.col("Month").cast(DateType()))


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
# Persist the final transformed table as a managed Delta table in the **silver** schema, overwriting any previous version.

# CELL ********************

df.write.mode("overwrite").format("delta").saveAsTable("silver.silver_acu_ST3_Budget_NonEx")
print("Write complete: silver.silver_acu_ST3_Budget_NonEx")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
