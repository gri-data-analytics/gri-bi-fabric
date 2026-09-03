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

# # ST1_Budget (Acu) — Bronze to Silver Transformation
# 
# **Source table:** `bronze.bronze_acu_Budget_Solid`  
# **Target table:** `silver.silver_acu_ST1_Budget`
# 
# This notebook replicates, step by step, the Power Query M logic used to build the **ST1_Budget(Acu)** table from the **Budget-Solid** Acumatica OData source.


# MARKDOWN ********************

# ## 1. Imports
# Load the PySpark functions needed for this transformation.

# CELL ********************

from pyspark.sql import functions as F


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Load Source Table
# Equivalent to the `Source` + `Navigation 1` steps — reads the Budget-Solid data already landed in bronze.

# CELL ********************

df = spark.table("bronze.bronze_acu_Budget_Solid")
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
# - `ST1_Formula72744fd4826e4fe59164f74933335972` → `BU`

# CELL ********************

df = (
    df.withColumnRenamed("CustomerCode", "Customer_ID")
      .withColumnRenamed("CustomerName", "Customer_Name")
      .withColumnRenamed("BudgetMonth", "Month")
      .withColumnRenamed("ST1_Formula72744fd4826e4fe59164f74933335972", "BU")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Preview Final Result
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

# ## 5. Write to Silver Schema
# Persist the final transformed table as a managed Delta table in the **silver** schema, overwriting any previous version.

# CELL ********************

df.write.mode("overwrite").format("delta").saveAsTable("silver.silver_acu_ST1_Budget")
print("Write complete: silver.silver_acu_ST1_Budget")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
