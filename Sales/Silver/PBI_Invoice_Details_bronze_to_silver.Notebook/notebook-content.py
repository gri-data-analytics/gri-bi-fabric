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

# # PBI_Invoice_Details — Bronze to Silver Transformation
# 
# **Source table:** `bronze.bronze_acu_pbi_invoice_details`  
# **Target table:** `silver.silver_acu_pbi_invoice_details`
# 
# This notebook replicates the Power Query M logic used for the **PBI Invoice Details** table.

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
# Equivalent to the `Source` step — reads the PBI Invoice Details data already landed in bronze. Run `printSchema()` first to confirm actual column names.

# CELL ********************

df = spark.table("bronze.bronze_acu_pbi_invoice_details")
df.printSchema()
display(df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Apply Transformations
# Equivalent to the Power Query **Changed Type** step.
# 
# Power Query transformation:
# - `BLDate` → Date

# CELL ********************

df = df.withColumn("BLDate", F.to_date(F.col("BLDate")))
# If ETADate is stored as string, uncomment the line below
# df = df.withColumn("ETADate", F.to_date(F.col("ETADate")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Preview Final Result
# Quick sanity check before writing the Silver table.

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
# Persist the transformed dataframe as a managed Delta table in the **silver** schema.

# CELL ********************

df.write.mode("overwrite").format("delta").saveAsTable("silver.silver_acu_pbi_invoice_details")
print("Write complete: silver.silver_acu_pbi_invoice_details")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
