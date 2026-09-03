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

# # ST3_NonExRSM 2025 — Bronze to Silver Transformation
# 
# **Source table:** `bronze.bronze_acu_NewBudgeRSM_Pne`  
# **Target table:** `silver.silver_acu_ST3_NonExRSM_2025`
# 
# This notebook replicates, step by step, the Power Query M logic used to build the **ST3_NonExRSM 2025** table from the **NewBudgeRSM_Pne** Acumatica OData source.


# MARKDOWN ********************

# ## 1. Imports
# Load the PySpark functions and types needed for conditional logic and casting.

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
# Equivalent to the `Source` + `Navigation 1` steps — reads the NewBudgeRSM_Pne data already landed in bronze.

# CELL ********************

df = spark.table("bronze.bronze_acu_NewBudgeRSM_Pne")
df.printSchema()
display(df.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Add Conditional Column `Custom`
# Equivalent to the **Inserted conditional column** step: uses `Name` if it's populated, falling back to `Code` when `Name` is null.
# 
# `Custom = if Name is null then Code else Name`

# CELL ********************

df = df.withColumn(
    "Custom",
    F.when(F.col("Name").isNull(), F.col("Code")).otherwise(F.col("Name"))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Select Only Relevant Columns
# Equivalent to the **Removed other columns** step: keeps only `Customer` and `Custom`, discarding everything else.

# CELL ********************

df = df.select("Customer", "Custom")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Rename `Custom` to `RSM`
# Equivalent to the **Renamed columns** step.

# CELL ********************

df = df.withColumnRenamed("Custom", "RSM")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Change Data Type of `RSM`
# Equivalent to the **Changed column type** step: ensures `RSM` is stored as a string/text type.

# CELL ********************

df = df.withColumn("RSM", F.col("RSM").cast(StringType()))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Remove Duplicates on `Customer`
# Equivalent to the **Removed duplicates** step: keeps only the first occurrence of each distinct `Customer`.

# CELL ********************

df = df.dropDuplicates(["Customer"])


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

df.write.mode("overwrite").format("delta").saveAsTable("silver.silver_acu_ST3_NonExRSM_2025")
print("Write complete: silver.silver_acu_ST3_NonExRSM_2025")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
