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

# # ST1_NonExCus — Bronze to Silver Transformation
# 
# **Source table:** `bronze.bronze_acu_NewCustomer_Solid`  
# **Target table:** `silver.silver_acu_ST1_NonExCus`
# 
# This notebook replicates, step by step, the Power Query M logic used to build the **ST1_NonExCus** table from the **NewCustomer_Solid** Acumatica OData source.


# MARKDOWN ********************

# ## 1. Imports
# Load the PySpark functions needed for renaming and value replacement.

# CELL ********************

from pyspark.sql import functions as F


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Load Source Table
# Equivalent to the `Source` + `Navigation 1` steps — reads the NewCustomer_Solid data already landed in bronze.

# CELL ********************

df = spark.table("bronze.bronze_acu_NewCustomer_Solid")
display(df.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Rename Columns
# Equivalent to the **Renamed columns** step:
# - `CustomerReferenceCode` → `Customer_ID`
# - `CustomerName` → `Customer_Name`
# - `CountryName` → `Country`
# - `Segment` → `Customer_Segment`
# - `DBNonSolid_Formula7e280310662a40c4a57a2742c7d6fcda` → `BU`

# CELL ********************

df = (
    df.withColumnRenamed("CustomerReferenceCode", "Customer_ID")
      .withColumnRenamed("CustomerName", "Customer_Name")
      .withColumnRenamed("CountryName", "Country")
      .withColumnRenamed("Segment", "Customer_Segment")
      .withColumnRenamed("DBNonSolid_Formula7e280310662a40c4a57a2742c7d6fcda", "BU")
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

# ## 5. Replace `"RM"` with `"Replacement Market"` in `Customer_Segment`
# Equivalent to the **Replaced value** step: expands the shorthand segment code `RM` into its full label `Replacement Market`.

# CELL ********************

df = df.withColumn(
    "Customer_Segment",
    F.when(F.col("Customer_Segment") == "RM", F.lit("Replacement Market")).otherwise(F.col("Customer_Segment"))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Preview Final Result
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

# ## 7. Write to Silver Schema
# Persist the final transformed table as a managed Delta table in the **silver** schema, overwriting any previous version.

# CELL ********************

df.write.mode("overwrite").format("delta").saveAsTable("silver.silver_acu_ST1_NonExCus")
print("Write complete: silver.silver_acu_ST1_NonExCus")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
