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

# # ST3_NonExCus — Bronze to Silver Transformation
# 
# **Source table:** `bronze.bronze_acu_NewCustomer_Pneumatic`  
# **Target table:** `silver.silver_acu_ST3_NonExCus`
# 
# This notebook replicates, step by step, the Power Query M logic used to build the **ST3_NonExCus** table from the **NewCustomer_Pneumatic** Acumatica OData source.


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
# Equivalent to the `Source` + `Navigation 1` steps — reads the NewCustomer_Pneumatic data already landed in bronze.

# CELL ********************

df = spark.table("bronze.bronze_acu_NewCustomer_Pneumatic")
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
# - `ST3_Formulac9f1c02324eb4a73ad4101ddd0568a7b` → `BU`

# CELL ********************

df = (
    df.withColumnRenamed("CustomerReferenceCode", "Customer_ID")
      .withColumnRenamed("CustomerName", "Customer_Name")
      .withColumnRenamed("CountryName", "Country")
      .withColumnRenamed("Segment", "Customer_Segment")
      .withColumnRenamed("ST3_Formulac9f1c02324eb4a73ad4101ddd0568a7b", "BU")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Remove Duplicates on `Customer_ID`
# Equivalent to the first **Removed duplicates** step: keeps only the first occurrence of each distinct `Customer_ID`.

# CELL ********************

df = df.dropDuplicates(["Customer_ID"])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Remove Full-Row Duplicates
# Equivalent to the second **Removed duplicates** step (`Table.Distinct` with no column list in M checks *every* column): removes any rows that are entirely identical across all columns.

# CELL ********************

df = df.dropDuplicates()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Replace `"OE"` with `"OEM"` in `Customer_Segment`
# Equivalent to the **Replaced value** step: standardizes the segment code so `OE` rows are reclassified as `OEM`.

# CELL ********************

df = df.withColumn(
    "Customer_Segment",
    F.when(F.col("Customer_Segment") == "OE", F.lit("OEM")).otherwise(F.col("Customer_Segment"))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Remove Duplicates on `Customer_ID` (Final Pass)
# Equivalent to the final **Removed duplicates** step: re-applies the `Customer_ID` de-duplication after the segment value replacement, in case the replacement introduced new duplicate `Customer_ID` rows.

# CELL ********************

df = df.dropDuplicates(["Customer_ID"])


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

df.write.mode("overwrite").format("delta").saveAsTable("silver.silver_acu_ST3_NonExCus")
print("Write complete: silver.silver_acu_ST3_NonExCus")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
