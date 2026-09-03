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

# # PBI Invoice Value (IFS) — Bronze to Silver Transformation
# 
# **Source table:** `bronze.bronze_ifs_PBI Invoice value` *(see note below)*  
# **Target table:** `silver.silver_ifs_PBI_Invoice_Value`
# 
# This notebook replicates, step by step, the Power Query M logic used to build the **PBI Invoice Value(IFS)** table from the IFS source 


# MARKDOWN ********************

# ## 1. Imports
# Load the PySpark functions and types needed for renaming, filtering, and casting.

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
# Reads the IFS invoice data already landed in bronze. Run `printSchema()` first to confirm actual column names before trusting the rename step below, since bronze ingestion has sanitized names differently on past tables (e.g. spaces converted to underscores).

# CELL ********************

df = spark.table("bronze.bronze_ifs_PBI_Invoice_value")
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
# - `INVOICE_DATE` → `InvoiceDate`
# - `CUSTOMER` → `Customer_ID`
# - `WEIGHT` → `Weight`
# - `USD_VALUE` → `Invoice Value` *(renamed further to `Invoice_Value` in cell 7 below, since Delta doesn't allow spaces in column names)*
# - `ITEMBRAND` → `ItemBrand`
# - `INVENTORYID` → `InventoryID`

# CELL ********************

df = (
    df.withColumnRenamed("INVOICE_DATE", "InvoiceDate")
      .withColumnRenamed("CUSTOMER", "Customer_ID")
      .withColumnRenamed("WEIGHT", "Weight")
      .withColumnRenamed("USD_VALUE", "Invoice_Value")
      .withColumnRenamed("ITEMBRAND", "ItemBrand")
      .withColumnRenamed("INVENTORYID", "InventoryID")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Filter Rows From May 1, 2023 Onward
# Equivalent to the first **Filtered rows** step: keeps only records with `InvoiceDate` on or after `2023-05-01 00:00:00`.

# CELL ********************

df = df.filter(F.col("InvoiceDate") >= F.lit("2023-05-01 00:00:00"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Add Column `DB`
# Equivalent to the **Added custom** step: tags every row with a constant source-system identifier.

# CELL ********************

df = df.withColumn("DB", F.lit("IFS"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Change Data Type of `DB`
# Equivalent to the **Changed column type** step: ensures `DB` is stored as a string/text type.

# CELL ********************

df = df.withColumn("DB", F.col("DB").cast(StringType()))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Filter Out Customer `CS0286`
# Equivalent to the second **Filtered rows** step: excludes the specific customer `CS0286` from the result set.

# CELL ********************

df = df.filter(F.col("Customer_ID") != "CS0286")


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

df.write.mode("overwrite").format("delta").saveAsTable("silver.silver_ifs_PBI_Invoice_Value")
print("Write complete: silver.silver_ifs_PBI_Invoice_Value")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
