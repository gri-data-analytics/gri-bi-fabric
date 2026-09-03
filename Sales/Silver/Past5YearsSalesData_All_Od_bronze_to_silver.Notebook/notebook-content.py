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

# # Past5YearsSalesData_All — Bronze to Silver Transformation
# 
# **Source table:** `bronze.bronze_excel_Past5YearsSalesData_All`  
# **Target table:** `silver.silver_excel_Past5YearsSalesData_All`
# 
# This notebook replicates, step by step, the Power Query M logic used to build the **Past5YearsSalesData_All** table from the SharePoint Excel workbook source.
# 
# > **Assumption:** unlike the earlier Acumatica-sourced tables, this source is an Excel file loaded via SharePoint (`Excel.Workbook`/`Web.Contents`), not an OData feed. I've assumed the bronze table is named `bronze_excel_Past5YearsSalesData_All` — please confirm/correct against your actual bronze schema listing, since no screenshot was provided for this one.


# MARKDOWN ********************

# ## 1. Imports
# Load the PySpark functions and types needed for casting, filtering, and column cleanup.

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType, StringType, DoubleType


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Load Source Table
# Equivalent to the `Source` + `Navigation 1` + `Promoted headers` steps — reads the Past5YearsSalesData_All sheet already landed in bronze with proper column headers.

# CELL ********************

df = spark.table("bronze.bronze_od_past5yearssalesdata_all")
display(df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Change Column Data Types
# Equivalent to the first **Changed column type** step: casts each column to its intended type.
# - `InvoiceDate` → date
# - `Invoice_no` → integer
# - `Customer_ID`, `Customer_Name`, `BU`, `ItemBrand` → text
# - `Weight`, `Invoice Value` → number
# - `Column9`, `Column10`, `Column11` → left as-is here (the M query set these to `any`; we'll cast them explicitly to text in the next stage, matching the later `Transform columns` step)

# CELL ********************

df = (
    df.withColumn("InvoiceDate", F.col("InvoiceDate").cast(DateType()))
      .withColumn("Invoice_no", F.col("Invoice_no").cast(IntegerType()))
      .withColumn("Customer_ID", F.col("Customer_ID").cast(StringType()))
      .withColumn("Customer_Name", F.col("Customer_Name").cast(StringType()))
      .withColumn("BU", F.col("BU").cast(StringType()))
      .withColumn("ItemBrand", F.col("ItemBrand").cast(StringType()))
      .withColumn("Weight", F.col("Weight").cast(DoubleType()))
      .withColumn("Invoice_Value", F.col("Invoice_Value").cast(DoubleType()))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Remove Column `Invoice_no`
# Equivalent to the **Removed columns** step: drops `Invoice_no`, which isn't needed downstream.

# CELL ********************

df = df.drop("Invoice_no")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Change Data Types of `Column9`, `Column10`, `Column11`
# Equivalent to the **Transform columns** step: casts these three columns to text.

# CELL ********************


df = (
    df.withColumn("Unnamed:_8", F.col("Unnamed:_8").cast(StringType()))
      .withColumn("Unnamed:_9", F.col("Unnamed:_9").cast(StringType()))
      .withColumn("Unnamed:_10", F.col("Unnamed:_10").cast(StringType()))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Replace Error Values in `Column9`, `Column10`, `Column11`
# Equivalent to the **Replace errors** step, which replaces any evaluation errors in these columns with `null`. A direct `.cast(StringType())` in Spark already returns `null` on failure rather than throwing, so this step is a no-op here and is included only for parity with the original query.

# CELL ********************

# No-op: Spark's cast() already returns null on conversion failure, matching the intent
# of the Power Query "Replace errors" step. Included here for traceability.


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Filter Rows Before May 1, 2023
# Equivalent to the **Filtered rows** step: keeps only records with `InvoiceDate` strictly before `2023-05-01`.

# CELL ********************

df = df.filter(F.col("InvoiceDate") < F.lit("2023-05-01"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. Add Column `DB`
# Equivalent to the **Added custom** step: tags every row with a constant source-system identifier.

# CELL ********************

df = df.withColumn("DB", F.lit("EXCEL"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 9. Change Data Type of `DB`
# Equivalent to the final **Changed column type** step: ensures `DB` is stored as a string/text type.

# CELL ********************

df = df.withColumn("DB", F.col("DB").cast(StringType()))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 10. Rename `Invoice Value` to `Invoice_Value`
# Not present in the original M query, but required here: Delta Lake rejects spaces in column names. Renaming this column now (rather than waiting for a write-time error, as happened with the earlier Invoice Details table) keeps the schema Delta-safe from the start.

# CELL ********************

df = df.withColumnRenamed("Invoice Value", "Invoice_Value")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

df = df.filter(col("Invoice_Value").isNotNull())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 11. Preview Final Result
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

# ## 12. Write to Silver Schema
# Persist the final transformed table as a managed Delta table in the **silver** schema, overwriting any previous version.

# CELL ********************

df.write.mode("overwrite").format("delta").saveAsTable("silver.silver_od_Past5YearsSalesData_All")
print("Write complete: silver.silver_od_Past5YearsSalesData_All")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
