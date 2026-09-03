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

# # OG Correction — Bronze to Silver Transformation
# 
# **Source table:** `bronze.bronze_od_og_correction`  
# **Target table:** `silver.silver_od_og_correction`
# 
# This notebook replicates, step by step, the Power Query M logic used to build the **OG Correction** table from the SharePoint Excel workbook (`OG Correction.xlsx`, sheet `Data`).
# 
# > **Verify column names first:** as seen with the Past5YearsSalesData table, bronze ingestion can silently change space/special-character column names (e.g. `Invoice Value` became `Invoice_Value`). Run cell 2 below and check `df.printSchema()` / the displayed preview before trusting the column names used in the rest of this notebook — adjust names in later cells if your actual bronze schema differs from the M script's `Order Nbr.`, `Customer Name`, `Order Total`, `Order Weight`, `USD Value`.


# MARKDOWN ********************

# ## 1. Imports
# Load the PySpark functions and types needed for casting and renaming.

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StringType, DoubleType


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Load Source Table
# Equivalent to the `Source` + `Navigation 1` + `Promoted headers` steps — reads the OG Correction data already landed in bronze with proper column headers.

# CELL ********************

df = spark.table("bronze.bronze_od_og_correction")
df.printSchema()
display(df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Change Column Data Types
# Equivalent to the **Changed column type** step: casts each column to its intended type.
# - `Order Nbr.` → text
# - `Date` → date
# - `Customer` → text
# - `Customer Name` → text
# - `Order Total` → number
# - `Order Weight` → number
# - `BU` → text
# - `USD Value` → number
# 
# *(If your bronze schema already has underscores instead of spaces in these names — as confirmed in cell 2 — update the column references below to match before running.)*

# CELL ********************

df = (
    df.withColumn("Order_Nbr_", F.col("Order_Nbr_").cast(StringType()))
      .withColumn("Date", F.col("Date").cast(DateType()))
      .withColumn("Customer", F.col("Customer").cast(StringType()))
      .withColumn("Customer_Name", F.col("Customer_Name").cast(StringType()))
      .withColumn("Order_Total", F.col("Order_Total").cast(DoubleType()))
      .withColumn("Order_Weight", F.col("Order_Weight").cast(DoubleType()))
      .withColumn("BU", F.col("BU").cast(StringType()))
      .withColumn("USD_Value", F.col("USD_Value").cast(DoubleType()))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Rename Columns
# Equivalent to the **Renamed columns** step:
# - `Order Total` → `OrderTotal`
# - `Order Weight` → `Weight`
# - `Customer` → `Customer_ID`
# - `Customer Name` → `Customer_Name`

# CELL ********************

df = (
    df.withColumnRenamed("Order_Total", "OrderTotal")
      .withColumnRenamed("Order_Weight", "Weight")
      .withColumnRenamed("Customer", "Customer_ID")
      .withColumnRenamed("Customer_Name", "Customer_Name")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Rename Remaining Space-Containing Columns
# Not present in the original M query, but required here: Delta Lake rejects spaces in column names. The M script's rename step (step 4) leaves `Order Nbr.` and `USD Value` untouched, so we rename them now to keep the schema Delta-safe:
# - `Order Nbr.` → `Order_Nbr`
# - `USD Value` → `USD_Value`

# CELL ********************

df = (
    df.withColumnRenamed("Order Nbr.", "Order_Nbr")
      .withColumnRenamed("USD Value", "USD_Value")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.withColumn("Source_System", F.lit("SP"))

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

df.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable("silver.silver_od_og_correction")
print("Write complete: silver.silver_od_og_correction")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
