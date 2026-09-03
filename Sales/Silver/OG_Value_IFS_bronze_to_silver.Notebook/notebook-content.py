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

# # OG_Value_IFS — Bronze to Silver Transformation
# 
# **Source table:** `bronze.bronze_ifs_OG_Value`  
# **Target table:** `silver.silver_ifs_og_value`
# 
# This notebook replicates the Power Query M logic used to build the **OG_Value_IFS** table. The original M query pulls directly from an Oracle SQL source; since that data has already been landed as-is in bronze, we read straight from there instead of re-issuing the SQL.
# 
# > **Note:** the original query filters at the SQL level (`state <> 'Cancelled'`, `customer_no <> 'CS0286'`, `catalog_type_db <> 'PKG'`, `company = 'GRISL'`, `contract = 'GRI01'`, and a left join to `dwc_active_prices`). If the bronze table was landed as a *raw, unfiltered* extract of `customer_order_line_cfv`, these filters would need to be reapplied here in PySpark. Run `printSchema()` and check a few rows in cell 2 — if the bronze table already reflects the filtered SQL result (i.e., it was ingested using this exact query), no extra filtering is needed and this notebook is complete as-is. If it's an unfiltered raw table instead, let me know and I'll add the equivalent filter/join logic.


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
# Equivalent to the `Source` step — reads the OG_Value_IFS data already landed in bronze. Run `printSchema()` first to confirm actual column names, and spot-check whether the filtering described in the original SQL (state, customer, catalog type) already appears to be applied.

# CELL ********************

df = spark.table("bronze.bronze_ifs_OG_Value")
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
# - `DATE` → `Date`
# - `CUSTOMER_NO` → `Customer_ID`
# - `NAME_` → `Customer_Name`
# - `REGION` → `Region`
# - `WEIGHT_` → `Weight`
# - `CF$_ITEM_TYPE` → `BU`
# - `USD_VAL` → `USD_Value` *(renamed directly to the underscore form here, rather than `USD Value` with a space, to stay consistent with the other OG Value tables and avoid the Delta column-naming error we've hit before)*

# CELL ********************

df = (
    df.withColumnRenamed("DATE", "Date")
      .withColumnRenamed("CUSTOMER_NO", "Customer_ID")
      .withColumnRenamed("NAME_", "Customer_Name")
      .withColumnRenamed("REGION", "Region")
      .withColumnRenamed("WEIGHT_", "Weight")
      .withColumnRenamed("CF$_ITEM_TYPE", "BU")
      .withColumnRenamed("USD_VAL", "USD_Value")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Add Source System Column
# 
# Adds a constant source-system identifier so records can be traced back to Oracle IFS when combined with other OG Value datasets.

# CELL ********************

df = df.withColumn("Source_System", F.lit("IFS"))

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

df.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable("silver.silver_ifs_og_value")
print("Write complete: silver.silver_ifs_og_value")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
