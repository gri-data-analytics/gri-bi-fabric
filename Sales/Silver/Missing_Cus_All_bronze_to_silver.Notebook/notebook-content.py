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

# # Missing_Cus_All — Bronze to Silver Transformation
# 
# **Source table:** `bronze.bronze_ifs_Missing_Cus_All`  
# **Target table:** `silver.silver_ifs_Missing_Cus_All`
# 
# This notebook replicates, step by step, the Power Query M logic used to build the **Missing_Cus_All** table. As with the other IFS tables, the script provided starts directly from `Source`, so we read straight from the bronze table.


# MARKDOWN ********************

# ## 1. Imports
# Load the PySpark functions needed for renaming, value replacement, and filtering.

# CELL ********************

from pyspark.sql import functions as F


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Load Source Table
# Reads the Missing_Cus_All data already landed in bronze. Run `printSchema()` first to confirm actual column names before trusting the rename step below.

# CELL ********************

df = spark.table("bronze.bronze_ifs_Missing_Cus_All")
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
# - `CUSTOMETID` → `Customer_ID` *(kept the exact source spelling, same as the Customer(IFS) table — verify against `printSchema()` output)*
# - `CUSTOMERNAME` → `Customer_Name`
# - `REGION` → `Region`
# - `CUSTOMERSEGMENT` → `Customer_Segment`
# - `COUNTRYNAME` → `Country`

# CELL ********************

df = (
    df.withColumnRenamed("CUSTOMETID", "Customer_ID")
      .withColumnRenamed("CUSTOMERNAME", "Customer_Name")
      .withColumnRenamed("REGION", "Region")
      .withColumnRenamed("CUSTOMERSEGMENT", "Customer_Segment")
      .withColumnRenamed("COUNTRYNAME", "Country")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Replace `"EUROPE & RUSSIA"` with `"EUROPE"` in `Region`
# Equivalent to the first **Replaced value** step: consolidates the Europe & Russia region into a single `EUROPE` label.

# CELL ********************

df = df.withColumn(
    "Region",
    F.when(F.col("Region") == "EUROPE & RUSSIA", F.lit("EUROPE")).otherwise(F.col("Region"))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Replace `"MIDDLE EAST & AFRICA"` with `"MEA & SA"` in `Region`
# Equivalent to the second **Replaced value** step: relabels the Middle East & Africa region as `MEA & SA`.

# CELL ********************

df = df.withColumn(
    "Region",
    F.when(F.col("Region") == "MIDDLE EAST & AFRICA", F.lit("MEA & SA")).otherwise(F.col("Region"))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Filter Out Customer `CS0205`
# Equivalent to the first **Filtered rows** step: excludes the specific customer `CS0205`.

# CELL ********************

df = df.filter(F.col("Customer_ID") != "CS0205")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Filter Out Customer `CS0225`
# Equivalent to the second **Filtered rows** step: excludes the specific customer `CS0225`.

# CELL ********************

df = df.filter(F.col("Customer_ID") != "CS0225")


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

df.write.mode("overwrite").format("delta").saveAsTable("silver.silver_ifs_Missing_Cus_All")
print("Write complete: silver.silver_ifs_Missing_Cus_All")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
