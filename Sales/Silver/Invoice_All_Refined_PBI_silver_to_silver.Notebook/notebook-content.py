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

# # Invoice All (Refined) — Silver to Silver Transformation
# 
# **Source table:** `silver.silver_invoice_all`  
# **Target table:** `silver.silver_invoice_all_refined`
# 
# This notebook replicates the Power Query M logic for this downstream **Invoice All** query, which reads from a Power Platform Dataflow. That dataflow is fed by the same underlying data as our `silver.silver_invoice_all` table, so we read directly from there instead of reconnecting to the dataflow.
# 
# > **Column name mapping:** the M script references `[Weight(MT)]`, `[USD Value($k)]`, and `[Invoice Value]` — these correspond to `Weight_MT`, `USD_Value_k`, and `Invoice_Value` in our silver table (renamed earlier to satisfy Delta's column-naming restrictions). Run `printSchema()` in cell 2 to confirm these exact names before proceeding.
# 
# > **Target table name assumption:** since this produces a filtered/refined version of Invoice All rather than overwriting the base table, I've named the output `silver_invoice_all_refined` — rename cell 8's target if you'd prefer something else.


# MARKDOWN ********************

# ## 1. Imports
# Load the PySpark functions and types needed for this transformation.

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Load Source Table
# Reads the already-built `silver_invoice_all` table. Run `printSchema()` first to confirm the exact names of `Weight_MT`, `USD_Value_k`, and `Invoice_Value`.

# CELL ********************

df = spark.table("silver.silver_invoice_all")
df.printSchema()
display(df.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Add Column `Zero Logic`
# Equivalent to the **Added Custom** step: sums `Weight_MT` and `USD_Value_k` to create a combined check value used to filter out fully-zero rows.
# 
# *(Renamed to `Zero_Logic` here — Delta doesn't allow spaces in column names.)*

# CELL ********************

df = df.withColumn("Zero_Logic", F.col("Weight_MT") + F.col("USD_Value_k"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Change Data Type of `Zero_Logic`
# Equivalent to the **Changed Type** step: ensures `Zero_Logic` is numeric (double).

# CELL ********************

df = df.withColumn("Zero_Logic", F.col("Zero_Logic").cast(DoubleType()))


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

# ## 5. Filter Out Rows Where `Zero_Logic` Equals Zero
# Equivalent to the **Filtered Rows** step: removes rows where both `Weight_MT` and `USD_Value_k` are zero (or cancel out to zero), since these are effectively empty/void transactions.

# CELL ********************

df = df.filter(F.col("Zero_Logic") != 0)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Add Column `Invoice $ k`
# Equivalent to the **Added Custom1** step: converts `Invoice_Value` to thousands.
# 
# *(Renamed to `Invoice_k` here — Delta doesn't allow `$` or spaces in column names.)*

# CELL ********************

df = df.withColumn("Invoice_k", F.col("Invoice_Value") / 1000)


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

# ## 7. Change Data Type of `Invoice_k`
# Equivalent to the **Changed Type1** step: ensures `Invoice_k` is numeric (double).

# CELL ********************

df = df.withColumn("Invoice_k", F.col("Invoice_k").cast(DoubleType()))


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
# Persist the final refined table as a managed Delta table in the **silver** schema, overwriting any previous version.

# CELL ********************

df.write.mode("overwrite").format("delta").saveAsTable("silver.silver_invoice_all_refined_pbi")
print("Write complete: silver.silver_invoice_all_refined_pbi")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
