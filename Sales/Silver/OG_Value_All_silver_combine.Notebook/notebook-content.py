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

# # OG Value All — Silver Combine Transformation
# 
# **Source tables:**
# - `silver.silver_od_og_correction` (OG Correction)
# - `silver.silver_acu_pbi_og_value` (PBI OG Value(Acu))
# - `silver.silver_ifs_og_value` (OG_Value_IFS) 
# 
# **Target table:** `silver.silver_og_value_all`
# 
# This notebook replicates the Power Query M logic that combines (unions) three OG Value sources, then adds a couple of derived columns.
# 


# MARKDOWN ********************

# ## 1. Imports
# Load the PySpark functions and types needed for this transformation.

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, TimestampType


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Load and Inspect the Three Source Tables
# Equivalent to the `Source` step's inputs. Load each silver table individually and compare schemas before combining, to catch any column-naming mismatches early — especially for the unconfirmed IFS table.

# CELL ********************

df_og_correction = spark.table("silver.silver_od_og_correction")
df_pbi_og_value  = spark.table("silver.silver_acu_pbi_og_value")
df_ifs_og_value  = spark.table("silver.silver_ifs_og_value")  # confirm this table name

print("--- OG Correction schema ---")
df_og_correction.printSchema()
print("--- PBI OG Value(Acu) schema ---")
df_pbi_og_value.printSchema()
print("--- OG_Value_IFS schema ---")
df_ifs_og_value.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Combine (Union) the Three Tables
# Equivalent to the **Source = Table.Combine(...)** step: stacks all three tables into one, aligning by column name and filling missing columns with `null` where a source doesn't have them.

# CELL ********************

df = df_og_correction.unionByName(df_pbi_og_value, allowMissingColumns=True)
df = df.unionByName(df_ifs_og_value, allowMissingColumns=True)

display(df.limit(20))
print(f"Combined row count: {df.count()}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Add Column `USD_Value($k)`
# Equivalent to the first **Added custom** step: converts `USD_Value` to thousands.
# 
# *(Renamed to `USD_Value_k` here — Delta doesn't allow `$`, `(`, or `)` in column names.)*

# CELL ********************

df = df.withColumn("USD_Value_k", F.col("USD_Value") / 1000)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Change Data Type of `USD_Value_k`
# Equivalent to the **Changed column type** step: ensures `USD_Value_k` is numeric (double).

# CELL ********************

df = df.withColumn("USD_Value_k", F.col("USD_Value_k").cast(DoubleType()))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Add Column `Weight(MT)`
# Equivalent to the second **Added custom** step: converts `Weight` to metric tons.
# 
# *(Renamed to `Weight_MT` here — Delta doesn't allow parentheses in column names.)*

# CELL ********************

df = df.withColumn("Weight_MT", F.col("Weight") / 1000)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Change Data Type of `Weight_MT`
# Equivalent to the **Changed column type** step: ensures `Weight_MT` is numeric (double).

# CELL ********************

df = df.withColumn("Weight_MT", F.col("Weight_MT").cast(DoubleType()))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. Change Data Type of `Date` to Text (Validation Pass)
# Equivalent to the **Transform columns** step: temporarily casts `Date` to text as a validity check before the final datetime conversion. In Spark, `.cast()` returns `null` on failure rather than throwing, so this doubles as the error-catching step too.

# CELL ********************

df = df.withColumn("Date", F.col("Date").cast(StringType()))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 9. Replace Error Values in `Date`
# Equivalent to the **Replace errors** step. Since Spark's `.cast()` already returns `null` on failure (no exceptions raised), there's nothing further to replace — this step is a no-op here, included only for parity with the original query.

# CELL ********************

# No-op: Spark's cast() already returns null on conversion failure.
# Included here for traceability with the Power Query step.


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 10. Change Data Type of `Date` to Datetime
# Equivalent to the final **Changed column type** step: casts the validated text value of `Date` back to a proper timestamp/datetime type.

# CELL ********************

df = df.withColumn("Date", F.col("Date").cast(TimestampType()))


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
# Persist the final combined table as a managed Delta table in the **silver** schema, overwriting any previous version.

# MARKDOWN ********************

# ## Add Zero Logic and Filter Zero Records
# 
# Creates a new column called `Zero_Logic` by summing `USD_Value_k` and `Weight_MT`, then removes rows where the result equals zero. This maps to the M steps:
# - Added Custom
# - Changed Type
# - Filtered Rows

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# Create Zero Logic column
df = df.withColumn(
    "Zero_Logic",
    (
        F.coalesce(F.col("USD_Value_k"), F.lit(0.0))
        + F.coalesce(F.col("Weight_MT"), F.lit(0.0))
    ).cast(DoubleType())
)

# Filter rows where Zero_Logic is not 0
df = df.filter(F.col("Zero_Logic") != 0)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable("silver.silver_og_value_all")
print("Write complete: silver.silver_og_value_all")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
