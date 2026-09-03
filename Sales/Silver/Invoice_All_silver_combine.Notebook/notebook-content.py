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
# Equivalent to the `Source` step's inputs. Load each silver table individually and compare schemas before combining, to catch any column-naming mismatches early.

# CELL ********************

df_acu = spark.table("silver.silver_acu_pbi_sales_test")
df_ifs = spark.table("silver.silver_ifs_pbi_invoice_value")
df_od  = spark.table("silver.silver_od_past5yearssalesdata_all")

print("--- Acu Invoice Details schema ---")
df_acu.printSchema()
print("--- IFS Invoice Value schema ---")
df_ifs.printSchema()
print("--- Past5Years Sales Data schema ---")
df_od.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Combine (Union) the Three Tables
# Equivalent to the **Source = Table.Combine(...)** step: stacks all three tables into one, aligning by column name and filling missing columns with `null` where a source doesn't have them (e.g. `Column9`–`Column11` only exist in the Past5Years table).

# CELL ********************

df = df_acu.unionByName(df_ifs, allowMissingColumns=True)
df = df.unionByName(df_od, allowMissingColumns=True)

display(df.limit(20))
print(f"Combined row count: {df.count()}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Add Column `Weight(MT)`
# Equivalent to the first **Added custom** step: converts `Weight` (in kg) to metric tons.
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

# ## 5. Change Data Type of `Weight_MT`
# Equivalent to the **Changed column type** step: ensures `Weight_MT` is numeric (double).

# CELL ********************

df = df.withColumn("Weight_MT", F.col("Weight_MT").cast(DoubleType()))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Add Column `USD Value($k)`
# Equivalent to the second **Added custom** step: converts `Invoice_Value` to thousands of USD.
# 
# *(Renamed to `USD_Value_k` here — Delta doesn't allow `$`, `(`, `)`, or spaces in column names.)*

# CELL ********************

df = df.withColumn("USD_Value_k", F.col("Invoice_Value") / 1000)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Change Data Type of `USD_Value_k`
# Equivalent to the **Changed column type** step: ensures `USD_Value_k` is numeric (double).

# CELL ********************

df = df.withColumn("USD_Value_k", F.col("USD_Value_k").cast(DoubleType()))


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

# ## 8. Change Data Type of `InvoiceDate` to Text (Validation Pass)
# Equivalent to the **Transform columns** step: temporarily casts `InvoiceDate` to text as a validity check before the final datetime conversion. In Spark, `.cast()` returns `null` on failure rather than throwing, so this doubles as the error-catching step too.

# CELL ********************

df = df.withColumn("InvoiceDate", F.col("InvoiceDate").cast(StringType()))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 9. Replace Error Values in `InvoiceDate`
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

# ## 10. Change Data Type of `BU` to Text (Validation Pass)
# Equivalent to the second **Transform columns** step: same validity-check cast applied to `BU`.

# CELL ********************

df = df.withColumn("BU", F.col("BU").cast(StringType()))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 11. Replace Error Values in `BU`
# Equivalent to the second **Replace errors** step — again a no-op in Spark for the same reason as step 9.

# CELL ********************

# No-op: Spark's cast() already returns null on conversion failure.


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 12. Filter Out Rows Where `InvoiceDate` Is Null
# Equivalent to the **Filtered rows** step: keeps only records where `InvoiceDate` successfully converted (i.e. is not null).

# CELL ********************

df = df.filter(F.col("InvoiceDate").isNotNull())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 13. Change Data Type of `InvoiceDate` to Datetime
# Equivalent to the final **Changed column type** step: casts the validated text value of `InvoiceDate` back to a proper timestamp/datetime type.

# CELL ********************

df = df.withColumn("InvoiceDate", F.col("InvoiceDate").cast(TimestampType()))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 14. Change Data Types of `Column9`, `Column10`, `Column11`
# Equivalent to the last **Transform columns** step: casts these (only present via the Past5Years table, `null` elsewhere) to text.

# CELL ********************

df = (
    df.withColumn("Unnamed:_8", F.col("Unnamed:_8").cast(StringType()))
      .withColumn("Unnamed:_9", F.col("Unnamed:_9").cast(StringType()))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 15. Replace Error Values in `Column9`, `Column10`, `Column11`
# Equivalent to the last **Replace errors** step — no-op in Spark, included for parity.

# CELL ********************

# No-op: Spark's cast() already returns null on conversion failure.


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 16. Preview Final Result
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

# ## 17. Write to Silver Schema
# Persist the final combined table as a managed Delta table in the **silver** schema, overwriting any previous version.

# CELL ********************

df.write.mode("overwrite").format("delta").saveAsTable("silver.silver_invoice_all")
print("Write complete: silver.silver_invoice_all")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
