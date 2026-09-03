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

# # Silver → Gold: Build `dim_salesperson`
# 
# **Source table:** `silver.silver_customer_all`
# **Target table:** `gold.dim_salesperson`
# 
# This notebook runs top to bottom in order:
# 
# 1. Read the silver customer table
# 2. Select the salesperson columns and reduce to distinct salespeople
# 3. Grain check — confirm each `SalespersonID` maps to exactly one name/RSM
# 4. Column profile — nulls/blanks/distinct counts on `Salesperson` and `RSM`
# 5. Null/blank imputation on `Salesperson` and `RSM`
# 6. Generate `salesperson_sk` surrogate key
# 7. Add the Unknown Member row (`salesperson_sk = -1`)
# 8. Write the result to the gold layer as `dim_salesperson`
# 9. Validate the gold table
# 
# **Grain:** one row per `SalespersonID`. This connects directly to
# `fact_invoice[SalespersonID]` — no dependency on `dim_customer` or `dim_region`.
# 
# **Open item to confirm once real data is available:** there is also a second
# field, `SalesPersonID_2`, in the source — worth checking whether it's an exact
# duplicate of `SalespersonID` (same pattern as `ReferenceNbr_2`, `InventoryID_2`
# found earlier) or something genuinely different.


# MARKDOWN ********************

# ## Step 1 — Read the silver customer table

# CELL ********************

silver_table_name = "silver.silver_customer_all"

df_silver = spark.read.table(silver_table_name)

print(f"Source row count: {df_silver.count()}")
print(f"Source column count: {len(df_silver.columns)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2 — Select salesperson columns and reduce to distinct salespeople
# 
# The source is at customer grain (many customers can share the same
# salesperson), so this step de-duplicates down to one row per `SalespersonID`.

# CELL ********************

from pyspark.sql import functions as F

df_salesperson_selected = (
    df_silver
    .select("SalespersonID", "Salesperson", "RSM")
    .filter(F.col("SalespersonID").isNotNull() & (F.col("SalespersonID") != ""))
    .distinct()
)

print(f"Distinct salesperson rows: {df_salesperson_selected.count()}")
df_salesperson_selected.show(50, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3 — Grain check
# 
# If the same `SalespersonID` shows up with more than one `Salesperson` name or
# `RSM` value, that means `SalespersonID` isn't a clean one-row-per-salesperson
# key and needs to be investigated before writing to gold.

# CELL ********************

grain_check = (
    df_salesperson_selected
    .groupBy("SalespersonID")
    .agg(F.countDistinct("Salesperson").alias("distinct_names"),
         F.countDistinct("RSM").alias("distinct_rsm"))
    .filter("distinct_names > 1 OR distinct_rsm > 1")
)

problem_count = grain_check.count()

if problem_count > 0:
    print(f"WARNING — {problem_count} SalespersonID value(s) map to more than one "
          f"name or RSM. Review before writing to gold:")
    grain_check.show(truncate=False)
    df_salesperson_selected = df_salesperson_selected.dropDuplicates(["SalespersonID"])
    print("\nProceeding by keeping only one occurrence per SalespersonID "
          "(temporary — should be resolved properly once the underlying cause is known).")
else:
    print("No grain problems found — each SalespersonID maps to exactly one name and RSM.")

print(f"\nRow count after grain check: {df_salesperson_selected.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 4 — Column profile: nulls, blanks, distinct counts
# 
# `SalespersonID` is already guaranteed non-null/non-blank from Step 2's filter.
# Check `Salesperson` and `RSM` for nulls/blanks before deciding on fill values.

# CELL ********************

cols_to_profile = ["SalespersonID", "Salesperson", "RSM"]
total_rows = df_salesperson_selected.count()

profile_rows = []
for c in cols_to_profile:
    null_count = df_salesperson_selected.filter(F.col(c).isNull()).count()
    blank_count = df_salesperson_selected.filter(F.trim(F.col(c)) == "").count()
    distinct_count = df_salesperson_selected.select(c).distinct().count()
    profile_rows.append((c, total_rows, null_count, blank_count, distinct_count))

df_profile = spark.createDataFrame(
    profile_rows,
    schema=["column_name", "total_rows", "null_count", "blank_string_count", "distinct_count"]
)
df_profile.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 5 — Null/blank imputation
# 
# Fill any nulls/blanks in `Salesperson` and `RSM` with `"Unknown"` so no
# descriptive attribute is ever null in the gold table.

# CELL ********************

df_salesperson_selected = df_salesperson_selected.withColumn(
    "Salesperson",
    F.when(
        (F.col("Salesperson").isNull()) | (F.trim(F.col("Salesperson")) == ""),
        "Unknown"
    ).otherwise(F.col("Salesperson"))
).withColumn(
    "RSM",
    F.when(
        (F.col("RSM").isNull()) | (F.trim(F.col("RSM")) == ""),
        "Unknown"
    ).otherwise(F.col("RSM"))
)

check = df_salesperson_selected.filter(
    (F.col("Salesperson").isNull()) | (F.trim(F.col("Salesperson")) == "") |
    (F.col("RSM").isNull()) | (F.trim(F.col("RSM")) == "")
).count()

print(f"Remaining nulls/blanks in Salesperson or RSM: {check}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 6 — Generate the `salesperson_sk` surrogate key
# 
# Ordered by `SalespersonID` so the surrogate key assignment is deterministic
# and reproducible on every run.

# CELL ********************

from pyspark.sql.window import Window

window = Window.orderBy("SalespersonID")

df_dim_salesperson = df_salesperson_selected.withColumn(
    "salesperson_sk",
    F.row_number().over(window)
)

df_dim_salesperson = df_dim_salesperson.select(
    "salesperson_sk",
    "SalespersonID",
    "Salesperson",
    "RSM"
)

print(f"Row count: {df_dim_salesperson.count()}")
print(f"Distinct salesperson_sk count: {df_dim_salesperson.select('salesperson_sk').distinct().count()}")
df_dim_salesperson.show(50, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 7 — Add the Unknown Member row (`salesperson_sk = -1`)
# 
# Lets fact rows with a missing or unmatched `SalespersonID` still join
# successfully instead of being dropped or left with a null `salesperson_sk`.

# CELL ********************

unknown_row = spark.createDataFrame(
    [(-1, "UNKNOWN", "Unknown", "Unknown")],
    schema=df_dim_salesperson.schema
)

df_dim_salesperson_final = unknown_row.unionByName(df_dim_salesperson)

print(f"Final row count (should be {df_dim_salesperson.count() + 1}): {df_dim_salesperson_final.count()}")
df_dim_salesperson_final.filter("salesperson_sk = -1").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 8 — Write the result to the gold layer as `dim_salesperson`

# CELL ********************

gold_table_name = "gold.dim_salesperson"

df_dim_salesperson_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(gold_table_name)

print(f"dim_salesperson table written successfully to: {gold_table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 9 — Validate the gold table

# CELL ********************

df_gold = spark.read.table(gold_table_name)

print(f"gold.dim_salesperson column count: {len(df_gold.columns)}")
print(f"gold.dim_salesperson row count: {df_gold.count()}")
df_gold.printSchema()
df_gold.show(50, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
