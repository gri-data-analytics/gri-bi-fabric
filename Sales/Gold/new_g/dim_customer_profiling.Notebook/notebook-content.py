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

# # Silver → Gold: `dim_customer` — Column Profiling
# 
# **Source table:** `silver.silver_customer_all`
# **Target table (later step):** `gold.dim_customer`
# 
# This notebook is the **profiling step** before the final build. It:
# 
# 1. Reads the silver customer table
# 2. Selects only the confirmed `dim_customer` columns (no `dim_region` join — Region/SubRegion are kept as plain attributes)
# 3. Profiles each column for nulls, blanks, and distinct counts
# 4. Checks for duplicate `Customer_ID` (grain check — dimension must be one row per `Customer_ID`)
# 5. Checks blank/null `Region`/`SubRegion` specifically
# 
# **Grain:** one row per `Customer_ID`.
# 
# **Final column list (planned):** `Customer_ID` (business key), `Customer_Name`, `Country`, `Location`, `Customer_Segment`, `Region`, `SubRegion`.
# 
# Run this first and review the output before moving to the build notebook (null-fill, surrogate key, unknown member row, write to gold).

# MARKDOWN ********************

# ## Step 1 — Read the silver customer table

# CELL ********************

silver_table_name = "silver.silver_customer_all"

df_silver = spark.read.table(silver_table_name)

print(f"Silver customer — row count: {df_silver.count()}, column count: {len(df_silver.columns)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2 — Select the confirmed `dim_customer` columns
# 
# No `dim_region` join needed — `Region` and `SubRegion` stay as regular descriptive
# attributes on `dim_customer` (removed the duplicate `"Region"` entry from the
# original select as well).

# CELL ********************

df_customer_selected = df_silver.select(
    "Customer_ID",
    "Customer_Name",
    "Country",
    "Location",
    "Customer_Segment",
    "Region",
    "SubRegion"
)

print(f"Selected column count: {len(df_customer_selected.columns)}")
print(f"Row count: {df_customer_selected.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3 — Column profile: nulls, blanks, distinct counts
# 
# Run this on every selected column to see exactly what needs a null/blank fill
# default before the dimension is built.

# CELL ********************

from pyspark.sql import functions as F

cols_to_profile = [
    "Customer_ID", "Customer_Name", "Country",
    "Location", "Customer_Segment", "Region", "SubRegion"
]
total_rows = df_customer_selected.count()

profile_rows = []
for c in cols_to_profile:
    null_count = df_customer_selected.filter(F.col(c).isNull()).count()
    blank_count = df_customer_selected.filter(F.trim(F.col(c)) == "").count()
    distinct_count = df_customer_selected.select(c).distinct().count()
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

# CELL ********************

same_684_check = df_customer_selected.filter(
    ((F.col("Location").isNull()) | (F.trim(F.col("Location")) == "")) !=
    ((F.col("SubRegion").isNull()) | (F.trim(F.col("SubRegion")) == ""))
).count()

print(f"Rows where Location-missing and SubRegion-missing DON'T match: {same_684_check}")

if same_684_check == 0:
    print("Confirmed — it's the exact same 684 customers missing both Location and SubRegion.")
else:
    print(f"Not identical — {same_684_check} rows differ between the two.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_customer_selected = df_customer_selected.withColumn(
    "Location",
    F.when(
        (F.col("Location").isNull()) | (F.trim(F.col("Location")) == ""),
        "Unknown"
    ).otherwise(F.col("Location"))
).withColumn(
    "SubRegion",
    F.when(
        (F.col("SubRegion").isNull()) | (F.trim(F.col("SubRegion")) == ""),
        "Unknown"
    ).otherwise(F.col("SubRegion"))
)

# Re-check
check = df_customer_selected.filter(
    (F.col("Location").isNull()) | (F.trim(F.col("Location")) == "") |
    (F.col("SubRegion").isNull()) | (F.trim(F.col("SubRegion")) == "")
).count()

print(f"Remaining nulls/blanks in Location or SubRegion: {check}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_customer_selected = df_customer_selected.withColumn(
    "Customer_Segment",
    F.when(
        (F.col("Customer_Segment").isNull()) | (F.trim(F.col("Customer_Segment")) == ""),
        "Unknown"
    ).otherwise(F.col("Customer_Segment"))
)

# Re-check all three columns together
check = df_customer_selected.filter(
    (F.col("Location").isNull()) | (F.trim(F.col("Location")) == "") |
    (F.col("SubRegion").isNull()) | (F.trim(F.col("SubRegion")) == "") |
    (F.col("Customer_Segment").isNull()) | (F.trim(F.col("Customer_Segment")) == "")
).count()

print(f"Remaining nulls/blanks in Location, SubRegion, or Customer_Segment: {check}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

cols_to_profile = [
    "Customer_ID", "Customer_Name", "Country",
    "Location", "Customer_Segment", "Region", "SubRegion"
]
total_rows = df_customer_selected.count()

profile_rows = []
for c in cols_to_profile:
    null_count = df_customer_selected.filter(F.col(c).isNull()).count()
    blank_count = df_customer_selected.filter(F.trim(F.col(c)) == "").count()
    distinct_count = df_customer_selected.select(c).distinct().count()
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

# ## Step 4 — Duplicate `Customer_ID` check
# 
# The dimension grain is one row per `Customer_ID`. If any `Customer_ID` appears
# more than once here, that has to be resolved (dedupe logic or a grain fix)
# before the surrogate key step — otherwise the surrogate key won't be unique
# per business key.

# CELL ********************

dupe_customers = (
    df_customer_selected
    .groupBy("Customer_ID")
    .count()
    .filter("count > 1")
)

print(f"Duplicate Customer_ID count: {dupe_customers.count()}")
dupe_customers.show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Next step
# 
# Review the profile output, duplicate count, and blank Region/SubRegion count
# above. Once reviewed, move to the build notebook: null/blank fill per column →
# generate `customer_sk` surrogate key → add the Unknown Member row (SK = -1) →
# write to `gold.dim_customer`.

# CELL ********************

from pyspark.sql.window import Window

window = Window.orderBy("Customer_ID")

df_dim_customer = df_customer_selected.withColumn(
    "customer_sk",
    F.row_number().over(window)
)

# Reorder so the surrogate key leads, business key right after
df_dim_customer = df_dim_customer.select(
    "customer_sk",
    "Customer_ID",
    "Customer_Name",
    "Country",
    "Location",
    "Customer_Segment",
    "Region",
    "SubRegion"
)

print(f"Row count: {df_dim_customer.count()}")
print(f"Distinct customer_sk count: {df_dim_customer.select('customer_sk').distinct().count()}")
df_dim_customer.show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

unknown_row = spark.createDataFrame(
    [(-1, "UNKNOWN", "Unknown", "Unknown", "Unknown", "Unknown", "Unknown", "Unknown")],
    schema=df_dim_customer.schema
)
df_dim_customer_final = unknown_row.unionByName(df_dim_customer)
print(f"Final row count (should be 911): {df_dim_customer_final.count()}")
df_dim_customer_final.filter("customer_sk = -1").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_table_name = "gold.dim_customer"

df_dim_customer_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(gold_table_name)

print(f"dim_customer written to: {gold_table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
