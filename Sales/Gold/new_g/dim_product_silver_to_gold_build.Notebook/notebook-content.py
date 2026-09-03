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

# # Silver → Gold: Build `dim_product`
# 
# **Source table:** `silver.silver_invoice_all_refined_pbi`
# **Target table:** `gold.dim_product`
# 
# This notebook runs top to bottom in order:
# 
# 1. Read the silver invoice table
# 2. Select `InventoryID`, `ItemBrand`, `Description`, reduce to distinct products
# 3. Grain check — confirm each `InventoryID` maps to exactly one brand/description
# 4. Column profile — nulls/blanks/distinct counts on `ItemBrand` and `Description`
# 5. Null/blank imputation on `ItemBrand` and `Description`
# 6. Generate `product_sk` surrogate key
# 7. Add the Unknown Member row (`product_sk = -1`)
# 8. Write the result to the gold layer as `dim_product`
# 9. Validate the gold table
# 
# **Grain:** one row per `InventoryID`.
# 
# **Why `Description` and not `TransactionDescr`:** `ItemBrand` and `Description`
# are confirmed constant for the same `InventoryID` across every invoice line
# (true product attributes), but `TransactionDescr` varies even for the same
# product (it's line-specific text, not a stable product description).


# MARKDOWN ********************

# ## Step 1 — Read the silver invoice table

# CELL ********************

silver_table_name = "silver.silver_invoice_all_refined_pbi"

df_silver = spark.read.table(silver_table_name)

print(f"Source row count: {df_silver.count()}")
print(f"Source column count: {len(df_silver.columns)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2 — Select product columns and reduce to distinct products
# 
# The source is at invoice-line grain (the same product appears on many invoice
# lines), so this step de-duplicates down to one row per `InventoryID`.

# CELL ********************

from pyspark.sql import functions as F

df_product_selected = (
    df_silver
    .select("InventoryID", "ItemBrand", "Description")
    .filter(F.col("InventoryID").isNotNull() & (F.col("InventoryID") != ""))
    .distinct()
)

print(f"Distinct product rows: {df_product_selected.count()}")
df_product_selected.show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3 — Grain check
# 
# If the same `InventoryID` shows up with more than one `ItemBrand` or
# `Description`, that means `InventoryID` isn't a clean one-row-per-product key
# and needs to be investigated before writing to gold.

# CELL ********************

grain_check = (
    df_product_selected
    .groupBy("InventoryID")
    .agg(F.countDistinct("ItemBrand").alias("distinct_brands"),
         F.countDistinct("Description").alias("distinct_descriptions"))
    .filter("distinct_brands > 1 OR distinct_descriptions > 1")
)

problem_count = grain_check.count()

if problem_count > 0:
    print(f"WARNING — {problem_count} InventoryID value(s) map to more than one "
          f"brand or description. Review before writing to gold:")
    grain_check.show(truncate=False)
    df_product_selected = df_product_selected.dropDuplicates(["InventoryID"])
    print("\nProceeding by keeping only one occurrence per InventoryID "
          "(temporary — should be resolved properly once the underlying cause is known).")
else:
    print("No grain problems found — each InventoryID maps to exactly one brand and description.")

print(f"\nRow count after grain check: {df_product_selected.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 4 — Column profile: nulls, blanks, distinct counts
# 
# `InventoryID` is already guaranteed non-null/non-blank from Step 2's filter.
# Check `ItemBrand` and `Description` for nulls/blanks before deciding on fill
# values.

# CELL ********************

cols_to_profile = ["InventoryID", "ItemBrand", "Description"]
total_rows = df_product_selected.count()

profile_rows = []
for c in cols_to_profile:
    null_count = df_product_selected.filter(F.col(c).isNull()).count()
    blank_count = df_product_selected.filter(F.trim(F.col(c)) == "").count()
    distinct_count = df_product_selected.select(c).distinct().count()
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
# Fill any nulls/blanks in `ItemBrand` and `Description` with `"Unknown"` so no
# descriptive attribute is ever null in the gold table.

# CELL ********************

df_product_selected = df_product_selected.withColumn(
    "ItemBrand",
    F.when(
        (F.col("ItemBrand").isNull()) | (F.trim(F.col("ItemBrand")) == ""),
        "Unknown"
    ).otherwise(F.col("ItemBrand"))
).withColumn(
    "Description",
    F.when(
        (F.col("Description").isNull()) | (F.trim(F.col("Description")) == ""),
        "Unknown"
    ).otherwise(F.col("Description"))
)

check = df_product_selected.filter(
    (F.col("ItemBrand").isNull()) | (F.trim(F.col("ItemBrand")) == "") |
    (F.col("Description").isNull()) | (F.trim(F.col("Description")) == "")
).count()

print(f"Remaining nulls/blanks in ItemBrand or Description: {check}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 6 — Generate the `product_sk` surrogate key
# 
# Ordered by `InventoryID` so the surrogate key assignment is deterministic and
# reproducible on every run.

# CELL ********************

from pyspark.sql.window import Window

window = Window.orderBy("InventoryID")

df_dim_product = df_product_selected.withColumn(
    "product_sk",
    F.row_number().over(window)
)

df_dim_product = df_dim_product.select(
    "product_sk",
    "InventoryID",
    "ItemBrand",
    "Description"
)

print(f"Row count: {df_dim_product.count()}")
print(f"Distinct product_sk count: {df_dim_product.select('product_sk').distinct().count()}")
df_dim_product.show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 7 — Add the Unknown Member row (`product_sk = -1`)
# 
# Lets fact rows with a missing or unmatched `InventoryID` still join
# successfully instead of being dropped or left with a null `product_sk`.

# CELL ********************

unknown_row = spark.createDataFrame(
    [(-1, "UNKNOWN", "Unknown", "Unknown")],
    schema=df_dim_product.schema
)

df_dim_product_final = unknown_row.unionByName(df_dim_product)

print(f"Final row count (should be {df_dim_product.count() + 1}): {df_dim_product_final.count()}")
df_dim_product_final.filter("product_sk = -1").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 8 — Write the result to the gold layer as `dim_product`

# CELL ********************

gold_table_name = "gold.dim_product"

df_dim_product_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(gold_table_name)

print(f"dim_product table written successfully to: {gold_table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 9 — Validate the gold table

# CELL ********************

df_gold = spark.read.table(gold_table_name)

print(f"gold.dim_product column count: {len(df_gold.columns)}")
print(f"gold.dim_product row count: {df_gold.count()}")
df_gold.printSchema()
df_gold.show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
