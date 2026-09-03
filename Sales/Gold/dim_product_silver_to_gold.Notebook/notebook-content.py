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
# 4. Write the result to the gold layer as `dim_product`
# 5. Validate the gold table
# 
# **Grain:** one row per `InventoryID`.
# 
# **Why `Description` and not `TransactionDescr`:** we checked both columns earlier —
# `ItemBrand` and `Description` are confirmed constant for the same `InventoryID`
# across every invoice line (true product attributes), but `TransactionDescr`
# actually **varies** even for the same product (it's line-specific text, not a
# stable product description). So `Description` is the correct column to use here.

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

from pyspark.sql.functions import col

df_product_selected = (
    df_silver
    .select("InventoryID", "ItemBrand", "Description")
    .filter(col("InventoryID").isNotNull() & (col("InventoryID") != ""))
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

from pyspark.sql.functions import countDistinct

grain_check = (
    df_product_selected
    .groupBy("InventoryID")
    .agg(countDistinct("ItemBrand").alias("distinct_brands"),
         countDistinct("Description").alias("distinct_descriptions"))
    .filter("distinct_brands > 1 OR distinct_descriptions > 1")
)

problem_count = grain_check.count()

if problem_count > 0:
    print(f"WARNING — {problem_count} InventoryID value(s) map to more than one "
          f"brand or description. Review before writing to gold:")
    grain_check.show(truncate=False)
    df_dim_product = df_product_selected.dropDuplicates(["InventoryID"])
    print("\nProceeding by keeping only the first occurrence of each InventoryID "
          "(temporary — should be resolved properly once the underlying cause is known).")
else:
    print("No grain problems found — each InventoryID maps to exactly one brand and description.")
    df_dim_product = df_product_selected

print(f"\nFinal dim_product row count: {df_dim_product.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 4 — Write the result to the gold layer as `dim_product`

# CELL ********************

gold_table_name = "gold.dim_product"

df_dim_product.write \
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

# ## Step 5 — Validate the gold table

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
