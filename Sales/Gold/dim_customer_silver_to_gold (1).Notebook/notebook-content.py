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

# # Silver → Gold: Build `dim_customer`
# 
# **Source table:** `silver.silver_customer_all_refined_pbi`
# **Also reads:** `gold.dim_region` (must already exist — run the `dim_region` notebook first)
# **Target table:** `gold.dim_customer`
# 
# This notebook runs top to bottom in order:
# 
# 1. Read the silver customer table and the already-built `gold.dim_region`
# 2. Select only the confirmed columns for `dim_customer`
# 3. Join to `dim_region` to attach the `Region_ID` foreign key
# 4. Check column/row count
# 5. Write the result to the gold layer as `dim_customer`
# 6. Validate the gold table
# 
# **Grain:** one row per `Customer_ID`.
# 
# **Final column list:** `Customer_ID` (PK), `Customer_Name`, `Country`, `Location`,
# `Customer_Segment`, `Region_ID` (FK → `dim_region`).
# 
# **Open item:** `Location` was empty in the sample file used to design this table —
# this notebook still carries it through correctly if populated in production, but
# its actual grain/meaning hasn't been verified yet.

# MARKDOWN ********************

# ## Step 1 — Read the silver customer table and the gold region dimension

# CELL ********************

silver_table_name = "silver.silver_customer_all"
dim_region_table_name = "gold.dim_region"

df_silver = spark.read.table(silver_table_name)
df_dim_region = spark.read.table(dim_region_table_name)

print(f"Silver customer — row count: {df_silver.count()}, column count: {len(df_silver.columns)}")
print(f"gold.dim_region — row count: {df_dim_region.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2 — Select only the confirmed dim_customer columns
# 
# Keeping `Region` and `SubRegion` for now too — they're needed as the join keys
# in the next step, and get dropped afterward once `Region_ID` is attached.

# CELL ********************

df_customer_selected = df_silver.select(
    "Customer_ID",
    "Customer_Name",
    "Country",
    "Location",
    "Customer_Segment",
    "Region",
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

# ## Step 3 — Join to dim_region to attach Region_ID
# 
# Left join so customers with a blank or unmatched region still stay in the
# table with a null `Region_ID`, rather than being silently dropped.

# CELL ********************

df_dim_customer = (
    df_customer_selected
    .join(df_dim_region, on=["Region", "SubRegion"], how="left")
    .select(
        "Customer_ID",
        "Customer_Name",
        "Country",
        "Location",
        "Customer_Segment",
        "Region",
        "SubRegion"
    )
)

unmatched_count = df_dim_customer.filter("Region_ID IS NULL").count()
print(f"Customers with no matching Region_ID after the join: {unmatched_count}")

print(f"\ndim_customer row count: {df_dim_customer.count()}")
print(f"dim_customer column count: {len(df_dim_customer.columns)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# ## Step 4 — Check column and row count
# 
# Confirm the row count still matches the original customer count from Step 1
# (the join should not have created duplicate rows — if it did, `dim_region`
# does not have a clean one-row-per-Region+SubRegion grain).

# CELL ********************

original_row_count = df_silver.count()
final_row_count = df_dim_customer.count()

print(f"Original silver row count: {original_row_count}")
print(f"Final dim_customer row count: {final_row_count}")

if final_row_count != original_row_count:
    print("\nWARNING — row counts don't match. The join likely produced duplicate "
          "rows, which usually means dim_region has more than one row for the same "
          "Region + SubRegion combination. Check dim_region's grain before proceeding.")
else:
    print("\nRow counts match — join did not duplicate any customers.")

print("\nFinal columns:")
for c in df_dim_customer.columns:
    print(f"  - {c}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 5 — Write the result to the gold layer as `dim_customer`

# CELL ********************

gold_table_name = "gold.dim_customer"

df_dim_customer.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(gold_table_name)

print(f"dim_customer table written successfully to: {gold_table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 6 — Validate the gold table

# CELL ********************

df_gold = spark.read.table(gold_table_name)

print(f"gold.dim_customer column count: {len(df_gold.columns)}")
print(f"gold.dim_customer row count: {df_gold.count()}")
df_gold.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
