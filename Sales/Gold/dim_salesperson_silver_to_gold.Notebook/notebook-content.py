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
# **Source table:** `silver.silver_customer_all_refined_pbi`
# **Target table:** `gold.dim_salesperson`
# 
# This notebook runs top to bottom in order:
# 
# 1. Read the silver customer table
# 2. Select the salesperson columns and reduce to distinct salespeople
# 3. Check for any `SalespersonID` that maps to more than one name/RSM (a grain problem, if it happens)
# 4. Write the result to the gold layer as `dim_salesperson`
# 5. Validate the gold table
# 
# **Grain:** one row per `SalespersonID`. This connects directly to
# `fact_invoice[SalespersonID]` — no dependency on `dim_customer` or `dim_region`.
# 
# **Open item to confirm once real data is available:** `SalespersonID` and
# `Salesperson` (name) were both empty in the sample file used to design this —
# this notebook still works correctly once populated, but hasn't been validated
# against real values yet. There is also a second field, `SalesPersonID_2`, in
# the source — worth checking once real data is available whether it's an exact
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

from pyspark.sql.functions import col

df_salesperson_selected = (
    df_silver
    .select("SalespersonID", "Salesperson", "RSM")
    .filter(col("SalespersonID").isNotNull() & (col("SalespersonID") != ""))
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

# ## Step 3 — Check for grain problems
# 
# If the same `SalespersonID` shows up with more than one `Salesperson` name or
# `RSM` value, that means `SalespersonID` isn't a clean one-row-per-salesperson
# key and needs to be investigated before writing to gold.

# CELL ********************

from pyspark.sql.functions import countDistinct

grain_check = (
    df_salesperson_selected
    .groupBy("SalespersonID")
    .agg(countDistinct("Salesperson").alias("distinct_names"),
         countDistinct("RSM").alias("distinct_rsm"))
    .filter("distinct_names > 1 OR distinct_rsm > 1")
)

problem_count = grain_check.count()

if problem_count > 0:
    print(f"WARNING — {problem_count} SalespersonID value(s) map to more than one "
          f"name or RSM. Review before writing to gold:")
    grain_check.show(truncate=False)
    df_dim_salesperson = df_salesperson_selected.dropDuplicates(["SalespersonID"])
    print("\nProceeding by keeping only the first occurrence of each SalespersonID "
          "(temporary — should be resolved properly once the underlying cause is known).")
else:
    print("No grain problems found — each SalespersonID maps to exactly one name and RSM.")
    df_dim_salesperson = df_salesperson_selected

print(f"\nFinal dim_salesperson row count: {df_dim_salesperson.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 4 — Write the result to the gold layer as `dim_salesperson`

# CELL ********************

gold_table_name = "gold.dim_salesperson"

df_dim_salesperson.write \
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

# ## Step 5 — Validate the gold table

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
