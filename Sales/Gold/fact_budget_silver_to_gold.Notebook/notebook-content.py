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

# # Silver → Gold: Build `fact_budget`
# 
# **Source table:** `silver.silver_budget_all_refined_pbi`
# **Target table:** `gold.fact_budget`
# 
# This notebook runs top to bottom in order:
# 
# 1. Read the silver table, check column/row count
# 2. Drop unwanted columns (empty columns + descriptive column not needed in the fact table)
# 3. Check column/row count after the drop
# 4. Rename remaining columns to business-friendly names
# 5. Write the final result to the gold layer as `fact_budget`
# 6. Validate the gold table by reading it back fresh
# 
# **Important:** use **Run all** rather than running cells individually out of order — steps 2 through 6 all depend on the DataFrame produced by the previous step.
# 
# **Open item to flag with your tech lead before finalizing:** `Customer_ID` + `Month` + `BU` is not a unique combination in this data — some combinations repeat (up to 8 times). This may indicate a hidden grain dimension (e.g. product/class) not present in this extract, or genuine duplicate rows. This notebook does **not** attempt to resolve that — it only does column cleanup — so the row count in `fact_budget` will match the silver source exactly.


# MARKDOWN ********************

# ## Step 1 — Read the silver table and check its size
# 
# Load the table and print how many columns and rows it has before any changes.

# CELL ********************

silver_table_name = "silver.silver_budget_all_refined_pbi"

df_silver = spark.read.table(silver_table_name)

print(f"BEFORE — Column count: {len(df_silver.columns)}")
print(f"BEFORE — Row count: {df_silver.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2 — Drop unwanted columns
# 
# **Empty columns** (zero non-null values across the whole table):
# - `FinBud`
# - `FinBudAmt`
# - `ST1_Formula6aad0cff645741ca85e4a890fb4a6d00`
# 
# **Descriptive column** (text attribute that belongs in a dimension table, not the fact table):
# - `Customer_Name` — belongs in Dim_Customer
# 
# **Flag/unclear-purpose column dropped per your decision:**
# - `Z` — only contained `0`/`1` values with no confirmed business meaning
# 
# Only columns that actually exist in the table get dropped — safe against naming differences between environments.

# CELL ********************

columns_to_drop = [
    # Empty columns
    "FinBud",
    "FinBudAmt",
    "ST1_Formula6aad0cff645741ca85e4a890fb4a6d00",

    # Descriptive column (belongs in Dim_Customer)
    "Customer_Name",

    # Dropped per your decision — unclear-purpose flag column
    "Z",
]

columns_to_drop_present = [c for c in columns_to_drop if c in df_silver.columns]
columns_to_drop_missing = [c for c in columns_to_drop if c not in df_silver.columns]

df_fact = df_silver.drop(*columns_to_drop_present)

print(f"Dropped {len(columns_to_drop_present)} columns.")

if columns_to_drop_missing:
    print(f"\nNote: {len(columns_to_drop_missing)} column(s) in the drop list were not found "
          f"in this table (check for naming differences):")
    for c in columns_to_drop_missing:
        print(f"  - {c}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3 — Check column and row count after the drop
# 
# Confirm the row count is unchanged (dropping columns never removes rows) and the column count has gone down as expected.

# CELL ********************

print(f"AFTER DROP — Column count: {len(df_fact.columns)}")
print(f"AFTER DROP — Row count: {df_fact.count()}")

print("\nRemaining columns:")
for c in df_fact.columns:
    print(f"  - {c}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 4 — Rename columns to business-friendly names
# 
# Renames remaining raw column names to names a business user would recognize
# (e.g. `BU` → `Business_Unit`, `SalesBudAmt` → `Sales_Budget_Amount`). Collision-checked:
# if a rename would overwrite an existing column that isn't itself being renamed away,
# it's skipped and reported rather than silently causing a duplicate column name.
# 
# **Not renamed / flagged for follow-up:**
# - `Zero Logic` → `Zero_Logic` — only casing cleaned, meaning still unconfirmed with the tech lead (same open item as in `fact_invoice`)

# CELL ********************

column_rename_map = {
    "Customer_ID": "Customer_ID",
    "BU": "Business_Unit",
    "Month": "Budget_Month",
    "Weight": "Budget_Weight",
    "Weight(MT)": "Budget_Weight_MetricTons",
    "SalesBudAmt": "Sales_Budget_Amount",
    "Sales_BudAmt($k)": "Sales_Budget_Amount_Thousands",
    "Zero Logic": "Zero_Logic",
}

current_columns = set(df_fact.columns)

# Sanity check: does this map cover every column currently in df_fact?
missing_from_map = [c for c in df_fact.columns if c not in column_rename_map]
if missing_from_map:
    print(f"WARNING — these columns exist but aren't in the rename map: {missing_from_map}")

safe_renames = {}
skipped = {}
for old_name, new_name in column_rename_map.items():
    if old_name not in current_columns:
        continue
    collision = new_name in current_columns and new_name != old_name and new_name not in column_rename_map
    if collision:
        skipped[old_name] = new_name
    else:
        safe_renames[old_name] = new_name

if skipped:
    print("SKIPPED renames due to naming collision:")
    for o, n in skipped.items():
        print(f"  - {o} -> {n}")

for old_name, new_name in safe_renames.items():
    df_fact = df_fact.withColumnRenamed(old_name, new_name)

# Duplicate check before writing
if len(df_fact.columns) != len(set(df_fact.columns)):
    from collections import Counter
    dupes = [c for c, n in Counter(df_fact.columns).items() if n > 1]
    raise ValueError(f"STOPPED — duplicate column names would be created: {dupes}")

print(f"\nRenamed {len(safe_renames)} columns.")
print("Final column list after rename:")
for c in df_fact.columns:
    print(f"  - {c}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 5 — Write the result to the gold layer as `fact_budget`
# 
# Saved as a managed Delta table in the **gold** schema. `mode("overwrite")` replaces
# the table if it already exists, so this cell is safe to re-run. This is the only
# write in the notebook — it happens after every transformation above.

# CELL ********************

gold_table_name = "gold.fact_budget"

df_fact.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(gold_table_name)

print(f"fact_budget table written successfully to: {gold_table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 6 — Validate the gold table
# 
# Read back `gold.fact_budget` fresh from disk (not from memory) and confirm the
# row/column counts and schema match what Step 4 produced.

# CELL ********************

df_gold = spark.read.table(gold_table_name)

print(f"gold.fact_budget column count: {len(df_gold.columns)}")
print(f"gold.fact_budget row count: {df_gold.count()}")
df_gold.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
