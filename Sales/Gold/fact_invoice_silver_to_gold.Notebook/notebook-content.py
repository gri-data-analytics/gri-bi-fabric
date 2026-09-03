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

# # Silver → Gold: Build `fact_invoice`
# 
# **Source table:** `silver.silver_invoice_all_refined_pbi`
# **Target table:** `gold.fact_invoice`
# 
# This notebook is fully self-contained and runs top to bottom in order:
# 
# 1. Read the silver table, check column/row count
# 2. Drop all unwanted columns (empty columns + descriptive/categorical columns + confirmed duplicate columns)
# 3. Check column/row count after the drop
# 4. Rename remaining columns to business-friendly names
# 5. Write the final result to the gold layer as `fact_invoice`
# 6. Validate the gold table by reading it back fresh
# 
# **Important:** always use **Run all** for this notebook rather than running cells individually out of order — steps 2 through 6 all depend on the DataFrame produced by the previous step.

# MARKDOWN ********************

# ## Step 1 — Read the silver table and check its size
# 
# Load the table and print how many columns and rows it has before any changes.

# CELL ********************

silver_table_name = "silver.silver_invoice_all_refined_pbi"

df_silver = spark.read.table(silver_table_name)

print(f"BEFORE — Column count: {len(df_silver.columns)}")
print(f"BEFORE — Row count: {df_silver.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2 — Drop all unwanted columns
# 
# Combined list of every column being removed from `fact_invoice`, for three reasons:
# 
# **Empty / placeholder columns** (zero non-null values across the whole table — both naming variants covered, since the sample file and production table name these differently):
# - `CuryRateID`, `Type_4`, `ReceiptNbr_2`, `LineNbr_2`
# - `Column9`, `Column10`, `Column11` (sample-file naming)
# - `Unnamed:_8`, `Unnamed:_9`, `Unnamed:_10` (production naming — note: these actually contain real data (tire construction/category text), included here as a deliberate exclusion decision, not because they're truly empty)
# 
# **Descriptive/categorical columns** (text attributes that belong in a dimension table, not the fact table):
# - `Type`, `Status`, `Customer_Name`, `Name`, `Currency`, `ItemBrand`, `Type_2`, `TransactionDescr`, `Description`, `TranType`, `Type_3`, `DB`
# 
# **Duplicate columns** (confirmed identical values to the column being kept):
# - `ReferenceNbr_2` (duplicate of `ReferenceNbr`)
# - `InventoryID_2` (duplicate of `InventoryID`)
# - `ReceiptNbr` (duplicate of `ShipmentID`)
# - `LineAmount2($)` / `LineAmount2_USD` (duplicate of `LineAmount`)
# - `Invoice Value` / `Invoice_Value` (duplicate of `LineAmount`)
# - `Weight` (duplicate of `LineWeight`)
# 
# Only columns that actually exist in the table get dropped — safe against naming differences between environments.


# CELL ********************

columns_to_drop = [
    # Empty / placeholder columns
    "CuryRateID",
    "Type_4",
    "ReceiptNbr_2",
    "LineNbr_2",
    "Column9",
    "Column10",
    "Column11",
    "Unnamed:_8",
    "Unnamed:_9",
    "Unnamed:_10",

    # Descriptive / categorical columns (belong in dimension tables)
    "Type",
    "Status",
    "Customer_Name",
    "Name",
    "Type_2",
    "TransactionDescr",
    "Description",
    "TranType",
    "Type_3",
    "DB",

    # Duplicate columns (confirmed identical values to the column being kept)
    "ReferenceNbr_2",
    "InventoryID_2",
    "ReceiptNbr",
    #"Invoice Value",
    #"Invoice_Value",
    "LineAmount2($)",
    "LineAmount2_USD"
]

columns_to_drop_present = [c for c in columns_to_drop if c in df_silver.columns]
columns_to_drop_missing = [c for c in columns_to_drop if c not in df_silver.columns]

df_fact = df_silver.drop(*columns_to_drop_present)

print(f"Dropped {len(columns_to_drop_present)} columns.")

if columns_to_drop_missing:
    print(f"\nNote: {len(columns_to_drop_missing)} column(s) in the drop list were not found "
          f"in this table (expected — naming differs between sample file and production):")
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
# Renames remaining raw/system column names to names a business user would recognize
# (e.g. `InventoryID` → `Product_ID`, `Amount` → `TotalAmount`). Collision-checked:
# if a rename would overwrite an existing column that isn't itself being renamed away,
# it's skipped and reported rather than silently causing a duplicate column name.
# 
# **Not renamed / flagged for follow-up:**
# - `Zero_Logic` / `Zero Logic` — meaning unconfirmed with the tech lead, left as-is rather than guessed
# - `InventoryItem_baseWeight` → `ProductBaseWeight` and `TotalVolume` → `ShipmentTotalVolume` — renamed here, but both are still pending a future move to `Dim_Item` / `Dim_Shipment` once approved

# CELL ********************

column_rename_map = {
    "ReferenceNbr": "Reference_Number",
    "LineNbr": "Line_Number",
    "InvoiceDate": "Invoice_Date",
    "Amount": "Amount",
    "Customer_ID": "Customer_ID",
    "SalespersonID": "Salesperson_ID",
    "InventoryItem_baseWeight": "Product_BaseWeight",
    "Quantity": "Quantity_Sold",
    "LineWeight": "Line_Weight",
    "BU": "Business_Unit",
    "CurrencyRate": "Exchange_Rate",
    "LineAmount": "Line_Amount",
    "DocumentDate": "Document_Date",
    "ShipmentID": "Shipment_ID",
    "RateReciprocal": "RateReciprocal",
    "LineVolume": "Line_Volume",
    "Freight": "Freight_Cost",
    "IFSShipmentNo": "IFS_Shipment_Number",
    "TotalVolume": "Total_Volume",
    "UpdatedLineVolume": "Updated_Line_Volume",
    "Volume": "Volume",
    "InventoryID": "Product_ID",
    "ClassID": "Class_ID",
    "Weight_MT": "Weight_MetricTons",
    "USD_Value_k": "USD_Value_Thousands",
    "Zero_Logic": "Zero_Logic",
    "Invoice_k": "Invoice_Value_Thousands",
}

current_columns = set(df_fact.columns)

# Sanity check: does this map cover every column currently in df_fact?
missing_from_map = [c for c in df_fact.columns if c not in column_rename_map]
if missing_from_map:
    print(f"WARNING — these columns exist but aren't in the rename map: {missing_from_map}")

for old_name, new_name in column_rename_map.items():
    if old_name in current_columns:
        df_fact = df_fact.withColumnRenamed(old_name, new_name)

# Duplicate check before writing
if len(df_fact.columns) != len(set(df_fact.columns)):
    from collections import Counter
    dupes = [c for c, n in Counter(df_fact.columns).items() if n > 1]
    raise ValueError(f"STOPPED — duplicate column names would be created: {dupes}")

print(f"Renamed columns. Final schema ({len(df_fact.columns)} columns):")
df_fact.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import when, col, isnan

df_fact = df_fact.withColumn(
    "USD_Value_Thousands",
    when(isnan(col("USD_Value_Thousands")), 0)
    .when(col("USD_Value_Thousands") == float("inf"), 0)
    .when(col("USD_Value_Thousands") == float("-inf"), 0)
    .otherwise(col("USD_Value_Thousands"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, isnan

bad_rows = df_fact.filter(
    isnan(col("USD_Value_Thousands")) |
    (col("USD_Value_Thousands") == float("inf")) |
    (col("USD_Value_Thousands") == float("-inf"))
)

print("Bad Rows:", bad_rows.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 5 — Write the result to the gold layer as `fact_invoice`
# 
# Saved as a managed Delta table in the **gold** schema. `mode("overwrite")` replaces
# the table if it already exists, so this cell is safe to re-run. This is the only
# write in the notebook — it happens after every transformation above, so the table
# on disk always reflects the fully cleaned and renamed DataFrame.

# CELL ********************

gold_table_name = "gold.fact_invoice"

df_fact.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(gold_table_name)

print(f"fact_invoice table written successfully to: {gold_table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

df_gold = spark.read.table(gold_table_name)
display(df_gold)

print(f"gold.fact_invoice column count: {len(df_gold.columns)}")
print(f"gold.fact_invoice row count: {df_gold.count()}")
df_gold.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

invalid_count = df_gold.filter(
    F.col("Invoice_Value").isNull() |
    (F.trim(F.col("Invoice_Value")) == "") |
    (F.lower(F.col("Invoice_Value")) == "nan") |
    (F.lower(F.col("Invoice_Value")) == "n/a")
).count()

print(f"Invalid Value Count: {invalid_count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

invalid_count = df_gold.filter(
    F.col("Invoice_Value").isNull() |
    (F.trim(F.col("Invoice_Value")) == "") |
    (F.lower(F.col("Invoice_Value")) == "nan") |
    (F.lower(F.col("Invoice_Value")) == "n/a")
)
display(invalid_count)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df_gold = df_gold.filter(
    ~(
        F.col("Invoice_Value").isNull() |
        (F.trim(F.col("Invoice_Value")) == "") |
        (F.lower(F.col("Invoice_Value")) == "nan") |
        (F.lower(F.col("Invoice_Value")) == "n/a")
    )
)

display(df_gold)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_table_name = "gold.fact_invoice"

df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(gold_table_name)

print(f"fact_invoice table written successfully to: {gold_table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
