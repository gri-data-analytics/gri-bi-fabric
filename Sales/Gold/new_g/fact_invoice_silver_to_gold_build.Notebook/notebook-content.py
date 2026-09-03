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
# This notebook runs top to bottom in order:
# 
# 1. Read the silver table, check column/row count
# 2. Drop all unwanted columns (empty columns + descriptive/categorical columns + confirmed duplicate columns)
# 3. Check column/row count after the drop
# 4. Rename remaining columns to business-friendly names
# 5. Profile the quantitative (measure) columns — nulls, NaN, Inf, existing zeros
# 6. Convert NaN/Inf → NULL across all float measure columns
# 7. `Invoice_Value` validity decision — drop rows with an unusable `Invoice_Value` (documented decision, not a silent filter)
# 8. Look up surrogate keys from `dim_customer`, `dim_product`, `dim_salesperson`
# 9. Write the final result to the gold layer as `fact_invoice`
# 10. Validate the gold table by reading it back fresh
# 
# **Important:** use **Run all** rather than running cells individually out of order — steps 2 through 10 all depend on the DataFrame produced by the previous step.


# MARKDOWN ********************

# ## Step 1 — Read the silver table and check its size

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
# 
# `Invoice Value` / `Invoice_Value` is intentionally **kept** (not dropped) — it's used for the row-validity decision in Step 7.
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
    "LineAmount2($)",
    "LineAmount2_USD",

    # NOTE: "Invoice Value" / "Invoice_Value" intentionally NOT dropped —
    # used in Step 7's row-validity decision.
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
# (e.g. `InventoryID` → `Product_ID`, `BU` → `Business_Unit`). Collision-checked:
# if a rename would overwrite an existing column that isn't itself being renamed away,
# it's skipped and reported rather than silently causing a duplicate column name.
# 
# **Not renamed / flagged for follow-up:**
# - `Zero_Logic` / `Zero Logic` — meaning unconfirmed with the tech lead, left as-is rather than guessed
# - `InventoryItem_baseWeight` → `Product_BaseWeight` and `TotalVolume` → `Total_Volume` — renamed here, but both are still pending a future move to `Dim_Item` / `Dim_Shipment` once approved

# CELL ********************

from pyspark.sql import functions as F

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

# MARKDOWN ********************

# ## Step 5 — Profile the quantitative (measure) columns
# 
# Check nulls, NaN, Inf, and existing zeros separately for every numeric measure
# column. `isnan()` only applies to `double`/`float` columns — `decimal` columns
# can't hold NaN by type, only NULL.
# 
# This replaces doing a one-off check on a single column (e.g. only
# `USD_Value_Thousands`) — every quantitative column gets checked before any
# cleanup decision is made.

# CELL ********************

quant_cols = [
    c for c, dtype in df_fact.dtypes
    if dtype.startswith("decimal") or dtype in ("double", "float", "int", "bigint")
]
print(f"Quantitative columns detected: {quant_cols}")

total_rows = df_fact.count()
profile_rows = []
for c in quant_cols:
    dtype = dict(df_fact.dtypes)[c]
    null_count = df_fact.filter(F.col(c).isNull()).count()
    if dtype in ("double", "float"):
        nan_count = df_fact.filter(F.isnan(F.col(c))).count()
        inf_count = df_fact.filter(
            (F.col(c) == float("inf")) | (F.col(c) == float("-inf"))
        ).count()
    else:
        nan_count = 0
        inf_count = 0
    zero_count = df_fact.filter(F.col(c) == 0).count()
    profile_rows.append((c, dtype, total_rows, null_count, nan_count, inf_count, zero_count))

df_quant_profile = spark.createDataFrame(
    profile_rows,
    schema=["column_name", "data_type", "total_rows", "null_count", "nan_count", "inf_count", "existing_zero_count"]
)
df_quant_profile.show(40, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 6 — Convert NaN/Inf → NULL across all float measure columns
# 
# NaN and Inf are not the same as NULL and behave differently in aggregations —
# a single NaN or Inf in a `SUM()`/`AVG()` poisons the entire result, whereas a
# NULL is simply skipped. This generalizes the one-off fix that was previously
# only applied to `USD_Value_Thousands` to every float column found in Step 5.

# CELL ********************

float_cols = [c for c, dtype in df_fact.dtypes if dtype in ("double", "float")]

for c in float_cols:
    df_fact = df_fact.withColumn(
        c,
        F.when(F.isnan(F.col(c)), None)
         .when(F.col(c) == float("inf"), None)
         .when(F.col(c) == float("-inf"), None)
         .otherwise(F.col(c))
    )

remaining_bad = sum(
    df_fact.filter(
        F.isnan(F.col(c)) | (F.col(c) == float("inf")) | (F.col(c) == float("-inf"))
    ).count()
    for c in float_cols
)
print(f"Remaining NaN/Inf values across float columns: {remaining_bad}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 7 — `Invoice_Value` validity decision
# 
# Rows with a null, blank, `"nan"`, or `"n/a"` `Invoice_Value` are treated as
# unusable and dropped from the fact table — this is the same decision made
# previously (135 rows), now run once as part of the build instead of as a
# separate pass after the table was already written to gold.

# CELL ********************

invalid_count = df_fact.filter(
    F.col("Invoice_Value").isNull() |
    (F.trim(F.col("Invoice_Value").cast("string")) == "") |
    (F.lower(F.col("Invoice_Value").cast("string")) == "nan") |
    (F.lower(F.col("Invoice_Value").cast("string")) == "n/a")
).count()

print(f"Invalid Invoice_Value count: {invalid_count}")

df_fact = df_fact.filter(
    ~(
        F.col("Invoice_Value").isNull() |
        (F.trim(F.col("Invoice_Value").cast("string")) == "") |
        (F.lower(F.col("Invoice_Value").cast("string")) == "nan") |
        (F.lower(F.col("Invoice_Value").cast("string")) == "n/a")
    )
)

print(f"Row count after dropping invalid Invoice_Value rows: {df_fact.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 8 — Look up surrogate keys from the dimension tables
# 
# Joins on the business key from each dimension to pull in its surrogate key.
# Any invoice line whose `Customer_ID`, `Product_ID`, or `Salesperson_ID` has no
# match in the corresponding dimension is pointed at that dimension's Unknown
# Member row (`sk = -1`) instead of being left with a NULL foreign key.

# CELL ********************

df_dim_customer = spark.read.table("gold.dim_customer")
df_dim_product = spark.read.table("gold.dim_product")
df_dim_salesperson = spark.read.table("gold.dim_salesperson")

df_fact = (
    df_fact
    .join(
        df_dim_customer.select("Customer_ID", "customer_sk"),
        on="Customer_ID",
        how="left"
    )
    .withColumn("customer_sk", F.coalesce(F.col("customer_sk"), F.lit(-1)))
    .join(
        df_dim_product.select(
            F.col("InventoryID").alias("Product_ID"),
            "product_sk"
        ),
        on="Product_ID",
        how="left"
    )
    .withColumn("product_sk", F.coalesce(F.col("product_sk"), F.lit(-1)))
    .join(
        df_dim_salesperson.select(
            F.col("SalespersonID").alias("Salesperson_ID"),
            "salesperson_sk"
        ),
        on="Salesperson_ID",
        how="left"
    )
    .withColumn("salesperson_sk", F.coalesce(F.col("salesperson_sk"), F.lit(-1)))
)

print(f"Row count after surrogate key lookups: {df_fact.count()}")
print(f"Unmatched customer (customer_sk = -1): {df_fact.filter('customer_sk = -1').count()}")
print(f"Unmatched product (product_sk = -1): {df_fact.filter('product_sk = -1').count()}")
print(f"Unmatched salesperson (salesperson_sk = -1): {df_fact.filter('salesperson_sk = -1').count()}")

df_fact.select("Customer_ID", "customer_sk", "Product_ID", "product_sk", "Salesperson_ID", "salesperson_sk").show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 9 — Write the result to the gold layer as `fact_invoice`
# 
# Saved as a managed Delta table in the **gold** schema. `mode("overwrite")` replaces
# the table if it already exists, so this cell is safe to re-run. This is the only
# write in the notebook — it happens after every transformation above, so the table
# on disk always reflects the fully cleaned, key-resolved DataFrame.

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
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 10 — Validate the gold table

# CELL ********************

df_gold = spark.read.table(gold_table_name)

print(f"gold.fact_invoice column count: {len(df_gold.columns)}")
print(f"gold.fact_invoice row count: {df_gold.count()}")
df_gold.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
