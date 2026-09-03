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

# # IFS Shipment — Bronze to Silver Transformation
# 
# **Source table:** `bronze.bronze_ifs_IFS_Shipment`  
# **Lookup tables:**
# - `bronze.bronze_acu_pbi_proforma_invoices`
# - `silver.silver_acu_pbi_sales_orders_with_bl_so`
# - `silver.silver_acu_pbi_invoice_details`
# 
# **Target table:** `silver.silver_ifs_ifs_shipment`
# 
# This notebook converts the remaining Power Query transformations after the Oracle SQL `Source` step. The Oracle SQL was already executed during ingestion, so this notebook starts directly from the ingested bronze table and applies only the merge, expand, add-column, type, rename, and write logic.

# MARKDOWN ********************

# ## 1. Imports
# 
# Load the PySpark functions required for this transformation.

# CELL ********************

from pyspark.sql import functions as F
from collections import defaultdict


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Load Bronze IFS Shipment Table
# 
# Maps to the Power Query `Source` result. The SQL query was already run during ingestion, so the source here is the bronze Delta table `bronze.bronze_ifs_IFS_Shipment`.
# 
# Run the schema and sample check first to confirm the exact column names.

# CELL ********************

df = spark.table("bronze.bronze_ifs_IFS_Shipment")

df.printSchema()
display(df.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Rename IFS Custom Columns to Delta-Safe Names
# 
# The original query returns IFS custom-field columns such as `CF$_SHIPMENT_ID`. Delta/Fabric can be sensitive to special characters in column names, so this step renames them early to underscore-safe names.
# 
# If Fabric already sanitized any names during ingestion, this cell only renames the columns that actually exist.

# CELL ********************

rename_map = {
    "CF$_SHIPMENT_ID": "CF_SHIPMENT_ID",
    "CF$_AC_SO_NO": "CF_AC_SO_NO",
    "CF$_AC_SO_LINE_NO": "CF_AC_SO_LINE_NO",
    "CF$_AC_PO_NO": "CF_AC_PO_NO",
    "CF$_SHIP_DATE": "CF_SHIP_DATE"
}

for old_name, new_name in rename_map.items():
    if old_name in df.columns:
        df = df.withColumnRenamed(old_name, new_name)

df.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Change Shipment ID Type
# 
# Maps to Power Query step `Changed Type`.
# 
# Converts `CF_SHIPMENT_ID` to text/string so it can safely join to Acumatica shipment/reference fields.

# CELL ********************

df = df.withColumn(
    "CF_SHIPMENT_ID",
    F.col("CF_SHIPMENT_ID").cast("string")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Load PBI Proforma Invoices Lookup
# 
# Loads the converted silver table for `PBI Proforma Invoices`. This lookup is used to bring `ReferenceNumber` by matching shipment number.

# CELL ********************

proforma_df = spark.table("bronze.bronze_acu_pbi_proforma_invoices")

proforma_df.printSchema()
display(proforma_df.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Prepare PBI Proforma Invoices Lookup
# 
# Maps to the lookup side of Power Query step `Merged Queries` and the later expand of `ReferenceNumber`.
# 
# Only the required columns are selected and renamed to avoid duplicate column issues after the join.

# CELL ********************

proforma_lookup_df = proforma_df.select(
    F.col("ShipmentNumber").cast("string").alias("Proforma_ShipmentNumber"),
    F.col("ReferenceNumber").cast("string").alias("PBI_Proforma_Invoices_ReferenceNumber")
)

proforma_lookup_df.printSchema()
display(proforma_lookup_df.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Left Join with PBI Proforma Invoices
# 
# Maps to Power Query steps:
# - `Merged Queries`
# - `Expanded PBI Proforma Invoices`
# 
# Performs a left outer join on `CF_SHIPMENT_ID = ShipmentNumber` and expands only `ReferenceNumber`.

# CELL ********************

df = (
    df.join(
        proforma_lookup_df,
        F.col("CF_SHIPMENT_ID") == F.col("Proforma_ShipmentNumber"),
        "left"
    )
    .drop("Proforma_ShipmentNumber")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. Load PBI Sales Orders with BL SO Lookup
# 
# Loads the converted silver table for `PBI Sales Orders with BL  SO`. This lookup is used to bring the PI number derived from blanket sales order logic.

# CELL ********************

sales_orders_df = spark.table("silver.silver_acu_pbi_sales_orders_with_bl_so")

sales_orders_df.printSchema()
display(sales_orders_df.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 9. Prepare Sales Orders Lookup
# 
# Maps to the lookup side of Power Query step `Merged Queries1` and the expand of `PI from BL`.
# 
# The code supports either `PI_from_BL` or `PI from BL`, depending on how the previous sales-order notebook was saved.

# CELL ********************

pi_from_bl_col = None
for candidate in ["PI_from_BL", "PI from BL"]:
    if candidate in sales_orders_df.columns:
        pi_from_bl_col = candidate
        break

if pi_from_bl_col is None:
    raise Exception("Could not find PI-from-BL column. Expected either 'PI_from_BL' or 'PI from BL'.")

sales_orders_lookup_df = sales_orders_df.select(
    F.col("ACU_OrderNbr").cast("string").alias("Sales_ACU_OrderNbr"),
    F.col(pi_from_bl_col).cast("string").alias("PBI_Sales_Orders_with_BL_SO_PI_from_BL")
)

sales_orders_lookup_df.printSchema()
display(sales_orders_lookup_df.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 10. Left Join with PBI Sales Orders with BL SO
# 
# Maps to Power Query steps:
# - `Merged Queries1`
# - `Expanded PBI Sales Orders with BL  SO`
# - `Changed Type1`
# 
# Performs a left outer join on `CF_AC_SO_NO = ACU_OrderNbr` and expands only PI-from-BL as a text value.

# CELL ********************

df = (
    df.join(
        sales_orders_lookup_df,
        F.col("CF_AC_SO_NO") == F.col("Sales_ACU_OrderNbr"),
        "left"
    )
    .drop("Sales_ACU_OrderNbr")
)

df = df.withColumn(
    "PBI_Sales_Orders_with_BL_SO_PI_from_BL",
    F.col("PBI_Sales_Orders_with_BL_SO_PI_from_BL").cast("string")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 11. Add PI No
# 
# Maps to Power Query steps:
# - `Added Custom`
# - `Changed Type2`
# 
# If `PBI_Proforma_Invoices_ReferenceNumber` is null, this uses `PBI_Sales_Orders_with_BL_SO_PI_from_BL`; otherwise it uses the proforma invoice reference number.

# CELL ********************

df = df.withColumn(
    "PI_No",
    F.when(
        F.col("PBI_Proforma_Invoices_ReferenceNumber").isNull(),
        F.col("PBI_Sales_Orders_with_BL_SO_PI_from_BL")
    ).otherwise(
        F.col("PBI_Proforma_Invoices_ReferenceNumber")
    ).cast("string")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 12. Load PBI Invoice Details Lookup
# 
# Loads the converted silver table for `PBI Invoice Details`. This lookup is used to bring `BLDate` by shipment number.

# CELL ********************

invoice_details_df = spark.table("silver.silver_acu_pbi_invoice_details")

invoice_details_df.printSchema()
display(invoice_details_df.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 13. Prepare PBI Invoice Details Lookup
# 
# Maps to the lookup side of Power Query step `Merged Queries2` and the expand of `BLDate`.
# 
# Only `ShipmentNo` and `BLDate` are selected to prevent duplicate key columns from being carried into the output.

# CELL ********************

invoice_details_lookup_df = invoice_details_df.select(
    F.col("ShipmentNo").cast("string").alias("Invoice_ShipmentNo"),
    F.to_date(F.col("BLDate")).alias("PBI_Invoice_Details_BLDate")
)

invoice_details_lookup_df.printSchema()
display(invoice_details_lookup_df.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 14. Left Join with PBI Invoice Details
# 
# Maps to Power Query steps:
# - `Merged Queries2`
# - `Expanded PBI Invoice Details`
# 
# Performs a left outer join on `CF_SHIPMENT_ID = ShipmentNo` and expands only `BLDate`.

# CELL ********************

df = (
    df.join(
        invoice_details_lookup_df,
        F.col("CF_SHIPMENT_ID") == F.col("Invoice_ShipmentNo"),
        "left"
    )
    .drop("Invoice_ShipmentNo")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 15. Rename ACU Sales Order Number
# 
# Maps to Power Query step `Renamed Columns`.
# 
# Renames `CF_AC_SO_NO` to `ACU_OrderNbr`.

# CELL ********************

df = df.withColumnRenamed("CF_AC_SO_NO", "ACU_OrderNbr")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 16. Duplicate Column Safety Fix
# 
# Before writing to Delta, this cell checks for duplicate column names using case-insensitive comparison. If duplicates exist, the later duplicate columns are renamed with `_dup2`, `_dup3`, etc. This avoids write failures such as `COLUMN_ALREADY_EXISTS`.
# 
# Ideally this should print an empty duplicate list because lookup tables were selected carefully before joining.

# CELL ********************

seen = defaultdict(int)
new_cols = []
duplicates_found = []

for c in df.columns:
    key = c.lower()
    seen[key] += 1
    if seen[key] == 1:
        new_cols.append(c)
    else:
        new_name = f"{c}_dup{seen[key]}"
        new_cols.append(new_name)
        duplicates_found.append((c, new_name))

if duplicates_found:
    print("Duplicate columns renamed:", duplicates_found)
    df = df.toDF(*new_cols)
else:
    print("No duplicate column names found.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 17. Preview Final Result
# 
# Review the final transformed DataFrame before writing it to the silver layer.

# CELL ********************

df.printSchema()
display(df.limit(20))
print(f"Row count: {df.count()}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 18. Write to Silver Table
# 
# Persists the transformed result as a managed Delta table in the silver schema. `overwriteSchema` is included because this table may be rebuilt while transformation logic is being refined.

# CELL ********************

df.write     .mode("overwrite")     .format("delta")     .option("overwriteSchema", "true")     .saveAsTable("silver.silver_ifs_ifs_shipment")

print("Write complete: silver.silver_ifs_ifs_shipment")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
