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

# # PBI_Sales_Orders_with_BL_SO — Bronze to Silver Transformation
# 
# **Source table:** `bronze.bronze_acu_pbi_sales_orders_with_bl_so`  
# **Lookup table:** `silver.silver_acu_pbi_invoice_details`  
# **Target table:** `silver.silver_acu_pbi_sales_orders_with_bl_so`
# 
# This notebook replicates the Power BI transformations for **PBI Sales Orders with BL SO** using PySpark in Microsoft Fabric.


# MARKDOWN ********************

# ## 1. Imports
# Load the PySpark functions needed for this transformation.

# CELL ********************

from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Load Source Tables
# Read the Bronze Sales Orders table and the Silver Invoice Details lookup table.

# CELL ********************

df_sales = spark.table("bronze.bronze_acu_PBI_Sales_Orders_with_BL__SO")
df_invoice = spark.table("silver.silver_acu_pbi_invoice_details")

df_sales.printSchema()
display(df_sales.limit(10))
display(df_invoice.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Create Confirm Qty

# CELL ********************

df = df_sales.withColumn("Confirm Qty", F.col("Quantity"))
display(df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Rename Item Column

# CELL ********************

df = df.withColumnRenamed("itemclass_Formula9d33aae141b44560b817b104db907d74","Item")
display(df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Join Invoice Details

# CELL ********************

invoice = df_invoice.select("SalesOrderNumber","InventoryID","BLDate","ETADate")
df = df.join(invoice,(df.OrderNbr==invoice.SalesOrderNumber)&(df.InventoryID==invoice.InventoryID),"left").drop(invoice.SalesOrderNumber)
display(df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

from pyspark.sql.functions import col

invoice = df_invoice.select(
    "SalesOrderNumber",
    col("InventoryID").alias("InventoryID2"),
    "BLDate",
    "ETADate"
)

df = (
    df.alias("so")
    .join(
        invoice.alias("inv"),
        (col("so.OrderNbr") == col("inv.SalesOrderNumber")) &
        (col("so.InventoryID") == col("inv.InventoryID2")),
        "left"
    )
    .drop("SalesOrderNumber")
)

display(df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Create PI from BL

# CELL ********************

df = df.withColumn("PI from BL",F.when(F.col("BlanketSORefNbr").isNull(),F.lit(None)).otherwise(F.concat(F.lit("GRISG-BLPI-"),F.substring(F.col("BlanketSORefNbr"),3,6))))
display(df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Create BL_No_Map

# CELL ********************

df = df.withColumn("BL_No_Map",F.concat_ws("-",F.col("BlanketSORefNbr"),F.col("Item")))
display(df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. Create Segment_New

# CELL ********************

df = df.withColumn("Segment_New",F.when(F.col("Item")=="ST2","MH").when(F.col("Item")=="ST3","AG/CON").otherwise(F.lit(None)))
display(df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 9. Rename Order Number

# CELL ********************

df = df.withColumnRenamed("OrderNbr","ACU_OrderNbr")
display(df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Rename Column Names to Standard format

# CELL ********************

for old_name in df.columns:
    new_name = (
        old_name.replace(" ", "_")
                .replace("/", "_")
                .replace("-", "_")
    )
    if old_name != new_name:
        df = df.withColumnRenamed(old_name, new_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 10. Validate Schema

# CELL ********************

df.printSchema()
print(f"Row count: {df.count()}")
display(df.limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 11. Write to Silver

# CELL ********************

df.write.mode("overwrite").format("delta").saveAsTable("silver.silver_acu_pbi_sales_orders_with_bl_so")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
