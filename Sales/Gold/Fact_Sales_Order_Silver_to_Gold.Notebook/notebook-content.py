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

# # Fact_Sales_Order — Silver to Gold Transformation
# 
# **Source Table:** `silver.silver_og_value_all`
# 
# **Target Table:** `gold.fact_sales_order`
# 
# **Grain:** One row per Sales Order Line (`OrderNbr`,`LineNbr`,`InventoryID`).

# MARKDOWN ********************

# ## 1. Imports

# CELL ********************

from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Load Silver Table

# CELL ********************

df = spark.table("silver.silver_og_value_all")
display(df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Filter Valid Sales Order Lines
# Remove summary/SP records and retain only rows that have the required grain.

# CELL ********************

gold_df = (
    df
    .filter(F.col("OrderNbr").isNotNull())
    .filter(F.col("LineNbr").isNotNull())
    .filter(F.col("InventoryID").isNotNull())
)
print(gold_df.count())
display(gold_df.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Select Gold Fact Columns

# CELL ********************

gold_df = gold_df.select(
    F.col("OrderNbr"),
    F.col("LineNbr"),
    F.col("Date").alias("OrderDate"),
    F.col("PlannedShipDate"),
    F.col("WantedDeliveryDate"),
    F.col("CustomerID"),
    F.col("InventoryID"),
    F.col("SalespersonID"),
    F.col("Currency"),
    F.col("CountryName"),
    F.col("Quantity").alias("OrderQty"),
    F.col("Shipped").alias("ShippedQty"),
    F.col("RemainingQty"),
    F.col("UnshippedQty"),
    F.col("Weight").alias("OrderWeightKg"),
    F.col("RemainingWeightKg"),
    F.col("OrderTotal").alias("OrderValue"),
    F.col("USD_Value").alias("USDValue"),
    F.col("RemainingValueCustomerCurrency"),
    F.col("soline_unbilledAmt").alias("UnbilledAmount"),
    F.col("UnitPrice"),
    F.col("Status"),
    F.col("OrderType"),
    F.col("ItemType"),
    F.col("PONbr"),
    F.col("CustomerOrderNbr"),
    F.col("BlanketSORefNbr"),
    F.col("Source_System").alias("SourceSystem")
)
display(gold_df.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Remove Duplicate Sales Order Lines

# CELL ********************

gold_df = gold_df.dropDuplicates(
    ["OrderNbr","LineNbr","InventoryID"]
)

print(gold_df.count())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Validate Gold Schema

# CELL ********************

gold_df.printSchema()
display(gold_df.limit(20))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Write Gold Table

# CELL ********************

gold_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("gold.fact_sales_order")

print("Gold table created successfully.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
