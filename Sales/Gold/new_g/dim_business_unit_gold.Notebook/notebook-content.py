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

# # dim_business_unit — Gold Dimension Table
# 
# **Source:** static reference/lookup data (not derived from an existing bronze/silver table)  
# **Target table:** `gold.dim_business_unit`
# 
# This notebook recreates a small manually-maintained lookup table that maps short `BU` codes to their full descriptive names, and saves it as a dimension table in the **gold** schema.
# 
# > Since this is static reference data rather than a transformation of an existing source table, it's defined directly in code below. If this mapping ever needs to grow (e.g. more business units added), just extend the list in cell 2.


# MARKDOWN ********************

# ## 1. Imports
# Load the PySpark functions and types needed to build this small reference table.

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Define the Business Unit Lookup Data
# Equivalent to the manually-entered table shown in the screenshot: maps each `BU` code to its full business unit name.

# CELL ********************

schema = StructType([
    StructField("Business_Unit", StringType(), True),
    StructField("Business_Unit_Name", StringType(), True),
])

data = [
    ("ST1", "SOLID"),
    ("ST3", "PNEUMATIC"),
]

df = spark.createDataFrame(data, schema=schema)
display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Preview Final Result
# Quick sanity check of the dimension table before writing it out.

# CELL ********************

display(df)
print(f"Row count: {df.count()}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Write to Gold Schema
# Persist the dimension table as a managed Delta table in the **gold** schema, overwriting any previous version.

# CELL ********************

df.write.mode("overwrite").format("delta").saveAsTable("gold.dim_business_unit")
print("Write complete: gold.dim_business_unit")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
