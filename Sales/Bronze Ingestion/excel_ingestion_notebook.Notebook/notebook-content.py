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

# # 📊 Excel Ingestion Notebook — SharePoint OneDrive
# **Purpose:** Called by `pipeline_SharePointOnedrive` ForEach activity.
# Reads one Excel file from Lakehouse Files shortcut and writes to Bronze Delta table.
# 
# ### Base Parameters passed from Pipeline
# | Parameter | Pipeline Expression |
# |---|---|
# | `pipeline_name` | `@pipeline().DisplayName` |
# | `source_file_path` | `@item().source_connection` |
# | `source_table` | `@item().source_table` |
# | `target_table` | `@item().target_table` |
# | `load_type` | `@item().load_type` |

# PARAMETERS CELL ********************

# ============================================================
# BASE PARAMETERS
# Pipeline overwrites these at runtime via 'Base parameters'
# DO NOT remove this cell — 'parameters' tag is required
# ============================================================
#pipeline_name    = ""
#source_file_path = ""
#source_table     = ""
#target_table     = ""
#load_type        = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 1 — Imports
# ============================================================
from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession.builder.getOrCreate()
print(f"✅ Spark session ready.")
print(f"   pipeline_name    : {pipeline_name}")
print(f"   source_file_path : {source_file_path}")
print(f"   source_table     : {source_table}")
print(f"   target_table     : {target_table}")
print(f"   load_type        : {load_type}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 2 — Read Excel File from Lakehouse Files
# ============================================================

# Full path to file in Lakehouse
full_path = f"/lakehouse/default/Files/{source_file_path}"
print(f"📂 Reading file from: {full_path}")

# Read with pandas first (handles Excel format)
pdf = pd.read_excel(full_path, engine='openpyxl')

# Clean column names — remove spaces and special characters
pdf.columns = [
    col.strip()
       .replace(' ', '_')
       .replace('-', '_')
       .replace('(', '')
       .replace(')', '')
       .replace('/', '_')
       .replace('.', '_')
    for col in pdf.columns
]

print(f"✅ File read successfully!")
print(f"   Rows    : {len(pdf)}")
print(f"   Columns : {list(pdf.columns)}")

# Convert to Spark DataFrame
df = spark.createDataFrame(pdf)
df.printSchema()
df.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 3 — Write to Bronze Delta Table
# ============================================================
import json

# Full target table path
full_target = f"LH_Param_Demo.bronze.{target_table}"

# Write mode based on load_type
write_mode = "overwrite" if load_type.upper() == "OVERWRITE" else "append"

print(f"📝 Writing to: {full_target} | mode: {write_mode}")

df.write \
  .format("delta") \
  .mode(write_mode) \
  .option("overwriteSchema", "true") \
  .saveAsTable(full_target)

read_count  = df.count()
write_count = df.count()

print(f"✅ Successfully written!")
print(f"   source_table : {source_table}")
print(f"   target_table : {full_target}")
print(f"   rows written : {write_count}")

# Make counts available for event log as a JSON object
exit_payload = json.dumps({
    "read_count": read_count,
    "write_count": write_count
})

mssparkutils.notebook.exit(exit_payload)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 3 — Write to Bronze Delta Table
# ============================================================

# Full target table path
full_target = f"LH_Param_Demo.bronze.{target_table}"

# Write mode based on load_type
write_mode = "overwrite" if load_type.upper() == "OVERWRITE" else "append"

print(f"📝 Writing to: {full_target} | mode: {write_mode}")

df.write \
  .format("delta") \
  .mode(write_mode) \
  .option("overwriteSchema", "true") \
  .saveAsTable(full_target)

read_count  = df.count()
write_count = df.count()

print(f"✅ Successfully written!")
print(f"   source_table : {source_table}")
print(f"   target_table : {full_target}")
print(f"   rows written : {write_count}")

# Make counts available for event log
mssparkutils.notebook.exit(str(write_count))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
