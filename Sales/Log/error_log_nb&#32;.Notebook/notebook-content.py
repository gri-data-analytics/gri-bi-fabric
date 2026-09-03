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

# PARAMETERS CELL ********************

# ============================================================
# BASE PARAMETERS
# Pipeline overwrites these at runtime via 'Base parameters'
# DO NOT remove this cell — 'parameters' tag is required
# ============================================================
#pipeline_name       = ""
#activity_name       = ""
#source_table        = ""
#target_table        = ""
#load_type           = ""
#error_code          = ""
#error_message       = ""
#activity_start_time = ""
#activity_end_time   = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

# Create schema 'log' inside LH_Param_Demo if it doesn't exist
spark.sql("CREATE SCHEMA IF NOT EXISTS LH_Param_Demo.log")
print("✅ Schema 'LH_Param_Demo.log' ready.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

# ----------------------------------------------------------
# Derive transformation_type from activity_name
# ----------------------------------------------------------
if activity_name.strip().lower().startswith("gold"):
    transformation_type = "Silver to Gold"
elif activity_name.strip().lower().startswith("silver"):
    transformation_type = "Bronze to Silver"
else:
    transformation_type = "Source to Bronze"

# ----------------------------------------------------------
# Build DataFrame and write to error_log
# ----------------------------------------------------------
log_df = spark.createDataFrame(
    [(
        transformation_type,
        pipeline_name,
        activity_name,
        source_table,
        target_table,
        load_type,
        error_code,
        error_message,
        activity_start_time,
        activity_end_time
    )],
    [
        "transformation_type",
        "pipeline_name",
        "activity_name",
        "source_table",
        "target_table",
        "load_type",
        "error_code",
        "error_message",
        "activity_start_time",
        "activity_end_time"
    ]
)

log_df.show(truncate=False)

log_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("LH_Param_Demo.log.error_log")

print(f"❌ Error log written successfully for source_table = {source_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
