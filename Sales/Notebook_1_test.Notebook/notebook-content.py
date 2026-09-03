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

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 1: Read the table
df = spark.sql("SELECT * FROM LH_Param_Demo.gold.fact_invoice")

# Step 2: Write as a single CSV
df.coalesce(1).write.mode("overwrite").option("header", "true").csv("Files/fact_invoice_export")

# Step 3: Rename the part file to a clean name, directly under Files
import os
mount_path = "/lakehouse/default/Files/fact_invoice_export"
dst = "/lakehouse/default/Files/fact_invoice.csv"

for f in os.listdir(mount_path):
    if f.endswith(".csv"):
        os.rename(os.path.join(mount_path, f), dst)

print("Done. File is at Files/fact_invoice.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

df = spark.sql("SELECT * FROM gold.fact_invoice")

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM gold.fact_invoice")

output_path = "Files/fact_invoice_export"

df.coalesce(1) \
  .write \
  .mode("overwrite") \
  .option("header", "true") \
  .csv(output_path)

print("Export completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
