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

import os

# Search all Excel files under Files/Documents
base_path = "/lakehouse/default/Files/Documents"

excel_files = []
for root, dirs, files in os.walk(base_path):
    for file in files:
        if file.endswith(".xlsx") or file.endswith(".xls"):
            full_path = os.path.join(root, file)
            # Show relative path from Files/
            relative = full_path.replace("/lakehouse/default/Files/", "")
            excel_files.append((file, relative))

# Print all found Excel files
for name, path in sorted(excel_files):
    print(f"{name:60s} → {path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os

base_path = "/lakehouse/default/Files/Documents"

target_files = [
    "OG Correction",
    "Past5YearsSalesData_ST3",
    "missingCus",
    "MissingRSM",
    "Missing_RSM_All",
    "Past5YearsSalesData_All",
    "Morning Huddle BI Data Source",
    "BOM_Cost_Budget - Copy",
    "Region Part no",
    "Defects Categories"
]

results = {}

for root, dirs, files in os.walk(base_path):
    for f in files:
        file_stem = os.path.splitext(f)[0]  # filename without extension
        if file_stem in target_files:
            full_path = os.path.join(root, f)
            rel_path = full_path.replace("/lakehouse/default/Files/", "")
            results[file_stem] = rel_path

# Print in the same order as your target list, flagging anything not found
print(f"{'File':<35} | Relative Path")
print("-" * 100)
for name in target_files:
    if name in results:
        print(f"{name:<35} | {results[name]}")
    else:
        print(f"{name:<35} | ⚠️ NOT FOUND")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
