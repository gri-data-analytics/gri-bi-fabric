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

# # Non Ext RSMs — Silver Combine Transformation
# 
# **Source tables:**
# - `silver.silver_acu_st3_nonexrsm_2025` (ST3_NonExRSM 2025)
# - `silver.silver_acu_st1_nonexrsm_2025` (ST1_NonExRSM 2025)
# - `silver.silver_ifs_rsm_2025` (RSM(IFS) 2025) — **⏸ ACCESS PENDING, commented out below**
# 
# **Target table:** `silver.silver_non_ext_rsms`
# 
# This notebook replicates the Power Query M logic that combines (unions) three RSM mapping sources, then standardizes the customer ID column and removes duplicates.
# 
# > **⏸ Access pending:** you don't yet have access to the `RSM(IFS) 2025` source table, so all code referencing `df_ifs_rsm` is commented out below. The notebook currently runs end-to-end using only the two ST3/ST1 NonExRSM 2025 tables. **Once you gain access to the IFS table, uncomment the three marked blocks** (in cells 2, 3, and note in cell 3's union) to bring it into the combine, then re-run the whole notebook. Everything else stays the same either way.


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

# ## 2. Load and Inspect the Source Tables
# Equivalent to the `Source` step's inputs. Load each silver table individually and compare schemas before combining, to catch any column-naming mismatches early.
# 
# **⏸ `RSM(IFS) 2025` is commented out below — uncomment once you have access.**

# CELL ********************

df_st3_nonexrsm = spark.table("silver.silver_acu_st3_nonexrsm_2025")
df_st1_nonexrsm = spark.table("silver.silver_acu_st1_nonexrsm_2025")

# --- UNCOMMENT ONCE YOU HAVE ACCESS TO RSM(IFS) 2025 ---
# df_ifs_rsm = spark.table("silver.silver_ifs_rsm_2025")  # confirm this table name

print("--- ST3_NonExRSM 2025 schema ---")
df_st3_nonexrsm.printSchema()
print("--- ST1_NonExRSM 2025 schema ---")
df_st1_nonexrsm.printSchema()

# print("--- RSM(IFS) 2025 schema ---")
# df_ifs_rsm.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Combine (Union) the Source Tables
# Equivalent to the **Source = Table.Combine(...)** step: stacks the tables into one, aligning by column name and filling missing columns with `null` where a source doesn't have them.
# 
# **⏸ Currently unions only the two available Acu tables.** The line that adds `RSM(IFS) 2025` into the union is commented out — uncomment it once `df_ifs_rsm` is available (from cell 2) to complete the three-way combine.

# CELL ********************

df = df_st3_nonexrsm.unionByName(df_st1_nonexrsm, allowMissingColumns=True)

# --- UNCOMMENT ONCE YOU HAVE ACCESS TO RSM(IFS) 2025 ---
# df = df.unionByName(df_ifs_rsm, allowMissingColumns=True)

display(df.limit(20))
print(f"Combined row count: {df.count()}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Rename `Customer` to `Customer_ID`
# Equivalent to the **Renamed columns** step. Note: the ST3/ST1 NonExRSM 2025 tables were already built with a `Customer` column (not `Customer_ID`) in our earlier notebooks, so this rename should apply cleanly. Once `RSM(IFS) 2025` is added back into the union in cell 3, verify its source column is also named `Customer` (check the schema printout in cell 2) — if it's named differently, this rename step will need to be adjusted.

# CELL ********************

df = df.withColumnRenamed("Customer", "Customer_ID")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Remove Duplicates on `Customer_ID`
# Equivalent to the **Removed duplicates** step: keeps only the first occurrence of each distinct `Customer_ID`.

# CELL ********************

df = df.dropDuplicates(["Customer_ID"])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Preview Final Result
# Quick sanity check of the transformed dataframe before writing it out. **Note: while `RSM(IFS) 2025` is excluded, this preview and any downstream write reflects only the two Acu sources — it is a partial/incomplete result until the IFS table is added back in.**

# CELL ********************

display(df.limit(20))
print(f"Row count: {df.count()}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Write to Silver Schema
# Persist the combined table as a managed Delta table in the **silver** schema, overwriting any previous version.
# 
# **⚠️ Recommendation:** since this currently excludes `RSM(IFS) 2025`, consider holding off on running this final write cell until you have access to that table and have uncommented cells 2 and 3 above — otherwise `silver_non_ext_rsms` will be written as an incomplete/partial table.

# CELL ********************

df.write.mode("overwrite").format("delta").saveAsTable("silver.silver_non_ext_rsms")
print("Write complete: silver.silver_non_ext_rsms")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
