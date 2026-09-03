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

# # Customer_All — Silver Combine Transformation
# 
# **Source tables:**
# - `silver.silver_acu_pbi_customer` (PBI Customer(Acu))
# - `silver.silver_acu_st1_nonexcus` (ST1_NonExCus)
# - `silver.silver_acu_st3_nonexcus` (ST3_NonExCus)
# - `silver.silver_ifs_customer` (Customer(IFS))
# - `silver.silver_ifs_missing_cus_all` (Missing_Cus_All)
# - `silver.silver_non_ext_rsms` (Non Ext RSMs) — used later for a lookup join
# 
# **Target table:** `silver.silver_customer_all`
# 
# This notebook replicates, step by step, the full Power Query M logic for **Customer_All** — combining five customer sources, standardizing region/segment labels, then joining in RSM assignments from the Non Ext RSMs lookup table.
# 
# > **⚠️ Dependency on a partial table:** `silver_non_ext_rsms` currently excludes the `RSM(IFS) 2025` source, since you don't have access to it yet (see the Non_Ext_RSMs notebook). That means the RSM lookup performed in steps 14-16 below will be missing any RSM assignments that would have come from IFS customers. Once you gain access and re-run the Non_Ext_RSMs notebook with the IFS table included, **re-run this notebook too** so the RSM join picks up the complete lookup data.


# MARKDOWN ********************

# ## 1. Imports
# Load the PySpark functions and types needed for this transformation.

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import StringType


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Load and Inspect the Five Source Tables
# Equivalent to the `Source` step's inputs. Load each silver table individually and compare schemas before combining, to catch any column-naming mismatches early.

# CELL ********************

df_pbi_cus     = spark.table("silver.silver_acu_pbi_customer")
df_st1_nonex   = spark.table("silver.silver_acu_st1_nonexcus")
df_st3_nonex   = spark.table("silver.silver_acu_st3_nonexcus")
df_ifs_cus     = spark.table("silver.silver_ifs_customer")
df_missing_cus = spark.table("silver.silver_ifs_missing_cus_all")

print("--- PBI Customer(Acu) schema ---")
df_pbi_cus.printSchema()
print("--- ST1_NonExCus schema ---")
df_st1_nonex.printSchema()
print("--- ST3_NonExCus schema ---")
df_st3_nonex.printSchema()
print("--- Customer(IFS) schema ---")
df_ifs_cus.printSchema()
print("--- Missing_Cus_All schema ---")
df_missing_cus.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Combine (Union) the Five Tables
# Equivalent to the **Source = Table.Combine(...)** step: stacks all five tables into one, aligning by column name and filling missing columns with `null` where a source doesn't have them.

# CELL ********************

df = df_pbi_cus.unionByName(df_st1_nonex, allowMissingColumns=True)
df = df.unionByName(df_st3_nonex, allowMissingColumns=True)
df = df.unionByName(df_ifs_cus, allowMissingColumns=True)
df = df.unionByName(df_missing_cus, allowMissingColumns=True)

display(df.limit(20))
print(f"Combined row count: {df.count()}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Remove Unneeded Columns
# Equivalent to the **Removed columns** step: drops a long list of columns not needed downstream. Using `.drop()` here since it's safe to call even if a column doesn't exist in the combined schema — several of these columns likely only exist in a subset of the five source tables.

# CELL ********************

df = df.drop(
    "ParentAccount", "CountryC", "City", "CurrencyID", "TermDescription", "TermID",
    "CustomerStatus", "CreatedOn", "LastModifiedOn", "ShippingTerms", "Email", "SubRegion",
    "CustomerPartIDAvailable", "AddressID", "AccountID", "LocationID", "ContactID",
    "SalespersonID", "ActivationStatus", "SalesPersonID_2", "Customer", "Location",
    "BU", "Refid"
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# ## 5. Standardize `Region` Values
# Equivalent to the six **Replaced value** steps that normalize inconsistent `Region` labels across the combined sources into consistent uppercase categories:
# - `MIDDLE EAST, AFRICA & SOUTH AMERICA` → `MEA & SA`
# - `Europe` → `EUROPE`
# - `MEA & S.America` → `MEA & SA`
# - `North America` → `NORTH AMERICA`
# - `north America` → `NORTH AMERICA`
# - `PB` → `PRIVATE BRAND`
# - `APAC` → `ASIA PACIFIC`

# CELL ********************

region_replacements = {
    "MIDDLE EAST, AFRICA & SOUTH AMERICA": "MEA & SA",
    "Europe": "EUROPE",
    "MEA & S.America": "MEA & SA",
    "North America": "NORTH AMERICA",
    "north America": "NORTH AMERICA",
    "PB": "PRIVATE BRAND",
    "APAC": "ASIA PACIFIC",
}

region_expr = F.col("Region")
for old_value, new_value in region_replacements.items():
    region_expr = F.when(F.col("Region") == old_value, F.lit(new_value)).otherwise(region_expr)

df = df.withColumn("Region", region_expr)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Standardize `Customer_Segment` Values
# Equivalent to the two **Replaced value** steps that consolidate segment labels:
# - `Replacement Market` → `AM`
# - `Institutional Sales` → `AM`

# CELL ********************

df = df.withColumn(
    "Customer_Segment",
    F.when(F.col("Customer_Segment").isin("Replacement Market", "Institutional Sales"), F.lit("AM"))
     .otherwise(F.col("Customer_Segment"))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Change Data Type of `Export_Category`
# Equivalent to the **Transform columns** step: ensures `Export_Category` is stored as a string/text type.

# CELL ********************

df = df.withColumn("Export_Category", F.col("Export_Category").cast(StringType()))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. Replace Error Values in `Export_Category`
# Equivalent to the **Replace errors** step. Spark's `.cast()` already returns `null` on failure rather than throwing, so this step is a no-op here, included only for parity with the original query.

# CELL ********************

# No-op: Spark's cast() already returns null on conversion failure.


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 9. Remove Full-Row Duplicates
# Equivalent to the **Removed duplicates** step (`Table.Distinct` with no column list checks every column): removes any rows that are entirely identical across all columns.

# CELL ********************

df = df.dropDuplicates()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 10. Add Column `Name`
# Equivalent to the **Added custom** step: creates a `Name` column that duplicates `Customer_Name`.

# CELL ********************

df = df.withColumn("Name", F.col("Customer_Name"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 11. Change Data Type of `Name`
# Equivalent to the **Transform columns 1** step: ensures `Name` is stored as a string/text type.

# CELL ********************

df = df.withColumn("Name", F.col("Name").cast(StringType()))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 12. Replace Error Values in `Name`
# Equivalent to the **Replace errors 1** step — no-op in Spark, included for parity.

# CELL ********************

# No-op: Spark's cast() already returns null on conversion failure.


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 13. Replace `"MIDDLE EAST & AFRICA"` with `"MEA"` in `Region`
# Equivalent to **Replaced value 9**. Note this runs *after* step 5 already mapped a similarly-worded value (`MIDDLE EAST, AFRICA & SOUTH AMERICA` → `MEA & SA`) — this is a distinct, shorter phrase (`MIDDLE EAST & AFRICA`, no South America) mapping to a different target (`MEA`, not `MEA & SA`).

# CELL ********************

df = df.withColumn(
    "Region",
    F.when(F.col("Region") == "MIDDLE EAST & AFRICA", F.lit("MEA")).otherwise(F.col("Region"))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 14. Left-Join RSM Lookup Data
# Equivalent to the **Merged queries** step: performs a left outer join against the `Non Ext RSMs` table on `Customer_ID`, bringing in the `RSM` assignment where available.
# 
# > **⚠️ See the notebook header note:** `silver_non_ext_rsms` currently excludes IFS RSM data pending access. Rows that would have matched an IFS-sourced RSM assignment will come back with `null` for `RSM_lookup` until that table is completed and this notebook is re-run.

# CELL ********************

df_rsm_lookup = spark.table("silver.silver_non_ext_rsms").select(
    F.col("Customer_ID"),
    F.col("RSM").alias("RSM_lookup")
)

df = df.join(df_rsm_lookup, on="Customer_ID", how="left")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 15. Resolve Conditional `RSM` Value
# Equivalent to the **Expanded Non Ext RSMs** + **Inserted conditional column** steps combined: picks the existing `RSM` value if present and non-blank; otherwise falls back to the joined `RSM_lookup` value; otherwise defaults to `"No RSM"`.
# 
# `Custom = if RSM is null/blank then (if RSM_lookup is null/blank then "No RSM" else RSM_lookup) else RSM`
# 
# > **Note:** the original M query's source table for this combine doesn't appear to carry a pre-existing `RSM` column before the merge (based on the earlier per-source notebooks), so in practice this likely always falls through to the `RSM_lookup`/`"No RSM"` branch. The logic is preserved exactly as written for parity — if your combined `df` doesn't have an `RSM` column at this point, this cell will error; in that case let me know and I'll simplify it to just use `RSM_lookup` with a `"No RSM"` fallback.

# CELL ********************

has_rsm = F.col("RSM").isNotNull() & (F.col("RSM") != "")
has_rsm_lookup = F.col("RSM_lookup").isNotNull() & (F.col("RSM_lookup") != "")

df = df.withColumn(
    "Custom",
    F.when(has_rsm, F.col("RSM"))
     .otherwise(F.when(has_rsm_lookup, F.col("RSM_lookup")).otherwise(F.lit("No RSM")))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 16. Remove Intermediate RSM Columns
# Equivalent to the **Removed columns 1** step: drops the original `RSM` and joined `RSM_lookup` columns now that `Custom` holds the resolved value.

# CELL ********************

df = df.drop("RSM", "RSM_lookup")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 17. Change Data Type of `Custom`
# Equivalent to the **Changed column type** step: ensures `Custom` is stored as a string/text type.

# CELL ********************

df = df.withColumn("Custom", F.col("Custom").cast(StringType()))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 18. Rename `Custom` to `RSM`
# Equivalent to the **Renamed columns** step.

# CELL ********************

df = df.withColumnRenamed("Custom", "RSM")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 19. Remove Duplicates on `Customer_ID`
# Equivalent to the final **Removed duplicates 1** step: keeps only the first occurrence of each distinct `Customer_ID`.

# CELL ********************

df = df.dropDuplicates(["Customer_ID"])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 20. Preview Final Result
# Quick sanity check of the transformed dataframe before writing it out.

# CELL ********************

display(df.limit(20))
print(f"Row count: {df.count()}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 21. Write to Silver Schema
# Persist the final combined table as a managed Delta table in the **silver** schema, overwriting any previous version.
# 
# **⚠️ Reminder:** this reflects a partial RSM lookup (IFS access pending) — consider re-running this notebook once `silver_non_ext_rsms` is complete.

# CELL ********************

df.write.mode("overwrite").format("delta").saveAsTable("silver.silver_customer_all")
print("Write complete: silver.silver_customer_all")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
