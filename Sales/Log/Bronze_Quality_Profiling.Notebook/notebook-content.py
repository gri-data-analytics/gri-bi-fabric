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

# # Bronze Layer Data Quality & Profiling Notebook
# **Source-agnostic | Auto-discovers tables | Re-runnable | Historical trending**
# 
# This notebook automatically discovers every table in your Bronze schema (Acumatica, Oracle IFS,
# SharePoint Online Lists, SharePoint OneDrive, or anything added later), profiles every column, and
# writes the results into two Delta tables under a `quality` schema:
# 
# | Output table | Grain | Purpose |
# |---|---|---|
# | `quality.table_quality_summary` | 1 row per table per run | row counts, duplicates, freshness, overall health |
# | `quality.column_quality_profile` | 1 row per column per table per run | nulls, distincts, min/max, patterns |
# 
# **You do not need to edit this notebook when new tables are added to Bronze.**
# You only touch the `SOURCE_PREFIX_MAP` config (Cell 2) the day you onboard a genuinely **new source
# system** (e.g. a 5th ERP) — new tables from existing sources are picked up automatically.
# 
# Schedule this notebook (Fabric pipeline / notebook schedule) to run after your Bronze load pipelines
# finish, and you get a continuously growing quality history you can build a Power BI report on top of.


# CELL ********************

# CELL 1 — Imports
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime, timezone
import uuid
import traceback

spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

# Some source systems (commonly Oracle IFS) store placeholder/default dates like
# 0001-01-01 or timestamps before 1900. Spark 3.x throws a SparkUpgradeException on
# these by default because the old Julian and modern Gregorian calendars disagree
# for such ancient values. These settings tell Spark to just read them as-is
# (no rebasing, no exception) instead of crashing the whole notebook.
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cell 2 — Configuration
# Everything you would ever need to change lives here. Nothing below this cell needs editing when
# new **tables** show up in Bronze — only when a brand-new **source system** is onboarded (add one
# line to `SOURCE_PREFIX_MAP`).


# CELL ********************

# CELL 2 — Configuration

# Schema (folder) in the Lakehouse that holds your raw Bronze tables
BRONZE_SCHEMA = "bronze"

# Schema where quality results will be written (created automatically if missing)
QUALITY_SCHEMA = "quality"

# Map table-name PREFIXES -> friendly source system name.
# This is the ONLY place you touch when a genuinely new source system is onboarded.
# Matching is case-insensitive and checks "startswith", based on the naming convention
# visible in your event_log (bronze_acu_..., bronze_ifs_..., bronze_od_...).
SOURCE_PREFIX_MAP = {
    "bronze_acu_":  "Acumatica",
    "bronze_ifs_":  "Oracle IFS",
    "bronze_od_":   "SharePoint / OneDrive",
    "bronze_sp_":   "SharePoint Online List",
}

# Tables that don't match any prefix above are still profiled — just tagged "Unmapped"
# so they show up clearly in your quality dashboard instead of being silently skipped.
DEFAULT_SOURCE_LABEL = "Unmapped (update SOURCE_PREFIX_MAP)"

# Column-name keyword -> (regex, human label) validity checks.
# Applied generically to any STRING column whose name CONTAINS the keyword — works the
# same way regardless of which source system the table came from.
PATTERN_RULES = {
    "email":     (r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$", "email_format"),
    "phone":     (r"^[0-9+()\-\s]{6,20}$",                             "phone_format"),
    "zip":       (r"^[0-9A-Za-z\-\s]{3,10}$",                          "postal_code_format"),
    "postcode":  (r"^[0-9A-Za-z\-\s]{3,10}$",                          "postal_code_format"),
    "url":       (r"^https?://",                                        "url_format"),
}

# Performance guard-rail: tables above this row count are profiled on a SAMPLE for
# column-level stats (distincts / min / max / mean). Row count & duplicate checks always
# run on the FULL table. Set to None to always profile full data.
LARGE_TABLE_ROW_THRESHOLD = 5_000_000
SAMPLE_FRACTION = 0.05
SAMPLE_SEED = 42

# A duplicate-row-rate above this % gets flagged in the summary table
DUPLICATE_ROW_ALERT_PCT = 1.0
# A null-rate above this % on any single column gets flagged
NULL_ALERT_PCT = 20.0


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cell 3 — Setup output schema + helper functions


# CELL ********************

# CELL 3 — Setup + helpers

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {QUALITY_SCHEMA}")

def list_bronze_tables(schema: str):
    """Auto-discover every table currently registered under the Bronze schema.
    New tables appear here automatically the moment they exist — nothing to configure."""
    rows = spark.sql(f"SHOW TABLES IN {schema}").collect()
    return [r["tableName"] for r in rows if not r["isTemporary"]]

def get_source_system(table_name: str) -> str:
    tname = table_name.lower()
    for prefix, source in SOURCE_PREFIX_MAP.items():
        if tname.startswith(prefix.lower()):
            return source
    return DEFAULT_SOURCE_LABEL

def get_delta_last_update(full_table_name: str):
    """Pulls the last commit timestamp/operation from Delta history -> freshness signal."""
    try:
        dt = DeltaTable.forName(spark, full_table_name)
        h = dt.history(1).select("timestamp", "operation", "operationMetrics").collect()[0]
        return h["timestamp"], h["operation"], h["operationMetrics"]
    except Exception:
        return None, None, None

def safe_count(df):
    try:
        return df.count()
    except Exception:
        return None


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cell 4 — Column-level profiling logic
# For every column, regardless of source system, this computes:
# - **Completeness**: null count / null %
# - **Uniqueness**: distinct count / distinct %, flags fully-null or constant columns
# - **Validity**: type-appropriate stats (numeric min/max/mean/stddev, string length/blanks,
#   date min/max) plus regex pattern checks for columns that look like emails/phones/zip/url


# CELL ********************

# CELL 4 — Column profiling

NUMERIC_TYPES = ("int", "bigint", "double", "float", "decimal", "short", "long", "tinyint", "smallint")

def profile_column(df, col_name, dtype, total_rows, is_sampled, sampled_rows):
    col = F.col(f"`{col_name}`")
    base = df.select(
        F.count(col).alias("non_null_count"),
        F.approx_count_distinct(col).alias("distinct_count"),
    ).collect()[0]

    non_null = base["non_null_count"] or 0
    distinct = base["distinct_count"] or 0
    ref_rows = sampled_rows if is_sampled else total_rows
    null_count = (ref_rows - non_null) if ref_rows else None
    null_pct = round((null_count / ref_rows) * 100, 2) if ref_rows else None
    distinct_pct = round((distinct / ref_rows) * 100, 2) if ref_rows else None

    result = {
        "column_name": col_name,
        "data_type": dtype,
        "null_count": null_count,
        "null_pct": null_pct,
        "distinct_count": distinct,
        "distinct_pct": distinct_pct,
        "min_value": None,
        "max_value": None,
        "mean_value": None,
        "stddev_value": None,
        "min_length": None,
        "max_length": None,
        "blank_string_count": None,
        "pattern_check_applied": None,
        "pattern_invalid_count": None,
        "is_fully_null": (null_count == ref_rows) if ref_rows else None,
        "is_constant_column": (distinct <= 1 and non_null > 0),
        "is_null_alert": (null_pct is not None and null_pct >= NULL_ALERT_PCT),
    }

    dtype_lower = dtype.lower()
    try:
        if any(t in dtype_lower for t in NUMERIC_TYPES):
            stats = df.select(
                F.min(col).alias("mn"), F.max(col).alias("mx"),
                F.mean(col).alias("avg"), F.stddev(col).alias("sd"),
            ).collect()[0]
            result["min_value"] = stats["mn"]
            result["max_value"] = stats["mx"]
            result["mean_value"] = round(stats["avg"], 4) if stats["avg"] is not None else None
            result["stddev_value"] = round(stats["sd"], 4) if stats["sd"] is not None else None

        elif "date" in dtype_lower or "timestamp" in dtype_lower:
            stats = df.select(F.min(col).alias("mn"), F.max(col).alias("mx")).collect()[0]
            result["min_value"] = str(stats["mn"]) if stats["mn"] is not None else None
            result["max_value"] = str(stats["mx"]) if stats["mx"] is not None else None

        elif "string" in dtype_lower:
            stats = df.select(
                F.min(F.length(col)).alias("minlen"),
                F.max(F.length(col)).alias("maxlen"),
                F.sum(F.when(F.trim(col) == "", 1).otherwise(0)).alias("blanks"),
            ).collect()[0]
            result["min_length"] = stats["minlen"]
            result["max_length"] = stats["maxlen"]
            result["blank_string_count"] = stats["blanks"]

            col_lower = col_name.lower()
            for keyword, (regex, label) in PATTERN_RULES.items():
                if keyword in col_lower:
                    invalid = df.filter(col.isNotNull() & (~col.rlike(regex))).count()
                    result["pattern_check_applied"] = label
                    result["pattern_invalid_count"] = invalid
                    break
    except Exception as e:
        result["profiling_error"] = str(e)[:500]

    return result


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cell 5 — Table-level profiling logic
# Row count, duplicate rows, schema shape, freshness (from Delta history), and roll-up of the
# column profiles into one health record per table.


# CELL ********************

# CELL 5 — Table profiling

def profile_table(table_name, run_id, run_ts):
    full_name = f"{BRONZE_SCHEMA}.{table_name}"
    source_system = get_source_system(table_name)

    try:
        df = spark.table(full_name)
    except Exception as e:
        return {
            "table_name": table_name, "source_system": source_system,
            "row_count": None, "column_count": None,
            "duplicate_row_count": None, "duplicate_row_pct": None,
            "avg_null_pct": None, "fully_null_column_count": None,
            "constant_column_count": None, "is_sampled": False,
            "last_delta_operation": None, "last_delta_update_ts": None,
            "read_error": str(e)[:500],
            "duplicate_row_alert": False,
            "run_id": run_id, "run_timestamp": run_ts,
        }, []

    # Everything below can fail on a single bad table (e.g. ancient/malformed dates,
    # unhashable nested columns from SharePoint list exports, etc). Wrapping it means
    # ONE bad table logs an error row instead of killing the entire run.
    try:
        total_rows = safe_count(df)

        dup_rows = None
        try:
            if total_rows:
                distinct_rows = df.dropDuplicates().count()
                dup_rows = total_rows - distinct_rows
        except Exception as e:
            dup_rows = None
            dup_error = str(e)[:300]
        else:
            dup_error = None

        is_sampled = bool(LARGE_TABLE_ROW_THRESHOLD and total_rows and total_rows > LARGE_TABLE_ROW_THRESHOLD)
        profiling_df = df.sample(fraction=SAMPLE_FRACTION, seed=SAMPLE_SEED) if is_sampled else df
        if is_sampled:
            profiling_df = profiling_df.cache()
        sampled_rows = profiling_df.count() if is_sampled else total_rows

        last_ts, last_op, _ = get_delta_last_update(full_name)

        col_rows = []
        for field in df.schema.fields:
            try:
                info = profile_column(profiling_df, field.name, field.dataType.simpleString(),
                                       total_rows, is_sampled, sampled_rows)
            except Exception as e:
                info = {
                    "column_name": field.name, "data_type": field.dataType.simpleString(),
                    "null_count": None, "null_pct": None, "distinct_count": None, "distinct_pct": None,
                    "min_value": None, "max_value": None, "mean_value": None, "stddev_value": None,
                    "min_length": None, "max_length": None, "blank_string_count": None,
                    "pattern_check_applied": None, "pattern_invalid_count": None,
                    "is_fully_null": None, "is_constant_column": None, "is_null_alert": False,
                    "profiling_error": str(e)[:500],
                }
            info.update({
                "table_name": table_name, "source_system": source_system,
                "is_sampled": is_sampled, "run_id": run_id, "run_timestamp": run_ts,
            })
            col_rows.append(info)

        if is_sampled:
            profiling_df.unpersist()

        null_pcts = [c["null_pct"] for c in col_rows if c.get("null_pct") is not None]
        avg_null_pct = round(sum(null_pcts) / len(null_pcts), 2) if null_pcts else None
        fully_null_cols = sum(1 for c in col_rows if c.get("is_fully_null"))
        constant_cols = sum(1 for c in col_rows if c.get("is_constant_column"))
        dup_pct = round((dup_rows / total_rows) * 100, 2) if (dup_rows is not None and total_rows) else None

        summary_row = {
            "table_name": table_name,
            "source_system": source_system,
            "row_count": total_rows,
            "column_count": len(df.schema.fields),
            "duplicate_row_count": dup_rows,
            "duplicate_row_pct": dup_pct,
            "avg_null_pct": avg_null_pct,
            "fully_null_column_count": fully_null_cols,
            "constant_column_count": constant_cols,
            "is_sampled": is_sampled,
            "last_delta_operation": last_op,
            "last_delta_update_ts": last_ts.isoformat() if last_ts else None,
            "read_error": dup_error,
            "duplicate_row_alert": (dup_pct is not None and dup_pct >= DUPLICATE_ROW_ALERT_PCT),
            "run_id": run_id,
            "run_timestamp": run_ts,
        }
        return summary_row, col_rows

    except Exception as e:
        # Table-level catch-all: one bad table becomes one error row, run continues.
        err = f"{type(e).__name__}: {str(e)[:400]}"
        summary_row = {
            "table_name": table_name, "source_system": source_system,
            "row_count": None, "column_count": None,
            "duplicate_row_count": None, "duplicate_row_pct": None,
            "avg_null_pct": None, "fully_null_column_count": None,
            "constant_column_count": None, "is_sampled": False,
            "last_delta_operation": None, "last_delta_update_ts": None,
            "read_error": err,
            "duplicate_row_alert": False,
            "run_id": run_id, "run_timestamp": run_ts,
        }
        return summary_row, []


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cell 6 — Run across every table in Bronze
# This is the loop that makes the notebook reusable: it iterates over **whatever tables currently
# exist** in the Bronze schema. Add a new table to Bronze, re-run this notebook, and it's included —
# zero code changes.


# CELL ********************

# CELL 6 — Run

run_id = str(uuid.uuid4())
run_ts = datetime.now(timezone.utc)

bronze_tables = list_bronze_tables(BRONZE_SCHEMA)
print(f"Discovered {len(bronze_tables)} tables in schema \'{BRONZE_SCHEMA}\':")
for t in bronze_tables:
    print(" -", t)

table_summary_rows = []
column_profile_rows = []

for tbl in bronze_tables:
    print(f"Profiling {tbl} ...")
    try:
        summary_row, col_rows = profile_table(tbl, run_id, run_ts)
    except Exception as e:
        # Should rarely hit this since profile_table already catches internally,
        # but this guarantees the loop NEVER dies on a single table.
        print(f"  !! Unexpected failure on {tbl}: {e}")
        summary_row = {
            "table_name": tbl, "source_system": get_source_system(tbl),
            "row_count": None, "column_count": None,
            "duplicate_row_count": None, "duplicate_row_pct": None,
            "avg_null_pct": None, "fully_null_column_count": None,
            "constant_column_count": None, "is_sampled": False,
            "last_delta_operation": None, "last_delta_update_ts": None,
            "read_error": f"{type(e).__name__}: {str(e)[:400]}",
            "duplicate_row_alert": False,
            "run_id": run_id, "run_timestamp": run_ts,
        }
        col_rows = []

    if summary_row:
        table_summary_rows.append(summary_row)
    column_profile_rows.extend(col_rows)

failed = [r["table_name"] for r in table_summary_rows if r.get("read_error")]
print(f"Done. {len(table_summary_rows)} tables profiled, {len(column_profile_rows)} column records generated.")
if failed:
    print(f"{len(failed)} table(s) hit an error (see read_error column in the summary table): {failed}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Quick in-memory fix — avoids re-running the 17-min profiling loop
for r in column_profile_rows:
    if r.get("mean_value") is not None:
        r["mean_value"] = float(r["mean_value"])
    if r.get("stddev_value") is not None:
        r["stddev_value"] = float(r["stddev_value"])
print("Fixed", len(column_profile_rows), "column records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cell 7 — Persist results to Delta (history builds up over time)
# Written in **append** mode with `mergeSchema` on, so if you extend the profiling logic later
# (e.g. add a new check), old history isn't broken.


# CELL ********************

# CELL 7 — Write results

table_summary_schema = StructType([
    StructField("table_name", StringType()),
    StructField("source_system", StringType()),
    StructField("row_count", LongType()),
    StructField("column_count", IntegerType()),
    StructField("duplicate_row_count", LongType()),
    StructField("duplicate_row_pct", DoubleType()),
    StructField("avg_null_pct", DoubleType()),
    StructField("fully_null_column_count", IntegerType()),
    StructField("constant_column_count", IntegerType()),
    StructField("is_sampled", BooleanType()),
    StructField("last_delta_operation", StringType()),
    StructField("last_delta_update_ts", StringType()),
    StructField("read_error", StringType()),
    StructField("duplicate_row_alert", BooleanType()),
    StructField("run_id", StringType()),
    StructField("run_timestamp", TimestampType()),
])

column_profile_schema = StructType([
    StructField("table_name", StringType()),
    StructField("source_system", StringType()),
    StructField("column_name", StringType()),
    StructField("data_type", StringType()),
    StructField("null_count", LongType()),
    StructField("null_pct", DoubleType()),
    StructField("distinct_count", LongType()),
    StructField("distinct_pct", DoubleType()),
    StructField("min_value", StringType()),
    StructField("max_value", StringType()),
    StructField("mean_value", DoubleType()),
    StructField("stddev_value", DoubleType()),
    StructField("min_length", IntegerType()),
    StructField("max_length", IntegerType()),
    StructField("blank_string_count", LongType()),
    StructField("pattern_check_applied", StringType()),
    StructField("pattern_invalid_count", LongType()),
    StructField("is_fully_null", BooleanType()),
    StructField("is_constant_column", BooleanType()),
    StructField("is_null_alert", BooleanType()),
    StructField("is_sampled", BooleanType()),
    StructField("profiling_error", StringType()),
    StructField("run_id", StringType()),
    StructField("run_timestamp", TimestampType()),
])

def to_df_safe(rows, schema):
    # cast numeric-looking min/max to string for mixed-type columns so union across
    # heterogeneous source columns (numeric vs string vs date) never breaks the write
    clean_rows = []
    for r in rows:
        rr = dict(r)
        if "min_value" in rr and rr["min_value"] is not None:
            rr["min_value"] = str(rr["min_value"])
        if "max_value" in rr and rr["max_value"] is not None:
            rr["max_value"] = str(rr["max_value"])
        clean_rows.append(rr)
    return spark.createDataFrame(clean_rows, schema=schema)

table_summary_df = to_df_safe(table_summary_rows, table_summary_schema)
column_profile_df = to_df_safe(column_profile_rows, column_profile_schema)

(table_summary_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(f"{QUALITY_SCHEMA}.table_quality_summary"))

(column_profile_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(f"{QUALITY_SCHEMA}.column_quality_profile"))

print(f"Written to {QUALITY_SCHEMA}.table_quality_summary and {QUALITY_SCHEMA}.column_quality_profile (run_id={run_id})")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cell 8 — Quick look at this run's results
# Table-level scorecard first, then any columns that tripped an alert threshold.


# CELL ********************

# CELL 8 — Review this run

display(
    spark.table(f"{QUALITY_SCHEMA}.table_quality_summary")
    .filter(F.col("run_id") == run_id)
    .orderBy(F.col("duplicate_row_alert").desc(), F.col("avg_null_pct").desc())
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 8b — Columns that failed a quality threshold this run

display(
    spark.table(f"{QUALITY_SCHEMA}.column_quality_profile")
    .filter(F.col("run_id") == run_id)
    .filter(
        (F.col("is_null_alert") == True) |
        (F.col("is_fully_null") == True) |
        (F.col("is_constant_column") == True) |
        ((F.col("pattern_invalid_count").isNotNull()) & (F.col("pattern_invalid_count") > 0))
    )
    .orderBy(F.col("source_system"), F.col("table_name"), F.col("null_pct").desc())
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## What this covers, and how to extend it
# 
# **Dimensions checked per table**
# - Row count, column count, duplicate full-row count/%
# - Freshness — last Delta commit timestamp & operation (WRITE/MERGE/OVERWRITE)
# - Roll-up health signals: average null %, count of fully-null columns, count of constant columns
# 
# **Dimensions checked per column**
# - Completeness: null count/%
# - Uniqueness: distinct count/%, fully-null flag, constant-value flag
# - Validity: numeric min/max/mean/stddev, string min/max length + blank count, date min/max,
#   regex pattern validity for columns that look like email/phone/zip/url
# - All alerts are threshold-driven from Cell 2, so tuning sensitivity doesn't require touching logic
# 
# **Why it's source-agnostic**
# - Tables are discovered live from `SHOW TABLES IN bronze` — nothing hardcoded per table
# - Source system is inferred from a prefix→name dictionary, not per-table logic
# - Column checks run off each column's Spark data type + name pattern, not source-specific rules
# - Adding a new **table** from Acumatica/IFS/SharePoint/OneDrive needs zero changes
# - Adding a genuinely new **source system** needs one line in `SOURCE_PREFIX_MAP`
# 
# **Natural next steps**
# - Point a Power BI report at `quality.table_quality_summary` / `quality.column_quality_profile`
#   for a trending quality dashboard across runs
# - Schedule this notebook right after your Bronze load pipeline in a Fabric pipeline
# - If you have known primary keys per table, add an optional `PRIMARY_KEY_MAP = {"table": ["col1","col2"]}`
#   config and a duplicate-by-key check alongside the existing full-row duplicate check
# - If you want referential integrity checks (e.g. orders → customers), add a small `FK_CHECKS` config
#   list and a generic anti-join count function — same pattern as the pattern-rule checks above

