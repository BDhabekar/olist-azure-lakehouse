# Databricks notebook source
# MAGIC %md
# MAGIC # nb_bronze_load — the files Data Factory landed become bronze Delta tables
# MAGIC
# MAGIC **What this notebook does.** For every table in the list it is given: `COPY INTO` the bronze Delta table from the folder where Data Factory wrote that table's Parquet files. `COPY INTO` remembers which files it has loaded, so a second run loads nothing. Each row keeps where it came from (`_source_file`, `_source_file_time`) and when it was loaded (`_loaded_at`). One audit row per table per run is written to `bronze.load_audit`.
# MAGIC
# MAGIC **Bronze is "as received".** The tables take their columns from the files; nothing is declared, renamed or filtered here. That is silver's job.
# MAGIC
# MAGIC **Parameters** (the job passes them; by hand, type them into the boxes at the top):
# MAGIC
# MAGIC | Parameter | Meaning |
# MAGIC |---|---|
# MAGIC | `p_catalog` | the Unity Catalog catalog that holds the schemas `bronze`, `silver`, `gold` |
# MAGIC | `p_storage_account` | the ADLS Gen2 storage account that holds the containers |
# MAGIC | `p_tables` | the list of tables as JSON — the rows of the control table `etl.ingest_config`, handed over by Data Factory. This notebook uses `table_name` |
# MAGIC | `p_run_id` | the pipeline's run id, written to the audit rows. Left empty on a run by hand: the notebook then makes one that starts with `manual-` |
# MAGIC
# MAGIC No storage account, catalog or table name is typed into a cell.

# COMMAND ----------

# Cell 2 - the parameter boxes. This cell only creates them, so that it always succeeds:
# Databricks shows a box only after the cell that creates it has finished without an error.
dbutils.widgets.text("p_catalog", "")
dbutils.widgets.text("p_storage_account", "")
dbutils.widgets.text("p_tables", "")
dbutils.widgets.text("p_run_id", "")

# COMMAND ----------

# Cell 3 - read the parameters. The notebook stops here if one is missing: a load must never guess its environment.
import json, re, uuid

CATALOG     = dbutils.widgets.get("p_catalog").strip()
ACCOUNT     = dbutils.widgets.get("p_storage_account").strip()
TABLES_JSON = dbutils.widgets.get("p_tables").strip()
RUN_ID      = dbutils.widgets.get("p_run_id").strip() or "manual-" + str(uuid.uuid4())

missing = [name for name, value in [("p_catalog", CATALOG), ("p_storage_account", ACCOUNT), ("p_tables", TABLES_JSON)] if value == ""]
if missing:
    raise ValueError("Parameter not given: " + ", ".join(missing)
                     + ". Type the value into the box at the top of the notebook, or let the job pass it.")
if not re.fullmatch(r"[A-Za-z0-9_]+", CATALOG):
    raise ValueError(f"p_catalog '{CATALOG}' is not a plain catalog name (letters, digits, underscore).")
if not re.fullmatch(r"[a-z0-9]{3,24}", ACCOUNT):
    raise ValueError(f"p_storage_account '{ACCOUNT}' is not a storage account name (3-24 lower-case letters and digits).")
if not re.fullmatch(r"[A-Za-z0-9_-]+", RUN_ID):
    raise ValueError(f"p_run_id '{RUN_ID}' holds characters a run id never has.")

config = json.loads(TABLES_JSON)          # the rows of etl.ingest_config, one object per table
tables = [row["table_name"] for row in config]
bad    = [t for t in tables if not re.fullmatch(r"[a-z0-9_]+", t)]
if not tables or bad:
    raise ValueError(f"p_tables must be a JSON list of objects with a plain table_name. Not accepted: {bad or 'an empty list'}")

LANDING = f"abfss://bronze@{ACCOUNT}.dfs.core.windows.net/olist"        # where Data Factory wrote the Parquet files
BRONZE  = f"abfss://bronze@{ACCOUNT}.dfs.core.windows.net/delta/olist"  # where the bronze Delta tables are stored
print(f"run_id = {RUN_ID}\ncatalog = {CATALOG}\n{len(tables)} tables: {', '.join(tables)}")

# COMMAND ----------

# Cell 4 - BUILD (first run only): the audit table of the bronze load. One row per table per run.
PROPERTIES = "'delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true'"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.bronze.load_audit (
      run_id         STRING    NOT NULL,
      table_name     STRING    NOT NULL,
      status         STRING    NOT NULL,
      rows_loaded    BIGINT,
      rows_in_table  BIGINT,
      started_at     TIMESTAMP,
      finished_at    TIMESTAMP,
      error_message  STRING
    )
    COMMENT 'One row per bronze table per run of the bronze load: how many rows the run loaded, how many the table then held, and whether it succeeded.'
    LOCATION '{BRONZE}/load_audit'
    TBLPROPERTIES ({PROPERTIES})""")

def audit(table, status, started, rows_loaded=None, rows_in_table=None, error=None):
    number = lambda v: "NULL" if v is None else str(v)
    text   = "NULL" if error is None else "'" + re.sub(r"[^A-Za-z0-9 _.,:;()\[\]-]", " ", error)[:500] + "'"
    spark.sql(f"""INSERT INTO {CATALOG}.bronze.load_audit VALUES (
        '{RUN_ID}', '{table}', '{status}', {number(rows_loaded)}, {number(rows_in_table)},
        TIMESTAMP'{started}', current_timestamp(), {text})""")

# COMMAND ----------

# Cell 5 - BUILD: the bronze load, the same statements for every table in the list.
for t in tables:
    full    = f"{CATALOG}.bronze.{t}"
    started = spark.sql("SELECT CAST(current_timestamp() AS STRING) AS s").first()["s"]
    try:
        # 1. the table - created empty the first time; its columns come from the files
        spark.sql(f"CREATE TABLE IF NOT EXISTS {full} LOCATION '{BRONZE}/{t}'")
        have = {r["key"] for r in spark.sql(f"SHOW TBLPROPERTIES {full}").collect()}
        if "delta.autoOptimize.optimizeWrite" not in have or "delta.autoOptimize.autoCompact" not in have:
            spark.sql(f"ALTER TABLE {full} SET TBLPROPERTIES ({PROPERTIES})")

        # 2. load every file not loaded before, with where each row came from and when it was loaded
        loaded = spark.sql(f"""
            COPY INTO {full}
            FROM (SELECT *,
                         _metadata.file_name              AS _source_file,
                         _metadata.file_modification_time AS _source_file_time,
                         current_timestamp()              AS _loaded_at
                  FROM '{LANDING}/{t}/')
            FILEFORMAT = PARQUET
            FORMAT_OPTIONS ('mergeSchema' = 'true')
            COPY_OPTIONS ('mergeSchema' = 'true')
        """).first()["num_inserted_rows"]

        # 3. record the run
        audit(t, "succeeded", started, loaded, spark.table(full).count())
    except Exception as e:
        audit(t, "failed", started, error=str(e))
        raise

display(spark.sql(f"""
    SELECT table_name, status, rows_loaded, rows_in_table, error_message
    FROM {CATALOG}.bronze.load_audit
    WHERE run_id = '{RUN_ID}'
    ORDER BY finished_at"""))
