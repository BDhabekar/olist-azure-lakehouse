# Databricks notebook source
# MAGIC %md
# MAGIC # nb_silver_load — bronze becomes silver: one row per key, checked, merged
# MAGIC
# MAGIC **What this notebook does**, for every table in the list it is given:
# MAGIC
# MAGIC 1. reads the table's bookmark and takes only the bronze rows loaded after it (the window);
# MAGIC 2. reads from the catalog what the silver table promises — its columns, the NOT NULL columns, the CHECK rules;
# MAGIC 3. sets aside the rows of the window that break a rule (`silver.quarantine`, with the rule's name, recorded once);
# MAGIC 4. keeps the newest row per key from the good rows;
# MAGIC 5. merges them into the silver table — inserted when the key is new, updated only when the row is newer (and, for the reference tables, only when its content changed);
# MAGIC 6. checks that no key is held twice - and fails the table if one is;
# MAGIC 7. writes one audit row (`silver.load_audit`) and moves the bookmark — last, and only forward.
# MAGIC
# MAGIC Run twice over the same data it changes nothing the second time.
# MAGIC
# MAGIC **Parameters** (the job passes them; by hand, type them into the boxes at the top):
# MAGIC
# MAGIC | Parameter | Meaning |
# MAGIC |---|---|
# MAGIC | `p_catalog` | the Unity Catalog catalog that holds the schemas `bronze`, `silver`, `gold` |
# MAGIC | `p_tables` | the list of tables as JSON — the rows of the control table `etl.ingest_config`, handed over by Data Factory. This notebook uses `table_name`, `load_type` and `key_columns` |
# MAGIC | `p_run_id` | the pipeline's run id, written to the audit rows. Left empty on a run by hand: the notebook then makes one that starts with `manual-` |
# MAGIC
# MAGIC No catalog or table name is typed into a cell, and no storage account is needed: the tables are reached by name. The silver tables themselves are defined in `nb_silver_setup`.

# COMMAND ----------

# Cell 2 - the parameter boxes. This cell only creates them, so that it always succeeds:
# Databricks shows a box only after the cell that creates it has finished without an error.
dbutils.widgets.text("p_catalog", "")
dbutils.widgets.text("p_tables", "")
dbutils.widgets.text("p_run_id", "")

# COMMAND ----------

# Cell 3 - read the parameters. The notebook stops here if one is missing: a load must never guess its environment.
import json, re, uuid

# Every time literal and every conversion in this notebook is read in UTC, whatever the cluster's settings:
# silver stores the source's times as TIMESTAMP_NTZ (Oct 17), and bronze's TIMESTAMP becomes the source's
# wall-clock time only under UTC.
spark.conf.set("spark.sql.session.timeZone", "UTC")

CATALOG     = dbutils.widgets.get("p_catalog").strip()
TABLES_JSON = dbutils.widgets.get("p_tables").strip()
RUN_ID      = dbutils.widgets.get("p_run_id").strip() or "manual-" + str(uuid.uuid4())

missing = [name for name, value in [("p_catalog", CATALOG), ("p_tables", TABLES_JSON)] if value == ""]
if missing:
    raise ValueError("Parameter not given: " + ", ".join(missing)
                     + ". Type the value into the box at the top of the notebook, or let the job pass it.")
if not re.fullmatch(r"[A-Za-z0-9_]+", CATALOG):
    raise ValueError(f"p_catalog '{CATALOG}' is not a plain catalog name (letters, digits, underscore).")
if not re.fullmatch(r"[A-Za-z0-9_-]+", RUN_ID):
    raise ValueError(f"p_run_id '{RUN_ID}' holds characters a run id never has.")

# How "newest" is decided and what the update compares follows from how the table is copied:
#   incremental -> the source stamps every change (modified_at)
#   full        -> the whole table arrives each run; the newest file wins (_source_file_time)
NEWEST = {
    "incremental": ("modified_at DESC, _source_file_time DESC, _source_file", "modified_at"),
    "full":        ("_source_file_time DESC, _source_file",                   "_source_file_time"),
}

# table: (key columns, "newest first" sort, column the update condition compares) - built from the list, not typed
cfg, no_key = {}, []
for row in json.loads(TABLES_JSON):
    name, load_type, keys = row["table_name"], row["load_type"], row["key_columns"]
    if not re.fullmatch(r"[a-z0-9_]+", name) or load_type not in NEWEST or not re.fullmatch(r"[a-z0-9_, ]+", keys):
        raise ValueError(f"p_tables: this row is not accepted: {row}")
    if keys.strip() == "none":
        no_key.append(name)               # a table without a key cannot be merged; it is not loaded by this notebook
        continue
    newest, guard = NEWEST[load_type]
    cfg[name] = ([k.strip() for k in keys.split(",")], newest, guard)
if not cfg:
    raise ValueError("p_tables holds no table with key columns.")

print(f"run_id = {RUN_ID}\ncatalog = {CATALOG}")
for name, (key, newest, guard) in cfg.items():
    print(f"  {name}: key = {', '.join(key)}; update when {guard} is newer")
if no_key:
    print(f"  not loaded here (no key): {', '.join(no_key)}")

# COMMAND ----------

# Cell 4 - the two helpers of the load.
NOT_BUSINESS = {"_source_file", "_source_file_time", "row_hash"}   # where a row came from is not its content

def hash_expr(cols):
    # one fingerprint for the listed columns: pack them into JSON (empty values written as null), then sha2.
    # MUST stay as it is: the hashes already stored in silver were made with exactly this expression.
    return "sha2(to_json(struct(" + ", ".join(cols) + "), map('ignoreNullFields', 'false')), 256)"

def audit(t, status, old_mark, new_mark, started, n_read=None, n_q=None, n_ins=None, n_upd=None, n_silver=None, n_dup=None, error=None):
    val = lambda v: "NULL" if v is None else str(v)
    err = "NULL" if error is None else "'" + re.sub(r"[^A-Za-z0-9 _.,:;()\[\]-]", " ", error)[:500] + "'"
    spark.sql(f"""INSERT INTO {CATALOG}.silver.load_audit VALUES (
        '{RUN_ID}', '{t}', '{status}', TIMESTAMP'{old_mark}', TIMESTAMP'{new_mark}',
        {val(n_read)}, {val(n_q)}, {val(n_ins)}, {val(n_upd)}, {val(n_silver)}, {val(n_dup)},
        TIMESTAMP'{started}', current_timestamp(), {err})""")

# COMMAND ----------

# Cell 5 - BUILD: the silver load, the same statements for every table in the list.
for t, (key, newest, guard) in cfg.items():
    bronze  = f"{CATALOG}.bronze.{t}"
    silver  = f"{CATALOG}.silver.{t}"
    started = spark.sql("SELECT CAST(current_timestamp() AS STRING) AS s").first()["s"]

    # 0. A table loaded for the first time gets its bookmark at the very beginning
    if spark.sql(f"SELECT 1 FROM {CATALOG}.silver.load_state WHERE table_name = '{t}'").first() is None:
        spark.sql(f"""INSERT INTO {CATALOG}.silver.load_state (table_name, last_loaded_at, updated_at)
                      VALUES ('{t}', TIMESTAMP'1900-01-01 00:00:00', current_timestamp())""")

    # 1. The window: after the bookmark, up to the newest load in bronze right now (both read as text)
    old_mark = spark.sql(f"SELECT CAST(last_loaded_at AS STRING) AS s FROM {CATALOG}.silver.load_state WHERE table_name = '{t}'").first()["s"]
    new_mark = spark.sql(f"SELECT CAST(MAX(_loaded_at) AS STRING) AS s FROM {bronze}").first()["s"] or old_mark   # an empty bronze table: nothing to read
    window   = f"_loaded_at > TIMESTAMP'{old_mark}' AND _loaded_at <= TIMESTAMP'{new_mark}'"
    try:
        rows_read = spark.sql(f"SELECT COUNT(*) AS n FROM {bronze} WHERE {window}").first()["n"]

        # 2. What the silver table promises (from the catalog): its columns, the NOT NULL columns, the CHECK rules
        info = spark.sql(f"""
            SELECT column_name, is_nullable, upper(data_type) AS data_type FROM {CATALOG}.information_schema.columns
            WHERE table_schema = 'silver' AND table_name = '{t}'
            ORDER BY ordinal_position""").collect()
        if not info:
            raise ValueError(f"{silver} does not exist. Add its definition to nb_silver_setup and run that notebook first.")
        cols     = [r["column_name"] for r in info]
        required = [r["column_name"] for r in info if r["is_nullable"] == "NO"]
        ntz      = {r["column_name"] for r in info if r["data_type"] == "TIMESTAMP_NTZ"}   # source times: bronze TIMESTAMP -> silver TIMESTAMP_NTZ
        if guard == "modified_at" and "modified_at" not in required:
            required.append("modified_at")
        checks = {r["key"].replace("delta.constraints.", ""): r["value"]
                  for r in spark.sql(f"SHOW TBLPROPERTIES {silver}").collect()
                  if r["key"].startswith("delta.constraints.")}

        has_hash = "row_hash" in cols
        business = [c for c in cols if c not in NOT_BUSINESS]
        from_bronze = [c for c in cols if c != "row_hash"]                      # the columns bronze really has
        select_in   = ", ".join(f"CAST({c} AS TIMESTAMP_NTZ) AS {c}" if c in ntz else c for c in from_bronze) \
                      + (f", {hash_expr(business)} AS row_hash" if has_hash else "")
        col_list = ", ".join(cols)
        keys     = ", ".join(key)
        on       = " AND ".join(f"t.{k} = s.{k}" for k in key)
        # a row is BAD if a required column is empty OR a value rule is false; GOOD is the exact opposite
        bad      = " OR ".join([f"{c} IS NULL" for c in required] + [f"NOT ({e})" for e in checks.values()])
        good     = " AND ".join([f"{c} IS NOT NULL" for c in required] + [f"coalesce(({e}), true)" for e in checks.values()])
        reason   = "concat_ws('; ', " + ", ".join(
                       [f"CASE WHEN {c} IS NULL THEN 'missing {c}' END" for c in required] +
                       [f"CASE WHEN NOT ({e}) THEN 'broke rule {n}' END" for n, e in checks.items()]) + ")"
        update_if = f"s.{guard} > t.{guard}" + (" AND NOT (s.row_hash <=> t.row_hash)" if has_hash else "")

        # 3. BUILD: set the bad rows of the window aside (recorded once)
        q = spark.sql(f"""
            MERGE INTO {CATALOG}.silver.quarantine AS q
            USING (SELECT '{t}' AS table_name, {reason} AS reason,
                          to_json(struct(*), map('ignoreNullFields', 'false')) AS row_json,
                          sha2(to_json(struct(*), map('ignoreNullFields', 'false')), 256) AS row_hash,
                          _source_file, _source_file_time, _loaded_at
                   FROM {bronze}
                   WHERE ({bad}) AND {window}) AS s
            ON q.table_name = s.table_name AND q._source_file = s._source_file AND q.row_hash = s.row_hash
            WHEN NOT MATCHED THEN INSERT
              (table_name, reason, row_json, row_hash, _source_file, _source_file_time, _loaded_at, quarantined_at)
              VALUES (s.table_name, s.reason, s.row_json, s.row_hash, s._source_file, s._source_file_time, s._loaded_at, current_timestamp())""").first()

        # 4. BUILD: newest row per key, from the good rows of the window only
        spark.sql(f"""
            CREATE OR REPLACE TEMPORARY VIEW v_{t}_latest AS
            WITH ranked AS (
                SELECT {select_in}, ROW_NUMBER() OVER (PARTITION BY {keys} ORDER BY {newest}) AS rn
                FROM {bronze}
                WHERE {good} AND {window}
            )
            SELECT {col_list} FROM ranked WHERE rn = 1""")

        # 5. BUILD: the MERGE into silver
        m = spark.sql(f"""
            MERGE INTO {silver} AS t
            USING v_{t}_latest AS s
            ON {on}
            WHEN MATCHED AND {update_if} THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *""").first()

        # 6. CHECK: no key twice in silver. A key held twice would be counted twice in every gold total, so the
        #    table fails here: the except branch below writes a "failed" audit row and the bookmark is not moved.
        dup = spark.sql(f"SELECT {keys} FROM {silver} GROUP BY {keys} HAVING COUNT(*) > 1").count()
        if dup > 0:
            raise RuntimeError(f"{silver} holds {dup} key(s) more than once after the MERGE; the bookmark is not moved.")

        # 7. Record the run, then move the bookmark LAST - and only forward
        audit(t, "succeeded", old_mark, new_mark, started, rows_read, q["num_inserted_rows"],
              m["num_inserted_rows"], m["num_updated_rows"], spark.table(silver).count(), dup)
        spark.sql(f"""UPDATE {CATALOG}.silver.load_state
                      SET last_loaded_at = TIMESTAMP'{new_mark}', updated_at = current_timestamp()
                      WHERE table_name = '{t}' AND last_loaded_at < TIMESTAMP'{new_mark}'""")
    except Exception as e:
        audit(t, "failed", old_mark, new_mark, started, error=str(e))
        raise

# COMMAND ----------

# Cell 6 - what this run did, read back from the audit table, and what is in quarantine.
display(spark.sql(f"""
    SELECT table_name, status, bronze_rows_read, rows_quarantined, rows_inserted, rows_updated, rows_in_silver, duplicate_keys
    FROM {CATALOG}.silver.load_audit WHERE run_id = '{RUN_ID}' ORDER BY finished_at"""))
display(spark.sql(f"SELECT table_name, reason, _source_file FROM {CATALOG}.silver.quarantine"))
