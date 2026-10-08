# Databricks notebook source
# MAGIC %md
# MAGIC # nb_silver_setup — every silver table, as declared
# MAGIC
# MAGIC **What this notebook is.** The definition of every table in the silver schema: its columns, which of them must be filled (NOT NULL), its value rules (CHECK), where it is stored, what one row is (the comment) and its maintenance properties.
# MAGIC
# MAGIC **Safe to run again.** Every statement first looks at what exists. On an environment that is already built it changes nothing; on an empty one it creates all twelve tables.
# MAGIC
# MAGIC **Parameters** (the job passes them; by hand, type them into the boxes at the top):
# MAGIC
# MAGIC | Parameter | Meaning |
# MAGIC |---|---|
# MAGIC | `p_catalog` | the Unity Catalog catalog that holds the schemas `bronze`, `silver`, `gold` |
# MAGIC | `p_storage_account` | the ADLS Gen2 storage account that holds the containers `bronze`, `silver`, `gold` |
# MAGIC
# MAGIC Nothing about the environment is typed into a cell. The same file runs in development and in production.
# MAGIC
# MAGIC Written from the catalog's own definitions (`SHOW CREATE TABLE`, Tue 6 Oct 2026). The build record that led here is `nb_silver_merge`.

# COMMAND ----------

# Cell 2 - the parameter boxes. This cell only creates them, so that it always succeeds:
# Databricks shows a box only after the cell that creates it has finished without an error.
dbutils.widgets.text("p_catalog", "")
dbutils.widgets.text("p_storage_account", "")

# COMMAND ----------

# Cell 3 - read the parameters. The notebook stops here if one is missing: a load must never guess its environment.
import re

CATALOG = dbutils.widgets.get("p_catalog").strip()
ACCOUNT = dbutils.widgets.get("p_storage_account").strip()

missing = [name for name, value in [("p_catalog", CATALOG), ("p_storage_account", ACCOUNT)] if value == ""]
if missing:
    raise ValueError("Parameter not given: " + ", ".join(missing)
                     + ". Type the value into the box at the top of the notebook, or let the job pass it.")
if not re.fullmatch(r"[A-Za-z0-9_]+", CATALOG):
    raise ValueError(f"p_catalog '{CATALOG}' is not a plain catalog name (letters, digits, underscore).")
if not re.fullmatch(r"[a-z0-9]{3,24}", ACCOUNT):
    raise ValueError(f"p_storage_account '{ACCOUNT}' is not a storage account name (3-24 lower-case letters and digits).")

SILVER = f"abfss://silver@{ACCOUNT}.dfs.core.windows.net/olist"   # one folder per silver table under this
print(f"catalog = {CATALOG}   silver tables are stored under {SILVER}")

# COMMAND ----------

# Cell 4 - the definitions. One entry per table: its columns, and what one row is.
# NOT NULL = the source guarantees the column is filled. The silver load reads these rules from the catalog
# and sets aside (quarantines) any row that breaks one, so tightening a rule is a change here and nowhere else.
TABLES = {

    # ---- the five tables that change at the source (they carry modified_at) ----
    "orders": ("""
  order_id                       STRING    NOT NULL,
  customer_id                    STRING    NOT NULL,
  order_status                   STRING    NOT NULL,
  order_purchase_timestamp       TIMESTAMP NOT NULL,
  order_approved_at              TIMESTAMP,
  order_delivered_carrier_date   TIMESTAMP,
  order_delivered_customer_date  TIMESTAMP,
  order_estimated_delivery_date  TIMESTAMP NOT NULL,
  modified_at                    TIMESTAMP NOT NULL,
  _source_file                   STRING,
  _source_file_time              TIMESTAMP""",
        "One row per order: the newest version of each order received from the source."),

    "customers": ("""
  customer_id               STRING    NOT NULL,
  customer_unique_id        STRING    NOT NULL,
  customer_zip_code_prefix  STRING    NOT NULL,
  customer_city             STRING    NOT NULL,
  customer_state            STRING    NOT NULL,
  modified_at               TIMESTAMP NOT NULL,
  _source_file              STRING,
  _source_file_time         TIMESTAMP""",
        "One row per customer_id, the id an order carries (customer_unique_id identifies the person): the newest version received from the source."),

    "order_items": ("""
  order_id             STRING        NOT NULL,
  order_item_id        INT           NOT NULL,
  product_id           STRING        NOT NULL,
  seller_id            STRING        NOT NULL,
  shipping_limit_date  TIMESTAMP     NOT NULL,
  price                DECIMAL(10,2) NOT NULL,
  freight_value        DECIMAL(10,2) NOT NULL,
  modified_at          TIMESTAMP     NOT NULL,
  _source_file         STRING,
  _source_file_time    TIMESTAMP""",
        "One row per item line of an order (order_id + order_item_id): the newest version received from the source."),

    "order_payments": ("""
  order_id              STRING        NOT NULL,
  payment_sequential    INT           NOT NULL,
  payment_type          STRING        NOT NULL,
  payment_installments  INT           NOT NULL,
  payment_value         DECIMAL(10,2) NOT NULL,
  modified_at           TIMESTAMP     NOT NULL,
  _source_file          STRING,
  _source_file_time     TIMESTAMP""",
        "One row per payment line of an order (order_id + payment_sequential): the newest version received from the source."),

    "order_reviews": ("""
  review_id                STRING    NOT NULL,
  order_id                 STRING    NOT NULL,
  review_score             INT       NOT NULL,
  review_comment_title     STRING,
  review_comment_message   STRING,
  review_creation_date     TIMESTAMP NOT NULL,
  review_answer_timestamp  TIMESTAMP NOT NULL,
  modified_at              TIMESTAMP NOT NULL,
  _source_file             STRING,
  _source_file_time        TIMESTAMP""",
        "One row per review of an order (review_id + order_id): the newest version received from the source."),

    # ---- the three reference tables, copied whole on every run (row_hash = fingerprint of the content) ----
    "products": ("""
  product_id                  STRING NOT NULL,
  product_category_name       STRING,
  product_name_lenght         INT,
  product_description_lenght  INT,
  product_photos_qty          INT,
  product_weight_g            INT,
  product_length_cm           INT,
  product_height_cm           INT,
  product_width_cm            INT,
  _source_file                STRING,
  _source_file_time           TIMESTAMP,
  row_hash                    STRING""",
        "One row per product: the newest copy received from the source."),

    "sellers": ("""
  seller_id               STRING NOT NULL,
  seller_zip_code_prefix  STRING NOT NULL,
  seller_city             STRING NOT NULL,
  seller_state            STRING NOT NULL,
  _source_file            STRING,
  _source_file_time       TIMESTAMP,
  row_hash                STRING""",
        "One row per seller: the newest copy received from the source, rewritten only when its content changes."),

    "product_category_translation": ("""
  product_category_name          STRING NOT NULL,
  product_category_name_english  STRING NOT NULL,
  _source_file                   STRING,
  _source_file_time              TIMESTAMP,
  row_hash                       STRING""",
        "One row per product category name with its English name: the newest copy received from the source, rewritten only when its content changes."),

    # ---- no key: rebuilt whole ----
    "geolocation": ("""
  geolocation_zip_code_prefix  STRING NOT NULL,
  geolocation_lat              DOUBLE,
  geolocation_lng              DOUBLE,
  geolocation_city             STRING,
  geolocation_state            STRING""",
        "Recorded map points of zip-code prefixes, exact repeats removed. No key: rebuilt whole by overwrite."),

    # ---- the load's own tables ----
    "quarantine": ("""
  table_name         STRING    NOT NULL,
  reason             STRING    NOT NULL,
  row_json           STRING    NOT NULL,
  row_hash           STRING    NOT NULL,
  _source_file       STRING,
  _source_file_time  TIMESTAMP,
  _loaded_at         TIMESTAMP,
  quarantined_at     TIMESTAMP NOT NULL""",
        "Rows the silver load refused: which table, why, the whole row as JSON, the file it came from, when it was first refused."),

    "load_state": ("""
  table_name      STRING    NOT NULL,
  last_loaded_at  TIMESTAMP NOT NULL,
  updated_at      TIMESTAMP NOT NULL""",
        "One row per silver table: how far silver has read in its bronze table (the newest _loaded_at already processed)."),

    "load_audit": ("""
  run_id            STRING    NOT NULL,
  table_name        STRING    NOT NULL,
  status            STRING    NOT NULL,
  window_from       TIMESTAMP,
  window_to         TIMESTAMP,
  bronze_rows_read  BIGINT,
  rows_quarantined  BIGINT,
  rows_inserted     BIGINT,
  rows_updated      BIGINT,
  rows_in_silver    BIGINT,
  duplicate_keys    BIGINT,
  started_at        TIMESTAMP,
  finished_at       TIMESTAMP,
  error_message     STRING""",
        "One row per silver table per run of the silver load: what was read, quarantined, inserted and updated, and whether it succeeded."),
}

# Value rules (Delta CHECK constraints): table, rule name, what a valid value is.
RULES = [
    ("orders",         "order_status_known",         "order_status IN ('created','approved','invoiced','processing','shipped','delivered','unavailable','canceled')"),
    ("order_items",    "price_not_negative",         "price >= 0"),
    ("order_items",    "freight_not_negative",       "freight_value >= 0"),
    ("order_payments", "payment_value_not_negative", "payment_value >= 0"),
    ("order_payments", "installments_not_negative",  "payment_installments >= 0"),
    ("order_reviews",  "review_score_1_to_5",        "review_score BETWEEN 1 AND 5"),
]

# Maintenance properties every table carries: the tables are external, so nothing compacts them unless we ask.
PROPERTIES = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact":   "true",
}
print(f"{len(TABLES)} table definitions, {len(RULES)} value rules")

# COMMAND ----------

# Cell 5 - BUILD: bring the silver schema to these definitions. Only what differs is changed.
def table_properties(full_name):
    return {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {full_name}").collect()}

exists_now = {r["table_name"]: r["comment"] for r in spark.sql(f"""
    SELECT table_name, comment FROM {CATALOG}.information_schema.tables
    WHERE table_schema = 'silver'""").collect()}

property_list = ", ".join(f"'{k}' = '{v}'" for k, v in PROPERTIES.items())
actions = []
for t, (columns, comment) in TABLES.items():
    full = f"{CATALOG}.silver.{t}"

    # 1. the table itself - created only if it is not there
    created = t not in exists_now
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full} ({columns}
        )
        COMMENT '{comment}'
        LOCATION '{SILVER}/{t}'
        TBLPROPERTIES ({property_list})""")

    # 2. the maintenance properties - set only where one is missing
    have        = table_properties(full)
    props_to_set = [k for k, v in PROPERTIES.items() if have.get(k) != v]
    if props_to_set:
        spark.sql(f"ALTER TABLE {full} SET TBLPROPERTIES ("
                  + ", ".join(f"'{k}' = '{PROPERTIES[k]}'" for k in props_to_set) + ")")

    # 3. the comment (what one row is) - written only where it differs
    comment_set = (not created) and (exists_now.get(t) or "") != comment
    if comment_set:
        spark.sql(f"COMMENT ON TABLE {full} IS '{comment}'")

    # 4. the value rules - added only where one is missing
    rules_added = []
    for rule_table, rule_name, rule in RULES:
        if rule_table == t and f"delta.constraints.{rule_name}" not in have:
            spark.sql(f"ALTER TABLE {full} ADD CONSTRAINT {rule_name} CHECK ({rule})")
            rules_added.append(rule_name)

    actions.append((t, "created" if created else "already there", len(props_to_set),
                    "written" if comment_set else "-", ", ".join(rules_added) or "-"))

display(spark.createDataFrame(actions,
    "table_name string, table string, properties_set long, comment string, rules_added string"))

# COMMAND ----------

# Cell 6 - CHECK: what the catalog holds now, one row per table. Claim: twelve tables, each with its
# NOT NULL columns, its value rules, both maintenance properties and a comment.
columns_now = {r["table_name"]: (r["n_columns"], r["n_not_null"]) for r in spark.sql(f"""
    SELECT table_name, COUNT(*) AS n_columns,
           SUM(CASE WHEN is_nullable = 'NO' THEN 1 ELSE 0 END) AS n_not_null
    FROM {CATALOG}.information_schema.columns
    WHERE table_schema = 'silver'
    GROUP BY table_name""").collect()}
comments_now = {r["table_name"]: r["comment"] for r in spark.sql(f"""
    SELECT table_name, comment FROM {CATALOG}.information_schema.tables
    WHERE table_schema = 'silver'""").collect()}

summary = []
for t in TABLES:
    have  = table_properties(f"{CATALOG}.silver.{t}")
    rules = sorted(k.replace("delta.constraints.", "") for k in have if k.startswith("delta.constraints."))
    n_columns, n_not_null = columns_now.get(t, (0, 0))
    summary.append((t, n_columns, n_not_null, ", ".join(rules) or "-",
                    "yes" if all(have.get(k) == v for k, v in PROPERTIES.items()) else "NO",
                    "yes" if (comments_now.get(t) or "") != "" else "NO"))

display(spark.createDataFrame(summary,
    "table_name string, columns long, not_null_columns long, value_rules string, maintenance_properties string, has_comment string"))
