# Databricks notebook source
# MAGIC %md
# MAGIC # nb_gold_setup — every gold table, as declared
# MAGIC
# MAGIC **What this notebook is.** The definition of every table in the gold schema — a star schema for reporting: four dimensions, two facts at two grains, three KPI tables. Columns, NOT NULL, where each table is stored, what one row is, maintenance properties.
# MAGIC
# MAGIC **Who runs it.** The owner of the catalog, by hand, once per environment and after any change to a definition — not the load job: the load's service principal may load gold (`SELECT`, `MODIFY`) but not create or alter a table (`nb_uc_grants`). The gold tables are filled by `nb_gold_load`, the job's third task.
# MAGIC
# MAGIC **Safe to run again.** A table that exists is left as it is; properties and comments are set only where they differ.
# MAGIC
# MAGIC **One cluster at a time.** Lab quota: the subscription allows one 4-core cluster at a time in its region, so the set-up cluster is terminated before the load job runs.
# MAGIC
# MAGIC | Parameter | Meaning |
# MAGIC |---|---|
# MAGIC | `p_catalog` | the Unity Catalog catalog that holds the schemas `bronze`, `silver`, `gold` |
# MAGIC | `p_storage_account` | the ADLS Gen2 storage account that holds the containers `bronze`, `silver`, `gold` |

# COMMAND ----------

# Cell 2 - the parameter boxes (this cell only creates them, so that it always succeeds).
dbutils.widgets.text("p_catalog", "")
dbutils.widgets.text("p_storage_account", "")

# COMMAND ----------

# Cell 3 - read the parameters. The notebook stops here if one is missing: it must never guess its environment.
import re

CATALOG = dbutils.widgets.get("p_catalog").strip()
ACCOUNT = dbutils.widgets.get("p_storage_account").strip()
missing = [n for n, v in [("p_catalog", CATALOG), ("p_storage_account", ACCOUNT)] if v == ""]
if missing:
    raise ValueError("Parameter not given: " + ", ".join(missing) + ". Type the value into the box at the top of the notebook.")
if not re.fullmatch(r"[A-Za-z0-9_]+", CATALOG):
    raise ValueError(f"p_catalog '{CATALOG}' is not a plain catalog name.")
if not re.fullmatch(r"[a-z0-9]{3,24}", ACCOUNT):
    raise ValueError(f"p_storage_account '{ACCOUNT}' is not a storage account name.")

GOLD = f"abfss://gold@{ACCOUNT}.dfs.core.windows.net/olist"     # one folder per gold table under this
print(f"catalog = {CATALOG}   gold tables are stored under {GOLD}")

# COMMAND ----------

# Cell 4 - the definitions. Times are TIMESTAMP_NTZ: the source's times carry no zone (Oct 17).
# Money is DECIMAL; sums of money DECIMAL(14,2). *_key = yyyymmdd as an INT, the key of dim_date.
TABLES = {
    # ---- dimensions ----
    "dim_customer": ("""
  customer_id               STRING NOT NULL,
  customer_unique_id        STRING NOT NULL,
  customer_zip_code_prefix  STRING NOT NULL,
  customer_city             STRING NOT NULL,
  customer_state            STRING NOT NULL""",
        "One row per customer_id, the id an order carries (customer_unique_id identifies the person)."),

    "dim_product": ("""
  product_id              STRING NOT NULL,
  category_name           STRING,
  category_name_english   STRING NOT NULL,
  product_weight_g        INT,
  product_length_cm       INT,
  product_height_cm       INT,
  product_width_cm        INT,
  product_photos_qty      INT""",
        "One row per product. category_name_english: the English name, else the source name, else unknown."),

    "dim_seller": ("""
  seller_id               STRING NOT NULL,
  seller_zip_code_prefix  STRING NOT NULL,
  seller_city             STRING NOT NULL,
  seller_state            STRING NOT NULL""",
        "One row per seller."),

    "dim_date": ("""
  date_key        INT     NOT NULL,
  date            DATE    NOT NULL,
  year            INT     NOT NULL,
  quarter         INT     NOT NULL,
  month           INT     NOT NULL,
  month_name      STRING  NOT NULL,
  year_month      STRING  NOT NULL,
  day_of_month    INT     NOT NULL,
  day_of_week     INT     NOT NULL,
  day_name        STRING  NOT NULL,
  is_weekend      BOOLEAN NOT NULL""",
        "One row per calendar day from the first to the last order purchase day."),

    # ---- facts ----
    "fact_order_items": ("""
  order_id            STRING        NOT NULL,
  order_item_id       INT           NOT NULL,
  customer_id         STRING        NOT NULL,
  product_id          STRING        NOT NULL,
  seller_id           STRING        NOT NULL,
  purchase_date_key   INT           NOT NULL,
  order_status        STRING        NOT NULL,
  price               DECIMAL(10,2) NOT NULL,
  freight_value       DECIMAL(10,2) NOT NULL,
  item_total          DECIMAL(12,2) NOT NULL,
  shipping_limit_ts   TIMESTAMP_NTZ NOT NULL""",
        "One row per item line of an order (order_id + order_item_id): what was sold, by whom, to whom, when, for how much."),

    "fact_orders": ("""
  order_id               STRING        NOT NULL,
  customer_id            STRING        NOT NULL,
  purchase_date_key      INT           NOT NULL,
  order_status           STRING        NOT NULL,
  purchase_ts            TIMESTAMP_NTZ NOT NULL,
  approved_ts            TIMESTAMP_NTZ,
  delivered_carrier_ts   TIMESTAMP_NTZ,
  delivered_customer_ts  TIMESTAMP_NTZ,
  estimated_delivery_ts  TIMESTAMP_NTZ NOT NULL,
  items                  INT           NOT NULL,
  items_value            DECIMAL(14,2) NOT NULL,
  freight_value          DECIMAL(14,2) NOT NULL,
  order_value            DECIMAL(14,2) NOT NULL,
  payment_value          DECIMAL(14,2) NOT NULL,
  review_score           INT,
  delivery_days          INT,
  is_late                BOOLEAN""",
        "One row per order: its totals, what was paid, its latest review, and how its delivery went. Order-level values live here and not on the item lines, so they are never counted once per item."),

    # ---- KPI tables (orders that were canceled or unavailable are not sales) ----
    "kpi_monthly_sales": ("""
  year_month           STRING        NOT NULL,
  month_start          DATE          NOT NULL,
  orders               BIGINT        NOT NULL,
  items                BIGINT        NOT NULL,
  revenue              DECIMAL(16,2) NOT NULL,
  avg_order_value      DECIMAL(12,2) NOT NULL""",
        "One row per month of purchase: orders, items and revenue (price + freight) of orders not canceled or unavailable."),

    "kpi_delivery_by_state": ("""
  customer_state       STRING       NOT NULL,
  delivered_orders     BIGINT       NOT NULL,
  avg_delivery_days    DECIMAL(6,2) NOT NULL,
  late_orders          BIGINT       NOT NULL,
  late_pct             DECIMAL(5,2) NOT NULL""",
        "One row per customer state: delivered orders, average days from purchase to delivery, share delivered after the estimated date."),

    "kpi_seller_ranking": ("""
  seller_id            STRING        NOT NULL,
  seller_state         STRING        NOT NULL,
  orders               BIGINT        NOT NULL,
  items                BIGINT        NOT NULL,
  revenue              DECIMAL(16,2) NOT NULL,
  avg_review_score     DECIMAL(4,2),
  revenue_rank         INT           NOT NULL""",
        "One row per seller with sales: orders, items, revenue, average review of its orders, rank by revenue (equal revenue = equal rank)."),
}

PROPERTIES = {
    "delta.autoOptimize.optimizeWrite":   "true",
    "delta.autoOptimize.autoCompact":     "true",
    "delta.deletedFileRetentionDuration": "interval 30 days",
}
print(f"{len(TABLES)} table definitions")

# COMMAND ----------

# Cell 5 - BUILD: bring the gold schema to these definitions. Only what differs is changed.
def table_properties(full):
    return {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {full}").collect()}

exists_now = {r["table_name"]: r["comment"] for r in spark.sql(f"""
    SELECT table_name, comment FROM {CATALOG}.information_schema.tables WHERE table_schema = 'gold'""").collect()}

property_list = ", ".join(f"'{k}' = '{v}'" for k, v in PROPERTIES.items())
actions = []
for t, (columns, comment) in TABLES.items():
    full = f"{CATALOG}.gold.{t}"
    created = t not in exists_now
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full} ({columns}
        )
        COMMENT '{comment}'
        LOCATION '{GOLD}/{t}'
        TBLPROPERTIES ({property_list})""")
    have = table_properties(full)
    to_set = [k for k, v in PROPERTIES.items() if have.get(k) != v]
    if to_set:
        spark.sql(f"ALTER TABLE {full} SET TBLPROPERTIES (" + ", ".join(f"'{k}' = '{PROPERTIES[k]}'" for k in to_set) + ")")
    comment_set = (not created) and (exists_now.get(t) or "") != comment
    if comment_set:
        spark.sql(f"COMMENT ON TABLE {full} IS '{comment}'")
    actions.append((t, "created" if created else "already there", len(to_set), "written" if comment_set else "-"))

display(spark.createDataFrame(actions, "table_name string, table string, properties_set long, comment string"))

# COMMAND ----------

# Cell 6 - CHECK: what the catalog holds now. Claim: nine tables, each with its columns and a comment.
display(spark.sql(f"""
    SELECT t.table_name, COUNT(c.column_name) AS columns,
           SUM(CASE WHEN c.is_nullable = 'NO' THEN 1 ELSE 0 END) AS not_null_columns,
           MAX(t.comment) IS NOT NULL AS has_comment
    FROM {CATALOG}.information_schema.tables t
    JOIN {CATALOG}.information_schema.columns c
      ON c.table_schema = t.table_schema AND c.table_name = t.table_name
    WHERE t.table_schema = 'gold'
    GROUP BY t.table_name ORDER BY t.table_name"""))
