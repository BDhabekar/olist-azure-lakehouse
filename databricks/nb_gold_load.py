# Databricks notebook source
# MAGIC %md
# MAGIC # nb_gold_load — silver becomes the gold star schema: built, checked, then published
# MAGIC
# MAGIC **What this notebook does.** It works out every gold table from silver as a temporary view: dimensions, then facts, then KPI tables. It reconciles those views to silver with 13 checks. Only when every check passes does it write the views into the gold tables with `INSERT OVERWRITE`. If a check fails the run stops before anything is written: the gold tables still hold the last good load, the job fails, the Data Factory pipeline fails and the failure alert mails.
# MAGIC
# MAGIC **Why the checks come before the write.** People and reports read gold. A load that overwrites first and checks afterwards raises the alarm with the wrong numbers already in place. Here a result that does not reconcile never reaches a table.
# MAGIC
# MAGIC **Why a full rebuild.** Gold is derived and small (about 225,000 rows): a rebuild takes seconds, can never drift from silver, and a second run gives the same result. Incremental processing lives where the volume is — the watermark at ingestion, `COPY INTO` in bronze, the bookmark and `MERGE` in silver. At 100 times the data, gold would switch to silver's change data feed and a `MERGE` of the changed orders.
# MAGIC
# MAGIC **What is not one transaction.** The nine tables are overwritten one after another. If the run is lost between two of them, gold is part new and part old until the next run, which rebuilds all nine. Cell 8 compares every table with its view after the write and fails the run on a difference.
# MAGIC
# MAGIC **Run as** the load's service principal (the job's third task, after `silver_load`). It may write gold rows (`MODIFY`) but not create tables: the tables are defined by `nb_gold_setup`. A temporary view belongs to the session and needs no privilege.
# MAGIC
# MAGIC | Parameter | Meaning |
# MAGIC |---|---|
# MAGIC | `p_catalog` | the Unity Catalog catalog that holds the schemas `silver` and `gold` |

# COMMAND ----------

# Cell 2 - the parameter box.
dbutils.widgets.text("p_catalog", "")

# COMMAND ----------

# Cell 3 - read the parameter; time zone pinned (the source's times are TIMESTAMP_NTZ; nothing here may depend on the cluster).
import re
spark.conf.set("spark.sql.session.timeZone", "UTC")

CATALOG = dbutils.widgets.get("p_catalog").strip()
if CATALOG == "":
    raise ValueError("Parameter not given: p_catalog.")
if not re.fullmatch(r"[A-Za-z0-9_]+", CATALOG):
    raise ValueError(f"p_catalog '{CATALOG}' is not a plain catalog name.")
S, G = f"{CATALOG}.silver", f"{CATALOG}.gold"

# The nine gold tables, in the order they are built and written: dimensions, facts, KPI tables.
GOLD_TABLES = ["dim_customer", "dim_product", "dim_seller", "dim_date", "fact_order_items", "fact_orders",
               "kpi_monthly_sales", "kpi_delivery_by_state", "kpi_seller_ranking"]
missing = [t for t in GOLD_TABLES if not spark.catalog.tableExists(f"{G}.{t}")]
if missing:
    raise ValueError(f"Gold tables missing: {', '.join(missing)}. The catalog's owner runs nb_gold_setup first.")
print(f"silver = {S}   gold = {G}")

# Orders that are sales. Canceled and unavailable orders are kept in the facts (with their status) but are not revenue.
SALE = "order_status NOT IN ('canceled', 'unavailable')"

def stage(table, select):
    # The new content of one gold table as the temporary view new_<table>. The view takes its column names from
    # the gold table itself, in the table's order, so a SELECT with one column too many or too few fails here,
    # before anything is written. Nothing in gold changes in this step.
    columns = ", ".join(f"`{c}`" for c in spark.table(f"{G}.{table}").columns)
    spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW new_{table} ({columns}) AS {select}")

# COMMAND ----------

# Cell 4 - BUILD the dimensions as views (from silver).
stage("dim_customer", f"""
SELECT customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state
FROM {S}.customers""")

stage("dim_product", f"""
SELECT p.product_id,
       p.product_category_name,
       coalesce(t.product_category_name_english, p.product_category_name, 'unknown'),
       p.product_weight_g, p.product_length_cm, p.product_height_cm, p.product_width_cm, p.product_photos_qty
FROM {S}.products p
LEFT JOIN {S}.product_category_translation t ON t.product_category_name = p.product_category_name""")

stage("dim_seller", f"""
SELECT seller_id, seller_zip_code_prefix, seller_city, seller_state
FROM {S}.sellers""")

# One row per day from the first to the last purchase day. day_of_week: 1 = Sunday ... 7 = Saturday (Spark's dayofweek).
stage("dim_date", f"""
SELECT CAST(date_format(d, 'yyyyMMdd') AS INT), d, year(d), quarter(d), month(d),
       date_format(d, 'MMMM'), date_format(d, 'yyyy-MM'), dayofmonth(d), dayofweek(d),
       date_format(d, 'EEEE'), dayofweek(d) IN (1, 7)
FROM (SELECT explode(sequence(MIN(CAST(order_purchase_timestamp AS DATE)),
                              MAX(CAST(order_purchase_timestamp AS DATE)),
                              INTERVAL 1 DAY)) AS d
      FROM {S}.orders)""")
print("dimensions staged")

# COMMAND ----------

# Cell 5 - BUILD the facts as views (from silver).
# fact_order_items: one row per item line; the order gives the customer, the day and the status.
stage("fact_order_items", f"""
SELECT i.order_id, i.order_item_id, o.customer_id, i.product_id, i.seller_id,
       CAST(date_format(o.order_purchase_timestamp, 'yyyyMMdd') AS INT),
       o.order_status, i.price, i.freight_value,
       CAST(i.price + i.freight_value AS DECIMAL(12,2)),
       i.shipping_limit_date
FROM {S}.order_items i
JOIN {S}.orders o ON o.order_id = i.order_id""")

# fact_orders: one row per order. Items, payments and the review are each reduced to one row per order BEFORE the
# join, so nothing is multiplied (an order with 3 items and 2 payments would otherwise count each payment 3 times).
# The review: the newest by creation date, then answer time, then review_id - a full tie rule, so the pick is repeatable.
stage("fact_orders", f"""
WITH items AS (
    SELECT order_id, COUNT(*) AS items, SUM(price) AS items_value, SUM(freight_value) AS freight_value
    FROM {S}.order_items GROUP BY order_id),
pay AS (
    SELECT order_id, SUM(payment_value) AS payment_value
    FROM {S}.order_payments GROUP BY order_id),
rev AS (
    SELECT order_id, review_score,
           ROW_NUMBER() OVER (PARTITION BY order_id
                              ORDER BY review_creation_date DESC, review_answer_timestamp DESC, review_id) AS rn
    FROM {S}.order_reviews)
SELECT o.order_id, o.customer_id,
       CAST(date_format(o.order_purchase_timestamp, 'yyyyMMdd') AS INT),
       o.order_status,
       o.order_purchase_timestamp, o.order_approved_at, o.order_delivered_carrier_date,
       o.order_delivered_customer_date, o.order_estimated_delivery_date,
       CAST(coalesce(i.items, 0) AS INT),
       CAST(coalesce(i.items_value, 0) AS DECIMAL(14,2)),
       CAST(coalesce(i.freight_value, 0) AS DECIMAL(14,2)),
       CAST(coalesce(i.items_value, 0) + coalesce(i.freight_value, 0) AS DECIMAL(14,2)),
       CAST(coalesce(p.payment_value, 0) AS DECIMAL(14,2)),
       r.review_score,
       CASE WHEN o.order_delivered_customer_date IS NOT NULL
            THEN datediff(CAST(o.order_delivered_customer_date AS DATE), CAST(o.order_purchase_timestamp AS DATE)) END,
       CASE WHEN o.order_delivered_customer_date IS NOT NULL
            THEN CAST(o.order_delivered_customer_date AS DATE) > CAST(o.order_estimated_delivery_date AS DATE) END
FROM {S}.orders o
LEFT JOIN items i ON i.order_id = o.order_id
LEFT JOIN pay   p ON p.order_id = o.order_id
LEFT JOIN rev   r ON r.order_id = o.order_id AND r.rn = 1""")
print("facts staged")

# COMMAND ----------

# Cell 6 - BUILD the KPI tables as views, from the new facts and dimensions only (gold is built from gold from here on).
stage("kpi_monthly_sales", f"""
SELECT d.year_month, MIN(d.date) AS month_start,
       COUNT(*), SUM(f.items),
       CAST(SUM(f.order_value) AS DECIMAL(16,2)),
       CAST(SUM(f.order_value) / COUNT(*) AS DECIMAL(12,2))
FROM new_fact_orders f
JOIN new_dim_date d ON d.date_key = f.purchase_date_key
WHERE f.{SALE}
GROUP BY d.year_month""")

stage("kpi_delivery_by_state", f"""
SELECT c.customer_state,
       COUNT(*),
       CAST(AVG(f.delivery_days * 1.0) AS DECIMAL(6,2)),
       SUM(CASE WHEN f.is_late THEN 1 ELSE 0 END),
       CAST(100.0 * SUM(CASE WHEN f.is_late THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(5,2))
FROM new_fact_orders f
JOIN new_dim_customer c ON c.customer_id = f.customer_id
WHERE f.order_status = 'delivered' AND f.delivered_customer_ts IS NOT NULL
GROUP BY c.customer_state""")

# Seller ranking. The review belongs to the order: each (seller, order) pair is taken once, so an order with three
# items from one seller counts its review once. RANK: equal revenue gets the equal rank, the next rank is skipped.
stage("kpi_seller_ranking", f"""
WITH sales AS (
    SELECT seller_id, order_id, COUNT(*) AS items, SUM(item_total) AS revenue
    FROM new_fact_order_items WHERE {SALE}
    GROUP BY seller_id, order_id),
per_seller AS (
    SELECT s.seller_id, COUNT(*) AS orders, SUM(s.items) AS items, SUM(s.revenue) AS revenue,
           AVG(f.review_score * 1.0) AS avg_review
    FROM sales s JOIN new_fact_orders f ON f.order_id = s.order_id
    GROUP BY s.seller_id)
SELECT p.seller_id, d.seller_state, p.orders, p.items,
       CAST(p.revenue AS DECIMAL(16,2)), CAST(p.avg_review AS DECIMAL(4,2)),
       CAST(RANK() OVER (ORDER BY p.revenue DESC) AS INT)
FROM per_seller p JOIN new_dim_seller d ON d.seller_id = p.seller_id""")
print("KPI tables staged")

# COMMAND ----------

# Cell 7 - CHECK: the new gold reconciles to silver. Every line must be true; otherwise the run fails here,
# and no gold table has been touched.
def one(sql):
    return spark.sql(sql).first()[0]

checks = [
    ("fact_order_items rows = silver order_items rows",
     one("SELECT COUNT(*) FROM new_fact_order_items"), one(f"SELECT COUNT(*) FROM {S}.order_items")),
    ("fact_orders rows = silver orders rows",
     one("SELECT COUNT(*) FROM new_fact_orders"), one(f"SELECT COUNT(*) FROM {S}.orders")),
    ("sum of price: fact_order_items = silver",
     one("SELECT SUM(price) FROM new_fact_order_items"), one(f"SELECT SUM(price) FROM {S}.order_items")),
    ("sum of freight: fact_order_items = silver",
     one("SELECT SUM(freight_value) FROM new_fact_order_items"), one(f"SELECT SUM(freight_value) FROM {S}.order_items")),
    ("sum of order_value: fact_orders = fact_order_items",
     one("SELECT SUM(order_value) FROM new_fact_orders"), one("SELECT SUM(item_total) FROM new_fact_order_items")),
    ("sum of payments: fact_orders = silver",
     one("SELECT SUM(payment_value) FROM new_fact_orders"), one(f"SELECT SUM(payment_value) FROM {S}.order_payments")),
    ("revenue: kpi_monthly_sales = fact_orders (sales only)",
     one("SELECT SUM(revenue) FROM new_kpi_monthly_sales"), one(f"SELECT CAST(SUM(order_value) AS DECIMAL(16,2)) FROM new_fact_orders WHERE {SALE}")),
    ("revenue: kpi_seller_ranking = fact_order_items (sales only)",
     one("SELECT SUM(revenue) FROM new_kpi_seller_ranking"), one(f"SELECT CAST(SUM(item_total) AS DECIMAL(16,2)) FROM new_fact_order_items WHERE {SALE}")),
    ("item lines with no customer in dim_customer", one("""SELECT COUNT(*) FROM new_fact_order_items f
        LEFT JOIN new_dim_customer d ON d.customer_id = f.customer_id WHERE d.customer_id IS NULL"""), 0),
    ("item lines with no product in dim_product", one("""SELECT COUNT(*) FROM new_fact_order_items f
        LEFT JOIN new_dim_product d ON d.product_id = f.product_id WHERE d.product_id IS NULL"""), 0),
    ("item lines with no seller in dim_seller", one("""SELECT COUNT(*) FROM new_fact_order_items f
        LEFT JOIN new_dim_seller d ON d.seller_id = f.seller_id WHERE d.seller_id IS NULL"""), 0),
    ("orders with no day in dim_date", one("""SELECT COUNT(*) FROM new_fact_orders f
        LEFT JOIN new_dim_date d ON d.date_key = f.purchase_date_key WHERE d.date_key IS NULL"""), 0),
    ("orders with no customer in dim_customer", one("""SELECT COUNT(*) FROM new_fact_orders f
        LEFT JOIN new_dim_customer d ON d.customer_id = f.customer_id WHERE d.customer_id IS NULL"""), 0),
]
result = [(name, str(got), str(expected), got == expected) for name, got, expected in checks]
display(spark.createDataFrame(result, "check string, gold string, expected string, passed boolean"))
failed = [r[0] for r in result if not r[3]]
if failed:
    raise RuntimeError("Gold does not reconcile to silver, so nothing was written: " + "; ".join(failed))
print(f"all {len(result)} checks passed")

# COMMAND ----------

# Cell 8 - PUBLISH: only now are the gold tables written, each from its checked view, in the build order.
for t in GOLD_TABLES:
    spark.sql(f"INSERT OVERWRITE {G}.{t} SELECT * FROM new_{t}")

# The write is nine statements, not one transaction: compare every table with its view before calling the run good.
not_equal = [t for t in GOLD_TABLES
             if one(f"SELECT COUNT(*) FROM {G}.{t}") != one(f"SELECT COUNT(*) FROM new_{t}")]
if not_equal:
    raise RuntimeError("Gold was written but these tables do not hold what was checked: " + ", ".join(not_equal))
print(f"{len(GOLD_TABLES)} gold tables published")

# COMMAND ----------

# Cell 9 - LOOK: what one run produced, one row per gold table.
display(spark.sql(" UNION ALL ".join(
    f"SELECT '{t}' AS table_name, COUNT(*) AS rows FROM {G}.{t}" for t in GOLD_TABLES)))
