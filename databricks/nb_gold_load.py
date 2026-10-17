# Databricks notebook source
# MAGIC %md
# MAGIC # nb_gold_load — silver becomes the gold star schema, rebuilt and reconciled
# MAGIC
# MAGIC **What this notebook does.** Rebuilds every gold table from silver with `INSERT OVERWRITE`: dimensions, then facts, then KPI tables. Then it reconciles gold to silver and **fails the run** if any check fails — a failed job fails the Data Factory pipeline, and the failure alert mails.
# MAGIC
# MAGIC **Why a full rebuild.** Gold is derived and small (about 225,000 rows): a rebuild takes seconds, can never drift from silver, and a second run gives the same result. Incremental processing lives where the volume is — the watermark at ingestion, `COPY INTO` in bronze, the bookmark and `MERGE` in silver. At 100 times the data, gold would switch to silver's change data feed and a `MERGE` of the changed orders.
# MAGIC
# MAGIC **Run as** the load's service principal (the job's third task, after `silver_load`). It may write gold rows (`MODIFY`) but not create tables: the tables are defined by `nb_gold_setup`.
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
missing = [t for t in ["dim_customer", "dim_product", "dim_seller", "dim_date", "fact_order_items", "fact_orders",
                       "kpi_monthly_sales", "kpi_delivery_by_state", "kpi_seller_ranking"]
           if spark.sql(f"SELECT 1 FROM {CATALOG}.information_schema.tables WHERE table_schema = 'gold' AND table_name = '{t}'").first() is None]
if missing:
    raise ValueError(f"Gold tables missing: {', '.join(missing)}. The catalog's owner runs nb_gold_setup first.")
print(f"silver = {S}   gold = {G}")

# Orders that are sales. Canceled and unavailable orders are kept in the facts (with their status) but are not revenue.
SALE = "order_status NOT IN ('canceled', 'unavailable')"

# COMMAND ----------

# Cell 4 - BUILD the dimensions.
spark.sql(f"""
INSERT OVERWRITE {G}.dim_customer
SELECT customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state
FROM {S}.customers""")

spark.sql(f"""
INSERT OVERWRITE {G}.dim_product
SELECT p.product_id,
       p.product_category_name,
       coalesce(t.product_category_name_english, p.product_category_name, 'unknown'),
       p.product_weight_g, p.product_length_cm, p.product_height_cm, p.product_width_cm, p.product_photos_qty
FROM {S}.products p
LEFT JOIN {S}.product_category_translation t ON t.product_category_name = p.product_category_name""")

spark.sql(f"""
INSERT OVERWRITE {G}.dim_seller
SELECT seller_id, seller_zip_code_prefix, seller_city, seller_state
FROM {S}.sellers""")

# One row per day from the first to the last purchase day. day_of_week: 1 = Sunday ... 7 = Saturday (Spark's dayofweek).
spark.sql(f"""
INSERT OVERWRITE {G}.dim_date
SELECT CAST(date_format(d, 'yyyyMMdd') AS INT), d, year(d), quarter(d), month(d),
       date_format(d, 'MMMM'), date_format(d, 'yyyy-MM'), dayofmonth(d), dayofweek(d),
       date_format(d, 'EEEE'), dayofweek(d) IN (1, 7)
FROM (SELECT explode(sequence(MIN(CAST(order_purchase_timestamp AS DATE)),
                              MAX(CAST(order_purchase_timestamp AS DATE)),
                              INTERVAL 1 DAY)) AS d
      FROM {S}.orders)""")
print("dimensions rebuilt")

# COMMAND ----------

# Cell 5 - BUILD the facts.
# fact_order_items: one row per item line; the order gives the customer, the day and the status.
spark.sql(f"""
INSERT OVERWRITE {G}.fact_order_items
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
spark.sql(f"""
INSERT OVERWRITE {G}.fact_orders
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
print("facts rebuilt")

# COMMAND ----------

# Cell 6 - BUILD the KPI tables, from the facts only (gold is built from gold from here on).
spark.sql(f"""
INSERT OVERWRITE {G}.kpi_monthly_sales
SELECT d.year_month, MIN(d.date) AS month_start,
       COUNT(*), SUM(f.items),
       CAST(SUM(f.order_value) AS DECIMAL(16,2)),
       CAST(SUM(f.order_value) / COUNT(*) AS DECIMAL(12,2))
FROM {G}.fact_orders f
JOIN {G}.dim_date d ON d.date_key = f.purchase_date_key
WHERE f.{SALE}
GROUP BY d.year_month""")

spark.sql(f"""
INSERT OVERWRITE {G}.kpi_delivery_by_state
SELECT c.customer_state,
       COUNT(*),
       CAST(AVG(f.delivery_days * 1.0) AS DECIMAL(6,2)),
       SUM(CASE WHEN f.is_late THEN 1 ELSE 0 END),
       CAST(100.0 * SUM(CASE WHEN f.is_late THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(5,2))
FROM {G}.fact_orders f
JOIN {G}.dim_customer c ON c.customer_id = f.customer_id
WHERE f.order_status = 'delivered' AND f.delivered_customer_ts IS NOT NULL
GROUP BY c.customer_state""")

# Seller ranking. The review belongs to the order: each (seller, order) pair is taken once, so an order with three
# items from one seller counts its review once. RANK: equal revenue gets the equal rank, the next rank is skipped.
spark.sql(f"""
INSERT OVERWRITE {G}.kpi_seller_ranking
WITH sales AS (
    SELECT seller_id, order_id, COUNT(*) AS items, SUM(item_total) AS revenue
    FROM {G}.fact_order_items WHERE {SALE}
    GROUP BY seller_id, order_id),
per_seller AS (
    SELECT s.seller_id, COUNT(*) AS orders, SUM(s.items) AS items, SUM(s.revenue) AS revenue,
           AVG(f.review_score * 1.0) AS avg_review
    FROM sales s JOIN {G}.fact_orders f ON f.order_id = s.order_id
    GROUP BY s.seller_id)
SELECT p.seller_id, d.seller_state, p.orders, p.items,
       CAST(p.revenue AS DECIMAL(16,2)), CAST(p.avg_review AS DECIMAL(4,2)),
       CAST(RANK() OVER (ORDER BY p.revenue DESC) AS INT)
FROM per_seller p JOIN {G}.dim_seller d ON d.seller_id = p.seller_id""")
print("KPI tables rebuilt")

# COMMAND ----------

# Cell 7 - CHECK: gold reconciles to silver. Every line must be true; otherwise the run fails here.
def one(sql):
    return spark.sql(sql).first()[0]

checks = [
    ("fact_order_items rows = silver order_items rows",
     one(f"SELECT COUNT(*) FROM {G}.fact_order_items"), one(f"SELECT COUNT(*) FROM {S}.order_items")),
    ("fact_orders rows = silver orders rows",
     one(f"SELECT COUNT(*) FROM {G}.fact_orders"), one(f"SELECT COUNT(*) FROM {S}.orders")),
    ("sum of price: fact_order_items = silver",
     one(f"SELECT SUM(price) FROM {G}.fact_order_items"), one(f"SELECT SUM(price) FROM {S}.order_items")),
    ("sum of freight: fact_order_items = silver",
     one(f"SELECT SUM(freight_value) FROM {G}.fact_order_items"), one(f"SELECT SUM(freight_value) FROM {S}.order_items")),
    ("sum of order_value: fact_orders = fact_order_items",
     one(f"SELECT SUM(order_value) FROM {G}.fact_orders"), one(f"SELECT SUM(item_total) FROM {G}.fact_order_items")),
    ("sum of payments: fact_orders = silver",
     one(f"SELECT SUM(payment_value) FROM {G}.fact_orders"), one(f"SELECT SUM(payment_value) FROM {S}.order_payments")),
    ("revenue: kpi_monthly_sales = fact_orders (sales only)",
     one(f"SELECT SUM(revenue) FROM {G}.kpi_monthly_sales"), one(f"SELECT CAST(SUM(order_value) AS DECIMAL(16,2)) FROM {G}.fact_orders WHERE {SALE}")),
    ("revenue: kpi_seller_ranking = fact_order_items (sales only)",
     one(f"SELECT SUM(revenue) FROM {G}.kpi_seller_ranking"), one(f"SELECT CAST(SUM(item_total) AS DECIMAL(16,2)) FROM {G}.fact_order_items WHERE {SALE}")),
    ("item lines with no customer in dim_customer", one(f"""SELECT COUNT(*) FROM {G}.fact_order_items f
        LEFT JOIN {G}.dim_customer d ON d.customer_id = f.customer_id WHERE d.customer_id IS NULL"""), 0),
    ("item lines with no product in dim_product", one(f"""SELECT COUNT(*) FROM {G}.fact_order_items f
        LEFT JOIN {G}.dim_product d ON d.product_id = f.product_id WHERE d.product_id IS NULL"""), 0),
    ("item lines with no seller in dim_seller", one(f"""SELECT COUNT(*) FROM {G}.fact_order_items f
        LEFT JOIN {G}.dim_seller d ON d.seller_id = f.seller_id WHERE d.seller_id IS NULL"""), 0),
    ("orders with no day in dim_date", one(f"""SELECT COUNT(*) FROM {G}.fact_orders f
        LEFT JOIN {G}.dim_date d ON d.date_key = f.purchase_date_key WHERE d.date_key IS NULL"""), 0),
    ("orders with no customer in dim_customer", one(f"""SELECT COUNT(*) FROM {G}.fact_orders f
        LEFT JOIN {G}.dim_customer d ON d.customer_id = f.customer_id WHERE d.customer_id IS NULL"""), 0),
]
result = [(name, str(got), str(expected), got == expected) for name, got, expected in checks]
display(spark.createDataFrame(result, "check string, gold string, expected string, passed boolean"))
failed = [r[0] for r in result if not r[3]]
if failed:
    raise RuntimeError("Gold does not reconcile to silver: " + "; ".join(failed))
print(f"all {len(result)} checks passed")

# COMMAND ----------

# Cell 8 - LOOK: what one run produced, one row per gold table.
display(spark.sql(" UNION ALL ".join(
    f"SELECT '{t}' AS table_name, COUNT(*) AS rows FROM {G}.{t}" for t in
    ["dim_customer", "dim_product", "dim_seller", "dim_date", "fact_order_items", "fact_orders",
     "kpi_monthly_sales", "kpi_delivery_by_state", "kpi_seller_ranking"])))
