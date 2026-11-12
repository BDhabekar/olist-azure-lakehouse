"""nb_gold_load on made-up rows: the numbers it must produce, and what it must not do.

The rows are small enough to work out by hand. Four orders:
  o1  delivered on time   2 items (10.00 + 1.00, 20.00 + 2.00)   paid in two parts (20.00 + 13.00)   two reviews (5, then 1)
  o2  delivered late      1 item  (50.00 + 5.00)                 paid 55.00                          one review (4)
  o3  canceled            1 item  (40.00 + 4.00)                 paid 44.00
  o4  shipped             no item lines                          paid 5.00
"""
import pytest

from conftest import CATALOG, NOTEBOOKS, run_notebook

S, G = f"{CATALOG}.silver", f"{CATALOG}.gold"
GOLD_TABLES = ["dim_customer", "dim_product", "dim_seller", "dim_date", "fact_order_items", "fact_orders",
               "kpi_monthly_sales", "kpi_delivery_by_state", "kpi_seller_ranking"]
FILE = "'f.parquet', TIMESTAMP'2026-10-08 00:00:00'"     # _source_file, _source_file_time
MOD = "TIMESTAMP_NTZ'2026-10-08 00:00:00'"               # modified_at


def ntz(text):
    return f"TIMESTAMP_NTZ'{text}'"


def load_silver(spark):
    spark.sql(f"""INSERT INTO {S}.customers VALUES
        ('c1', 'u1', '01000', 'sao paulo', 'SP', {MOD}, {FILE}),
        ('c2', 'u2', '20000', 'rio de janeiro', 'RJ', {MOD}, {FILE}),
        ('c3', 'u3', '01001', 'sao paulo', 'SP', {MOD}, {FILE})""")
    spark.sql(f"""INSERT INTO {S}.orders VALUES
        ('o1', 'c1', 'delivered', {ntz('2018-01-05 10:00:00')}, {ntz('2018-01-05 10:05:00')}, {ntz('2018-01-06 08:00:00')},
               {ntz('2018-01-10 12:00:00')}, {ntz('2018-01-12 00:00:00')}, {MOD}, {FILE}),
        ('o2', 'c2', 'delivered', {ntz('2018-01-31 23:30:00')}, {ntz('2018-01-31 23:40:00')}, {ntz('2018-02-02 08:00:00')},
               {ntz('2018-02-20 23:45:00')}, {ntz('2018-02-15 00:00:00')}, {MOD}, {FILE}),
        ('o3', 'c3', 'canceled',  {ntz('2018-02-03 09:00:00')}, NULL, NULL, NULL, {ntz('2018-02-20 00:00:00')}, {MOD}, {FILE}),
        ('o4', 'c1', 'shipped',   {ntz('2018-02-10 09:00:00')}, {ntz('2018-02-10 09:10:00')}, {ntz('2018-02-11 08:00:00')},
               NULL, {ntz('2018-02-25 00:00:00')}, {MOD}, {FILE})""")
    ship = ntz('2018-01-08 00:00:00')
    spark.sql(f"""INSERT INTO {S}.order_items VALUES
        ('o1', 1, 'p1', 's1', {ship}, 10.00, 1.00, {MOD}, {FILE}),
        ('o1', 2, 'p2', 's1', {ship}, 20.00, 2.00, {MOD}, {FILE}),
        ('o2', 1, 'p1', 's2', {ship}, 50.00, 5.00, {MOD}, {FILE}),
        ('o3', 1, 'p2', 's2', {ship}, 40.00, 4.00, {MOD}, {FILE})""")
    spark.sql(f"""INSERT INTO {S}.order_payments VALUES
        ('o1', 1, 'credit_card', 1, 20.00, {MOD}, {FILE}),
        ('o1', 2, 'voucher',     1, 13.00, {MOD}, {FILE}),
        ('o2', 1, 'credit_card', 3, 55.00, {MOD}, {FILE}),
        ('o3', 1, 'boleto',      1, 44.00, {MOD}, {FILE}),
        ('o4', 1, 'credit_card', 1,  5.00, {MOD}, {FILE})""")
    spark.sql(f"""INSERT INTO {S}.order_reviews VALUES
        ('r1', 'o1', 5, NULL, NULL, {ntz('2018-01-11 00:00:00')}, {ntz('2018-01-12 10:00:00')}, {MOD}, {FILE}),
        ('r2', 'o1', 1, NULL, NULL, {ntz('2018-01-12 00:00:00')}, {ntz('2018-01-13 10:00:00')}, {MOD}, {FILE}),
        ('r3', 'o2', 4, NULL, NULL, {ntz('2018-02-21 00:00:00')}, {ntz('2018-02-22 10:00:00')}, {MOD}, {FILE})""")
    spark.sql(f"""INSERT INTO {S}.products VALUES
        ('p1', 'beleza_saude', 10, 100, 1, 500, 10, 10, 10, {FILE}, 'h1'),
        ('p2', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, {FILE}, 'h2')""")
    spark.sql(f"""INSERT INTO {S}.sellers VALUES
        ('s1', '01002', 'sao paulo', 'SP', {FILE}, 'h1'),
        ('s2', '30000', 'belo horizonte', 'MG', {FILE}, 'h2')""")
    spark.sql(f"INSERT INTO {S}.product_category_translation VALUES ('beleza_saude', 'health_beauty', {FILE}, 'h1')")


def run_gold(spark, catalog=CATALOG):
    return run_notebook(spark, "nb_gold_load.py", {"p_catalog": catalog})


def rows(spark, sql):
    return [tuple(r) for r in spark.sql(sql).collect()]


def snapshot(spark):
    return {t: sorted(map(str, rows(spark, f"SELECT * FROM {G}.{t}"))) for t in GOLD_TABLES}


@pytest.fixture()
def loaded(lake):
    load_silver(lake)
    return lake


def test_row_counts_of_every_gold_table(loaded):
    run_gold(loaded)
    counts = {t: loaded.table(f"{G}.{t}").count() for t in GOLD_TABLES}
    assert counts == {
        "dim_customer": 3, "dim_product": 2, "dim_seller": 2,
        "dim_date": 37,                      # 2018-01-05 to 2018-02-10, every day
        "fact_order_items": 4, "fact_orders": 4,
        "kpi_monthly_sales": 2, "kpi_delivery_by_state": 2, "kpi_seller_ranking": 2,
    }


def test_an_order_with_two_items_and_two_payments_counts_each_payment_once(loaded):
    run_gold(loaded)
    assert rows(loaded, f"""SELECT items, CAST(items_value AS STRING), CAST(freight_value AS STRING),
                                   CAST(order_value AS STRING), CAST(payment_value AS STRING)
                            FROM {G}.fact_orders WHERE order_id = 'o1'""") == [(2, "30.00", "3.00", "33.00", "33.00")]


def test_the_newest_review_of_an_order_is_the_one_kept(loaded):
    run_gold(loaded)
    assert rows(loaded, f"SELECT review_score FROM {G}.fact_orders WHERE order_id = 'o1'") == [(1,)]


def test_an_order_without_item_lines_stays_in_fact_orders_with_zero_value(loaded):
    run_gold(loaded)
    assert rows(loaded, f"""SELECT items, CAST(order_value AS STRING), CAST(payment_value AS STRING), review_score
                            FROM {G}.fact_orders WHERE order_id = 'o4'""") == [(0, "0.00", "5.00", None)]


def test_delivery_days_and_the_late_flag(loaded):
    run_gold(loaded)
    assert rows(loaded, f"""SELECT order_id, purchase_date_key, delivery_days, is_late
                            FROM {G}.fact_orders ORDER BY order_id""") == [
        ("o1", 20180105, 5, False),
        ("o2", 20180131, 20, True),        # purchased late on Jan 31: the day key must not slip to February
        ("o3", 20180203, None, None),
        ("o4", 20180210, None, None),
    ]


def test_canceled_orders_are_not_revenue(loaded):
    run_gold(loaded)
    assert rows(loaded, f"""SELECT year_month, orders, items, CAST(revenue AS STRING), CAST(avg_order_value AS STRING)
                            FROM {G}.kpi_monthly_sales ORDER BY year_month""") == [
        ("2018-01", 2, 3, "88.00", "44.00"),
        ("2018-02", 1, 0, "0.00", "0.00"),     # o4 only: o3 is canceled
    ]


def test_delivery_by_state_counts_delivered_orders_only(loaded):
    run_gold(loaded)
    assert rows(loaded, f"""SELECT customer_state, delivered_orders, CAST(avg_delivery_days AS STRING), late_orders, CAST(late_pct AS STRING)
                            FROM {G}.kpi_delivery_by_state ORDER BY customer_state""") == [
        ("RJ", 1, "20.00", 1, "100.00"),
        ("SP", 1, "5.00", 0, "0.00"),
    ]


def test_seller_ranking_leaves_out_canceled_sales_and_counts_a_review_once_per_order(loaded):
    run_gold(loaded)
    assert rows(loaded, f"""SELECT seller_id, seller_state, orders, items, CAST(revenue AS STRING), CAST(avg_review_score AS STRING), revenue_rank
                            FROM {G}.kpi_seller_ranking ORDER BY revenue_rank""") == [
        ("s2", "MG", 1, 1, "55.00", "4.00", 1),
        ("s1", "SP", 1, 2, "33.00", "1.00", 2),     # two items of one order: its review (1) is counted once
    ]


def test_a_product_without_a_category_is_named_unknown(loaded):
    run_gold(loaded)
    assert rows(loaded, f"SELECT product_id, category_name_english FROM {G}.dim_product ORDER BY product_id") == [
        ("p1", "health_beauty"), ("p2", "unknown")]


def test_a_second_run_gives_the_same_gold(loaded):
    run_gold(loaded)
    first = snapshot(loaded)
    run_gold(loaded)
    assert snapshot(loaded) == first


def test_a_failed_check_writes_nothing(loaded):
    run_gold(loaded)
    before = snapshot(loaded)
    # An item line whose order is not in silver.orders: fact_order_items would lose it, so the row-count check fails.
    # A new customer goes in as well: had the run written anything, dim_customer would now hold four rows.
    loaded.sql(f"INSERT INTO {S}.order_items VALUES ('ghost', 1, 'p1', 's1', {ntz('2018-01-08 00:00:00')}, 9.00, 1.00, {MOD}, {FILE})")
    loaded.sql(f"INSERT INTO {S}.customers VALUES ('c9', 'u9', '01009', 'sao paulo', 'SP', {MOD}, {FILE})")
    with pytest.raises(RuntimeError, match="nothing was written.*fact_order_items rows = silver order_items rows"):
        run_gold(loaded)
    assert snapshot(loaded) == before


def test_a_failed_check_on_the_first_run_leaves_gold_empty(lake):
    load_silver(lake)
    lake.sql(f"INSERT INTO {S}.order_items VALUES ('ghost', 1, 'p1', 's1', {ntz('2018-01-08 00:00:00')}, 9.00, 1.00, {MOD}, {FILE})")
    with pytest.raises(RuntimeError, match="nothing was written"):
        run_gold(lake)
    assert all(lake.table(f"{G}.{t}").count() == 0 for t in GOLD_TABLES)


def test_a_missing_gold_table_stops_the_run_before_anything_is_built(loaded):
    loaded.sql(f"DROP TABLE {G}.kpi_seller_ranking")
    with pytest.raises(ValueError, match="Gold tables missing: kpi_seller_ranking"):
        run_gold(loaded)
    assert loaded.table(f"{G}.dim_customer").count() == 0


@pytest.mark.parametrize("catalog", ["", "   ", "olist; DROP", "a.b"])
def test_a_catalog_that_is_not_a_plain_name_is_refused(loaded, catalog):
    with pytest.raises(ValueError, match="p_catalog"):
        run_gold(loaded, catalog)


def test_the_notebook_writes_gold_only_after_the_checks():
    text = (NOTEBOOKS / "nb_gold_load.py").read_text(encoding="utf-8")
    assert text.count("INSERT OVERWRITE {G}") == 1
    assert text.index('raise RuntimeError("Gold does not reconcile') < text.index("INSERT OVERWRITE {G}")
