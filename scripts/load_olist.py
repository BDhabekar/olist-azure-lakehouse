"""
Olist loader -> Azure SQL or a local SQL Server, schema `olist`.

Loads all 9 CSVs from the Kaggle Brazilian E-Commerce dataset.
No primary or foreign keys are declared, on purpose: the keys are
established later, in the lakehouse, by a grain census - not
assumed from the files.

Usage:
    pip install -r scripts/requirements.txt
    export OLIST_SQL_SERVER="yourserver.database.windows.net"
    export OLIST_SQL_DB="yourdb"
    export OLIST_SQL_USER="youruser"
    export OLIST_SQL_PASSWORD="yourpassword"
    python load_olist.py /path/to/csv/folder

Local SQL Server (this project's on-premises source, database
OlistOLTP). Two optional switches:
    OLIST_SQL_TRUSTED=yes     Windows authentication: no user, no
                              password, no port (the driver signs in
                              as the Windows account running Python).
    OLIST_SQL_TRUST_CERT=yes  accept the server's self-signed TLS
                              certificate (a laptop SQL Server has no
                              CA-issued one). Leave unset for Azure SQL.
The wrapper that sets these for the laptop is
sql/local-source/02_load_olist_archive.cmd
"""

import os
import sys
import time

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

# ---------------------------------------------------------------
# File -> table name. Keys are the CSV filenames as Kaggle ships
# them; values are the short table names used everywhere else.
# ---------------------------------------------------------------
FILES = {
    "olist_customers_dataset.csv":            "customers",
    "olist_geolocation_dataset.csv":          "geolocation",
    "olist_order_items_dataset.csv":          "order_items",
    "olist_order_payments_dataset.csv":       "order_payments",
    "olist_order_reviews_dataset.csv":        "order_reviews",
    "olist_orders_dataset.csv":               "orders",
    "olist_products_dataset.csv":             "products",
    "olist_sellers_dataset.csv":              "sellers",
    "product_category_name_translation.csv":  "product_category_translation",
}

# Approximate row counts, for the verification step. If your load
# lands well outside these, something truncated silently.
EXPECTED = {
    "customers":                     99_441,
    "geolocation":                1_000_163,
    "order_items":                  112_650,
    "order_payments":               103_886,
    "order_reviews":                 99_224,
    "orders":                        99_441,
    "products":                      32_951,
    "sellers":                        3_095,
    "product_category_translation":      71,
}

# Timestamp columns, loaded as text then converted in SQL.
TIMESTAMP_COLS = {
    "orders": [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ],
    "order_items": ["shipping_limit_date"],
    "order_reviews": [
        "review_creation_date",
        "review_answer_timestamp",
    ],
}


def build_engine():
    server = os.environ["OLIST_SQL_SERVER"]
    database = os.environ["OLIST_SQL_DB"]
    yes = ("yes", "true", "1")
    trusted = os.environ.get("OLIST_SQL_TRUSTED", "no").lower() in yes
    trust_cert = (
        "yes"
        if os.environ.get("OLIST_SQL_TRUST_CERT", "no").lower() in yes
        else "no"
    )
    query = {
        "driver": "ODBC Driver 18 for SQL Server",
        "Encrypt": "yes",
        "TrustServerCertificate": trust_cert,
    }

    if trusted:
        # Windows authentication. With no user in the URL, SQLAlchemy
        # adds Trusted_Connection=Yes to the ODBC string by itself.
        url = URL.create("mssql+pyodbc", host=server, database=database,
                         query=query)
    else:
        # URL.create escapes the user and the password, so characters
        # such as @ : / # in a password cannot break the connection URL.
        url = URL.create(
            "mssql+pyodbc",
            username=os.environ["OLIST_SQL_USER"],
            password=os.environ["OLIST_SQL_PASSWORD"],
            host=server,
            port=1433,
            database=database,
            query=query,
        )
    # fast_executemany batches inserts instead of one round trip
    # per row. On geolocation (1M rows) this is the difference
    # between ~4 minutes and ~40.
    return create_engine(url, fast_executemany=True)


def ensure_schema(engine):
    with engine.begin() as conn:
        conn.execute(
            text(
                "IF NOT EXISTS (SELECT 1 FROM sys.schemas "
                "WHERE name = 'olist') EXEC('CREATE SCHEMA olist')"
            )
        )
    print("schema olist ready")


def load_one(engine, csv_path, table):
    # dtype=str keeps everything as text on the way in. Numeric
    # and timestamp conversion happens deliberately, in SQL,
    # where the failures are visible.
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False,
                     na_values=[""])

    start = time.time()
    df.to_sql(
        table,
        engine,
        schema="olist",
        if_exists="replace",   # rerunnable: drop and reload
        index=False,
        chunksize=1000,
    )
    secs = time.time() - start
    print(f"  {table:<30} {len(df):>9,} rows  {secs:6.1f}s")
    return len(df)


def convert_types(engine):
    """Numeric and datetime conversion, done in SQL so failures
    surface as NULLs you can count rather than silent coercion."""
    numeric = {
        "order_items": [("price", "DECIMAL(10,2)"),
                        ("freight_value", "DECIMAL(10,2)"),
                        ("order_item_id", "INT")],
        "order_payments": [("payment_sequential", "INT"),
                           ("payment_installments", "INT"),
                           ("payment_value", "DECIMAL(10,2)")],
        "order_reviews": [("review_score", "INT")],
        "products": [("product_weight_g", "INT"),
                     ("product_length_cm", "INT"),
                     ("product_height_cm", "INT"),
                     ("product_width_cm", "INT"),
                     ("product_photos_qty", "INT"),
                     ("product_name_lenght", "INT"),
                     ("product_description_lenght", "INT")],
        "geolocation": [("geolocation_lat", "FLOAT"),
                        ("geolocation_lng", "FLOAT")],
    }

    with engine.begin() as conn:
        for table, cols in numeric.items():
            for col, sqltype in cols:
                conn.execute(text(
                    f"UPDATE olist.{table} SET {col} = "
                    f"NULLIF(LTRIM(RTRIM({col})), '')"
                ))
                conn.execute(text(
                    f"ALTER TABLE olist.{table} "
                    f"ALTER COLUMN {col} {sqltype}"
                ))

        for table, cols in TIMESTAMP_COLS.items():
            for col in cols:
                conn.execute(text(
                    f"UPDATE olist.{table} SET {col} = "
                    f"NULLIF(LTRIM(RTRIM({col})), '')"
                ))
                conn.execute(text(
                    f"ALTER TABLE olist.{table} "
                    f"ALTER COLUMN {col} DATETIME2"
                ))
    print("type conversion done")


def verify(engine, loaded):
    print("\nverification")
    ok = True
    with engine.connect() as conn:
        for table, expected in EXPECTED.items():
            actual = conn.execute(
                text(f"SELECT COUNT(*) FROM olist.{table}")
            ).scalar()
            flag = "ok" if actual == loaded.get(table) else "MISMATCH"
            if actual != loaded.get(table):
                ok = False
            drift = "" if abs(actual - expected) < 50 else "  <- check"
            print(f"  {table:<30} {actual:>9,}  "
                  f"(expect ~{expected:,}) {flag}{drift}")
    print("\nall tables match the dataframe counts" if ok
          else "\nSOMETHING TRUNCATED - investigate before going on")


def main():
    folder = sys.argv[1] if len(sys.argv) > 1 else "."
    engine = build_engine()
    ensure_schema(engine)

    loaded = {}
    print("\nloading")
    for filename, table in FILES.items():
        path = os.path.join(folder, filename)
        if not os.path.exists(path):
            print(f"  MISSING: {filename}")
            continue
        loaded[table] = load_one(engine, path, table)

    convert_types(engine)
    verify(engine, loaded)


if __name__ == "__main__":
    main()
