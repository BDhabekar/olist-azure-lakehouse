/* =====================================================================
   Olist E-commerce Lakehouse on Azure
   LOCAL SOURCE · step 3 of 7 · create the nine "live application" tables
   ---------------------------------------------------------------------
   Run in : SSMS, connected to localhost, database OlistOLTP
   Creates: src.orders, src.order_items, src.order_payments,
            src.order_reviews, src.customers      <- change every day
            src.products, src.sellers,
            src.product_category_translation,
            src.geolocation                       <- reference data
   Safe to re-run: yes, but it EMPTIES src (drops and recreates all nine).

   Three decisions made here:

   1. modified_at DATETIME2(3) on the five tables that change.
      It is the WATERMARK COLUMN: "when was this row last written?".
      ADF copies only rows whose modified_at is later than the last value
      it saw. DATETIME2(3) = millisecond precision, on purpose: the value
      travels SQL Server -> ADF -> Azure SQL -> ADF -> SQL Server as text,
      and 3 decimals survive that round trip exactly. A 7-decimal value
      that gets cut to 3 on the way would make ">" skip or repeat rows.

   2. No modified_at on products, sellers, translation, geolocation.
      Many real reference tables have no timestamp. With nothing to
      compare, the only honest load is a full copy every run.

   3. No PRIMARY KEY, no FOREIGN KEY - deliberately. The database will
      not tell you what is unique; the grain census in the lakehouse (what
      one row represents, table by table) decides the keys silver uses.

   Types: ids are 32-character text, so VARCHAR(32). Money is
   DECIMAL(10,2), never FLOAT (FLOAT cannot store 0.10 exactly). Zip
   prefixes stay text - '01037' as a number would lose its leading zero.
   ===================================================================== */

-- Wrong-server guard. This file is for the SQL Server on the
-- laptop. On Azure SQL (engine edition 5) it stops here: SET NOEXEC ON
-- makes every statement below compile but not run.
IF CAST(SERVERPROPERTY('EngineEdition') AS INT) = 5
BEGIN
    RAISERROR('WRONG SERVER: this script is for the laptop SQL Server (localhost), not Azure SQL. Nothing below was run. Change the connection and press F5 again.', 16, 1);
    SET NOEXEC ON;
END;
GO

USE OlistOLTP;
GO

DROP TABLE IF EXISTS src.orders;
DROP TABLE IF EXISTS src.order_items;
DROP TABLE IF EXISTS src.order_payments;
DROP TABLE IF EXISTS src.order_reviews;
DROP TABLE IF EXISTS src.customers;
DROP TABLE IF EXISTS src.products;
DROP TABLE IF EXISTS src.sellers;
DROP TABLE IF EXISTS src.product_category_translation;
DROP TABLE IF EXISTS src.geolocation;
GO

/* ---------- the five tables that change (incremental load) ---------- */

CREATE TABLE src.orders (
    order_id                       VARCHAR(32)   NOT NULL,
    customer_id                    VARCHAR(32)   NOT NULL,
    order_status                   VARCHAR(20)   NOT NULL,
    order_purchase_timestamp       DATETIME2(0)  NOT NULL,
    order_approved_at              DATETIME2(0)  NULL,
    order_delivered_carrier_date   DATETIME2(0)  NULL,
    order_delivered_customer_date  DATETIME2(0)  NULL,
    order_estimated_delivery_date  DATETIME2(0)  NOT NULL,
    modified_at                    DATETIME2(3)  NOT NULL
);

CREATE TABLE src.order_items (
    order_id             VARCHAR(32)    NOT NULL,
    order_item_id        INT            NOT NULL,
    product_id           VARCHAR(32)    NOT NULL,
    seller_id            VARCHAR(32)    NOT NULL,
    shipping_limit_date  DATETIME2(0)   NOT NULL,
    price                DECIMAL(10,2)  NOT NULL,
    freight_value        DECIMAL(10,2)  NOT NULL,
    modified_at          DATETIME2(3)   NOT NULL
);

CREATE TABLE src.order_payments (
    order_id              VARCHAR(32)    NOT NULL,
    payment_sequential    INT            NOT NULL,
    payment_type          VARCHAR(20)    NOT NULL,
    payment_installments  INT            NOT NULL,
    payment_value         DECIMAL(10,2)  NOT NULL,
    modified_at           DATETIME2(3)   NOT NULL
);

CREATE TABLE src.order_reviews (
    review_id                VARCHAR(32)     NOT NULL,
    order_id                 VARCHAR(32)     NOT NULL,
    review_score             INT             NOT NULL,
    review_comment_title     NVARCHAR(100)   NULL,
    review_comment_message   NVARCHAR(1000)  NULL,
    review_creation_date     DATETIME2(0)    NOT NULL,
    review_answer_timestamp  DATETIME2(0)    NOT NULL,
    modified_at              DATETIME2(3)    NOT NULL
);

CREATE TABLE src.customers (
    customer_id               VARCHAR(32)   NOT NULL,
    customer_unique_id        VARCHAR(32)   NOT NULL,
    customer_zip_code_prefix  VARCHAR(5)    NOT NULL,
    customer_city             NVARCHAR(60)  NOT NULL,
    customer_state            CHAR(2)       NOT NULL,
    modified_at               DATETIME2(3)  NOT NULL
);

/* ---------- reference tables with no timestamp (full load) ---------- */

CREATE TABLE src.products (
    product_id                  VARCHAR(32)   NOT NULL,
    product_category_name       NVARCHAR(60)  NULL,
    product_name_lenght         INT           NULL,   -- "lenght": the source's own spelling, kept as-is
    product_description_lenght  INT           NULL,
    product_photos_qty          INT           NULL,
    product_weight_g            INT           NULL,
    product_length_cm           INT           NULL,
    product_height_cm           INT           NULL,
    product_width_cm            INT           NULL
);

CREATE TABLE src.sellers (
    seller_id               VARCHAR(32)   NOT NULL,
    seller_zip_code_prefix  VARCHAR(5)    NOT NULL,
    seller_city             NVARCHAR(60)  NOT NULL,
    seller_state            CHAR(2)       NOT NULL
);

CREATE TABLE src.product_category_translation (
    product_category_name          NVARCHAR(60)  NOT NULL,
    product_category_name_english  VARCHAR(60)   NOT NULL
);

CREATE TABLE src.geolocation (
    geolocation_zip_code_prefix  VARCHAR(5)    NOT NULL,
    geolocation_lat              FLOAT         NOT NULL,
    geolocation_lng              FLOAT         NOT NULL,
    geolocation_city             NVARCHAR(60)  NOT NULL,
    geolocation_state            CHAR(2)       NOT NULL
);
GO

/* ---------- an index on every watermark column ----------------------
   The incremental query filters on modified_at. Without an index SQL
   Server reads the whole table on every run, however few rows are new.
   With one it jumps straight to the new rows. In production this is the
   first thing a database administrator asks an ingestion team for.     */

CREATE INDEX ix_orders_modified_at          ON src.orders          (modified_at);
CREATE INDEX ix_order_items_modified_at     ON src.order_items     (modified_at);
CREATE INDEX ix_order_payments_modified_at  ON src.order_payments  (modified_at);
CREATE INDEX ix_order_reviews_modified_at   ON src.order_reviews   (modified_at);
CREATE INDEX ix_customers_modified_at       ON src.customers       (modified_at);
GO

-- Proof: nine rows, every row_count 0.
SELECT t.name AS table_name, SUM(p.rows) AS row_count
FROM sys.tables AS t
JOIN sys.partitions AS p
  ON p.object_id = t.object_id AND p.index_id IN (0, 1)
WHERE SCHEMA_NAME(t.schema_id) = 'src'
GROUP BY t.name
ORDER BY t.name;
GO
SET NOEXEC OFF;   -- end of the wrong-server guard
GO
