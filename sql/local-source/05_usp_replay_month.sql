/* =====================================================================
   Olist E-commerce Lakehouse on Azure
   LOCAL SOURCE · step 5 of 7 · etl.usp_replay_month   (lab scaffolding)
   ---------------------------------------------------------------------
   Run in : SSMS, connected to localhost, database OlistOLTP. Press F5 once.
   Expect : "Commands completed successfully", then TWO red error messages
            (Msg 50003 and Msg 50002). Both are the guards working - see
            the tests at the bottom. Nothing in src changes when you run
            this file.

   WHAT IT IS
   A stored procedure that copies ONE CALENDAR MONTH of orders - with the
   items, payments, reviews and customers that belong to those orders -
   from the archive (olist.*) into the live tables (src.*), stamping
   every copied row's modified_at with the time of the call. To Data
   Factory those rows look exactly like new business.

   WHY IT EXISTS
   It stands in for customers. Nobody is placing orders in this database,
   so without it the pipeline has nothing new to pick up and "incremental"
   cannot be proved. In a job you never write this: the source belongs to
   another team and real users create the changes. It is test equipment.

   HOW IT IS USED
       EXEC etl.usp_replay_month @month_start = '2018-01-01';
   returns five rows: table_name, rows_inserted.

   THE KEY IDEA
   Every row of one call gets the SAME modified_at, and all five tables
   are written in ONE transaction. So a watermark can never fall between
   an order and its own items: Data Factory sees the whole month or none
   of it.
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

CREATE OR ALTER PROCEDURE etl.usp_replay_month
    @month_start DATE                       -- first day of the month to replay, e.g. '2018-01-01'
AS
BEGIN
    SET NOCOUNT ON;                         -- no "(n rows affected)" messages
    SET XACT_ABORT ON;                      -- any error rolls the whole transaction back

    -- The month is the half-open range [@month_start, @month_end):
    -- start included, first day of the next month excluded.
    DECLARE @month_end DATE         = DATEADD(MONTH, 1, @month_start);
    DECLARE @stamp     DATETIME2(3) = SYSUTCDATETIME();     -- ONE timestamp for the whole call
    DECLARE @n_orders INT, @n_items INT, @n_payments INT, @n_reviews INT, @n_customers INT;

    -- Guard 1: the parameter must be the first day of a month.
    IF @month_start IS NULL OR DAY(@month_start) <> 1
        THROW 50003, '@month_start must be the first day of a month. Nothing replayed.', 1;

    -- Guard 2: a month already in src must not be replayed again -
    -- every row would be inserted twice.
    IF EXISTS (SELECT 1
               FROM src.orders
               WHERE order_purchase_timestamp >= @month_start
                 AND order_purchase_timestamp <  @month_end)
        THROW 50002, 'That month is already in src.orders. Nothing replayed.', 1;

    BEGIN TRANSACTION;

        -- 1. The orders purchased in the month.
        INSERT INTO src.orders
            (order_id, customer_id, order_status, order_purchase_timestamp,
             order_approved_at, order_delivered_carrier_date,
             order_delivered_customer_date, order_estimated_delivery_date, modified_at)
        SELECT o.order_id, o.customer_id, o.order_status, o.order_purchase_timestamp,
               o.order_approved_at, o.order_delivered_carrier_date,
               o.order_delivered_customer_date, o.order_estimated_delivery_date, @stamp
        FROM olist.orders AS o
        WHERE o.order_purchase_timestamp >= @month_start
          AND o.order_purchase_timestamp <  @month_end;
        SET @n_orders = @@ROWCOUNT;         -- rows the statement just above inserted

        -- 2. Children follow their order's purchase month, not their own dates.
        INSERT INTO src.order_items
            (order_id, order_item_id, product_id, seller_id,
             shipping_limit_date, price, freight_value, modified_at)
        SELECT i.order_id, i.order_item_id, i.product_id, i.seller_id,
               i.shipping_limit_date, i.price, i.freight_value, @stamp
        FROM olist.order_items AS i
        JOIN olist.orders AS o
          ON o.order_id = i.order_id
        WHERE o.order_purchase_timestamp >= @month_start
          AND o.order_purchase_timestamp <  @month_end;
        SET @n_items = @@ROWCOUNT;

        INSERT INTO src.order_payments
            (order_id, payment_sequential, payment_type,
             payment_installments, payment_value, modified_at)
        SELECT p.order_id, p.payment_sequential, p.payment_type,
               p.payment_installments, p.payment_value, @stamp
        FROM olist.order_payments AS p
        JOIN olist.orders AS o
          ON o.order_id = p.order_id
        WHERE o.order_purchase_timestamp >= @month_start
          AND o.order_purchase_timestamp <  @month_end;
        SET @n_payments = @@ROWCOUNT;

        INSERT INTO src.order_reviews
            (review_id, order_id, review_score, review_comment_title,
             review_comment_message, review_creation_date,
             review_answer_timestamp, modified_at)
        SELECT r.review_id, r.order_id, r.review_score, r.review_comment_title,
               r.review_comment_message, r.review_creation_date,
               r.review_answer_timestamp, @stamp
        FROM olist.order_reviews AS r
        JOIN olist.orders AS o
          ON o.order_id = r.order_id
        WHERE o.order_purchase_timestamp >= @month_start
          AND o.order_purchase_timestamp <  @month_end;
        SET @n_reviews = @@ROWCOUNT;

        -- 3. The customer record each of those orders points to.
        INSERT INTO src.customers
            (customer_id, customer_unique_id, customer_zip_code_prefix,
             customer_city, customer_state, modified_at)
        SELECT c.customer_id, c.customer_unique_id, c.customer_zip_code_prefix,
               c.customer_city, c.customer_state, @stamp
        FROM olist.customers AS c
        JOIN olist.orders AS o
          ON o.customer_id = c.customer_id
        WHERE o.order_purchase_timestamp >= @month_start
          AND o.order_purchase_timestamp <  @month_end;
        SET @n_customers = @@ROWCOUNT;

    COMMIT TRANSACTION;

    -- What the caller sees: five rows, in a fixed order.
    SELECT table_name, rows_inserted
    FROM (
        SELECT 1 AS sort_no, 'orders' AS table_name, @n_orders AS rows_inserted
        UNION ALL SELECT 2, 'order_items',    @n_items
        UNION ALL SELECT 3, 'order_payments', @n_payments
        UNION ALL SELECT 4, 'order_reviews',  @n_reviews
        UNION ALL SELECT 5, 'customers',      @n_customers
    ) AS r
    ORDER BY sort_no;
END;
GO

/* ---------- two tests that change NOTHING ----------------------------
   Each call is stopped by a guard before any row is inserted, so both
   end in a red error message. That is the pass condition.              */

-- Test 1 - not the first day of a month. Expect: Msg 50003.
EXEC etl.usp_replay_month @month_start = '2018-01-15';
GO

-- Test 2 - December 2017 is already in src (the seed put it there).
-- Expect: Msg 50002.
EXEC etl.usp_replay_month @month_start = '2017-12-01';
GO
SET NOEXEC OFF;   -- end of the wrong-server guard
GO
