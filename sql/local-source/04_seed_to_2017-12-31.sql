/* =====================================================================
   Olist E-commerce Lakehouse on Azure
   LOCAL SOURCE · step 4 of 7 · seed src with history up to 31 Dec 2017
   ---------------------------------------------------------------------
   Run in : SSMS, connected to localhost, database OlistOLTP
   Needs  : olist.* loaded (step 2) and src.* created (step 3)
   Does   : copies into src every order PURCHASED BEFORE 2018-01-01, with
            the items, payments, reviews and customers that belong to
            those orders; copies the four reference tables in full.
            Every copied row gets the same modified_at - the moment this
            script ran - because that is when the row "arrived" in src.
   Runs once. To start over: run step 3 again (it empties src), then this.

   Step 5's procedure uses the same pattern with a one-month window
   instead of "before 2018".
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

SET NOCOUNT ON;      -- do not print "(n rows affected)" after each statement
SET XACT_ABORT ON;   -- any error rolls the whole transaction back

DECLARE @cutoff DATETIME2(0) = '2018-01-01';        -- first instant NOT included
DECLARE @stamp  DATETIME2(3) = SYSUTCDATETIME();    -- one timestamp for the whole seed

IF EXISTS (SELECT 1 FROM src.orders)
    THROW 50001, 'src.orders already has rows. The seed runs once. To start over, run 03_create_src_tables.sql again, then this file.', 1;

BEGIN TRANSACTION;

    -- 1. The parent: orders purchased before the cutoff.
    INSERT INTO src.orders
        (order_id, customer_id, order_status, order_purchase_timestamp,
         order_approved_at, order_delivered_carrier_date,
         order_delivered_customer_date, order_estimated_delivery_date, modified_at)
    SELECT o.order_id, o.customer_id, o.order_status, o.order_purchase_timestamp,
           o.order_approved_at, o.order_delivered_carrier_date,
           o.order_delivered_customer_date, o.order_estimated_delivery_date, @stamp
    FROM olist.orders AS o
    WHERE o.order_purchase_timestamp < @cutoff;

    -- 2. Children follow their order: a child row is copied when the order
    --    it belongs to was purchased before the cutoff. Each join goes from
    --    the child to ONE order, so it cannot multiply child rows.
    INSERT INTO src.order_items
        (order_id, order_item_id, product_id, seller_id,
         shipping_limit_date, price, freight_value, modified_at)
    SELECT i.order_id, i.order_item_id, i.product_id, i.seller_id,
           i.shipping_limit_date, i.price, i.freight_value, @stamp
    FROM olist.order_items AS i
    JOIN olist.orders AS o
      ON o.order_id = i.order_id
    WHERE o.order_purchase_timestamp < @cutoff;

    INSERT INTO src.order_payments
        (order_id, payment_sequential, payment_type,
         payment_installments, payment_value, modified_at)
    SELECT p.order_id, p.payment_sequential, p.payment_type,
           p.payment_installments, p.payment_value, @stamp
    FROM olist.order_payments AS p
    JOIN olist.orders AS o
      ON o.order_id = p.order_id
    WHERE o.order_purchase_timestamp < @cutoff;

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
    WHERE o.order_purchase_timestamp < @cutoff;

    -- 3. Customers: the customer record that each of those orders points to.
    INSERT INTO src.customers
        (customer_id, customer_unique_id, customer_zip_code_prefix,
         customer_city, customer_state, modified_at)
    SELECT c.customer_id, c.customer_unique_id, c.customer_zip_code_prefix,
           c.customer_city, c.customer_state, @stamp
    FROM olist.customers AS c
    JOIN olist.orders AS o
      ON o.customer_id = c.customer_id
    WHERE o.order_purchase_timestamp < @cutoff;

    -- 4. Reference tables: everything, no timestamp to stamp.
    INSERT INTO src.products
        (product_id, product_category_name, product_name_lenght,
         product_description_lenght, product_photos_qty, product_weight_g,
         product_length_cm, product_height_cm, product_width_cm)
    SELECT product_id, product_category_name, product_name_lenght,
           product_description_lenght, product_photos_qty, product_weight_g,
           product_length_cm, product_height_cm, product_width_cm
    FROM olist.products;

    INSERT INTO src.sellers
        (seller_id, seller_zip_code_prefix, seller_city, seller_state)
    SELECT seller_id, seller_zip_code_prefix, seller_city, seller_state
    FROM olist.sellers;

    INSERT INTO src.product_category_translation
        (product_category_name, product_category_name_english)
    SELECT product_category_name, product_category_name_english
    FROM olist.product_category_translation;

    INSERT INTO src.geolocation
        (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng,
         geolocation_city, geolocation_state)
    SELECT geolocation_zip_code_prefix, geolocation_lat, geolocation_lng,
           geolocation_city, geolocation_state
    FROM olist.geolocation;

COMMIT TRANSACTION;

PRINT 'Seed finished. Now run 07_verify_source.sql.';
GO
SET NOEXEC OFF;   -- end of the wrong-server guard
GO
