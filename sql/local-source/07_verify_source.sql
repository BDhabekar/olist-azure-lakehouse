/* =====================================================================
   Olist E-commerce Lakehouse on Azure
   LOCAL SOURCE · step 7 of 7 · verify src - run after the seed and after
   every replay
   ---------------------------------------------------------------------
   Run in : SSMS, connected to localhost, database OlistOLTP
   Returns: one result set, nine rows - a row count per src table, the
            count expected straight after the seed, and for the five
            changing tables the newest modified_at (the value ADF's
            watermark will pick up).
   After a replay the five changing tables are higher than
   expected_after_seed by exactly what the procedure reported, and
   newest_modified_at moves forward. The four reference tables never move.
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

SELECT 'orders' AS table_name, COUNT(*) AS row_count, 45430 AS expected_after_seed,
       CONVERT(VARCHAR(23), MAX(modified_at), 121) AS newest_modified_at
FROM src.orders
UNION ALL
SELECT 'order_items', COUNT(*), 51234, CONVERT(VARCHAR(23), MAX(modified_at), 121)
FROM src.order_items
UNION ALL
SELECT 'order_payments', COUNT(*), 47871, CONVERT(VARCHAR(23), MAX(modified_at), 121)
FROM src.order_payments
UNION ALL
SELECT 'order_reviews', COUNT(*), 45370, CONVERT(VARCHAR(23), MAX(modified_at), 121)
FROM src.order_reviews
UNION ALL
SELECT 'customers', COUNT(*), 45430, CONVERT(VARCHAR(23), MAX(modified_at), 121)
FROM src.customers
UNION ALL
SELECT 'products', COUNT(*), 32951, NULL
FROM src.products
UNION ALL
SELECT 'sellers', COUNT(*), 3095, NULL
FROM src.sellers
UNION ALL
SELECT 'product_category_translation', COUNT(*), 71, NULL
FROM src.product_category_translation
UNION ALL
SELECT 'geolocation', COUNT(*), 1000163, NULL
FROM src.geolocation;

-- The newest order in src: 2017-12-31 straight after the seed; the last
-- day of the replayed month after each replay.
SELECT MIN(order_purchase_timestamp) AS oldest_purchase,
       MAX(order_purchase_timestamp) AS newest_purchase
FROM src.orders;
GO
SET NOEXEC OFF;   -- end of the wrong-server guard
GO
