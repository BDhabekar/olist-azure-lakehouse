/* =====================================================================
   Olist E-commerce Lakehouse on Azure
   LOCAL SOURCE · src_11 · change ONE existing order, with a late-evening time
   (lab scaffolding: the test for TIMESTAMP_NTZ in silver)
   ---------------------------------------------------------------------
   Run in : SSMS, connected to localhost, database OlistOLTP. Press F5 once.
   When   : AFTER nb_silver_setup has converted dev's silver tables
            (cell 5), BEFORE the dev load that proves the conversion.
   Expect : three result grids -
              1. "before" : the chosen order, order_status = shipped
              2. "after"  : the same order, order_status = delivered,
                            order_delivered_customer_date = 2018-02-20 23:45:00,
                            a NEW modified_at
              3. rows_changed = 1, rows_in_src_orders unchanged
   Safe to re-run: yes. A second run finds an order already carrying
            2018-02-20 23:45:00, changes nothing, and says so (rows_changed = 0).

   WHY 23:45
   It is the time-zone test. Read as a UTC moment, 23:45 on Feb 20 is
   05:15 on Feb 21 in India. Silver stores it as TIMESTAMP_NTZ, so it
   must read 2018-02-20 23:45:00 - and DATE(...) = 2018-02-20 - in every
   session, whatever its time zone.

   The laptop's source is shared by the dev and prod pipelines: this
   one change reaches both. The dev load proves it first; the prod load
   after the prod conversion proves it again.
   ===================================================================== */

-- Wrong-server guard. This file is for the SQL Server on the laptop.
IF CAST(SERVERPROPERTY('EngineEdition') AS INT) = 5
BEGIN
    RAISERROR('WRONG SERVER: this script is for the laptop SQL Server (localhost), not Azure SQL. Nothing below was run. Change the connection and press F5 again.', 16, 1);
    SET NOEXEC ON;
END;
GO

USE OlistOLTP;
GO

SET NOCOUNT ON;

DECLARE @marker   DATETIME2(3) = '2018-02-20 23:45:00';   -- the late-evening delivery time
DECLARE @stamp    DATETIME2(3) = SYSUTCDATETIME();         -- the time of the change, like every modified_at
DECLARE @changed  INT          = 0;
DECLARE @order_id VARCHAR(32);

-- Already done? Then show that order and change nothing.
SELECT @order_id = order_id FROM src.orders WHERE order_delivered_customer_date = @marker;

IF @order_id IS NULL
    -- The earliest-purchased order still 'shipped' (ties broken by order_id, so the choice is repeatable).
    SELECT TOP 1 @order_id = order_id
    FROM src.orders
    WHERE order_status = 'shipped'
    ORDER BY order_purchase_timestamp, order_id;

-- 1. The row before the change.
SELECT 'before' AS moment, order_id, order_status,
       CONVERT(VARCHAR(23), order_delivered_customer_date, 121) AS order_delivered_customer_date,
       CONVERT(VARCHAR(23), modified_at, 121) AS modified_at
FROM src.orders WHERE order_id = @order_id;

-- 2. The change - only if the order is still 'shipped'.
IF EXISTS (SELECT 1 FROM src.orders WHERE order_id = @order_id AND order_status = 'shipped')
BEGIN
    UPDATE src.orders
    SET order_status                  = 'delivered',
        order_delivered_customer_date = @marker,
        modified_at                   = @stamp
    WHERE order_id = @order_id AND order_status = 'shipped';
    SET @changed = @@ROWCOUNT;
END
ELSE
    PRINT 'NOT CHANGED: an earlier run of this script already changed this order.';

-- 3. The row after the change, and the proof that nothing else moved.
SELECT 'after' AS moment, order_id, order_status,
       CONVERT(VARCHAR(23), order_delivered_customer_date, 121) AS order_delivered_customer_date,
       CONVERT(VARCHAR(23), modified_at, 121) AS modified_at
FROM src.orders WHERE order_id = @order_id;

SELECT @changed AS rows_changed, (SELECT COUNT(*) FROM src.orders) AS rows_in_src_orders;
GO
SET NOEXEC OFF;   -- end of the wrong-server guard
GO
