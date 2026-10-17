/* =====================================================================
   Olist E-commerce Lakehouse on Azure
   LOCAL SOURCE · src_09 · change ONE existing order   (lab scaffolding)
   ---------------------------------------------------------------------
   Run in : SSMS, connected to localhost, database OlistOLTP. Press F5 once.
   When   : AFTER a month (here February 2018) has been replayed and
            loaded through to silver.
   Expect : three result grids -
              1. "before" : the order, order_status = shipped
              2. "after"  : the same order, order_status = delivered,
                            a delivery date, a NEW modified_at
              3. rows_changed = 1, rows_in_src_orders unchanged (59,427)
   Safe to re-run: yes. A second run finds the order already delivered,
            changes nothing, and says so on the Messages tab
            (rows_changed = 0).

   WHAT IT IS
   One UPDATE on one row of src.orders: an order that arrived in the
   February load as 'shipped' is now marked 'delivered', and - as a real
   application would - the row's modified_at is set to the time of the
   change.

   WHY IT EXISTS
   It stands in for the business. The replay procedure only ever ADDS
   rows, so until now the pipeline has never seen a row that CHANGED.
   This is the smallest possible change: one row, one table. In a job
   you never write this - the application does it thousands of times a
   day. It is test equipment, like etl.usp_replay_month.

   THE KEY IDEA
   The pipeline finds changed rows the same way it finds new ones:
   modified_at is later than the watermark. An application that changes
   a row WITHOUT moving modified_at is invisible to this pipeline - that
   is the weakness of every timestamp watermark, and the reason change
   data capture exists.

   AFTER THIS RUN the laptop's src.orders differs from the public Olist
   archive by this one order (status and delivery date).
   ===================================================================== */

-- Wrong-server guard. This file is for the SQL Server on the laptop.
-- On Azure SQL (engine edition 5) it stops here: SET NOEXEC ON makes
-- every statement below compile but not run.
IF CAST(SERVERPROPERTY('EngineEdition') AS INT) = 5
BEGIN
    RAISERROR('WRONG SERVER: this script is for the laptop SQL Server (localhost), not Azure SQL. Nothing below was run. Change the connection and press F5 again.', 16, 1);
    SET NOEXEC ON;
END;
GO

USE OlistOLTP;
GO

SET NOCOUNT ON;                                   -- no "(n rows affected)" messages

-- The earliest February 2018 order whose status is 'shipped'
-- (purchased 2018-02-01 22:40:26, handed to the carrier 2018-02-03).
DECLARE @order_id VARCHAR(32)  = 'a584b2acfafd53fc605e76baa4a5cfaf';
DECLARE @stamp    DATETIME2(3) = SYSUTCDATETIME();   -- the time of the change (UTC), millisecond precision like every modified_at
DECLARE @changed  INT          = 0;

-- 1. The row before the change.
SELECT 'before' AS moment, order_id, order_status, order_delivered_customer_date,
       CONVERT(VARCHAR(23), modified_at, 121) AS modified_at
FROM src.orders
WHERE order_id = @order_id;

-- 2. The change - only if the order is there and still 'shipped'.
IF NOT EXISTS (SELECT 1 FROM src.orders WHERE order_id = @order_id)
    PRINT 'NOT CHANGED: this order is not in src.orders. Replay February 2018 first: EXEC etl.usp_replay_month @month_start = ''2018-02-01'';';
ELSE IF NOT EXISTS (SELECT 1 FROM src.orders WHERE order_id = @order_id AND order_status = 'shipped')
    PRINT 'NOT CHANGED: this order is no longer ''shipped'' - an earlier run of this script already changed it.';
ELSE
BEGIN
    UPDATE src.orders
    SET order_status                  = 'delivered',
        order_delivered_customer_date = '2018-02-09 18:42:00',
        modified_at                   = @stamp          -- the line the whole pipeline depends on
    WHERE order_id = @order_id
      AND order_status = 'shipped';
    SET @changed = @@ROWCOUNT;                          -- rows the UPDATE just above changed
END;

-- 3. The row after the change, and the proof that nothing else moved.
SELECT 'after' AS moment, order_id, order_status, order_delivered_customer_date,
       CONVERT(VARCHAR(23), modified_at, 121) AS modified_at
FROM src.orders
WHERE order_id = @order_id;

SELECT @changed AS rows_changed,
       (SELECT COUNT(*) FROM src.orders) AS rows_in_src_orders;
GO
SET NOEXEC OFF;   -- end of the wrong-server guard
GO

/* ---- To put the order back (to undo this script) ---------------------
   Commented out so F5 on the whole file can never undo by accident.
   Highlight from UPDATE to the semicolon (dashes outside the selection)
   and press F5. It stamps modified_at again, so the next pipeline run
   carries the order a third time - as 'shipped'.                        */
-- UPDATE src.orders SET order_status = 'shipped', order_delivered_customer_date = NULL, modified_at = SYSUTCDATETIME() WHERE order_id = 'a584b2acfafd53fc605e76baa4a5cfaf' AND order_status = 'delivered';
