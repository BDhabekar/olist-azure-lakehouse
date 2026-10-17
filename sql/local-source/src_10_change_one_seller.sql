/* =====================================================================
   Olist E-commerce Lakehouse on Azure
   LOCAL SOURCE · src_10 · change ONE seller   (lab scaffolding)
   ---------------------------------------------------------------------
   Run in : SSMS, connected to localhost, database OlistOLTP. Press F5 once.
   When   : after the row_hash column has been added to the silver
            reference tables.
   Expect : three result grids -
              1. "before" : the seller, city santo andre, zip prefix 09080
              2. "after"  : the same seller, city sao bernardo do campo,
                            zip prefix 09750
              3. rows_changed = 1, rows_in_src_sellers unchanged (3,095)
   Safe to re-run: yes. A second run finds the seller already moved,
            changes nothing, and says so on the Messages tab.

   WHAT IT IS
   One UPDATE on one row of src.sellers: a seller moves to the next town.

   WHY IT EXISTS
   src.sellers has NO timestamp column - nothing in the row says it
   changed. The pipeline therefore copies the whole table on every run,
   and the lakehouse has to find the one changed row among 3,095 by
   comparing content. This script makes exactly one row different, so the
   proof can be read off one number: 1 row updated in silver.

   THE KEY IDEA
   A table without a last-modified column can only be loaded in full, and
   changes in it can only be found by comparison - a stored hash of the
   business columns is the standard way.

   AFTER THIS RUN the laptop's src.sellers differs from the public Olist
   archive by this one seller (city and zip prefix).
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

-- The first seller by seller_id.
DECLARE @seller_id VARCHAR(32) = '0015a82c2db000af6aaaf3ae2ecb0532';
DECLARE @changed   INT         = 0;

-- 1. The row before the change.
SELECT 'before' AS moment, seller_id, seller_zip_code_prefix, seller_city, seller_state
FROM src.sellers
WHERE seller_id = @seller_id;

-- 2. The change - only if the seller is there and still in santo andre.
IF NOT EXISTS (SELECT 1 FROM src.sellers WHERE seller_id = @seller_id)
    PRINT 'NOT CHANGED: this seller is not in src.sellers.';
ELSE IF NOT EXISTS (SELECT 1 FROM src.sellers WHERE seller_id = @seller_id AND seller_city = N'santo andre')
    PRINT 'NOT CHANGED: this seller is no longer in santo andre - an earlier run of this script already changed it.';
ELSE
BEGIN
    UPDATE src.sellers
    SET seller_city            = N'sao bernardo do campo',
        seller_zip_code_prefix = '09750'
    WHERE seller_id = @seller_id
      AND seller_city = N'santo andre';
    SET @changed = @@ROWCOUNT;                          -- rows the UPDATE just above changed
END;

-- 3. The row after the change, and the proof that nothing else moved.
SELECT 'after' AS moment, seller_id, seller_zip_code_prefix, seller_city, seller_state
FROM src.sellers
WHERE seller_id = @seller_id;

SELECT @changed AS rows_changed,
       (SELECT COUNT(*) FROM src.sellers) AS rows_in_src_sellers;
GO
SET NOEXEC OFF;   -- end of the wrong-server guard
GO

/* ---- To put the seller back (to undo this script) --------------------
   Commented out so F5 on the whole file can never undo by accident.     */
-- UPDATE src.sellers SET seller_city = N'santo andre', seller_zip_code_prefix = '09080' WHERE seller_id = '0015a82c2db000af6aaaf3ae2ecb0532' AND seller_city = N'sao bernardo do campo';
