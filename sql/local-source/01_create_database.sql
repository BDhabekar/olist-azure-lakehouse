/* =====================================================================
   Olist E-commerce Lakehouse on Azure
   LOCAL SOURCE · step 1 of 7 · create the database and its three schemas
   ---------------------------------------------------------------------
   Run in : SSMS, connected to  localhost  (Windows authentication)
   Creates: database OlistOLTP on this laptop's SQL Server, with schemas
              olist = full 2016-2018 history (the archive we replay from)
              src   = the "live application" tables that ADF reads
              etl   = helper procedure that simulates new business days
   Safe to re-run: yes (every statement checks before creating).
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

IF DB_ID('OlistOLTP') IS NULL
    CREATE DATABASE OlistOLTP;
GO

-- SIMPLE recovery = the transaction log is emptied at each checkpoint.
-- Right for a lab that loads 1M rows and never takes log backups.
-- Production OLTP databases run FULL recovery so they can restore to a
-- point in time; you would never switch that on someone else's source.
ALTER DATABASE OlistOLTP SET RECOVERY SIMPLE;
GO

USE OlistOLTP;
GO

IF SCHEMA_ID('olist') IS NULL EXEC('CREATE SCHEMA olist');
IF SCHEMA_ID('src')   IS NULL EXEC('CREATE SCHEMA src');
IF SCHEMA_ID('etl')   IS NULL EXEC('CREATE SCHEMA etl');
GO

-- Proof: three rows, one per schema.
SELECT name AS schema_name
FROM sys.schemas
WHERE name IN ('olist', 'src', 'etl')
ORDER BY name;
GO
SET NOEXEC OFF;   -- end of the wrong-server guard
GO
