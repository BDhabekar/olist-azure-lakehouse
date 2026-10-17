/* =====================================================================
   Olist E-commerce Lakehouse on Azure
   LOCAL SOURCE · optional · back up the OlistOLTP database to one file
   ---------------------------------------------------------------------
   Run in : SSMS, connected to localhost (any database selected)

   You do NOT need this to keep your work. SQL Server writes every
   committed change to its own files on disk the moment a statement
   finishes; the database is still there after you close SSMS or restart
   the laptop. A BACKUP is a second copy in a single .bak file, for the
   day the laptop's disk fails or you want to go back to an earlier state
   without re-running the scripts.

   The file name has no folder, so SQL Server writes it to its default
   backup folder - the one place its service account is sure to have
   write permission. The SELECT below prints the full path.

   The .bak file does not go into Git: it is a large binary. What goes
   into Git is the set of scripts 01-07, which rebuild the database from
   the CSV files in about five minutes. The scripts are the ground truth.
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

-- 1. Take the backup. INIT = overwrite the previous OlistOLTP.bak;
--    COMPRESSION = smaller file; CHECKSUM = verify pages while writing;
--    STATS = 10 prints progress every 10 percent.
BACKUP DATABASE OlistOLTP
TO DISK = N'OlistOLTP.bak'
WITH INIT, COMPRESSION, CHECKSUM, STATS = 10;
GO

-- 2. Where it went, when, and how big.
SELECT TOP (1)
       bs.database_name,
       bs.backup_finish_date,
       CAST(bs.compressed_backup_size / 1048576.0 AS DECIMAL(10,1)) AS size_mb,
       bmf.physical_device_name AS backup_file
FROM msdb.dbo.backupset AS bs
JOIN msdb.dbo.backupmediafamily AS bmf
  ON bmf.media_set_id = bs.media_set_id
WHERE bs.database_name = 'OlistOLTP'
ORDER BY bs.backup_finish_date DESC;
GO

/* 3. To go back to that copy later (this REPLACES the current database,
      so it is commented out - highlight from RESTORE to the semicolon
      and press F5 only when you mean it; close other query windows that
      use OlistOLTP first):

-- RESTORE DATABASE OlistOLTP FROM DISK = N'OlistOLTP.bak' WITH REPLACE;
*/
GO
SET NOEXEC OFF;   -- end of the wrong-server guard
GO
