/* =====================================================================
   Olist E-commerce Lakehouse on Azure
   LOCAL SOURCE · step 6 of 7 · a read-only identity for Azure Data Factory
   ---------------------------------------------------------------------
   Run in : SSMS, connected to localhost, database OlistOLTP

   LOGIN vs USER - two different objects:
   * LOGIN = an identity at the SERVER level: "may this name connect to
     this SQL Server at all?" It carries the password.
   * USER  = that login's identity inside ONE DATABASE: "what may it do
     here?" Permissions are granted to the user.

   Part A is done in the SSMS screens, NOT in this file, because it needs
   a password and a password must never be saved in a file that goes to
   Git. Part B (below) has no secret in it.

   PART A - create the login (SSMS screens, 2 minutes)
     Object Explorer -> localhost -> Security -> right-click Logins
       -> New Login...
     Login name                 : adf_reader
     SQL Server authentication  : selected
     Password                   : a long one you generate now; you paste
                                  the same value into the Key Vault
                                  secret sql-olist-adf-reader-password
     Enforce password policy    : ticked
     Enforce password expiration: UNTICKED (a service identity that
                                  expires stops the pipeline at 2 a.m.)
     User must change password  : UNTICKED
     Default database           : OlistOLTP
     -> OK

   PART B - run this file: the database user and the single permission.
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

IF USER_ID('adf_reader') IS NULL
    CREATE USER adf_reader FOR LOGIN adf_reader;
GO

-- Least privilege: read the src schema and nothing else. No write, no
-- access to the olist archive, no access to the etl procedure.
GRANT SELECT ON SCHEMA::src TO adf_reader;
GO

/* ---------- prove it - run these THREE blocks one at a time ----------
   EXECUTE AS USER makes this window act as adf_reader until REVERT.     */

-- Block 1: must return a number (the read works).
EXECUTE AS USER = 'adf_reader';
SELECT COUNT(*) AS orders_visible_to_adf_reader FROM src.orders;
REVERT;
GO

-- Block 2: must FAIL with "The SELECT permission was denied" - the
-- archive is not readable by the pipeline identity.
EXECUTE AS USER = 'adf_reader';
SELECT COUNT(*) FROM olist.orders;
REVERT;
GO

-- Block 3: run after Block 2 failed, to be sure the window is you again.
-- Must show your Windows account or dbo, NOT adf_reader. If it shows
-- adf_reader, run REVERT; once more.
SELECT USER_NAME() AS i_am;
GO
SET NOEXEC OFF;   -- end of the wrong-server guard
GO
