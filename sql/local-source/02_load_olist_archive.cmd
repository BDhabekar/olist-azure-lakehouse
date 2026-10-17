@echo off
rem ======================================================================
rem  Olist E-commerce Lakehouse on Azure - LOCAL SOURCE - step 2 of 7
rem  Loads the 9 Olist CSV files into the SQL Server on THIS laptop:
rem    server localhost, database OlistOLTP, schema olist (1.55M rows).
rem  This is the full 2016-2018 history - the archive the replay reads.
rem  Auth: Windows authentication. No password is stored in this file.
rem  Needs: 01_create_database.sql already run (the database must exist).
rem  Safe to re-run: every table is dropped and reloaded.
rem  Usage: 02_load_olist_archive.cmd <folder with the 9 CSVs>
rem  Run from: any prompt with Python 3 and ODBC Driver 18 for SQL Server,
rem            after: pip install -r scripts\requirements.txt
rem ======================================================================
set OLIST_SQL_SERVER=localhost
set OLIST_SQL_DB=OlistOLTP
set OLIST_SQL_TRUSTED=yes
set OLIST_SQL_TRUST_CERT=yes
if "%~1"=="" (echo Usage: 02_load_olist_archive.cmd ^<folder with the 9 Olist CSV files^> & exit /b 1)
python "%~dp0..\..\scripts\load_olist.py" "%~1"
