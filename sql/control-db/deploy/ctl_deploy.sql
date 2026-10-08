/* =====================================================================
   Olist E-commerce Lakehouse on Azure
   CONTROL DATABASE - the deployment script (run by the pipeline)
   ---------------------------------------------------------------------
   Run by : the deployment pipeline (cicd/scripts/deploy-control-db.ps1),
            against the control database of ONE environment, signed in
            as the deployment identity. Two values are handed in:
              $(ExpectedDb)   the database this run is meant for
              $(FactoryUser)  the Data Factory whose managed identity
                              reads and writes schema etl
   Makes  : schema etl with
              etl.ingest_config  WHAT to load: one row per source table -
                                 how it is copied, its key, and how far it
                                 has been copied (the watermark)
              etl.run_log        WHAT HAPPENED: one row per table per
                                 pipeline run
              etl.usp_log_run, etl.usp_update_watermark
   Safe to run again: yes - that is the point. Every statement looks at
   what exists first. On a database that is up to date it changes nothing.

   WHAT IT NEVER TOUCHES: last_watermark and the run log. They are the
   pipeline's memory, not configuration. A deployment that reset a
   watermark would make the next run copy the whole source again.

   WHAT IT DOES OWN: the list of tables (configuration as code). A table
   is added, switched off or given another key by changing the VALUES
   list below and merging the change - the same in every environment.
   ===================================================================== */
SET NOCOUNT ON;

-- Wrong-database guard. The run stops here unless it is connected to the
-- Azure SQL database it was started for.
DECLARE @db SYSNAME = DB_NAME();
IF CAST(SERVERPROPERTY('EngineEdition') AS INT) <> 5 OR @db <> N'$(ExpectedDb)'
BEGIN
    RAISERROR('WRONG SERVER OR DATABASE: connected to "%s", expected the Azure SQL control database "$(ExpectedDb)". Nothing was run.', 16, 1, @db);
    SET NOEXEC ON;
END;
GO

IF SCHEMA_ID('etl') IS NULL EXEC('CREATE SCHEMA etl');
GO

IF OBJECT_ID('etl.ingest_config') IS NULL
CREATE TABLE etl.ingest_config (
    table_name        VARCHAR(100)  NOT NULL PRIMARY KEY,
    source_schema     VARCHAR(50)   NOT NULL,
    load_type         VARCHAR(20)   NOT NULL
        CHECK (load_type IN ('incremental', 'full')),
    watermark_column  VARCHAR(100)  NOT NULL,               -- 'modified_at', or 'none' for a full load
    last_watermark    DATETIME2(3)  NOT NULL,               -- copied up to and including this instant (UTC)
    active            BIT           NOT NULL,               -- 1 = the pipeline loads it, 0 = skipped
    updated_at        DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME(),
    key_columns       VARCHAR(400)  NOT NULL                -- the column(s) that identify one row; 'none' = no key
        CONSTRAINT DF_ingest_config_key_columns DEFAULT ('none')
);
GO

-- A database created before the key was kept here gets the column now.
IF COL_LENGTH('etl.ingest_config', 'key_columns') IS NULL
    ALTER TABLE etl.ingest_config
        ADD key_columns VARCHAR(400) NOT NULL
            CONSTRAINT DF_ingest_config_key_columns DEFAULT ('none');
GO

IF OBJECT_ID('etl.run_log') IS NULL
CREATE TABLE etl.run_log (
    log_id          BIGINT IDENTITY(1,1) PRIMARY KEY,
    run_id          VARCHAR(50)     NOT NULL,           -- the parent pipeline's run id: the same for all tables of one run
    table_name      VARCHAR(100)    NOT NULL,
    load_type       VARCHAR(20)     NOT NULL,
    status          VARCHAR(20)     NOT NULL
        CHECK (status IN ('succeeded', 'failed')),
    rows_copied     BIGINT          NULL,               -- NULL when the copy failed
    watermark_from  DATETIME2(3)    NULL,               -- copied rows are ABOVE this...
    watermark_to    DATETIME2(3)    NULL,               -- ...and up to and including this
    error_message   NVARCHAR(4000)  NULL,               -- NULL on success
    logged_at       DATETIME2(3)    NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

-- Writes one row to the run log. Data Factory calls it once per table per run,
-- on the success path or on the failure path.
CREATE OR ALTER PROCEDURE etl.usp_log_run
    @run_id          VARCHAR(50),
    @table_name      VARCHAR(100),
    @load_type       VARCHAR(20),
    @status          VARCHAR(20),
    @rows_copied     BIGINT          = NULL,
    @watermark_from  DATETIME2(3)    = NULL,
    @watermark_to    DATETIME2(3)    = NULL,
    @error_message   NVARCHAR(4000)  = NULL
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO etl.run_log
        (run_id, table_name, load_type, status, rows_copied,
         watermark_from, watermark_to, error_message)
    VALUES
        (@run_id, @table_name, @load_type, @status, @rows_copied,
         @watermark_from, @watermark_to, @error_message);
END;
GO

-- Records how far a table has been copied. Three rules:
--   1. called only AFTER a successful copy (the pipeline's green arrow);
--   2. forward only - an older value changes nothing, so a late re-run cannot drag the watermark back;
--   3. an unknown table is an error, not a silent update of zero rows.
CREATE OR ALTER PROCEDURE etl.usp_update_watermark
    @table_name     VARCHAR(100),
    @new_watermark  DATETIME2(3)
AS
BEGIN
    SET NOCOUNT ON;

    IF NOT EXISTS (SELECT 1 FROM etl.ingest_config WHERE table_name = @table_name)
        THROW 50010, 'Table is not in etl.ingest_config. Watermark not updated.', 1;

    UPDATE etl.ingest_config
    SET last_watermark = @new_watermark,
        updated_at     = SYSUTCDATETIME()
    WHERE table_name     = @table_name
      AND last_watermark <= @new_watermark;
END;
GO

-- The list of tables. A new row starts at watermark 1900-01-01, so its first
-- run copies everything. An existing row keeps its watermark, whatever changes.
-- geolocation: a million rows of reference data with no key; it is not loaded.
MERGE etl.ingest_config AS c
USING (VALUES
        ('orders',                       'src', 'incremental', 'modified_at', 'order_id',                    1),
        ('order_items',                  'src', 'incremental', 'modified_at', 'order_id,order_item_id',      1),
        ('order_payments',               'src', 'incremental', 'modified_at', 'order_id,payment_sequential', 1),
        ('order_reviews',                'src', 'incremental', 'modified_at', 'review_id,order_id',          1),
        ('customers',                    'src', 'incremental', 'modified_at', 'customer_id',                 1),
        ('products',                     'src', 'full',        'none',        'product_id',                  1),
        ('sellers',                      'src', 'full',        'none',        'seller_id',                   1),
        ('product_category_translation', 'src', 'full',        'none',        'product_category_name',       1),
        ('geolocation',                  'src', 'full',        'none',        'none',                        0)
      ) AS d (table_name, source_schema, load_type, watermark_column, key_columns, active)
   ON c.table_name = d.table_name
WHEN NOT MATCHED BY TARGET THEN
    INSERT (table_name, source_schema, load_type, watermark_column, last_watermark, active, key_columns)
    VALUES (d.table_name, d.source_schema, d.load_type, d.watermark_column, '1900-01-01', d.active, d.key_columns)
WHEN MATCHED AND (   c.source_schema    <> d.source_schema
                  OR c.load_type        <> d.load_type
                  OR c.watermark_column <> d.watermark_column
                  OR c.key_columns      <> d.key_columns
                  OR c.active           <> d.active) THEN
    UPDATE SET source_schema    = d.source_schema,
               load_type        = d.load_type,
               watermark_column = d.watermark_column,
               key_columns      = d.key_columns,
               active           = d.active,
               updated_at       = SYSUTCDATETIME();
GO

-- The Data Factory may read schema etl and execute its procedures - nothing else.
-- It gets no direct INSERT or UPDATE: the procedures do the writing.
IF DATABASE_PRINCIPAL_ID(N'$(FactoryUser)') IS NULL
    RAISERROR('The Data Factory identity [$(FactoryUser)] is not a user of this database. The server''s Microsoft Entra admin creates it once: CREATE USER [$(FactoryUser)] FROM EXTERNAL PROVIDER.', 16, 1);
ELSE
BEGIN
    GRANT SELECT  ON SCHEMA::etl TO [$(FactoryUser)];
    GRANT EXECUTE ON SCHEMA::etl TO [$(FactoryUser)];
END;
GO

-- What the database holds after this deployment.
SELECT table_name, load_type, key_columns, active, last_watermark
FROM   etl.ingest_config
ORDER  BY load_type DESC, table_name;
GO
SET NOEXEC OFF;
GO
