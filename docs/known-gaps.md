# Known gaps, and what changes at scale

[Back to the README](../README.md)

## Known gaps

Each of these is a choice I made knowingly. The last column is what I would build.

| Gap | Why it is not in this version | The design when built |
|---|---|---|
| Deletes at the source | A timestamp watermark cannot see a deleted row | SQL Server change tracking, read by Data Factory, and a delete branch (`WHEN NOT MATCHED BY SOURCE`, or a soft delete) in the silver `MERGE` |
| Retention | Full-load reference tables are re-copied each run, so bronze holds `products` once per run | Change detection at ingestion (copy only when the hash changes), old full-load copies removed from bronze, an ADLS lifecycle rule on landing files, scheduled `VACUUM` |
| `geolocation` | 1 million rows, no key and no consumer in gold | It is not loaded in dev or prod (`active = 0` in `etl.ingest_config`); its silver table is defined and empty. It becomes a gold table (one point per zip prefix) if a map is needed |
| The load identity owns the bronze tables | The load creates a bronze table the first time it meets one | Bronze tables created by the set-up step; the load gets `MODIFY` only |
| Storage open to all networks, shared-key access on, RA-GRS replication | Portal defaults, on a lab subscription with no private network | Private endpoints or a firewall; shared-key access off (every access here is by identity); replication chosen for the recovery need |
| Workspaces without secure cluster connectivity | No Public IP is set to No, so no NAT gateway is billed while idle | Secure cluster connectivity (No Public IP) with VNet injection and private endpoints to storage |
| One runtime node on a laptop | Lab constraint | A self-hosted runtime on always-on servers, with two or more nodes for high availability |
| `trustServerCertificate = true` | The laptop's SQL Server has a self-signed certificate | A CA-issued certificate with validation on |
| One reviewer | It is a solo project, so I approve my own pull requests | Required reviews from a second engineer |
| One Unity Catalog metastore for dev and prod | One metastore per region is the Databricks model | The same. Isolation comes from a catalog per environment bound to its own workspace, which this project already does |
| Set-up notebooks run by hand | `nb_uc_bootstrap`, `nb_silver_setup` and `nb_gold_setup` change table definitions and run as the catalog owner, once per environment | A schema-migration step in the pipeline, running as a dedicated owner identity |
| The daily schedule is stopped | Cost on a student subscription; every load was started by hand | The schedule switched on, through a pull request |
| Gold is written as nine statements | Each table is overwritten on its own. A run lost between two of them leaves gold part new and part old until the next run rebuilds all nine. The load compares every table with what it checked and fails on a difference | One transaction across the nine tables, or a staging schema that is swapped in |
| A transaction open for longer than the lag | The watermark stays two minutes behind the clock. A source transaction that takes longer than that to commit can still be skipped | Change tracking or CDC, which read the database's log and do not depend on a timestamp. It is the same fix as for deletes |
| Quarantined rows are released by hand | After the cause is fixed, the catalog owner moves the table's bookmark back (see the [runbook](runbook.md)) | A reprocess step that reads quarantine again when a rule or the source changes |
| Dev is not loaded by the pipeline before prod | A load needs a cluster, and no deployment starts one. I load dev myself before approving prod | A smoke load in dev as a pipeline stage, on a small fixed data set |
| Two tools are not pinned | Two deployment steps ask for the latest Azure PowerShell, and the `SqlServer` PowerShell module is installed without a version | Both pinned in the repository, like the other tools |
| The tests run only the gold load | Bronze and silver use `COPY INTO` and catalog views, which a local Spark does not have | The same tests against a test catalog in Databricks, or the load logic moved into functions that take DataFrames |

## What changes at 100 times the data

Ingestion would need a multi-node self-hosted runtime on servers, with copies run in parallel per table and partitioned inside large tables. Where the source allows it, change tracking or CDC would replace watermarks.

Bronze to silver would move from the SQL bookmark to Auto Loader or Structured Streaming with a checkpoint, with liquid clustering on the keys and scheduled `OPTIMIZE` and `VACUUM`.

Gold would become incremental, driven by silver's change data feed, with a `MERGE` of the changed orders in place of the full rebuild.

For orchestration, the loads would read the control table themselves. Today they receive it as a parameter, and parameters have size limits that hundreds of tables would hit. Each table would also need failure isolation with a retry queue.

For serving, Power BI would refresh from a Databricks SQL warehouse, with incremental refresh on the fact tables.
