# Known gaps, and what changes at scale

[← back to the README](../README.md)

## Known gaps (deliberate, named)

| Gap | Why it is not in this version | The design when built |
|---|---|---|
| **Deletes at the source** | A timestamp watermark cannot see a deleted row | SQL Server change tracking, read by Data Factory, and a delete branch (`WHEN NOT MATCHED BY SOURCE` / soft delete) in the silver `MERGE` |
| **Retention** | Full-load reference tables are re-copied each run; bronze holds `products` once per run | Change detection at ingestion (copy only when the hash changes), old full-load copies removed from bronze, an ADLS lifecycle rule on landing files, scheduled `VACUUM` |
| `geolocation` | 1 M rows, no key and no consumer in gold | Not loaded in dev or prod (`active = 0` in `etl.ingest_config`); its silver table is defined and empty. It becomes a gold table (one point per zip prefix) if a map is needed |
| Bronze tables owned by the load identity | The load creates a bronze table the first time it meets one | Bronze tables created by the set-up step; the load gets `MODIFY` only |
| Storage open to all networks, shared-key access on, RA-GRS replication | Portal defaults, on a lab subscription with no private network | Private endpoints or a firewall; shared-key access off (every access here is by identity); replication chosen for the recovery need |
| Workspaces without secure cluster connectivity | *No Public IP* = No, so no NAT gateway is billed while idle | Secure cluster connectivity (No Public IP) with VNet injection and private endpoints to storage |
| One runtime node on a laptop | Lab constraint | Self-hosted IR on always-on servers, 2+ nodes for high availability |
| `trustServerCertificate = true` | The laptop's SQL Server has a self-signed certificate | A CA-issued certificate with validation on |
| One reviewer | Solo project: I approve my own pull requests | Required reviews from a second engineer |
| One Unity Catalog metastore for dev and prod | One metastore per region is the Databricks model | Same — isolation comes from a catalog per environment bound to its own workspace, which this project does |
| Set-up notebooks run by hand | `nb_uc_bootstrap`, `nb_silver_setup`, `nb_gold_setup` change table definitions and run as the catalog owner, once per environment | A schema-migration step in the pipeline, running as a dedicated owner identity |
| Daily schedule stopped | Cost on a student subscription; every load was started deliberately | Schedule on, through a pull request |
| Gold is written as nine statements | Each table is overwritten on its own. A run lost between two of them leaves gold part new and part old until the next run rebuilds all nine; the load compares every table with what it checked and fails on a difference | One transaction across the nine tables, or a staging schema that is swapped in |
| A transaction open for longer than the lag | The watermark stays two minutes behind the clock. A source transaction that takes longer than that to commit can still be skipped | Change tracking or CDC, which read the database's log and not a timestamp - the same fix as deletes |
| Quarantined rows are released by hand | After the cause is fixed, the catalog owner moves the table's bookmark back ([runbook.md](runbook.md)) | A reprocess step that reads quarantine again when a rule or the source changes |
| Dev is not loaded by the pipeline before prod | A load needs a cluster, and no deployment starts one. The person approving prod runs dev first | A smoke load in dev as a pipeline stage, on a small fixed data set |
| The tests cover the gold load only | Bronze and silver use `COPY INTO` and catalog views that a local Spark does not have | The same tests against a test catalog in Databricks, or the load logic moved into functions that take DataFrames |

## What changes at 100× the data

- **Ingestion:** a multi-node self-hosted IR on servers; copies parallelized per table and partitioned within large tables; change tracking or CDC instead of watermarks where the source allows it.
- **Bronze → silver:** Auto Loader / Structured Streaming with a checkpoint instead of the SQL bookmark; liquid clustering on the keys; scheduled `OPTIMIZE` / `VACUUM`.
- **Gold:** incremental, driven by silver's change data feed with a `MERGE` of changed orders, instead of a full rebuild.
- **Orchestration:** the loads read the control table themselves instead of receiving it as a parameter (parameter size limits at hundreds of tables). Per-table failure isolation with a retry queue.
- **Serving:** a Databricks SQL warehouse for Power BI refresh, with incremental refresh on the fact tables.
