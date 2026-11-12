# Changelog

## 1.1.0 (November 2026)

### Changed

- Gold is checked before it is written. `nb_gold_load` works out the nine tables as views, runs the 13 reconciliation checks on them, and writes only when all pass. Before, it wrote first and checked afterwards, so a failed check raised the alert with wrong numbers already in the tables.
- One failed table no longer stops the others. In `nb_bronze_load` and `nb_silver_load` every table has its turn. The task fails at its end and the next layer does not start.
- The watermark stays two minutes behind the clock (`p_lag_seconds` in `pl_ingest_table`). A transaction still being written at the source is left for the next run and no longer risks being skipped. When no row is old enough yet, the old watermark is used again and nothing is copied.
- The watermark lookup, the copy and the watermark update retry twice, a minute apart.
- `pl_ingest_olist` runs one at a time (`concurrency` 1).

### Added

- `tests/`: the real gold notebook run on made-up rows on a local Spark session (18 tests), and checks on the pipeline and notebook files (32 tests). The build runs them on every pull request.
- `docs/`: the README's reference sections as pages of their own, and a runbook.
- This file.

### How far each change has been run

| Change | What has been seen |
|---|---|
| Gold checked before written | The tests pass on a local Spark 4.1 session with Parquet tables, including the case where a failed check writes nothing. On Databricks: one load in dev on November 12, 2026, where the pipeline and its job succeeded. A failing check has not been seen on Databricks |
| Failed-table handling in bronze and silver | It ran in the same dev load, where no table failed. One table failing while the others load, the case it exists for, has not been seen |
| Watermark lag | The new lookup query ran on the source SQL Server for all eight tables in the dev load. A transaction committing late has not been staged |
| Retries, one run at a time | Deployed and in place for that load. No retry fired and no second run was queued, so neither has been seen working |
| The tests in the build | Green on a Microsoft-hosted Azure DevOps agent (run #20261112.2) |

Version 1.1 was deployed to dev and prod on November 12, 2026 (runs #20261112.2 and #20261112.4). No load has been run in prod on it yet.

The dev load took 25 minutes from trigger to end, against about 15 for the first load of version 1.0.

## 1.0.0 (October 17, 2026)

First public release: incremental ingestion from SQL Server through Data Factory; bronze, silver and gold on Databricks; a Power BI report; two environments deployed by pull request; a failure alert. The numbers in the README are from this version.
