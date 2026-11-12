# Runbook

[Back to the README](../README.md)

How to start a load, read a failure, run again, release quarantined rows, deploy and roll back. Each section says whether I have actually done it or whether it is written from the code.

`<catalog>` is `olist_dev` or `olist_prod`. `<env>` is `dev` or `prod`.

## 1. Start a load by hand

I have done this for every load so far, because the schedule is stopped.

1. In the environment's Data Factory, open the pipeline `pl_ingest_olist` and choose Add trigger, Trigger now.
2. The run copies the changed rows and then starts the Databricks job `job_olist_lakehouse_<env>`. The first full load of version 1.0 took about 15 minutes. A version 1.1 run in dev with no new orders took 25 minutes.
3. The job starts one 4-core cluster. On a subscription with a small core quota, stop any other cluster first.

Only one run is active at a time. If you trigger a second one while a run is active, it waits in the queue.

## 2. A run failed: where to look

I have used the alert and the run log on a run I failed on purpose. The rest of this section is written from the code.

Read in this order and stop at the first place that names the cause.

| Step | Where | What it tells you |
|---|---|---|
| 1 | The alert mail | That a run of this factory failed, and when |
| 2 | Data Factory, Monitor, Pipeline runs | Which activity failed: a table's copy (`pl_ingest_table`) or the job (`job_lakehouse_load`) |
| 3 | `etl.run_log` in the control database | For a failed copy, the table and the error text: `SELECT TOP 20 * FROM etl.run_log ORDER BY logged_at DESC;` |
| 4 | Databricks, Job runs, the run | Which task failed (`bronze_load`, `silver_load` or `gold_load`) and its error message |
| 5 | `<catalog>.bronze.load_audit` or `<catalog>.silver.load_audit`, filtered on the run id | Which tables failed and why (`status = 'failed'`, `error_message`). The run id is the pipeline's run id |
| 6 | The error of `gold_load` | The names of the checks that failed |

What each kind of failure leaves behind:

| What failed | State afterwards |
|---|---|
| One table's copy | That table's watermark has not moved. Other tables were copied and their watermarks moved. The job did not start, so their files wait in the lake |
| One table in `bronze_load` | The other tables are loaded. Silver and gold did not start |
| One table in `silver_load` | That table's bookmark has not moved. The other tables are merged. Gold did not start and still holds the last good load |
| A check in `gold_load` | Silver is up to date. Gold is untouched |

## 3. Run again

Re-runs after a success are proven: they changed nothing in silver or gold. A re-run after each kind of failure is written from the code.

Fix the cause and start the pipeline again as in section 1. There is nothing to clean up first, because every step can be repeated:

- a table whose copy failed is copied again from its old watermark;
- `COPY INTO` remembers the files it has loaded and skips them;
- silver reads again from each table's bookmark, and its `MERGE` updates a row only when the incoming one is newer;
- gold is rebuilt whole, checked and then written.

## 4. Put new business into the source

This is the lab's stand-in for customers. I have done it for January and February 2018 and with three one-row changes.

1. On the source SQL Server, run `EXEC etl.usp_replay_month @month_start = '2018-03-01';` or one of the change scripts `src_09` to `src_11`.
2. Wait two minutes before starting the pipeline. The watermark stays two minutes behind the clock so that a transaction still being written is not skipped. A run started sooner copies nothing new, which is not an error, and the next run picks the rows up.
3. Start the pipeline as in section 1.

## 5. Release rows from quarantine

Written from the code. I have not done this yet.

A row in `<catalog>.silver.quarantine` broke a NOT NULL or CHECK rule of its silver table. The load has already moved past it, so the table has to be read again from before that row.

1. Find the rows and the reason:
   ```sql
   SELECT table_name, reason, _source_file, _loaded_at FROM <catalog>.silver.quarantine;
   ```
2. Fix the cause. If the rule was wrong, change it in `nb_silver_setup` and run that notebook as the catalog owner. If the data was wrong, correct it at the source. That stamps a new `modified_at`, so the next run brings the corrected row by itself and you can skip steps 3 and 4.
3. As the catalog owner, move that table's bookmark back to just before the rows were loaded:
   ```sql
   UPDATE <catalog>.silver.load_state
   SET last_loaded_at = TIMESTAMP'<a moment before the smallest _loaded_at from step 1>', updated_at = current_timestamp()
   WHERE table_name = '<table>';
   ```
4. Start the pipeline. Silver reads the window again. Rows that now pass are merged, rows already in silver are left alone, and a row that still fails stays in quarantine without being recorded twice.

The quarantine row itself stays, as a record of what happened.

## 6. Deploy a change

I have done this for every change since October 8, 2026.

1. Branch from a freshly pulled `main`, make the change and open a pull request.
2. The build runs: Data Factory validation, the bundle, the alert's Bicep and the tests. All must be green.
3. Squash and merge. The pipeline deploys to dev.
4. If the change touches a pipeline or a notebook, run dev once (section 1) before approving prod. The build shows that the change deploys and that gold's logic is right. Only a load shows that ingestion, bronze and silver still work.
5. Approve the `olist-prod` environment. The same package goes to prod.

The pipeline does not run the set-up notebooks (`nb_uc_bootstrap`, `nb_silver_setup`, `nb_gold_setup`) or `nb_uc_grants`. After a change to a table definition, the catalog owner runs the set-up notebook in each environment before the next load.

## 7. Roll back

Written from the code. I have not done this yet.

For code, revert the pull request on GitHub and merge the revert. The pipeline deploys the previous definitions to dev and, after approval, to prod.

Gold needs nothing. The next good run rebuilds it, and a failed check never writes.

Every silver table keeps 30 days of history. As the catalog owner, run `DESCRIBE HISTORY <catalog>.silver.<table>;` and then `RESTORE TABLE <catalog>.silver.<table> TO VERSION AS OF <n>;`. Then move the table's bookmark back as in section 5, so the next run reads forward from that point.

## 8. Turn the daily schedule on or off

Written from the code. I have not done this yet, because the schedule has stayed stopped to save cost.

Change `runtimeState` in `adf/trigger/tr_daily_ingest.json` to `Started` or `Stopped` by pull request. The deployment starts the triggers whose definition says `Started`. Don't switch it in the portal, because the next deployment would put it back.

The file holds one value for both environments, so `Started` starts the schedule in dev and in prod alike. To run it in prod only, the trigger's state first has to become a template parameter with a value per environment (`adf/arm-template-parameters-definition.json` and the deployment's overrides).
