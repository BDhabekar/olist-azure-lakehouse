# Olist E-commerce Lakehouse on Azure

An incremental data platform on Azure, built end to end. Orders from an on-premises SQL Server are copied by Azure Data Factory into ADLS Gen2, loaded by an Azure Databricks job through bronze, silver and gold Delta tables in Unity Catalog, and reported in Power BI. Every change reaches production through a pull request and an Azure DevOps pipeline that deploys to two separate environments, dev and prod.

All transformations are SQL on Databricks. Python only reads the parameters and runs the same SQL for each table in the list.

| Area | What |
|---|---|
| Source | SQL Server 2025 on a laptop, standing in for the on-premises network. The Brazilian E-Commerce Public Dataset by Olist: 9 tables, 1.55 M rows of history, replayed month by month as live business |
| Ingestion | Azure Data Factory with a self-hosted integration runtime. Watermark-based incremental copy driven by a control table, with a run log and a watermark per table |
| Lakehouse | Azure Databricks, Delta Lake, Unity Catalog external tables, medallion layers (bronze / silver / gold) |
| Serving | A gold star schema (4 dimensions, 2 facts, 3 KPI tables) and a Power BI report (Import mode) |
| Delivery | GitHub → Azure DevOps YAML pipeline → dev, then prod after approval. ARM templates for Data Factory, a Databricks Asset Bundle for the job, Bicep for monitoring |
| Proven | First load: 343,844 rows in 8 tables, equal to the source, in dev and in prod. 13 gold-to-silver reconciliation checks fail the run if any one breaks. A deliberately failed run sent the alert mail 2 min 45 s after it failed |

![Architecture. Data flows from SQL Server on the laptop through a self-hosted integration runtime into ADLS Gen2 (bronze, silver, gold) and on to Power BI. Data Factory orchestrates each run with an Azure SQL control database and Key Vault; a Databricks job loads the layers; Azure Monitor alerts on failed runs; Azure DevOps deploys dev, then prod after approval.](docs/images/architecture.png)

## How one run works

1. **Copy what changed.** Data Factory reads the list of tables from a control database. For each table it copies only the rows changed since the last watermark into the lake as Parquet, through a self-hosted runtime on the source machine. The watermark moves only after the copy succeeds.
2. **Bronze.** `COPY INTO` loads each landed file exactly once and stamps every row with the file it came from and the time it was loaded.
3. **Silver.** Rows that break a rule go to a quarantine table with the rule's name. The rest are reduced to the newest row per business key and merged. A key held twice fails the table.
4. **Gold.** The star schema is worked out from silver, reconciled with 13 checks, and written only when all 13 pass.
5. **When something fails,** the pipeline fails, the error text is in a run log, and an alert mail goes out. Gold keeps its last good load.

A second run over the same data changes nothing in any layer. The full walk-through, the layers, the business keys and the Power BI model are in [docs/how-it-works.md](docs/how-it-works.md).

## What was proven

- **First load, dev and prod:** 343,844 rows in 8 tables, every count equal to the source, 0 duplicate keys, 0 quarantined.
- **Incremental:** replaying one month added 6,728 new orders. Changing one order at the source updated exactly that row in silver. A re-run changed nothing.
- **Gold:** 13 of 13 reconciliation checks passed in dev and in prod, with every number equal between the two.
- **Alerting:** a deliberately failed run sent the alert mail 2 min 45 s after it failed.
- **A finding the checks surfaced:** payments exceed order value by 122,510.10 BRL, nearly all of it 622 paid orders that have no item lines in the source. So the report's revenue is order value, not payments.
- **Cost:** the first full load cost at most about ₹30 (well under US$1) on the meter.

These numbers are from the first release (October 2026). What has changed since, and what each change has been run on, is in [CHANGELOG.md](CHANGELOG.md). The evidence pictures and the cost detail are in [docs/numbers-and-cost.md](docs/numbers-and-cost.md).

![Sales overview](docs/images/powerbi-sales-overview.png)

## The decisions that matter most

| Decision | Choice | Why not the alternative |
|---|---|---|
| Reaching the on-premises database | A self-hosted runtime that only makes outbound connections on port 443 | An inbound port on the database is the first thing a security review refuses |
| Finding new rows | A watermark on `modified_at`, read before the copy and saved after it, and held two minutes behind the clock | A full reload every run grows with the table. A watermark with no lag skips a transaction that commits late |
| Writing silver | `MERGE` on a key found by a grain census, updating only when the incoming row is newer | Append doubles rows on a re-run. Overwrite loses history and gets slower every day |
| Bad rows | Quarantine with the broken rule's name; good rows still load | Dropping them hides the problem. Failing the load for one row stops the business for a typo |
| Publishing gold | Checked first, written second | Writing first means the alert arrives with the wrong numbers already in the report |
| Compute | A job cluster that exists only for the run | Serverless notebooks, measured here at about ₹3,000 in three days |
| Deployment | An ARM template generated by the build, signed in by workload identity federation | The Publish button, and a secret stored in the pipeline |

All 22 decisions, with what was rejected, are in [docs/design-decisions.md](docs/design-decisions.md).

## Delivery

A pull request runs the build: Data Factory validation, the Databricks bundle, the alert's Bicep, and the tests in `tests/`, which run the real gold notebook on made-up rows. A merge to `main` deploys to dev and then, after an approval, to prod. Dev and prod each have their own resource group, factory, storage, Key Vault, control database, Databricks workspace, catalog and identities, and no identity of one environment holds anything in the other.

![A merge to main: build, then dev, then prod after approval](docs/images/cicd-run-stages.png)

There are no keys, SAS tokens or personal access tokens anywhere. The one secret is the source database's password, and it lives in Key Vault.

More: [docs/environments-and-cicd.md](docs/environments-and-cicd.md) and [docs/security-and-access.md](docs/security-and-access.md).

## Known gaps

These are deliberate and named. The design for each is in [docs/known-gaps.md](docs/known-gaps.md), with what would change at 100 times the data.

- **Deletes at the source are not seen.** A timestamp watermark cannot see a deleted row. The fix is change tracking and a delete branch in the silver `MERGE`.
- **Nothing is ever removed.** Reference tables are re-copied whole each run, so bronze holds one copy of `products` per run.
- **The lab's network is open.** Storage accepts all networks and the workspaces have no private endpoints.
- **The source is one laptop.** One runtime node, a self-signed certificate.
- **The daily schedule is deployed but stopped,** to save cost. Every load so far was started by hand.
- **One reviewer.** I approve my own pull requests.

## Where to read more

| If you want | Open |
|---|---|
| The run step by step, the layers, the keys, the report model | [docs/how-it-works.md](docs/how-it-works.md) |
| Every design decision and what was rejected | [docs/design-decisions.md](docs/design-decisions.md) |
| The numbers, the evidence pictures, the cost | [docs/numbers-and-cost.md](docs/numbers-and-cost.md) |
| Environments, the pipeline, monitoring | [docs/environments-and-cicd.md](docs/environments-and-cicd.md) |
| Who may do what, and why | [docs/security-and-access.md](docs/security-and-access.md) |
| What to do when a run fails, how to re-run, how to release quarantined rows | [docs/runbook.md](docs/runbook.md) |
| The known gaps and the 100× design | [docs/known-gaps.md](docs/known-gaps.md) |
| Building it yourself | [docs/reproduce.md](docs/reproduce.md) |
| What changed in each version | [CHANGELOG.md](CHANGELOG.md) |

## Repository layout

```
adf/                    Data Factory as the dev factory writes it in Git mode: pipelines, datasets,
                        linked services, trigger, integration runtime
databricks/             Databricks Asset Bundle: databricks.yml, the job, and the notebooks (.py)
sql/local-source/       the on-premises source: database, tables, seed, month-replay procedure,
                        read-only login, checks, and three one-row change scripts used by the proofs
sql/control-db/deploy/  the control database: schema etl (config, run log, watermark procedures),
                        applied by the pipeline
scripts/                load_olist.py loads the 9 Olist CSVs into the source database
cicd/                   pipeline template, per-environment variables, deployment scripts,
                        the Data Factory build package
monitoring/alerts.bicep the failure alert
powerbi/                the report as a Power BI Project (.pbip)
tests/                  the gold notebook run on made-up rows; checks on the definitions
docs/                   the pages listed above, and the pictures
azure-pipelines.yml     build → dev → prod
```

## How it was built

Built in October 2026, in two steps. First a single environment — ingestion, bronze, silver and the job — each piece proven with real runs. Then dev and prod were created new, side by side, and filled only by the pipeline; every change since has gone through a [pull request](https://github.com/BDhabekar/olist-azure-lakehouse/pulls?q=is%3Apr+is%3Aclosed). The repository's first commits still show the Data Factory of that first environment, practice objects included; they were removed in `f61fed7`.

I built this with an AI assistant (Claude) as mentor and pair-programmer. The design decisions, the Azure build, and every run and check are mine; much of the code — notebooks, SQL scripts, pipeline YAML — was drafted with the assistant and reviewed and run by me.

## Data and licence

Brazilian E-Commerce Public Dataset by Olist ([Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)), licensed CC BY-NC-SA 4.0. No data is stored in this repository.

The code is under the MIT licence — see [LICENSE](LICENSE). The icons in the architecture picture are [Tabler Icons](https://github.com/tabler/tabler-icons) (MIT licence).
