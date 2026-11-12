# Reproducing it

[Back to the README](../README.md)

## 1. The source

Download the [Olist dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) and run `pip install -r scripts/requirements.txt`. You also need Microsoft ODBC Driver 18 for SQL Server.

On a SQL Server, run `sql/local-source/01` to `07` in order (`08` is an optional backup). `02` loads the CSVs. `04` and `05` seed the history up to 2017-12-31 and create `etl.usp_replay_month`, which replays one month as new business. `05` ends with two red errors on purpose: they are its own guard tests.

The login for step `06` is created in SSMS first. Its password goes into Key Vault and nowhere else.

## 2. Azure, once

Create a resource group that holds one Data Factory, which hosts the self-hosted runtime (installed on the source machine), and the two deployment identities, each with a federated credential for Azure DevOps. Register the resource provider `microsoft.insights`.

## 3. Azure, per environment

Create these in each environment:

- a resource group;
- a storage account with hierarchical namespace and four containers: `bronze`, `silver`, `gold` and `ucmanaged`;
- a Key Vault holding the source password as `sql-olist-adf-reader-password`;
- an Azure SQL server and the control database;
- a Data Factory, with the shared runtime granted to it;
- a Databricks workspace (Premium) with an Access Connector.

`ucmanaged` is the catalog's managed storage. Unity Catalog requires one because a metastore enabled automatically has no storage of its own, and it may not overlap the external tables, so it gets its own container. No table of this project lives there. They are all external, in `bronze`, `silver` and `gold`.

Give each identity the access listed in [security-and-access.md](security-and-access.md). The server's Microsoft Entra admin runs `sql/control-db/deploy/<env>_bootstrap_users.sql` once.

## 4. Unity Catalog, per environment

Register the Access Connector as a storage credential and create the job's service principal. Then, as the catalog owner, run `nb_uc_bootstrap`, `nb_silver_setup`, `nb_gold_setup` and `nb_uc_grants`, in that order, and bind the catalog to its workspace.

## 5. The pipeline

Point an Azure DevOps pipeline at `azure-pipelines.yml` and put your resource names in `cicd/variables/`. In Azure DevOps, create:

- the service connections `sc-olist-dev` and `sc-olist-prod` (workload identity federation);
- the environments `olist-dev` and `olist-prod`, with an approval on prod;
- the pipeline variables `subscriptionId`, `devRunAsSpAppId`, `prodRunAsSpAppId` and the secret `alertEmail`.

Merging to `main` deploys everything else. The first load is one triggered run of `pl_ingest_olist`.

## 6. Power BI

Open `powerbi/olist-gold.pbip`, then go to Transform data, Data source settings, Change Source, and enter your workspace's server hostname and a cluster's HTTP path. The file holds this project's prod workspace.

If sign-in fails with "User canceled authentication", turn off the enhanced sign-in experience under Options, Security.

Operating it afterwards (starting a load, replaying a month, reading a failure) is covered in the [runbook](runbook.md).
