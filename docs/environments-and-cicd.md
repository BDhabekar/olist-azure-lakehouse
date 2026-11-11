# Environments, CI/CD and monitoring

[← back to the README](../README.md)

## Environments and CI/CD

| | dev | prod |
|---|---|---|
| Resource group | `rg-olist-dev` | `rg-olist-prod` |
| Data Factory | `adf-olist-dev-bd`, the only factory connected to Git, with Publish disabled | `adf-olist-prod-bd`, never edited by hand |
| Storage · Key Vault · control DB | own copies | own copies |
| Databricks workspace · catalog | `dbw-olist-dev` · `olist_dev` | `dbw-olist-prod` · `olist_prod` |
| Job runs as | `sp-olist-lakehouse-dev` | `sp-olist-lakehouse-prod` |
| Deploys as | `id-olist-deploy-dev` | `id-olist-deploy-prod` |

One self-hosted runtime is shared from `rg-olist-shared` (a linked runtime in each factory), because the laptop can host only one runtime node.

**The pipeline (`azure-pipelines.yml`).** Every pull request runs the build. A merge to `main` runs build → dev → prod.

- **build:**
  - validates the factory and generates its ARM template with Microsoft's `@microsoft/azure-data-factory-utilities` — "automated publish": no Publish button and no `adf_publish` branch;
  - checks that every value that differs between dev and prod is a template parameter;
  - runs `databricks bundle validate`;
  - builds the alert's Bicep;
  - runs the tests in `tests/` on the build machine's own Spark: the real `nb_gold_load` notebook on made-up rows, and checks on the pipeline and notebook files. No workspace, no cluster.
- **deploy_dev → deploy_prod** use the same template, `cicd/templates/deploy-environment.yml`. The prod stage waits for an **approval** on the `olist-prod` environment. Each stage:
  1. deploys the Databricks bundle and reads the job id;
  2. creates or updates schema `etl` in the control database, waiting for the serverless database to wake;
  3. stops the triggers;
  4. applies the ARM template with that environment's values;
  5. cleans up and restarts the triggers meant to run;
  6. deploys the failure alert.

Every tool the pipeline installs is pinned — the agent image, Node.js, the Data Factory build package (with a lock file), the Databricks CLI, Bicep, and Python with the test packages — so a deployment changes only when the repository does.

![A merge to main: build, then dev, then prod after approval](images/cicd-run-stages.png)

<details>
<summary>The steps of one deployment</summary>

![Build jobs and the steps of the prod deployment](images/cicd-deploy-steps.png)

</details>

**Authentication without secrets in the pipeline.** Azure DevOps signs in to Azure with **workload identity federation**, one service connection per environment. Each deployment identity has rights in its own environment only; the pull-request build signs in as dev's identity, so a pull request can never reach prod. Pull requests from forks are not built.

**Branching.** Short-lived feature branches, squash-merged; GitHub deletes the merged branch. A ruleset on `main` requires a pull request with a green build check and blocks force pushes. The dev factory has *Publish* disabled, so it changes only through Git — and, with the rule on `main`, only through a pull request.

## Monitoring

`monitoring/alerts.bicep` creates, per environment:
- an action group `ag-olist-<env>-failures`;
- a metric alert `alert-olist-<env>-pipeline-failed` on the factory's **PipelineFailedRuns > 0**, over a 5-minute window, evaluated every minute. Severity is 3 in dev and 1 in prod.

The mail receiver is a secret pipeline variable. **Proven:** a deliberately failing triggered run fired the alert (Fired, then Resolved). Debug runs do not emit this metric, so the proof had to be a triggered run.

What to do when the alert arrives is in [runbook.md](runbook.md).
