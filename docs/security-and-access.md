# Security and access

[← back to the README](../README.md)

- **No keys, SAS tokens or personal access tokens anywhere:**
  - Data Factory uses its managed identity for the lake, the vault, the control database and the Databricks workspace;
  - Databricks reaches storage through an Access Connector and Unity Catalog external locations;
  - the only secret is the source SQL login's password, which lives in Key Vault (RBAC model).
- **Least privilege in Unity Catalog** (`nb_uc_grants`). The load's service principal gets `USE`, `SELECT`, `MODIFY` and, in bronze only, `CREATE TABLE`, plus `READ FILES` and `CREATE EXTERNAL TABLE` on the landing location. It cannot drop, alter or re-grant a silver or gold table, or write loose files. It does own the bronze tables, because the load creates each one the first time it meets it (named under *Known gaps*). Silver and gold definitions (`nb_silver_setup`, `nb_gold_setup`) are applied by the catalog owner.
- Each catalog is **bound to its own workspace**.
- The source login `adf_reader` is **read-only** on schema `src`.

**Who may do what** — the same in dev and in prod; no identity of one environment holds anything in the other:

| Identity | Access | On | Why |
|---|---|---|---|
| Data Factory (its managed identity) | Storage Blob Data Contributor | the storage account | writes the landed Parquet files |
| | Key Vault Secrets User | the Key Vault | reads the source login's password |
| | Contributor | the Databricks workspace | starts the job |
| | a database user with `SELECT` and `EXECUTE` on schema `etl` | the control database | reads the list of tables, writes the run log and the watermarks |
| | sharing permission | the shared self-hosted runtime | reaches the source |
| Access Connector (its managed identity) | Storage Blob Data Contributor | the storage account | Databricks reads and writes the lake (the Unity Catalog storage credential) |
| Deployment identity (user-assigned managed identity) | Contributor | its environment's resource group | deploys the factory and the alert |
| | `db_owner` | the control database | creates and updates schema `etl` |
| | **Use** on the job's service principal; manages the job | the Databricks workspace | deploys the job to run as that principal |
| Azure DevOps | a federated credential | the deployment identity | signs in without a secret |
| The job's service principal | the Unity Catalog grants above | the catalog and the landing location | runs the load |
