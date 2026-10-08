# Databricks notebook source
# MAGIC %md
# MAGIC # nb_uc_bootstrap — one environment's place in Unity Catalog
# MAGIC
# MAGIC **What this notebook does.** It tells Unity Catalog where one environment's lake is and creates the empty frame the load fills:
# MAGIC
# MAGIC 1. four **external locations**, one per container of the environment's storage account (`bronze`, `silver`, `gold`, `ucmanaged`) — the permission for Databricks to use those paths, through a storage credential and with no key;
# MAGIC 2. the **catalog**, with its managed storage in the container `ucmanaged`;
# MAGIC 3. the schemas `bronze`, `silver`, `gold`.
# MAGIC
# MAGIC It is run once per environment, by a person who may create external locations and catalogs — before the first deployment. Running it again changes nothing: every statement is `IF NOT EXISTS`.
# MAGIC
# MAGIC **Names it creates** follow from the catalog's name, so they are the same pattern in every environment:
# MAGIC
# MAGIC | Object | Name |
# MAGIC |---|---|
# MAGIC | external locations | `<catalog>_bronze`, `<catalog>_silver`, `<catalog>_gold`, `<catalog>_ucmanaged` |
# MAGIC | catalog | `<catalog>`, managed storage at `ucmanaged/<catalog>` |
# MAGIC | schemas | `<catalog>.bronze`, `<catalog>.silver`, `<catalog>.gold` |
# MAGIC
# MAGIC **Before it is run:** the identity behind the storage credential (the Access Connector's managed identity) holds the role *Storage Blob Data Contributor* on the storage account, and the four containers exist.
# MAGIC
# MAGIC **Parameters** (type them into the boxes at the top):
# MAGIC
# MAGIC | Parameter | Meaning |
# MAGIC |---|---|
# MAGIC | `p_catalog` | the catalog to create, e.g. `olist_prod` |
# MAGIC | `p_storage_account` | the environment's ADLS Gen2 storage account |
# MAGIC | `p_storage_credential` | the name of the Unity Catalog storage credential through which Databricks reaches that account |
# MAGIC
# MAGIC What comes after it, in this order: `nb_silver_setup` (the silver tables), then `nb_uc_grants` (what the load's service principal may do).

# COMMAND ----------

# Cell 2 - the parameter boxes. This cell only creates them, so that it always succeeds:
# Databricks shows a box only after the cell that creates it has finished without an error.
dbutils.widgets.text("p_catalog", "")
dbutils.widgets.text("p_storage_account", "")
dbutils.widgets.text("p_storage_credential", "")

# COMMAND ----------

# Cell 3 - read the parameters. The notebook stops here if one is missing or is not what it should be:
# a catalog must never be created on a guessed storage account.
import re

CATALOG    = dbutils.widgets.get("p_catalog").strip()
ACCOUNT    = dbutils.widgets.get("p_storage_account").strip()
CREDENTIAL = dbutils.widgets.get("p_storage_credential").strip()

missing = [name for name, value in [("p_catalog", CATALOG), ("p_storage_account", ACCOUNT), ("p_storage_credential", CREDENTIAL)] if value == ""]
if missing:
    raise ValueError("Parameter not given: " + ", ".join(missing) + ". Type the value into the box at the top of the notebook.")
if not re.fullmatch(r"[A-Za-z0-9_]+", CATALOG):
    raise ValueError(f"p_catalog '{CATALOG}' is not a plain catalog name (letters, digits, underscore).")
if not re.fullmatch(r"[a-z0-9]{3,24}", ACCOUNT):
    raise ValueError(f"p_storage_account '{ACCOUNT}' is not a storage account name (3-24 lower-case letters and digits).")
if not re.fullmatch(r"[A-Za-z0-9_-]+", CREDENTIAL):
    raise ValueError(f"p_storage_credential '{CREDENTIAL}' is not a storage credential name (letters, digits, underscore, hyphen).")

def url(container):
    return f"abfss://{container}@{ACCOUNT}.dfs.core.windows.net/"

print(f"catalog = {CATALOG}\nstorage account = {ACCOUNT}\nstorage credential = {CREDENTIAL}")

# COMMAND ----------

# Cell 4 - BUILD: one external location per container. Without them Databricks may not read or write
# a single path of the lake. The storage credential is the identity; the external location is the path
# that identity may be used for.
LOCATIONS = [
    ("bronze",    "the files Data Factory lands, and the bronze Delta tables"),
    ("silver",    "cleaned tables, one row per key"),
    ("gold",      "tables shaped for reporting"),
    ("ucmanaged", "managed storage of the catalog"),
]
for container, what in LOCATIONS:
    spark.sql(f"""
        CREATE EXTERNAL LOCATION IF NOT EXISTS `{CATALOG}_{container}`
        URL '{url(container)}'
        WITH (STORAGE CREDENTIAL `{CREDENTIAL}`)
        COMMENT 'Olist lakehouse, catalog {CATALOG} - container {container}: {what}'""")
    print(f"external location {CATALOG}_{container} -> {url(container)}")

# COMMAND ----------

# Cell 5 - BUILD: the catalog and its three schemas. The tables themselves are external: silver's are
# defined by nb_silver_setup, bronze's are created by the load the first time it meets a table.
spark.sql(f"""
    CREATE CATALOG IF NOT EXISTS {CATALOG}
    MANAGED LOCATION '{url("ucmanaged")}{CATALOG}'
    COMMENT 'Olist e-commerce lakehouse: bronze, silver, gold'""")

SCHEMAS = [
    ("bronze", "As received from the source, plus lineage columns. Append-only."),
    ("silver", "One row per key, typed, de-duplicated. Loaded with MERGE."),
    ("gold",   "Shaped for reporting. Built from silver."),
]
for schema, what in SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema} COMMENT '{what}'")
    print(f"schema {CATALOG}.{schema}")

# COMMAND ----------

# Cell 6 - CHECK: what Unity Catalog now holds for this environment.
# Claim: four external locations on this storage account and three schemas - seven rows.
found = []
for r in spark.sql("SHOW EXTERNAL LOCATIONS").collect():
    d = r.asDict()
    if str(d.get("name", "")).startswith(CATALOG + "_") and f"@{ACCOUNT}." in str(d.get("url", "")):
        found.append(("external location", str(d.get("name")), str(d.get("url"))))
for r in spark.sql(f"SHOW SCHEMAS IN {CATALOG}").collect():
    name = str(r[0])
    if name in ("bronze", "silver", "gold"):
        found.append(("schema", f"{CATALOG}.{name}", ""))
display(spark.createDataFrame(found, "kind string, name string, url string").orderBy("kind", "name"))
