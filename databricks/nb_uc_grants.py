# Databricks notebook source
# MAGIC %md
# MAGIC # nb_uc_grants — what the load's identity may do in Unity Catalog
# MAGIC
# MAGIC **What this notebook does.** It gives one identity — the service principal the load job runs as — exactly the privileges the load needs in Unity Catalog, and nothing more. It is run by the owner of the catalog (a person today, the deployment later), once per environment. Running it again changes nothing.
# MAGIC
# MAGIC | Granted on | Privileges | Because the load |
# MAGIC |---|---|---|
# MAGIC | the catalog | `USE CATALOG` | must be able to enter the catalog at all |
# MAGIC | schema `bronze` | `USE SCHEMA`, `SELECT`, `MODIFY`, `CREATE TABLE` | creates a bronze table the first time it meets one, loads it with `COPY INTO`, counts it, writes its audit rows |
# MAGIC | schema `silver` | `USE SCHEMA`, `SELECT`, `MODIFY` | reads what each table promises, merges into it, writes the quarantine, the audit rows and the bookmark. It creates nothing here: the silver tables are defined by `nb_silver_setup` |
# MAGIC | schema `gold` | `USE SCHEMA`, `SELECT`, `MODIFY` | loads the gold tables (the gold layer is the next piece of the project) |
# MAGIC | the external location of the landing files and the bronze tables | `READ FILES`, `CREATE EXTERNAL TABLE` | reads the Parquet files Data Factory landed (`COPY INTO … FROM 'abfss://…'`) and creates a bronze table at a path |
# MAGIC
# MAGIC A privilege given on a schema reaches every table in it: the ones there now and the ones made later.
# MAGIC
# MAGIC **Not granted:** `ALL PRIVILEGES`, `MANAGE`, ownership, `WRITE FILES`, anything on another catalog. The load cannot drop a table, change who may read it, or write loose files into the lake.
# MAGIC
# MAGIC **Parameters** (type them into the boxes at the top):
# MAGIC
# MAGIC | Parameter | Meaning |
# MAGIC |---|---|
# MAGIC | `p_catalog` | the Unity Catalog catalog that holds the schemas `bronze`, `silver`, `gold` |
# MAGIC | `p_principal` | the service principal's **Application ID** (36 characters with dashes) — Unity Catalog names a service principal by this id, not by its display name |
# MAGIC | `p_landing_location` | the name of the external location that covers the landing files and the bronze tables |
# MAGIC
# MAGIC No catalog, identity or location is typed into a cell: the prod environment runs this same notebook with its own three values.

# COMMAND ----------

# Cell 2 - the parameter boxes. This cell only creates them, so that it always succeeds:
# Databricks shows a box only after the cell that creates it has finished without an error.
dbutils.widgets.text("p_catalog", "")
dbutils.widgets.text("p_principal", "")
dbutils.widgets.text("p_landing_location", "")

# COMMAND ----------

# Cell 3 - read the parameters. The notebook stops here if one is missing or is not what it should be:
# a grant must never go to a guessed identity.
import re

CATALOG   = dbutils.widgets.get("p_catalog").strip()
PRINCIPAL = dbutils.widgets.get("p_principal").strip()
LOCATION  = dbutils.widgets.get("p_landing_location").strip()

missing = [name for name, value in [("p_catalog", CATALOG), ("p_principal", PRINCIPAL), ("p_landing_location", LOCATION)] if value == ""]
if missing:
    raise ValueError("Parameter not given: " + ", ".join(missing) + ". Type the value into the box at the top of the notebook.")
if not re.fullmatch(r"[A-Za-z0-9_]+", CATALOG):
    raise ValueError(f"p_catalog '{CATALOG}' is not a plain catalog name (letters, digits, underscore).")
if not re.fullmatch(r"[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", PRINCIPAL):
    raise ValueError(f"p_principal '{PRINCIPAL}' is not an Application ID. It is 36 characters with four dashes, "
                     "shown on the service principal's page - not the service principal's name.")
if not re.fullmatch(r"[A-Za-z0-9_]+", LOCATION):
    raise ValueError(f"p_landing_location '{LOCATION}' is not a plain external location name (letters, digits, underscore).")

WHO = f"`{PRINCIPAL}`"      # in back-ticks because of the dashes
print(f"catalog = {CATALOG}\nidentity = {PRINCIPAL}\nlanding location = {LOCATION}")

# COMMAND ----------

# Cell 4 - BUILD: the grants. One statement per object; the same statements in every environment.
GRANTS = [
    ("CATALOG",           CATALOG,             "USE CATALOG"),
    ("SCHEMA",            f"{CATALOG}.bronze", "USE SCHEMA, SELECT, MODIFY, CREATE TABLE"),
    ("SCHEMA",            f"{CATALOG}.silver", "USE SCHEMA, SELECT, MODIFY"),
    ("SCHEMA",            f"{CATALOG}.gold",   "USE SCHEMA, SELECT, MODIFY"),
    ("EXTERNAL LOCATION", LOCATION,            "READ FILES, CREATE EXTERNAL TABLE"),
]
for kind, name, privileges in GRANTS:
    spark.sql(f"GRANT {privileges} ON {kind} {name} TO {WHO}")
    print(f"granted on {kind.lower()} {name}: {privileges}")

# COMMAND ----------

# Cell 5 - CHECK: what the catalog now says this identity may do, object by object.
# Claim: the privileges of cell 4 and no others (13 rows when nothing else was ever granted to this identity).
found = []
for kind, name, _ in GRANTS:
    for r in spark.sql(f"SHOW GRANTS {WHO} ON {kind} {name}").collect():
        d = r.asDict()
        found.append((kind.lower(), name,
                      str(d.get("ActionType", r[1])),
                      str(d.get("ObjectType", r[2])).lower(),
                      str(d.get("ObjectKey", r[3]))))
display(spark.createDataFrame(found, "asked_for string, object string, privilege string, granted_on_type string, granted_on string")
             .orderBy("asked_for", "object", "privilege"))
