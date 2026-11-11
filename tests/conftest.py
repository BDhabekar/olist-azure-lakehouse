"""Shared pieces of the tests.

The tests run the real notebook files of databricks/ on a local Spark session, with made-up rows.
Three things stand in for Databricks:
  * the catalog is Spark's own (spark_catalog) and the tables are plain Parquet tables;
  * dbutils.widgets is a small fake that hands the notebook its parameters;
  * display() does nothing.
The table definitions are not copied here: they are read from nb_silver_setup.py and nb_gold_setup.py,
so a column added there is in the tests' tables the same day.
"""
import pathlib

import pytest
from pyspark.sql import SparkSession

REPO = pathlib.Path(__file__).resolve().parents[1]
NOTEBOOKS = REPO / "databricks"
CATALOG = "spark_catalog"
CELL_BREAK = "# COMMAND ----------"


class _Widgets:
    def __init__(self, values):
        self._values = dict(values)

    def text(self, name, default="", label=None):
        self._values.setdefault(name, default)

    def get(self, name):
        return self._values[name]


class FakeDbutils:
    def __init__(self, **values):
        self.widgets = _Widgets(values)


def run_notebook(spark, name, params, cells=None):
    """Run a notebook file as plain Python and return its variables.

    cells: how many cells to run, counted from the top (None = the whole notebook).
    """
    parts = (NOTEBOOKS / name).read_text(encoding="utf-8").split(CELL_BREAK)
    code = "\n".join(parts if cells is None else parts[:cells])
    namespace = {"spark": spark, "dbutils": FakeDbutils(**params), "display": lambda *a, **k: None}
    exec(compile(code, name, "exec"), namespace)
    return namespace


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    warehouse = tmp_path_factory.mktemp("warehouse")
    session = (
        SparkSession.builder.master("local[1]")
        .appName("olist-tests")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")     # as on Databricks
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def _definitions(spark, notebook):
    # The first four cells of a set-up notebook: the header, the parameter boxes, the parameters, the definitions.
    return run_notebook(spark, notebook, {"p_catalog": CATALOG, "p_storage_account": "teststorage"}, cells=4)["TABLES"]


@pytest.fixture()
def lake(spark):
    """Empty silver and gold schemas with every table of the two set-up notebooks. Rebuilt for each test."""
    for schema, notebook in (("silver", "nb_silver_setup.py"), ("gold", "nb_gold_setup.py")):
        spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{schema} CASCADE")
        spark.sql(f"CREATE SCHEMA {CATALOG}.{schema}")
        for table, (columns, _comment) in _definitions(spark, notebook).items():
            spark.sql(f"CREATE TABLE {CATALOG}.{schema}.{table} ({columns}\n) USING parquet")
    return spark
