"""Checks on the files themselves. Nothing here starts Spark or reaches Azure.

They guard decisions that are easy to undo by accident in an editor: a retry set back to 0, the lag taken out of
the watermark query, a notebook that stops at the first failed table.
"""
import json
import pathlib
import py_compile

import pytest

REPO = pathlib.Path(__file__).resolve().parents[1]
NOTEBOOKS = sorted((REPO / "databricks").glob("*.py"))


def pipeline(name):
    text = (REPO / "adf" / "pipeline" / f"{name}.json").read_text(encoding="utf-8-sig")
    return json.loads(text)["properties"]


def activities(properties):
    return {a["name"]: a for a in properties["activities"]}


@pytest.mark.parametrize("notebook", NOTEBOOKS, ids=lambda p: p.name)
def test_every_notebook_is_valid_python(notebook, tmp_path):
    py_compile.compile(str(notebook), cfile=str(tmp_path / "out.pyc"), doraise=True)


@pytest.mark.parametrize("path", sorted((REPO / "adf").rglob("*.json")), ids=lambda p: p.name)
def test_every_data_factory_file_is_valid_json(path):
    json.loads(path.read_text(encoding="utf-8-sig"))


def test_only_one_run_of_the_parent_pipeline_at_a_time():
    assert pipeline("pl_ingest_olist")["concurrency"] == 1


@pytest.mark.parametrize("activity", ["lkp_new_watermark", "cp_src_to_bronze", "sp_update_watermark"])
def test_the_steps_that_can_fail_for_a_moment_are_retried(activity):
    policy = activities(pipeline("pl_ingest_table"))[activity]["policy"]
    assert policy["retry"] >= 1 and policy["retryIntervalInSeconds"] >= 30


def test_the_new_watermark_waits_for_open_transactions_and_is_never_empty():
    properties = pipeline("pl_ingest_table")
    query = activities(properties)["lkp_new_watermark"]["typeProperties"]["source"]["sqlReaderQuery"]["value"]
    assert "p_lag_seconds" in query and "SYSUTCDATETIME()" in query      # the lag
    assert "COALESCE(MAX(" in query and "p_last_watermark" in query      # no rows old enough: the old watermark again
    assert properties["parameters"]["p_lag_seconds"]["defaultValue"] >= 60


def test_the_copy_reads_above_the_old_watermark_up_to_the_new_one():
    query = activities(pipeline("pl_ingest_table"))["cp_src_to_bronze"]["typeProperties"]["source"]["sqlReaderQuery"]["value"]
    assert "' > '''" in query and "' <= '''" in query


def test_the_copy_fails_instead_of_cutting_a_value_short():
    copy = activities(pipeline("pl_ingest_table"))["cp_src_to_bronze"]
    assert copy["typeProperties"]["translator"]["typeConversionSettings"]["allowDataTruncation"] is False


def test_the_job_starts_only_after_every_table_was_copied():
    job = activities(pipeline("pl_ingest_olist"))["job_lakehouse_load"]
    assert job["dependsOn"] == [{"activity": "fe_tables", "dependencyConditions": ["Succeeded"]}]


@pytest.mark.parametrize("notebook", ["nb_bronze_load.py", "nb_silver_load.py"])
def test_a_failed_table_does_not_stop_the_other_tables(notebook):
    text = (REPO / "databricks" / notebook).read_text(encoding="utf-8").replace("\r\n", "\n")
    assert "        failed.append(t)\n" in text        # the loop notes the table and goes on
    assert "\n        raise\n" not in text              # ...and no longer stops at the first error
    assert "\nif failed:\n    raise RuntimeError(" in text   # the run still fails, at its end
