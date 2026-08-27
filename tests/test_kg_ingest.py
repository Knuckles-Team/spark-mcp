"""Record -> OWL entity/relationship mapping for the Spark KG ingest tool."""

from spark_mcp.kg_ingest import (
    map_application,
    map_dataset_version,
    map_job,
    map_transform,
    map_transform_run,
    map_transform_run_chain,
)

_REMOTE_URL = "sc://spark-connect.apps.svc:15002"


def test_map_application_id_is_stable():
    node = map_application(_REMOTE_URL)
    assert node["id"] == "spark:SparkApplication:spark-connect.apps.svc:15002"
    assert node["node_type"] == "SparkApplication"


def test_map_job_and_transform_ids():
    run_record = {"run_id": "run-1", "kind": "sql"}
    job = map_job(run_record)
    assert job["id"] == "spark:SparkJob:run-1"

    transform = map_transform("agg_daily", "sql")
    assert transform["id"] == "spark:Transform:agg_daily"
    assert transform["kind"] == "sql"


def test_map_transform_run_carries_status_and_error():
    run_record = {
        "run_id": "run-1",
        "status": "failed",
        "error": "boom",
        "row_count": None,
        "submitted_at": "2026-08-26T00:00:00Z",
        "completed_at": "2026-08-26T00:00:01Z",
        "rerun_of": None,
    }
    node = map_transform_run(run_record)
    assert node["id"] == "spark:TransformRun:run-1"
    assert node["status"] == "failed"
    assert node["error"] == "boom"


def test_map_dataset_version_id():
    node = map_dataset_version("lakehouse.analytics.trino_verify", "998877")
    assert node["id"] == "spark:DatasetVersion:lakehouse.analytics.trino_verify.998877"
    assert node["table"] == "lakehouse.analytics.trino_verify"
    assert node["snapshotId"] == "998877"


def test_map_transform_run_chain_produces_full_relation_set():
    run_record = {
        "run_id": "run-1",
        "transform": "agg_daily",
        "kind": "sql",
        "status": "succeeded",
        "error": None,
        "row_count": 3,
        "submitted_at": "2026-08-26T00:00:00Z",
        "completed_at": "2026-08-26T00:00:01Z",
        "rerun_of": None,
        "output_table": "lakehouse.analytics.out",
        "output_snapshot_id": "555",
        "inputs": [
            {"table": "lakehouse.analytics.trino_verify", "as_of_version": "111"}
        ],
    }
    entities, rels = map_transform_run_chain(run_record, remote_url=_REMOTE_URL)

    entity_types = {e["node_type"] for e in entities}
    assert entity_types == {
        "SparkApplication",
        "SparkJob",
        "Transform",
        "TransformRun",
        "DatasetVersion",
    }

    rel_names = {r["relationship"] for r in rels}
    assert rel_names == {
        "hasJob",
        "runs",
        "executesTransform",
        "producedVersion",
        "consumedVersion",
    }

    produced = [r for r in rels if r["relationship"] == "producedVersion"][0]
    assert produced["target"] == "spark:DatasetVersion:lakehouse.analytics.out.555"

    consumed = [r for r in rels if r["relationship"] == "consumedVersion"][0]
    assert (
        consumed["target"]
        == "spark:DatasetVersion:lakehouse.analytics.trino_verify.111"
    )


def test_map_transform_run_chain_skips_unpinned_inputs():
    run_record = {
        "run_id": "run-2",
        "transform": "t2",
        "kind": "sql",
        "status": "succeeded",
        "error": None,
        "row_count": 0,
        "submitted_at": "2026-08-26T00:00:00Z",
        "completed_at": "2026-08-26T00:00:01Z",
        "rerun_of": None,
        "output_table": "lakehouse.analytics.out2",
        "output_snapshot_id": None,
        "inputs": [{"table": "lakehouse.analytics.src", "as_of_version": None}],
    }
    _entities, rels = map_transform_run_chain(run_record, remote_url=_REMOTE_URL)
    rel_names = {r["relationship"] for r in rels}
    assert "consumedVersion" not in rel_names
    assert "producedVersion" not in rel_names
