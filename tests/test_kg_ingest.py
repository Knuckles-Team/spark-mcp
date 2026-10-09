"""Record -> OWL entity/relationship mapping for the Spark KG ingest tool."""

from types import SimpleNamespace

import pytest
from agent_connector_sdk.ingest import IngestError, KnowledgeIngest

from spark_mcp.kg_ingest import (
    ingest_entities,
    map_application,
    map_dataset_version,
    map_job,
    map_transform,
    map_transform_run,
    map_transform_run_chain,
)

_REMOTE_URL = "sc://spark-connect.apps.svc:15002"


class _FakeTransport:
    """Fakes the transport boundary below KnowledgeIngest, per the fleet SDK
    migration recipe's ingestion test-double pattern — the SDK's own request
    builder/validation still runs on top of this.
    """

    def __init__(self):
        self.requests = []

    async def source_status(self, connector, stream):
        return SimpleNamespace(accepted_checkpoint=None)

    async def submit(self, request):
        self.requests.append(request)
        return SimpleNamespace(
            affected_count=len(request.records),
            relationship_count=len(request.relationships),
        )

    async def store_blob(self, data):
        raise AssertionError("spark-mcp ingestion carries no media")


@pytest.fixture
def ingest():
    transport = _FakeTransport()
    return KnowledgeIngest(transport, loop=None), transport


async def test_ingest_entities_submits_through_the_sdk(ingest):
    service, transport = ingest
    entity = map_application(_REMOTE_URL)

    result = await ingest_entities([entity], ingest=service)

    assert result == {"nodes": 1, "edges": 0}
    assert len(transport.requests) == 1
    assert transport.requests[0].records[0].record_id == entity["id"]


async def test_ingest_entities_rejects_an_empty_batch(ingest):
    service, _transport = ingest
    with pytest.raises(IngestError):
        await ingest_entities([], ingest=service)


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
