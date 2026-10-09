"""Native epistemic-graph ingestion for Spark Connect job/transform/lineage records.

CONCEPT:AU-KG.ingest.enterprise-source-extractor. The record-source twin of
``lakekeeper_mcp``/``egeria_mcp``'s kg_ingest modules: spark-mcp pushes what it
itself observed submitting Transforms over the live Spark Connect session (there is
no Spark History Server in this deployment to backfill from — services/spark/
AGENTS.md) into the epistemic-graph engine as typed OWL nodes (``:SparkApplication``,
``:SparkJob``, ``:Transform``, ``:TransformRun``, ``:DatasetVersion``), with
``hasJob``/``runs``/``executesTransform``/``producedVersion``/``consumedVersion``
relations.

The txn write path is the required ``agent_connector_sdk.ingest`` knowledge-ingest
facade (``KnowledgeIngest``/``current_ingest``) — never a bespoke write path. Node
ids follow ``spark:<class>:<externalId>``; ``node_type`` on each entity matches a
class federated by ``spark_mcp.ontology`` (``spark.ttl``). Batches at ≤500 entities
per ``ingest_entities`` call (egeria-mcp's convention). Raises ``IngestError``
rather than silently acking a partial batch.
"""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import urlparse

from agent_connector_sdk.ingest import (
    ChangeSet,
    Entity,
    IngestBinding,
    IngestError,
    KnowledgeIngest,
    Relationship,
    current_ingest,
)
from fastmcp import FastMCP
from pydantic import Field

logger = logging.getLogger("spark_mcp.kg")

_BINDING = IngestBinding(connector="spark-mcp", stream="spark")
_BATCH_SIZE = 500


def _to_entity(record: dict[str, Any]) -> Entity:
    return Entity(
        id=record.get("id"),
        node_type=record.get("node_type"),
        properties={
            k: v for k, v in record.items() if k not in ("id", "node_type")
        },
    )


def _to_relationship(record: dict[str, Any]) -> Relationship:
    props = {
        k: v
        for k, v in record.items()
        if k not in ("source", "target", "relationship")
    }
    return Relationship(
        source=record["source"],
        target=record["target"],
        relationship=record["relationship"],
        properties=props or None,
    )


async def ingest_entities(
    entities: list[dict[str, Any]],
    relationships: list[dict[str, Any]] | None = None,
    *,
    ingest: KnowledgeIngest | None = None,
) -> dict[str, int]:
    """Write canonical typed nodes and relationships through the EG ingest client."""
    if not entities:
        raise IngestError("ingest_entities needs at least one entity")
    change_set = ChangeSet(
        entities=tuple(_to_entity(e) for e in entities),
        relationships=tuple(_to_relationship(r) for r in relationships or ()),
    )
    service = ingest or current_ingest()
    receipt = await service.submit(_BINDING, change_set)
    return {"nodes": receipt.affected_count, "edges": receipt.relationship_count}


# ── record → entity/relationship mappers ─────────────────────────────────────
def _application_id(remote_url: str) -> str:
    parsed = urlparse(remote_url.replace("sc://", "http://", 1))
    netloc = parsed.netloc or remote_url
    return f"spark:SparkApplication:{netloc}"


def _job_id(run_id: str) -> str:
    return f"spark:SparkJob:{run_id}"


def _transform_id(transform: str) -> str:
    return f"spark:Transform:{transform}"


def _run_id(run_id: str) -> str:
    return f"spark:TransformRun:{run_id}"


def _dataset_version_id(table: str, version: str) -> str:
    return f"spark:DatasetVersion:{table}.{version}"


def map_application(remote_url: str) -> dict[str, Any]:
    return {
        "id": _application_id(remote_url),
        "node_type": "SparkApplication",
        "name": remote_url,
        "remoteUrl": remote_url,
        "externalToolId": remote_url,
    }


def map_job(run_record: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": _job_id(run_record["run_id"]),
        "node_type": "SparkJob",
        "name": f"job for run {run_record['run_id']}",
        "kind": run_record.get("kind"),
        "externalToolId": run_record["run_id"],
    }


def map_transform(transform: str, kind: str) -> dict[str, Any]:
    return {
        "id": _transform_id(transform),
        "node_type": "Transform",
        "name": transform,
        "kind": kind,
        "externalToolId": transform,
    }


def map_transform_run(run_record: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": _run_id(run_record["run_id"]),
        "node_type": "TransformRun",
        "name": f"run {run_record['run_id']}",
        "status": run_record.get("status"),
        "error": run_record.get("error"),
        "rowCount": run_record.get("row_count"),
        "submittedAt": run_record.get("submitted_at"),
        "completedAt": run_record.get("completed_at"),
        "rerunOf": run_record.get("rerun_of"),
        "externalToolId": run_record["run_id"],
    }


def map_dataset_version(table: str, version: str) -> dict[str, Any]:
    return {
        "id": _dataset_version_id(table, version),
        "node_type": "DatasetVersion",
        "name": f"{table}@{version}",
        "table": table,
        "snapshotId": version,
        "externalToolId": f"{table}.{version}",
    }


def map_transform_run_chain(
    run_record: dict[str, Any], *, remote_url: str
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """One TransformRun -> the full SparkApplication->...->DatasetVersion chain."""
    entities: list[dict[str, Any]] = []
    relationships: list[dict[str, Any]] = []

    app_entity = map_application(remote_url)
    job_entity = map_job(run_record)
    transform_entity = map_transform(
        run_record["transform"], run_record.get("kind", "sql")
    )
    run_entity = map_transform_run(run_record)

    entities.extend([app_entity, job_entity, transform_entity, run_entity])
    relationships.append(
        {
            "source": app_entity["id"],
            "target": job_entity["id"],
            "relationship": "hasJob",
        }
    )
    relationships.append(
        {"source": job_entity["id"], "target": run_entity["id"], "relationship": "runs"}
    )
    relationships.append(
        {
            "source": run_entity["id"],
            "target": transform_entity["id"],
            "relationship": "executesTransform",
        }
    )

    output_table = run_record.get("output_table")
    output_snapshot = run_record.get("output_snapshot_id")
    if output_table and output_snapshot:
        produced = map_dataset_version(output_table, output_snapshot)
        entities.append(produced)
        relationships.append(
            {
                "source": run_entity["id"],
                "target": produced["id"],
                "relationship": "producedVersion",
            }
        )

    for ref in run_record.get("inputs") or []:
        table = ref.get("table")
        version = ref.get("as_of_version")
        if not table or not version:
            continue
        consumed = map_dataset_version(table, version)
        entities.append(consumed)
        relationships.append(
            {
                "source": run_entity["id"],
                "target": consumed["id"],
                "relationship": "consumedVersion",
            }
        )

    return entities, relationships


async def ingest_run(
    run_record: dict[str, Any],
    *,
    remote_url: str,
    ingest: KnowledgeIngest | None = None,
) -> dict[str, int]:
    """Push one TransformRun's full chain into the KG. Never partially commits."""
    entities, relationships = map_transform_run_chain(run_record, remote_url=remote_url)

    total_nodes = 0
    total_edges = 0
    for start in range(0, len(entities), _BATCH_SIZE):
        batch = entities[start : start + _BATCH_SIZE]
        res = await ingest_entities(batch, None, ingest=ingest)
        total_nodes += res.get("nodes", 0)

    if relationships:
        anchor = entities[0]
        for start in range(0, len(relationships), _BATCH_SIZE):
            batch = relationships[start : start + _BATCH_SIZE]
            res = await ingest_entities([anchor], batch, ingest=ingest)
            total_edges += res.get("edges", 0)

    return {"nodes": total_nodes, "edges": total_edges, "run_id": run_record["run_id"]}


def register_ingest_tools(mcp: FastMCP) -> None:
    """Register the Wire-First KG TransformRun ingest tool."""

    @mcp.tool(
        annotations={
            "title": "Ingest Spark TransformRun Into KG",
            "readOnlyHint": False,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        },
        tags={"ingest", "mutating"},
    )
    async def spark_ingest_run(
        run_id: str = Field(
            description="run_id of a recorded TransformRun (from spark_list_transform_runs)."
        ),
    ) -> dict[str, int]:
        """Push one TransformRun's SparkApplication->SparkJob->Transform->TransformRun

        ->DatasetVersion chain into the KG as typed OWL nodes. Never partially
        commits — the EG ingest client raises ``IngestError`` rather than
        silently acking a partial batch.
        """
        from spark_mcp.auth import get_client

        api = get_client()
        result = api.list_transform_runs(limit=1000)
        run_record = next((r for r in result["runs"] if r["run_id"] == run_id), None)
        if run_record is None:
            raise ValueError(f"no TransformRun found for run_id={run_id!r}")
        return await ingest_run(run_record, remote_url=api.remote_url)
