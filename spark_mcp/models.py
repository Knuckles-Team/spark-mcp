"""Pydantic models for Spark Connect transform submission and lineage tracking."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class DatasetVersionRef(BaseModel):
    """One pinned input table reference — never an implicit 'latest' read.

    ``as_of_version`` pins the Iceberg snapshot id a Transform reads, so a
    ``spark_rerun_transform`` against the same manifest is deterministic (this
    lane's explicit Failure/idempotency invariant).
    """

    table: str = Field(description="Fully-qualified table, e.g. 'lakehouse.analytics.trino_verify'.")
    as_of_version: str | None = Field(
        default=None,
        description=(
            "Pinned Iceberg snapshot id (VERSION AS OF). Omit only for a first, "
            "non-reproducible run — every 'spark_rerun_transform' call must pass one."
        ),
    )


class TransformOutput(BaseModel):
    """The one table a Transform writes."""

    table: str = Field(description="Fully-qualified output table, e.g. 'lakehouse.analytics.agg'.")
    mode: Literal["append", "overwrite", "create"] = Field(
        default="append", description="Iceberg write mode for the output table."
    )


class TransformManifest(BaseModel):
    """A versioned Transform declaration — the unit ``spark_submit_transform`` accepts.

    Matches ``designs/MCP-SERVERS.md`` §3's sketch: ``transform``, ``kind``, ``body``,
    ``inputs[{table, as_of_version}]``, ``output{table}``.
    """

    transform: str = Field(description="Transform name (stable identifier across reruns).")
    kind: Literal["sql", "pyspark"] = Field(description="'sql' runs `body` via spark.sql(); 'pyspark' runs a constrained DataFrame-API expression.")
    body: str = Field(description="SQL text ('sql') or a single DataFrame-API expression evaluated against `spark`/`inputs` ('pyspark').")
    inputs: list[DatasetVersionRef] = Field(default_factory=list, description="Pinned input DatasetVersions.")
    output: TransformOutput = Field(description="The output table this Transform writes.")


class TransformRunRecord(BaseModel):
    """One recorded execution of a Transform — this package's own persisted view.

    No Spark History Server exists in this deployment (services/spark/AGENTS.md) —
    this record is what ``spark_list_transform_runs``/KG ingest key off, not Spark's
    own job history, which is lost once the driver process's job list ages out.
    """

    run_id: str
    transform: str
    kind: Literal["sql", "pyspark"]
    status: Literal["succeeded", "failed"]
    error: str | None = None
    inputs: list[DatasetVersionRef] = Field(default_factory=list)
    output_table: str
    output_snapshot_id: str | None = None
    row_count: int | None = None
    submitted_at: str
    completed_at: str | None = None
    rerun_of: str | None = None
