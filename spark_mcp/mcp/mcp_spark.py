"""Thin MCP wrappers around the Spark Connect API client.

Each tool is a thin shim: it parses params, calls the corresponding ``SparkApi``
method, and returns the result. All business logic lives in ``spark_mcp.api`` — these
tools add only the ``requires_approval`` framing note and the write-gate error
passthrough (this lane's DEC-CA-07 negative test).

Five tool groups, per this lane's contract and DEC-CA-08's package layout:
  * session     — spark_sql / spark_describe / spark_cancel
  * transforms  — spark_submit_transform / spark_list_transform_runs / spark_rerun_transform
  * catalog     — spark_list_lakekeeper_tables
  * lineage     — spark_lineage_config / spark_lineage_events
  * ops         — spark_application_status
"""

from __future__ import annotations

from typing import Any, Literal

from fastmcp import FastMCP
from pydantic import Field

from spark_mcp.auth import get_client
from spark_mcp.models import DatasetVersionRef, TransformManifest, TransformOutput


def register_spark_tools(mcp: FastMCP) -> None:
    """Register session/transforms/catalog/lineage/ops tools."""

    # ── session ───────────────────────────────────────────────────────────
    @mcp.tool(tags={"session"})
    async def spark_sql(
        query: str = Field(
            description="SQL statement to execute, e.g. 'SELECT 1' or a lakehouse table read."
        ),
        row_limit: int = Field(
            default=1000,
            description="Max rows to return (result is truncated, not failed, past this).",
        ),
    ) -> dict[str, Any]:
        """Execute one SQL statement over the live Spark Connect session.

        A Spark Connect endpoint that is unreachable raises a typed connection
        error (never a silent hang or an empty result) — this lane's acceptance
        gate 3's explicit known-bad case.
        """
        return get_client().sql(query, row_limit=row_limit)

    @mcp.tool(tags={"session"})
    async def spark_describe(
        table: str = Field(
            description="Fully-qualified table, e.g. 'lakehouse.analytics.trino_verify'."
        ),
    ) -> dict[str, Any]:
        """Describe one table's schema over the live Spark session."""
        return get_client().describe(table)

    @mcp.tool(
        annotations={
            "title": "Cancel Running Spark Operations",
            "readOnlyHint": False,
            "destructiveHint": True,
            "idempotentHint": True,
            "openWorldHint": True,
        },
        tags={"session", "mutating"},
    )
    async def spark_cancel(
        tag: str = Field(
            default="",
            description="Interrupt only operations tagged with this value; empty interrupts ALL running operations on this session.",
        ),
    ) -> dict[str, Any]:
        """Interrupt running Spark operation(s) on this session."""
        return get_client().cancel(tag or None)

    # ── transforms ────────────────────────────────────────────────────────
    @mcp.tool(
        annotations={
            "title": "Submit Spark Transform",
            "readOnlyHint": False,
            "destructiveHint": True,
            "idempotentHint": False,
            "openWorldHint": True,
        },
        tags={"transforms", "mutating", "write"},
    )
    async def spark_submit_transform(
        transform: str = Field(
            description="Transform name (stable identifier across reruns)."
        ),
        kind: Literal["sql", "pyspark"] = Field(
            description="'sql' runs `body` via spark.sql(); 'pyspark' evaluates a constrained DataFrame-API expression."
        ),
        body: str = Field(
            description="SQL text, or a single DataFrame-API expression for 'pyspark'."
        ),
        output_table: str = Field(
            description="Fully-qualified output table this Transform writes."
        ),
        output_mode: Literal["append", "overwrite", "create"] = Field(default="append"),
        inputs: list[dict[str, Any]] = Field(
            default_factory=list,
            description="Pinned input refs: [{'table': 'lakehouse.ns.t', 'as_of_version': '<snapshot_id>'}, ...]. Omit 'as_of_version' (never pass null) for a first, non-reproducible run.",
        ),
    ) -> dict[str, Any]:
        """Execute a versioned Transform manifest, recording a :TransformRun (DEC-CA-07).

        **Requires approval**: this is a mutating ``OntologyAction``
        (``spark_submit_transform`` in the intended DEC-CA-07 typed-Actions block —
        deferred to ``review_todos`` pending CA-32's ``ActionSpec`` schema merge, see
        this package's AGENTS.md) and is gated at the client layer by
        ``SPARK_ENABLE_TRANSFORMS`` (refuses, not silently executes, when unset —
        the same real mechanism as egeria-mcp's ``EGERIA_ENABLE_WRITE``). At the
        fleet-intent layer, an unapproved dispatch through ``dispatch_intent`` is
        refused by ``_approval_satisfied_by_session_load`` until the calling
        session has explicitly loaded this exact tool (BUG-040's fix) — this
        client-side gate is the second, always-enforced layer beneath that.

        Never re-reads "latest" implicitly for a pinned input — `as_of_version`
        rewrites every declared input to `table VERSION AS OF <snapshot_id>`.
        """
        manifest = TransformManifest(
            transform=transform,
            kind=kind,
            body=body,
            inputs=[DatasetVersionRef(**i) for i in inputs],
            output=TransformOutput(table=output_table, mode=output_mode),
        ).model_dump()
        return get_client().submit_transform(manifest)

    @mcp.tool(tags={"transforms"})
    async def spark_list_transform_runs(
        limit: int = Field(
            default=100, description="Max runs to return, most recent first."
        ),
        transform: str = Field(
            default="", description="Filter to one Transform name, or empty for all."
        ),
    ) -> dict[str, Any]:
        """List recorded TransformRuns (this package's own ledger — no Spark History Server exists)."""
        return get_client().list_transform_runs(
            limit=limit, transform=transform or None
        )

    @mcp.tool(
        annotations={
            "title": "Rerun Spark Transform",
            "readOnlyHint": False,
            "destructiveHint": True,
            "idempotentHint": False,
            "openWorldHint": True,
        },
        tags={"transforms", "mutating", "write"},
    )
    async def spark_rerun_transform(
        run_id: str = Field(
            description="run_id of a prior TransformRun (from spark_list_transform_runs)."
        ),
    ) -> dict[str, Any]:
        """Deterministically re-execute a prior TransformRun's manifest and pinned inputs.

        Same approval/write-gate posture as ``spark_submit_transform``.
        """
        return get_client().rerun_transform(run_id)

    # ── catalog ───────────────────────────────────────────────────────────
    @mcp.tool(tags={"catalog"})
    async def spark_list_lakekeeper_tables(
        namespace: str = Field(
            description="Namespace within the lakehouse catalog, e.g. 'analytics'."
        ),
    ) -> dict[str, Any]:
        """List tables in one namespace of the shared `lakehouse` Iceberg catalog

        (the same catalog Lakekeeper/Trino address — read-only, via `SHOW TABLES`).
        """
        return get_client().list_lakekeeper_tables(namespace)

    # ── lineage ───────────────────────────────────────────────────────────
    @mcp.tool(tags={"lineage"})
    async def spark_lineage_config() -> dict[str, Any]:
        """Report the OpenLineage listener configuration (server-side, static at startup)."""
        return get_client().lineage_config()

    @mcp.tool(tags={"lineage"})
    async def spark_lineage_events(
        limit: int = Field(default=20, description="Max recent RunEvents requested."),
    ) -> dict[str, Any]:
        """Not implemented here — delegates to kafka-mcp / CA-25's KG consumer (see docstring)."""
        return get_client().lineage_events(limit=limit)

    # ── ops ───────────────────────────────────────────────────────────────
    @mcp.tool(tags={"ops"})
    async def spark_application_status() -> dict[str, Any]:
        """Client-observed Spark Connect application/session status (no History Server exists)."""
        return get_client().application_status()
