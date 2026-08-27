"""Shared error type + small helpers for the Spark Connect API wrapper.

Unlike the fleet's REST-backed connectors (``lakekeeper_mcp``, ``jena_mcp``), Spark
Connect is a gRPC protocol, not HTTP/JSON — there is no ``requests.Session`` to wrap.
This module carries only the one thing every fleet API client needs regardless of
transport: a typed, non-degrading error so a caller (tool layer, KG ingest) can tell
"the job failed" from "Spark Connect is unreachable" from "the query returned zero
rows" — never collapsing all three into an empty result (the same ServiceNow-PDI-style
lesson recorded platform-wide, and this lane's own explicit "typed gRPC status, not an
opaque exit code" requirement).
"""

from __future__ import annotations

from typing import Any


class SparkApiError(RuntimeError):
    """A Spark Connect call failed with a typed, non-degrading condition.

    ``kind`` distinguishes the three failure classes this lane's contract calls out
    by name: ``"unreachable"`` (Spark Connect endpoint down / connection refused —
    never silently retried into a hang, never a `kubectl exec` fallback),
    ``"job_failed"`` (the query/job itself errored — a real Spark exception),
    and ``"session_evicted"`` (the gRPC session was torn down server-side mid-call).
    ``"disabled"`` covers the ``SPARK_ENABLE_TRANSFORMS``-gated write path.
    """

    def __init__(
        self,
        message: str,
        *,
        kind: str = "job_failed",
        status_code: int | None = None,
        body: Any = None,
    ):
        super().__init__(message)
        self.kind = kind
        self.status_code = status_code
        self.body = body


# The catalog scope landmine, named explicitly in services/spark/AGENTS.md and this
# lane's own Authority/invariants section: the shared Iceberg-REST/OAuth2 client
# convention defaults to "catalog" — Lakekeeper's Keycloak client is provisioned for
# "lakekeeper", and the default silently gets a token/scope Lakekeeper rejects
# (`invalid_scope: Invalid scopes: catalog`). Spark's SparkCatalog and Trino's Iceberg
# REST connector share the same underlying OAuth2 client code, so this is identical
# to the Trino/Lakekeeper failure mode.
LAKEHOUSE_CATALOG_SCOPE = "lakekeeper"
LAKEHOUSE_CATALOG_NAME = "lakehouse"
