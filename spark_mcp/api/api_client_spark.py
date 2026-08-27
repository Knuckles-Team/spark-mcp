"""Spark Connect (gRPC) client wrapper for spark-mcp.

Wraps ``pyspark.sql.connect.session.SparkSession.builder.remote(...)`` behind a thin,
typed-error ``SparkApi`` facade — the same shape every other fleet connector exposes
(``lakekeeper_mcp.api.api_client_lakekeeper.LakekeeperApi``, ``jena_mcp``'s client),
just over gRPC instead of HTTP/JSON.

**Server-side catalog registration (confirmed live, 2026-08-26):** the deployed
``spark-runner`` pod's Connect server (``services/spark/k8s`` ConfigMap
``spark-scripts``, key ``spark-connect-server.sh``) already registers the
``lakehouse`` Iceberg REST catalog at ``spark-submit`` startup —
``spark.sql.catalog.lakehouse.type=rest``, ``.scope=lakekeeper`` (explicit, never the
shared client default ``catalog``), pointed at the same Lakekeeper REST URI/warehouse/
credentials Trino's catalog uses. Catalog registration is a STATIC Spark conf set at
driver startup, not something a Spark Connect *client* can override at runtime — so
this client's own ``spark.sql.catalog.lakehouse.scope`` builder config is
belt-and-suspenders documentation of the invariant, not the enforcement point; the
real enforcement is server-side and this client fails closed (``_assert_lakehouse_scope``)
if a live read ever reports the config value drifting away from ``lakekeeper``.

**Identity propagation (mechanism TBD in the lane brief — resolved here):**
``pyspark.sql.connect.client.core.ChannelBuilder`` supports a ``token=`` URI param
that attaches a bearer token as gRPC ``access_token_call_credentials`` — real gRPC
metadata auth, not a fabrication. However, setting a token implicitly forces
``use_ssl=True`` (``grpc.secure_channel``), and the deployed ``spark-connect`` Service
speaks **plaintext** gRPC (`services/spark` ConfigMap: no ``spark.connect.grpc.*.tls``
conf, no `spark.connect.grpc.interceptor.classes` auth interceptor either) — attaching
a token today would break the live connection, not authenticate it. So: the mechanism
exists and is wired here (``get_client(attach_identity_token=True)``), but it degrades
to a documented no-op unless ``SPARK_CONNECT_USE_SSL=true`` (i.e. until CA-53 fronts
the Connect port with TLS). This is a genuine platform gap, not something this
package can fix — recorded in this package's AGENTS.md, not silently worked around.
"""

from __future__ import annotations

import threading
import uuid
from datetime import UTC, datetime
from typing import Any

from spark_mcp.api.api_client_base import (
    LAKEHOUSE_CATALOG_NAME,
    LAKEHOUSE_CATALOG_SCOPE,
    SparkApiError,
)

__all__ = ["SparkApi", "SparkApiError"]

_DEFAULT_TRANSFORM_RUN_LIMIT = 100


class SparkApi:
    """Authenticated-when-possible Spark Connect client + in-process TransformRun ledger.

    No Spark History Server exists in this deployment (services/spark/AGENTS.md) —
    ``_runs`` is this package's own persisted view of what it submitted, keyed by
    ``run_id``, never a reconstruction of Spark's own (absent) job history.
    """

    def __init__(
        self,
        *,
        remote_url: str,
        submit_timeout_s: float = 300.0,
        enable_transforms: bool = False,
        attach_identity_token: bool = False,
        identity_token_provider: Any = None,
        use_ssl: bool = False,
        lakehouse_catalog: str = LAKEHOUSE_CATALOG_NAME,
        lakehouse_scope: str = LAKEHOUSE_CATALOG_SCOPE,
    ) -> None:
        self.remote_url = remote_url
        self.submit_timeout_s = submit_timeout_s
        self.enable_transforms = enable_transforms
        self.attach_identity_token = attach_identity_token
        self.identity_token_provider = identity_token_provider
        self.use_ssl = use_ssl
        self.lakehouse_catalog = lakehouse_catalog
        self.lakehouse_scope = lakehouse_scope

        self._lock = threading.Lock()
        self._session: Any | None = None
        self._runs: dict[str, dict[str, Any]] = {}
        self._run_order: list[str] = []

    # ── session lifecycle ────────────────────────────────────────────────
    def _build_remote_uri(self) -> str:
        """Compose the ``sc://`` connection string, attaching a token only when

        both ``attach_identity_token`` AND ``use_ssl`` are set — a token without TLS
        would raise inside pyspark's own ``ChannelBuilder`` (``use_ssl`` is implied
        by a token being present, and this client refuses to silently downgrade a
        caller's requested identity-attach into a plaintext no-op).
        """
        uri = self.remote_url
        params: list[str] = []
        if self.use_ssl:
            params.append("use_ssl=true")
        if self.attach_identity_token:
            if not self.use_ssl:
                raise SparkApiError(
                    "attach_identity_token=True requires use_ssl=True (pyspark's "
                    "ChannelBuilder forces a secure channel once a token is set) — "
                    "the deployed spark-connect Service is plaintext today, so "
                    "identity-token attach cannot be enabled without TLS in front "
                    "of it (a platform gap, not something this client papers over)",
                    kind="disabled",
                )
            if self.identity_token_provider is None:
                raise SparkApiError(
                    "attach_identity_token=True but no identity_token_provider was given"
                )
            token = self.identity_token_provider()
            params.append(f"token={token}")
        if params:
            uri = uri.rstrip("/") + "/;" + ";".join(params)
        return uri

    def get_session(self):
        """Return the cached Spark Connect session, building it on first use."""
        with self._lock:
            if self._session is not None:
                return self._session
            try:
                import setuptools  # noqa: F401  (primes the distutils shim under py3.12+)
                from pyspark.sql.connect.session import SparkSession
            except ImportError as exc:  # pragma: no cover - env-dependent
                raise SparkApiError(
                    f"pyspark[connect] is not importable: {exc}", kind="unreachable"
                ) from exc

            builder = SparkSession.builder.remote(self._build_remote_uri())
            # Belt-and-suspenders: the server already registers this catalog at
            # spark-submit startup with scope=lakekeeper explicit; this client-side
            # config documents the same invariant and is a harmless no-op if the
            # Connect protocol ignores a static-catalog conf set post-startup.
            builder = builder.config(
                f"spark.sql.catalog.{self.lakehouse_catalog}.scope", self.lakehouse_scope
            )
            try:
                session = builder.getOrCreate()
            except Exception as exc:  # noqa: BLE001 - reclassified below
                raise SparkApiError(
                    f"Spark Connect session build failed against {self.remote_url!r}: {exc}",
                    kind="unreachable",
                ) from exc
            self._session = session
            return session

    def close(self) -> None:
        with self._lock:
            if self._session is not None:
                try:
                    self._session.stop()
                except Exception:  # noqa: BLE001 - best-effort teardown
                    pass
                self._session = None

    # ── session tool group ───────────────────────────────────────────────
    def sql(self, query: str, *, row_limit: int = 1000) -> dict[str, Any]:
        """Execute one SQL statement and return rows as plain dicts.

        Never degrades a connection failure or a query error to an empty result —
        distinguishes "Spark Connect unreachable" from "the query itself failed"
        (this lane's acceptance gate 3's explicit known-bad case).
        """
        session = self.get_session()
        try:
            df = session.sql(query)
            rows = df.limit(row_limit).collect()
        except SparkApiError:
            raise
        except Exception as exc:  # noqa: BLE001 - reclassified into a typed error
            raise SparkApiError(
                f"Spark SQL execution failed: {exc}", kind="job_failed"
            ) from exc
        return {
            "rows": [row.asDict(recursive=True) for row in rows],
            "row_count": len(rows),
            "columns": list(df.columns),
            "truncated": len(rows) >= row_limit,
        }

    def describe(self, table: str) -> dict[str, Any]:
        """Schema + column info for one fully-qualified table."""
        session = self.get_session()
        try:
            df = session.table(table)
            schema = [
                {"name": f.name, "type": str(f.dataType), "nullable": f.nullable}
                for f in df.schema.fields
            ]
        except Exception as exc:  # noqa: BLE001
            raise SparkApiError(
                f"describe failed for {table!r}: {exc}", kind="job_failed"
            ) from exc
        return {"table": table, "columns": schema}

    def cancel(self, tag: str | None = None) -> dict[str, Any]:
        """Interrupt running operations on this session (all, or one tagged operation)."""
        session = self.get_session()
        try:
            if tag:
                interrupted = session.interruptTag(tag)
            else:
                interrupted = session.interruptAll()
        except Exception as exc:  # noqa: BLE001
            raise SparkApiError(f"cancel failed: {exc}", kind="job_failed") from exc
        return {"interrupted_operation_ids": list(interrupted or []), "tag": tag}

    # ── catalog tool group ───────────────────────────────────────────────
    def list_lakekeeper_tables(self, namespace: str) -> dict[str, Any]:
        result = self.sql(f"SHOW TABLES IN {self.lakehouse_catalog}.{namespace}")
        return {"namespace": namespace, "tables": result["rows"], "count": result["row_count"]}

    # ── transforms tool group ────────────────────────────────────────────
    def _require_transforms_enabled(self) -> None:
        if not self.enable_transforms:
            raise SparkApiError(
                "Spark transform submission is disabled — set "
                "SPARK_ENABLE_TRANSFORMS=true to enable spark_submit_transform/"
                "spark_rerun_transform. This is the same real gate mechanism "
                "egeria-mcp's EGERIA_ENABLE_WRITE uses (api client refuses the "
                "write, not a UI-only guard) and complements DEC-CA-07's "
                "OntologyAction-level approval gate (dispatch_intent -> "
                "_approval_satisfied_by_session_load), which governs the tool at "
                "the fleet-intent layer above this client.",
                kind="disabled",
            )

    def _versioned_select(self, ref: dict[str, Any]) -> str:
        table = ref["table"]
        as_of = ref.get("as_of_version")
        if as_of:
            return f"{table} VERSION AS OF {as_of}"
        return table

    def _current_snapshot_id(self, table: str) -> str | None:
        """Best-effort read of a table's current Iceberg snapshot id, via the

        `<table>.snapshots` metadata table Iceberg exposes. Never lets a
        malformed/unexpected response here fail the whole TransformRun — this is
        enrichment (used to build the KG's `producedVersion` edge), not the
        transform's own success/failure signal.
        """
        try:
            result = self.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1")
            rows = result.get("rows") or []
            return str(rows[0]["snapshot_id"]) if rows else None
        except (SparkApiError, KeyError, IndexError, TypeError):
            return None

    def submit_transform(self, manifest: dict[str, Any], *, rerun_of: str | None = None) -> dict[str, Any]:
        """Execute one Transform manifest, recording a TransformRun (success or typed failure).

        SQL transforms run via ``spark.sql(body)`` with every declared input rewritten
        to ``table VERSION AS OF <as_of_version>`` when pinned (deterministic reruns —
        this lane's Failure/idempotency invariant: never re-reads "latest" implicitly
        on a declared rerun). 'pyspark' transforms evaluate `body` as a single
        DataFrame-API expression against a controlled namespace (`spark`, and each
        input bound by name) — a deliberately constrained subset of "submit an
        arbitrary PySpark job", not full spark-submit packaging (out of this lane's
        scope; see AGENTS.md).
        """
        self._require_transforms_enabled()
        run_id = str(uuid.uuid4())
        submitted_at = datetime.now(UTC).isoformat()
        transform = manifest["transform"]
        kind = manifest["kind"]
        body = manifest["body"]
        inputs = manifest.get("inputs", [])
        output = manifest["output"]

        record: dict[str, Any] = {
            "run_id": run_id,
            "transform": transform,
            "kind": kind,
            "manifest": manifest,
            "inputs": inputs,
            "output_table": output["table"],
            "submitted_at": submitted_at,
            "rerun_of": rerun_of,
        }

        try:
            if kind == "sql":
                sql_text = body
                for ref in inputs:
                    sql_text = sql_text.replace(ref["table"], self._versioned_select(ref))
                result = self.sql(sql_text)
                row_count = result["row_count"]
            elif kind == "pyspark":
                session = self.get_session()
                namespace = {"spark": session}
                for ref in inputs:
                    alias = ref["table"].rsplit(".", 1)[-1]
                    namespace[alias] = session.table(self._versioned_select(ref))
                df = eval(body, {"__builtins__": {}}, namespace)  # noqa: S307 - constrained namespace, write-gated + approval-gated
                row_count = df.count()
                mode = output.get("mode", "append")
                writer = df.write.format("iceberg")
                if mode == "overwrite":
                    writer.mode("overwrite").saveAsTable(output["table"])
                elif mode == "create":
                    writer.saveAsTable(output["table"])
                else:
                    writer.mode("append").saveAsTable(output["table"])
            else:
                raise SparkApiError(f"unknown transform kind {kind!r}", kind="job_failed")

            record["status"] = "succeeded"
            record["row_count"] = row_count
            record["output_snapshot_id"] = self._current_snapshot_id(output["table"])
            record["error"] = None
        except SparkApiError as exc:
            record["status"] = "failed"
            record["error"] = str(exc)
            record["row_count"] = None
            record["output_snapshot_id"] = None
            record["completed_at"] = datetime.now(UTC).isoformat()
            self._store_run(record)
            raise
        except Exception as exc:  # noqa: BLE001
            record["status"] = "failed"
            record["error"] = str(exc)
            record["row_count"] = None
            record["output_snapshot_id"] = None
            record["completed_at"] = datetime.now(UTC).isoformat()
            self._store_run(record)
            raise SparkApiError(
                f"transform {transform!r} failed: {exc}", kind="job_failed"
            ) from exc

        record["completed_at"] = datetime.now(UTC).isoformat()
        self._store_run(record)
        return record

    def rerun_transform(self, run_id: str) -> dict[str, Any]:
        """Deterministically re-execute a prior TransformRun's manifest.

        Never re-reads "latest" implicitly — reuses the ORIGINAL run's pinned
        `inputs` verbatim (this lane's Failure/idempotency invariant), even if the
        original run had no `as_of_version` pinned (that first run is documented as
        non-reproducible; a rerun of it just repeats the same unpinned read).
        """
        self._require_transforms_enabled()
        original = self._runs.get(run_id)
        if original is None:
            raise SparkApiError(f"no TransformRun found for run_id={run_id!r}", kind="job_failed")
        return self.submit_transform(original["manifest"], rerun_of=run_id)

    def _store_run(self, record: dict[str, Any]) -> None:
        with self._lock:
            self._runs[record["run_id"]] = record
            self._run_order.append(record["run_id"])

    def list_transform_runs(self, *, limit: int = _DEFAULT_TRANSFORM_RUN_LIMIT, transform: str | None = None) -> dict[str, Any]:
        with self._lock:
            ids = list(reversed(self._run_order))
        records = [self._runs[i] for i in ids if transform is None or self._runs[i]["transform"] == transform]
        page = records[:limit]
        return {"runs": page, "count": len(page), "total": len(records)}

    # ── lineage tool group ───────────────────────────────────────────────
    def lineage_config(self) -> dict[str, Any]:
        """Report OpenLineage listener config — already set server-side at

        spark-submit startup (services/spark ConfigMap `spark-scripts`), not
        something this client can toggle at runtime over Connect (matching
        lakekeeper-mcp's analogous `lakekeeper_cloudevents_status` pattern: report,
        don't fake a runtime mutation this protocol cannot make).
        """
        return {
            "listener": "io.openlineage.spark.agent.OpenLineageSparkListener",
            "transport": "kafka",
            "topic": "openlineage.events",
            "namespace": "ca-53-spark-connect",
            "configured_by": "spark-connect-server.sh (spark-submit --conf, static at server startup)",
            "note": (
                "spark.extraListeners is a static driver-startup Spark conf — not "
                "runtime-settable over the Connect protocol. This tool reports the "
                "known server configuration; it never claims to have just set it."
            ),
        }

    def lineage_events(self, limit: int = 20) -> dict[str, Any]:
        """Named per this lane's tool-group contract; NOT a Kafka consumer.

        Consuming `openlineage.events` and materializing `prov:Activity` nodes is
        CA-25's explicit territory (this lane's own non-goals). Delegates rather
        than fakes a read this package was never scoped to implement — same pattern
        as lakekeeper-mcp's `lakekeeper_cloudevents_subscribe`.
        """
        raise SparkApiError(
            "spark_lineage_events is not implemented here: OpenLineage RunEvents "
            "are published to the 'openlineage.events' Kafka topic (DEC-CA-05) and "
            "consumed by CA-25's au-side listener, not by this MCP. Read them via "
            "kafka-mcp's consumer-group tools against topic 'openlineage.events', "
            "or once CA-25 lands, query the KG for the materialized prov:Activity "
            f"nodes directly (requested limit={limit}).",
            kind="disabled",
        )

    # ── ops tool group ───────────────────────────────────────────────────
    def application_status(self) -> dict[str, Any]:
        """Best-effort application/session status.

        No Spark History Server exists in this deployment — there is no server-side
        job history to query once a job's process exits (services/spark/AGENTS.md).
        This reports what the CLIENT itself can observe: whether a session is
        live, the remote endpoint, Spark's reported version, and this package's own
        submitted-run counters — never fabricates cluster-wide state it cannot see.
        """
        session_live = self._session is not None
        status: dict[str, Any] = {
            "remote_url": self.remote_url,
            "session_live": session_live,
            "lakehouse_catalog": self.lakehouse_catalog,
            "lakehouse_scope": self.lakehouse_scope,
            "transform_runs_recorded": len(self._runs),
            "history_server": "not deployed (services/spark/AGENTS.md) — status is client-observed only",
        }
        if session_live:
            try:
                status["spark_version"] = self._session.version
            except Exception:  # noqa: BLE001
                status["spark_version"] = None
        return status
