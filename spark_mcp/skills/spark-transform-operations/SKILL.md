---
name: spark-transform-operations
skill_type: skill
description: >-
  Execute SQL over a live Spark Connect session and submit/track/rerun versioned
  Transform manifests against the shared Iceberg lakehouse catalog via the
  spark-mcp MCP server's session/transforms/catalog/lineage/ops tools. Use when
  the agent must query the lakehouse through Spark, run a governed SQL/PySpark
  transform that pins its input DatasetVersions, or check a Transform's run
  history/application status. Do NOT use to push Spark records into the KG (use
  spark-kg-ingestion) or to administer the Lakekeeper catalog directly (use
  lakekeeper-mcp's own skills).
license: MIT
tags: [spark, spark-connect, iceberg, transform, mcp]
metadata:
  author: Genius
  version: '0.1.0'
---
# Spark Transform Operations

Spark Connect is LIVE on `spark-connect.apps.svc:15002` (the `spark-runner` pod's
Connect server) — proven with a real `pyspark.sql.connect` session reading the
deployed `lakehouse.analytics.trino_verify` table (3 rows, matching Trino's own
proven read of the same table). The `lakehouse` Iceberg REST catalog is registered
SERVER-SIDE at spark-submit startup with `scope=lakekeeper` explicit — the same
landmine Trino/Lakekeeper hit if this is ever dropped.

## When to use
- Run ad hoc SQL against the lakehouse (`spark_sql`, `spark_describe`).
- Submit a versioned Transform manifest — SQL or a constrained PySpark
  DataFrame-API expression — that pins its input snapshots (`spark_submit_transform`).
- List or deterministically rerun a prior Transform execution
  (`spark_list_transform_runs`, `spark_rerun_transform`).
- List tables in a lakehouse namespace (`spark_list_lakekeeper_tables`).
- Check OpenLineage listener config or client-observed application status
  (`spark_lineage_config`, `spark_application_status`).
- Interrupt a running operation (`spark_cancel`).

## When NOT to use
- Pushing a TransformRun into the KG → `spark-kg-ingestion`.
- Reading OpenLineage RunEvents from Kafka → not implemented here; delegate to
  `kafka-mcp` (topic `openlineage.events`) or, once CA-25 lands, query the KG's
  materialized `prov:Activity` nodes directly.
- Falling back to `kubectl exec spark-sql` if Spark Connect is unreachable —
  this package never does that; it fails typed instead.

## Prerequisites & environment
Connect via the `mcp-client` skill against the **`spark-mcp`** MCP server.

| Variable | Required | Notes |
|----------|----------|-------|
| `SPARK_CONNECT_URL` | recommended | Default `sc://spark-connect.apps.svc:15002` |
| `SPARK_SUBMIT_TIMEOUT_S` | optional | Default 300s |
| `SPARK_LAKEHOUSE_CATALOG` / `SPARK_LAKEHOUSE_SCOPE` | optional | Default `lakehouse` / `lakekeeper` — never leave scope at the shared-client default `catalog` |
| `SPARK_ENABLE_TRANSFORMS` | ✅ for writes | `spark_submit_transform`/`spark_rerun_transform` refuse unless `true` |
| `SPARK_CONNECT_ATTACH_IDENTITY_TOKEN` / `SPARK_CONNECT_USE_SSL` | optional | Disabled by default — the deployed endpoint is plaintext gRPC; see `spark_mcp/auth.py` |

## Tools

| Tool | Purpose |
|------|---------|
| `spark_sql` | Execute one SQL statement, returns rows + columns |
| `spark_describe` | Table schema |
| `spark_cancel` | Interrupt running operation(s) |
| `spark_submit_transform` | Execute a versioned Transform (approval-gated, `SPARK_ENABLE_TRANSFORMS`) |
| `spark_list_transform_runs` | List this package's own TransformRun ledger (no Spark History Server exists) |
| `spark_rerun_transform` | Deterministically re-execute a prior run's pinned manifest |
| `spark_list_lakekeeper_tables` | List tables in one lakehouse namespace |
| `spark_lineage_config` | Report OpenLineage listener config (server-side, static) |
| `spark_lineage_events` | Not implemented — delegates to kafka-mcp/CA-25 |
| `spark_application_status` | Client-observed session/application status |

## Failure modes to expect
- An unreachable Spark Connect endpoint raises a typed connection error, never a
  silent hang or an empty result.
- `spark_submit_transform`/`spark_rerun_transform` refuse when `SPARK_ENABLE_TRANSFORMS`
  is unset — a fail-closed client-side gate beneath DEC-CA-07's fleet-intent approval
  layer (`dispatch_intent` -> `_approval_satisfied_by_session_load`).
- A rerun always reuses the ORIGINAL run's pinned `inputs` verbatim — never
  re-resolves "latest" implicitly.
