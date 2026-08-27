# spark-mcp

A Model Context Protocol (MCP) server, A2A agent, and API client for Spark Connect
(the Iceberg lakehouse transform/query surface) integration.

![MCP Server](https://badge.mcpx.dev?type=server 'MCP Server')

## Table of Contents
- [Overview](#overview)
- [Installation](#installation)
- [Usage](#usage)
- [Architecture](#architecture)
- [Environment Variables](#environment-variables)
- [MCP Tools](#mcp-tools)
- [Documentation](#documentation)

## Overview
`spark-mcp` exposes a standardized interface to the live Spark Connect deployment
via the Model Context Protocol — SQL/session execution, versioned Transform
submission/tracking/rerun against the shared Iceberg `lakehouse` catalog
(`scope=lakekeeper`, explicit — the same landmine Trino/Lakekeeper hit if dropped),
namespace/table catalog reads, OpenLineage config/status reporting, and a
Wire-First `spark_ingest_run` tool that pushes the
`SparkApplication -> SparkJob -> Transform -> TransformRun -> DatasetVersion` chain
into the epistemic-graph Knowledge Graph.

**Spark Connect is live**, confirmed 2026-08-26 against
`spark-connect.apps.svc:15002` with a real `pyspark.sql.connect` session — a query
against `lakehouse.analytics.trino_verify` returned the same 3 rows Trino's sibling
lane already proved reading/writing (cross-engine Iceberg interop). See
[docs/architecture.md](docs/architecture.md) for the full evidence and this
package's `AGENTS.md` for known gaps (identity-token attach, no Spark History
Server, `ActionSpec` typed-Actions deferral).

Talks directly to Spark over `pyspark[connect]` — the gRPC Connect client, never
`bin/spark-shell --remote` (does not work on the vanilla Spark image) and never a
`kubectl exec spark-sql` fallback.

## Installation

Pick the extra that matches what you want to run:

| Extra | Installs | Use when |
|-------|----------|----------|
| `spark-mcp[mcp]` | Connector-focused MCP server (`agent-utilities[mcp]` — FastMCP/FastAPI + `epistemic-graph[full]`) | You only run the **MCP server** (smallest install / image) |
| `spark-mcp[agent]` | Agent runtime (`agent-utilities[agent-runtime,logfire]` — model orchestration + `epistemic-graph[full]`) | You run the **integrated A2A agent** |
| `spark-mcp[all]` | Everything (`mcp` + `agent` + `logfire`) | Development / both surfaces |

```bash
uv pip install "spark-mcp[mcp]"
```

### Container images (`:mcp` vs `:agent`)

One multi-stage `docker/Dockerfile` builds two right-sized images, selected by `--target`:

```bash
docker build --target mcp   -t spark-mcp:mcp    .
docker build --target agent -t spark-mcp:agent   .
```

Both images install a headless JRE at runtime — `pyspark[connect]`'s gRPC client has
a JVM-side dependency, unlike the fleet's pure-Python REST connectors.

## Usage
Run the MCP server directly:
```bash
python -m spark_mcp
```

### MCP Configuration Example (stdio)

```json
{
  "mcpServers": {
    "spark-mcp": {
      "command": "uv",
      "args": ["run", "spark-mcp"],
      "env": {
        "MCP_TOOL_MODE": "intent",
        "SPARK_CONNECT_URL": "sc://spark-connect.apps.svc:15002",
        "SPARK_LAKEHOUSE_CATALOG": "lakehouse",
        "SPARK_LAKEHOUSE_SCOPE": "lakekeeper",
        "SPARK_ENABLE_TRANSFORMS": "False",
        "SPARKTOOL": "True"
      }
    }
  }
}
```

## Architecture

`api_client.py` wraps `api/api_client_spark.py`'s `SparkApi`, a thin gRPC client
over `pyspark.sql.connect.session.SparkSession`, behind `api/api_client_base.py`'s
typed-error layer (`SparkApiError` with a `kind`: `unreachable` / `job_failed` /
`session_evicted` / `disabled` — never a silent empty result). `auth.py` mints
Keycloak client-credentials identity tokens and can attach them as real gRPC bearer
credentials, but the deployed Spark Connect endpoint is plaintext gRPC today, so
attach is disabled by default (see `docs/architecture.md`).

`mcp/mcp_spark.py` registers five tool groups (`session`, `transforms`, `catalog`,
`lineage`, `ops`) via one `register_tool_surface(...)` call in `mcp_server.py`,
matching the fleet's one-registration-call convention. `kg_ingest.py` exposes
`ingest_entities`/`ingest_run` plus record-mapping helpers around the required
`native_ingest` (Wire-First) authority.

## Environment Variables

| Variable | Required | Notes |
|----------|----------|-------|
| `SPARK_CONNECT_URL` | recommended | Spark Connect gRPC endpoint. Defaults to `sc://spark-connect.apps.svc:15002`. |
| `SPARK_SUBMIT_TIMEOUT_S` | optional | Submission timeout budget. Defaults to `300`. |
| `SPARK_LAKEHOUSE_CATALOG` / `SPARK_LAKEHOUSE_SCOPE` | optional | Defaults to `lakehouse` / `lakekeeper` — never leave scope at the shared-client default `catalog`. |
| `SPARK_ENABLE_TRANSFORMS` | ✅ for writes | `spark_submit_transform`/`spark_rerun_transform` refuse unless `true`. |
| `SPARK_CONNECT_ATTACH_IDENTITY_TOKEN` / `SPARK_CONNECT_USE_SSL` | optional | Both must be `true` together — see `docs/architecture.md`. |
| `SPARK_SERVICE_CLIENT_ID` / `SPARK_SERVICE_CLIENT_SECRET` | for identity attach | Keycloak client-credentials. |
| `SPARK_OAUTH_SCOPE` / `SPARK_OAUTH_TOKEN_URL` / `SPARK_KEYCLOAK_URL` / `SPARK_KEYCLOAK_REALM` | optional | Token-mint configuration. |

See [.env.example](.env.example) for the full template.

## MCP Tools

| Group | Tools |
|-------|-------|
| session | `spark_sql`, `spark_describe`, `spark_cancel` |
| transforms | `spark_submit_transform`, `spark_list_transform_runs`, `spark_rerun_transform` |
| catalog | `spark_list_lakekeeper_tables` |
| lineage | `spark_lineage_config`, `spark_lineage_events` |
| ops | `spark_application_status` |
| ingest | `spark_ingest_run` |

## Documentation
- [Architecture](docs/architecture.md)
- [Configuration](docs/configuration.md)
- [AGENTS.md](AGENTS.md) — canonical agent-facing context, live-proof evidence, known gaps
