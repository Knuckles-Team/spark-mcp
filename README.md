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

<!-- ENV-VARS-TABLE:START -->

#### Package environment variables

| Variable | Example | Description |
|----------|---------|-------------|
| `SPARK_CONNECT_URL` | `sc://spark-connect.apps.svc:15002` |  |
| `SPARK_SUBMIT_TIMEOUT_S` | `300` |  |
| `SPARK_LAKEHOUSE_CATALOG` | `lakehouse` | document/verify the invariant client-side — same invalid_scope landmine as |
| `SPARK_LAKEHOUSE_SCOPE` | `lakekeeper` |  |
| `SPARK_ENABLE_TRANSFORMS` | `False` | egeria-mcp's EGERIA_ENABLE_WRITE) — spark_submit_transform/spark_rerun_transform |
| `SPARK_CONNECT_ATTACH_IDENTITY_TOKEN` | secret-injected | Service is plaintext gRPC; attaching a bearer token forces a TLS channel the |
| `SPARK_CONNECT_USE_SSL` | `False` |  |
| `SPARK_SERVICE_CLIENT_ID` | `spark-service` |  |
| `SPARK_SERVICE_CLIENT_SECRET` | secret-injected |  |
| `SPARK_OAUTH_SCOPE` | `lakekeeper` |  |
| `SPARK_OAUTH_TOKEN_URL` | secret-injected |  |
| `SPARK_KEYCLOAK_URL` | `https://keycloak.arpa` |  |
| `SPARK_KEYCLOAK_REALM` | `homelab` |  |
| `SPARKTOOL` | `True` |  |
| `INGESTTOOL` | `True` |  |

#### Inherited agent-utilities variables (apply to every connector)

| Variable | Example | Description |
|----------|---------|-------------|
| `TRANSPORT` | `stdio` | MCP transport: `stdio` \| `streamable-http` \| `sse` |
| `HOST` | `127.0.0.1` | Loopback bind host (set an authenticated ingress explicitly) |
| `PORT` | `8000` | Bind port (HTTP transports) |
| `MCP_TOOL_MODE` | `intent` | Tool surface: `intent` \| `condensed` \| `verbose` \| `both` |
| `MCP_ENABLED_TOOLS` | — | Comma-separated tool allow-list |
| `MCP_DISABLED_TOOLS` | — | Comma-separated tool deny-list |
| `MCP_ENABLED_TAGS` | — | Comma-separated tag allow-list |
| `MCP_DISABLED_TAGS` | — | Comma-separated tag deny-list |
| `EUNOMIA_TYPE` | `none` | Authorization mode: `none` \| `embedded` \| `remote` |
| `EUNOMIA_POLICY_FILE` | `mcp_policies.json` | Embedded Eunomia policy file |
| `EUNOMIA_REMOTE_URL` | — | Remote Eunomia authorization server URL |
| `ENABLE_OTEL` | `False` | Enable OpenTelemetry export |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | OTLP collector endpoint |
| `MCP_CLIENT_AUTH` | — | Outbound MCP child auth: `oidc-client-credentials` \| `basic` \| `none` |
| `OIDC_CLIENT_ID` | — | OIDC client id (service-account auth) |
| `OIDC_CLIENT_SECRET_REF` | `secret://identity/oidc-client-secret` | Runtime secret reference for the OIDC service account |
| `MCP_BASIC_AUTH_USERNAME` | — | HTTP Basic username (`MCP_CLIENT_AUTH=basic`) |
| `MCP_BASIC_AUTH_PASSWORD_REF` | `secret://identity/mcp-basic-password` | Runtime secret reference for HTTP Basic auth (`MCP_CLIENT_AUTH=basic`) |
| `DEBUG` | `False` | Verbose logging |
| `PYTHONUNBUFFERED` | `1` | Unbuffered stdout (recommended in containers) |
| `MCP_URL` | `http://localhost:8000/mcp` | URL of the MCP server the agent connects to |
| `PROVIDER` | `openai` | LLM provider for the agent |
| `MODEL_ID` | `gpt-4o` | Model id for the agent |
| `ENABLE_WEB_UI` | `True` | Serve the AG-UI web interface |

_15 package + 24 inherited variable(s). Auto-generated from `.env.example` + the shared agent-utilities set — do not edit._
<!-- ENV-VARS-TABLE:END -->


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

## Available MCP Tools

<!-- MCP-TOOLS-TABLE:START -->

#### Condensed action-routed tools (`MCP_TOOL_MODE=condensed`)

| MCP Tool | Toggle Env Var | Description |
|----------|----------------|-------------|
| `spark_ingest_run` | `INGESTTOOL` | Push one TransformRun's SparkApplication->SparkJob->Transform->TransformRun |

#### Verbose 1:1 API-mapped tools (`MCP_TOOL_MODE=verbose` or `both`)

<details>
<summary>12 per-operation tools — one per public API method (click to expand)</summary>

| MCP Tool | Toggle Env Var | Description |
|----------|----------------|-------------|
| `spark_application_status` | `SPARKTOOL` | Best-effort application/session status. |
| `spark_cancel` | `SPARKTOOL` | Interrupt running operations on this session (all, or one tagged operation). |
| `spark_close` | `SPARK_APITOOL` | Invoke the close operation. |
| `spark_describe` | `SPARKTOOL` | Schema + column info for one fully-qualified table. |
| `spark_get_session` | `SPARK_APITOOL` | Return the cached Spark Connect session, building it on first use. |
| `spark_lineage_config` | `SPARKTOOL` | Report OpenLineage listener config — already set server-side at |
| `spark_lineage_events` | `SPARKTOOL` | Named per this lane's tool-group contract; NOT a Kafka consumer. |
| `spark_list_lakekeeper_tables` | `SPARKTOOL` | Invoke the list_lakekeeper_tables operation. |
| `spark_list_transform_runs` | `SPARKTOOL` | Invoke the list_transform_runs operation. |
| `spark_rerun_transform` | `SPARKTOOL` | Deterministically re-execute a prior TransformRun's manifest. |
| `spark_sql` | `SPARKTOOL` | Execute one SQL statement and return rows as plain dicts. |
| `spark_submit_transform` | `SPARKTOOL` | Execute one Transform manifest, recording a TransformRun (success or typed failure). |

</details>

_1 action-routed tool(s) · 12 verbose 1:1 tool(s). Each is enabled unless its `<DOMAIN>TOOL` toggle is set false; `MCP_TOOL_MODE` selects the surface (**`intent` default** — the six verb-tools, granular set loaded on demand · `condensed` action-routed · `verbose` 1:1 · `both`). Auto-generated — do not edit._
<!-- MCP-TOOLS-TABLE:END -->
