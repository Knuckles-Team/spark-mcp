# Spark Mcp
## CLI or API | MCP

![PyPI - Version](https://img.shields.io/pypi/v/spark-mcp)
![MCP Server](https://badge.mcpx.dev?type=server 'MCP Server')
![PyPI - Downloads](https://img.shields.io/pypi/dd/spark-mcp)
![GitHub Repo stars](https://img.shields.io/github/stars/Knuckles-Team/spark-mcp)
![GitHub forks](https://img.shields.io/github/forks/Knuckles-Team/spark-mcp)
![GitHub contributors](https://img.shields.io/github/contributors/Knuckles-Team/spark-mcp)
![PyPI - License](https://img.shields.io/pypi/l/spark-mcp)
![GitHub](https://img.shields.io/github/license/Knuckles-Team/spark-mcp)
![GitHub last commit (by committer)](https://img.shields.io/github/last-commit/Knuckles-Team/spark-mcp)
![GitHub pull requests](https://img.shields.io/github/issues-pr/Knuckles-Team/spark-mcp)
![GitHub closed pull requests](https://img.shields.io/github/issues-pr-closed/Knuckles-Team/spark-mcp)
![GitHub issues](https://img.shields.io/github/issues/Knuckles-Team/spark-mcp)
![GitHub top language](https://img.shields.io/github/languages/top/Knuckles-Team/spark-mcp)
![GitHub language count](https://img.shields.io/github/languages/count/Knuckles-Team/spark-mcp)
![GitHub repo size](https://img.shields.io/github/repo-size/Knuckles-Team/spark-mcp)
![GitHub repo file count (file type)](https://img.shields.io/github/directory-file-count/Knuckles-Team/spark-mcp)
![PyPI - Wheel](https://img.shields.io/pypi/wheel/spark-mcp)
![PyPI - Implementation](https://img.shields.io/pypi/implementation/spark-mcp)

*Version: 0.4.1*

> **Documentation** — Installation, deployment, and usage across the API, CLI, MCP,
> and A2A agent interfaces are maintained in the
> [official documentation](https://knuckles-team.github.io/spark-mcp/).

---

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
package's `AGENTS.md` for known gaps (identity-token attach, `ActionSpec`
typed-Actions deferral).

Talks directly to Spark over `pyspark-client` (the thin Spark Connect client) for SQL, DataFrame and catalog operations, and over the Spark monitoring REST API (`/api/v1` on the driver UI and the History Server) for applications, jobs, stages, executors, storage, environment and SQL executions, never
`bin/spark-shell --remote` (does not work on the vanilla Spark image) and never a
`kubectl exec spark-sql` fallback.

## Installation

Pick the extra that matches what the operator want to run:

| Extra | Installs | Use when |
|-------|----------|----------|
| `spark-mcp[mcp]` | Connector-focused MCP server (`agent-connector-sdk` — FastMCP/FastAPI + `epistemic-graph[full]`) | The operator only run the **MCP server** (smallest install / image) |
| `spark-mcp[all]` | Everything (`mcp` + `agent` + `logfire`) | Development / both surfaces |

```bash
uv pip install "spark-mcp[mcp]"
```

### Container images (`:mcp`)

One multi-stage `docker/Dockerfile` builds the MCP-only image (`--target mcp`):

```bash
docker build --target mcp   -t spark-mcp:mcp    .
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

`mcp_server.py` serves one typed `spark_<operation>` tool per `SparkApi` method
(`spark_sql`, `spark_describe`, `spark_cancel`, `spark_submit_transform`,
`spark_rerun_transform`, `spark_list_transform_runs`, `spark_list_lakekeeper_tables`,
`spark_lineage_config`, `spark_lineage_events`, `spark_application_status`) through one
`register_tool_surface(...)` call; GET-like reads are read-only and `spark_cancel` is
destructive. `kg_ingest.py` exposes
`ingest_entities`/`ingest_run` plus record-mapping helpers around the required
`native_ingest` (Wire-First) authority.

## Python version

Python 3.14 (floor 3.12) against a Spark 4.1 server. `pyspark-client` 4.1 is the
thin Spark Connect client; it runs on numpy 2, pandas 3 and pyarrow wheels.
`pyspark-client` ships only a pure-Python sdist, so it is the one package built
from source (`[tool.pipelines] allow-source-build = ["pyspark-client"]`).

## Environment Variables

<!-- ENV-VARS-TABLE:START -->

#### Package environment variables

| Variable | Example | Description |
|----------|---------|-------------|
| `SPARK_CONNECT_URL` | `sc://spark-connect.apps.svc:15002` |  |
| `SPARK_UI_URL` | `http://spark-runner.apps.svc:4040` | driver monitoring REST |
| `SPARK_HISTORY_URL` | `http://spark-history.apps.svc:18080` | History Server REST |
| `SPARK_REST_TIMEOUT_S` | `30` |  |
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
| `SPARK_KEYCLOAK_URL` | `http://localhost:8080` |  |
| `SPARK_KEYCLOAK_REALM` | `homelab` |  |
| `SPARKTOOL` | `True` |  |
| `INGESTTOOL` | `True` |  |

#### Inherited agent-connector-sdk variables (apply to every connector)

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

_15 package + 24 inherited variable(s). Auto-generated from `.env.example` + the shared agent-connector-sdk set — do not edit._
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

---

## Repository Owners

<img width="100%" height="180em" src="https://github-readme-stats.vercel.app/api?username=example&show_icons=true&hide_border=true&&count_private=true&include_all_commits=true" />

![GitHub followers](https://img.shields.io/github/followers/example)
![GitHub User's stars](https://img.shields.io/github/stars/example)

---

## Contribute

Contributions are welcome! Please ensure code quality by running local checks before submitting pull requests:
- Format code using `ruff format .`
- Lint code using `ruff check .`
- Validate type-safety with `mypy .`
- Run test suites using `pytest`



<!-- GOVERNED-CAPABILITY:START -->
## Governed capability contract

This package ships a compact canonical skill surface with specialist procedures
kept as referenced workflows. The current MCP tools, skill metadata,
`connector_manifest.yml`, ontology, mappings, shapes, fixtures, migrations,
tool-schema fingerprints, and certification metadata form one versioned
capability contract. Validate them together; do not rely on stale tool names or
historical per-task skill wrappers.

Runtime endpoints, credentials, certificate trust, tenant identity, retention,
and observability policy are deployment inputs and are never packaged values.
See [Configuration, trust, and privacy](docs/configuration.md) before enabling a
network transport, connector ingestion, Arbora delegation, or trace export.
<!-- GOVERNED-CAPABILITY:END -->
