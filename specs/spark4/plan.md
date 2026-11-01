# CONN-SPARK-MCP-SPARK4 — Implementation plan

Governing spec: [spec.md](spec.md). Rows: [requirements.md](requirements.md).

## Summary

This plan states how `spark-mcp` delivers the rows of `CONN-SPARK-MCP-SPARK4`. The live caller is the `spark-mcp` console script. The owner is the `spark_mcp` package. The rows reuse the `scion-sdk` helpers and add no second authority. The plan changes no runtime code.

## Technical Context

- **Language/Version**: Python >=3.12, <3.15
- **Primary Dependencies**: scion-sdk[mcp]>=0.6.4,<0.7.0; fastmcp; further pins in `pyproject.toml` (scion-sdk[mcp]>=0.6.4,<0.7.0, httpx>=0.28.1, numpy>=2.5.4, pandas>=3.0.6, pyarrow>=26.0.0, pyspark-client>=4.1.3,<4.2.0)
- **Storage**: N/A, the connector keeps no store of its own
- **Testing**: pytest; each row names the proving test as `path::name`
- **Target Platform**: Linux container and stdio or streamable HTTP MCP clients
- **Project Type**: connector
- **Performance Goals**: N/A, the vendor service sets the latency
- **Constraints**: vendor calls go through the SDK governed client; no connector-local transport
- **Scale/Scope**: 5 active row(s) in this spec

## Constitution Check

- Organization constitution 1.5.0, spec-first change: pass. Every behaviour is a row of this spec with a bound or planned test.
- Repository article R-I, connector scope: pass. The rows stay inside this connector's one external system and reuse the SDK for shared capabilities.

## Project Structure

- `spark_mcp/api/api_client_spark.py` (existing)
- `spark_mcp/mcp_server.py` (existing)
- `spark_mcp/models.py` (existing)
- `tests/test_api_client.py` (existing)
- `spark_mcp/api/api_client_rest.py` (existing)
- `tests/test_rest_operations.py` (existing)
- `pyproject.toml` (existing)
- `tests/test_catalog_reads.py` (new, planned)
- `tests/test_source_build_allowlist.py` (new, planned)

## Phase 0: Research

No unknowns. Every row names its code and test; the caller chain of each row was read from the code.

## Phase 1: Design

### Summary

This plan states how `spark-mcp` delivers the rows of `CONN-SPARK-MCP-SPARK4`. The live caller is the `spark-mcp` console script. The owner is the `spark_mcp` package. The rows reuse the `scion-sdk` helpers and add no second authority. The plan changes no runtime code.

### Technical Context

- **Language/Version**: Python >=3.12, <3.15
- **Primary Dependencies**: scion-sdk[mcp]>=0.6.4,<0.7.0; fastmcp; further pins in `pyproject.toml` (scion-sdk[mcp]>=0.6.4,<0.7.0, httpx>=0.28.1, numpy>=2.5.4, pandas>=3.0.6, pyarrow>=26.0.0, pyspark-client>=4.1.3,<4.2.0)
- **Storage**: N/A, the connector keeps no store of its own
- **Testing**: pytest; each row names the proving test as `path::name`
- **Target Platform**: Linux container and stdio or streamable HTTP MCP clients
- **Project Type**: connector
- **Performance Goals**: N/A, the vendor service sets the latency
- **Constraints**: vendor calls go through the SDK governed client; no connector-local transport
- **Scale/Scope**: 5 active row(s) in this spec

### Constitution Check

- Organization constitution 1.5.0, spec-first change: pass. Every behaviour is a row of this spec with a bound or planned test.
- Repository article R-I, connector scope: pass. The rows stay inside this connector's one external system and reuse the SDK for shared capabilities.

### Project Structure

- `spark_mcp/api/api_client_spark.py` (existing)
- `spark_mcp/mcp_server.py` (existing)
- `spark_mcp/models.py` (existing)
- `tests/test_api_client.py` (existing)
- `spark_mcp/api/api_client_rest.py` (existing)
- `tests/test_rest_operations.py` (existing)
- `pyproject.toml` (existing)
- `tests/test_catalog_reads.py` (new, planned)
- `tests/test_source_build_allowlist.py` (new, planned)

### Phase 0: Research

No unknowns. Every row names its code and test; the caller chain of each row was read from the code.

### Phase 1: Design

### Summary

This plan states how `spark-mcp` delivers the rows of `CONN-SPARK-MCP-SPARK4`. The live caller is the `spark-mcp` console script. The owner is the `spark_mcp` package. The rows reuse the `scion-sdk` helpers and add no second authority. The plan changes no runtime code.

### Technical Context

- **Language/Version**: Python >=3.12, <3.15
- **Primary Dependencies**: scion-sdk[mcp]>=0.6.4,<0.7.0; fastmcp; further pins in `pyproject.toml` (scion-sdk[mcp]>=0.6.4,<0.7.0, httpx>=0.28.1, numpy>=2.5.4, pandas>=3.0.6, pyarrow>=26.0.0, pyspark-client>=4.1.3,<4.2.0)
- **Storage**: N/A, the connector keeps no store of its own
- **Testing**: pytest; each row names the proving test as `path::name`
- **Target Platform**: Linux container and stdio or streamable HTTP MCP clients
- **Project Type**: connector
- **Performance Goals**: N/A, the vendor service sets the latency
- **Constraints**: vendor calls go through the SDK governed client; no connector-local transport
- **Scale/Scope**: 5 active row(s) in this spec

### Constitution Check

- Organization constitution 1.5.0, spec-first change: pass. Every behaviour is a row of this spec with a bound or planned test.
- Repository article R-I, connector scope: pass. The rows stay inside this connector's one external system and reuse the SDK for shared capabilities.

### Project Structure

- `spark_mcp/api/api_client_spark.py` (existing)
- `tests/test_api_client.py` (existing)
- `spark_mcp/api/api_client_rest.py` (existing)
- `tests/test_rest_operations.py` (existing)
- `pyproject.toml` (existing)

### Phase 0: Research

No unknowns. Every row names its code and test; the caller chain of each row was read from the code.

### Phase 1: Design

Component: Spark connector
Code: ../../spark_mcp/api/api_client_spark.py ../../spark_mcp/api/api_client_rest.py

`spark_mcp/api/api_client_spark.py` holds the Spark Connect client. `spark_mcp/api/api_client_rest.py` holds the monitoring REST client. The MCP server registers one tool per public method through the SDK (SCION-CONNECTOR-CONTROL-R020). The `pyspark-client` dependency builds from source under PIPE-RELEASE-R010.

### Phase 2: Task approach

`tasks.md` holds one task per row, producer first. Rows that depend on none run in parallel. A task is checked when its `Spec:` trailer is on the default branch.

### Complexity Tracking

None. The constitution check passed.

### Build contract

- `pre-commit run --all-files` runs ruff check, ruff format, the shared `connector-standard`, `spec`, `secrets` and `dependency-readiness` gates at the pinned shared revision.
- `uv run pytest tests` runs the proving tests; the shared complexity, duplication and KISS gates apply their org thresholds and this repository sets no local override.
- A change adds no scanner suppression and no duplicate implementation of an SDK helper.

### Production caller chain


### Cross-repo seams

None. The rows depend only on the published `scion-sdk` package, which the connector imports.

### Phase 2: Task approach

`tasks.md` holds one task per row, producer first. Rows that depend on none run in parallel. A task is checked when its `Spec:` trailer is on the default branch.

### Complexity Tracking

None. The constitution check passed.

### Build contract

- `pre-commit run --all-files` runs ruff check, ruff format, the shared `connector-standard`, `spec`, `secrets` and `dependency-readiness` gates at the pinned shared revision.
- `uv run pytest tests` runs the proving tests; the shared complexity, duplication and KISS gates apply their org thresholds and this repository sets no local override.
- A change adds no scanner suppression and no duplicate implementation of an SDK helper.

### Production caller chain

- `CONN-SPARK-MCP-SPARK4-R001`: an MCP tool of `spark_mcp/mcp_server.py` calls `query_table` (`spark_mcp/api/api_client_spark.py:212`), `count_rows`, `summarize_table` or `explain_query`, which share one Spark Connect session.
- `CONN-SPARK-MCP-SPARK4-R002`: an MCP tool of `spark_mcp/mcp_server.py` calls `list_catalogs` (`spark_mcp/api/api_client_spark.py:310`), `list_databases`, `list_tables`, `list_columns` or `list_functions`.
- `CONN-SPARK-MCP-SPARK4-R003`: an MCP tool of `spark_mcp/mcp_server.py` calls one method of `spark_mcp/api/api_client_rest.py`, which sends the monitoring REST request to the driver or the history server.
- `CONN-SPARK-MCP-SPARK4-R004`: an MCP tool calls `submit_transform` (`spark_mcp/api/api_client_spark.py:475`), which calls `_require_transforms_enabled` (`spark_mcp/api/api_client_spark.py:436`); `cancel` (`spark_mcp/api/api_client_spark.py:413`) stops runs.
- `CONN-SPARK-MCP-SPARK4-R005`: the wheel-readiness gate (`dependency-readiness`) reads `[tool.pipelines] allow-source-build` in `pyproject.toml` before every release.

### Cross-repo seams

None. The rows depend only on the published `scion-sdk` package, which the connector imports.

## Phase 2: Task approach

`tasks.md` holds one task per row, producer first. Rows that depend on none run in parallel. A task is checked when its `Spec:` trailer is on the default branch.

## Complexity Tracking

None. The constitution check passed.

## Build contract

- `pre-commit run --all-files` runs ruff check, ruff format, the shared `connector-standard`, `spec`, `secrets` and `dependency-readiness` gates at the pinned shared revision.
- `uv run pytest tests` runs the proving tests; the shared complexity, duplication and KISS gates apply their org thresholds and this repository sets no local override.
- A change adds no scanner suppression and no duplicate implementation of an SDK helper.

## Production caller chain

- `CONN-SPARK-MCP-SPARK4-R001`: an MCP tool of `spark_mcp/mcp_server.py` calls `query_table` (`spark_mcp/api/api_client_spark.py:212`), `count_rows`, `summarize_table` or `explain_query`, which share one Spark Connect session.
- `CONN-SPARK-MCP-SPARK4-R002`: an MCP tool of `spark_mcp/mcp_server.py` calls `list_catalogs` (`spark_mcp/api/api_client_spark.py:310`), `list_databases`, `list_tables`, `list_columns` or `list_functions`.
- `CONN-SPARK-MCP-SPARK4-R003`: an MCP tool of `spark_mcp/mcp_server.py` calls one method of `spark_mcp/api/api_client_rest.py`, which sends the monitoring REST request to the driver or the history server.
- `CONN-SPARK-MCP-SPARK4-R004`: an MCP tool calls `submit_transform` (`spark_mcp/api/api_client_spark.py:475`), which calls `_require_transforms_enabled` (`spark_mcp/api/api_client_spark.py:436`); `cancel` (`spark_mcp/api/api_client_spark.py:413`) stops runs.
- `CONN-SPARK-MCP-SPARK4-R005`: the wheel-readiness gate (`dependency-readiness`) reads `[tool.pipelines] allow-source-build` in `pyproject.toml` before every release.

## Cross-repo seams

None. The rows depend only on the published `scion-sdk` package, which the connector imports.
