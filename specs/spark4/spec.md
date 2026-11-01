# CONN-SPARK-MCP-SPARK4 — Spark 4 queries and monitoring connector

Owner: spark-mcp.

## Purpose and user stories

An agent reads data and job state from a Spark 4 cluster. The connector runs read-only queries through Spark Connect. It reads application, job, stage and executor state from the monitoring REST API.

## Why a new spec

The repository had no local spec. The 0.4.0 release added the Spark Connect client and the monitoring REST client without rows.

## Scope and interfaces

In scope: Spark Connect queries, catalog reads, monitoring REST reads and the governed transform submit. Out of scope: cluster provisioning and the connector contract of the SDK.

## Delivery mapping

Component: spark-mcp connector

Code: ../../spark_mcp/__init__.py

## User Scenarios & Testing

### User Story 1 - Spark Connect read queries (Priority: P1)

The fleet operator and the engine rely on this behaviour of `spark-mcp`: `query_table`, `count_rows`, `summarize_table` and `explain_query` in `spark_mcp/api/api_client_spark.py` must run through one Spark Connect session. It is proven independently of the other stories.

**Acceptance Scenarios**:

1. **Given** the `spark-mcp` connector built from the default branch, **When** `tests/test_api_client.py::test_sql_returns_rows_and_columns` runs, **Then** `tests/test_api_client.py`: rows and columns return, and the two typed errors raise.

### User Story 2 - Catalog reads (Priority: P1)

The fleet operator and the engine rely on this behaviour of `spark-mcp`: `list_catalogs`, `list_databases`, `list_tables`, `list_columns` and `list_functions` must read the Spark 4 catalog. It is proven independently of the other stories.

**Acceptance Scenarios**:

1. **Given** the `spark-mcp` connector built from the default branch, **When** `tests/test_catalog_reads.py::test_each_catalog_read_carries_the_read_only_hint` runs, **Then** `tests/test_api_client.py` asserts each catalog read and its hint.

### User Story 3 - Monitoring REST reads (Priority: P1)

The fleet operator and the engine rely on this behaviour of `spark-mcp`: `spark_mcp/api/api_client_rest.py` must serve one tool per monitoring REST read: version, applications, jobs, stages, executors and SQL executions. It is proven independently of the other stories.

**Acceptance Scenarios**:

1. **Given** the `spark-mcp` connector built from the default branch, **When** `tests/test_rest_operations.py::test_history_server_is_selected_by_server` runs, **Then** `tests/test_rest_operations.py` asserts one tool per read and the server choice.

### User Story 4 - Governed transform submit (Priority: P2)

The fleet operator and the engine rely on this behaviour of `spark-mcp`: A SQL transform submit must refuse while transforms are disabled. It is proven independently of the other stories.

**Acceptance Scenarios**:

1. **Given** the `spark-mcp` connector built from the default branch, **When** `tests/test_api_client.py::test_submit_transform_refuses_when_disabled` runs, **Then** `tests/test_api_client.py`: the disabled refusal, the recorded run and both cancel forms.

### User Story 5 - Source-build dependency (Priority: P2)

The fleet operator and the engine rely on this behaviour of `spark-mcp`: `pyproject.toml` must name `pyspark-client` in `[tool.pipelines] allow-source-build`, per PIPE-RELEASE-R010. It is proven independently of the other stories.

**Acceptance Scenarios**:

1. **Given** the `spark-mcp` connector built from the default branch, **When** `tests/test_source_build_allowlist.py::test_only_pyspark_client_builds_from_source` runs, **Then** the wheel-readiness receipt lists exactly `pyspark-client`.

### Edge Cases

- The vendor service is unreachable or rejects the credential: the connector returns the SDK typed error and never a partial result.
- An argument outside the documented inputs is refused with the SDK typed refusal and causes no effect on the vendor service.

## Requirements

- **FR-001**: Spark Connect read queries. Rows: `CONN-SPARK-MCP-SPARK4-R001`.
- **FR-002**: Catalog reads. Rows: `CONN-SPARK-MCP-SPARK4-R002`.
- **FR-003**: Monitoring REST reads. Rows: `CONN-SPARK-MCP-SPARK4-R003`.
- **FR-004**: Governed transform submit. Rows: `CONN-SPARK-MCP-SPARK4-R004`.
- **FR-005**: Source-build dependency. Rows: `CONN-SPARK-MCP-SPARK4-R005`.

## Success Criteria

- **SC-001**: `tests/test_api_client.py`: rows and columns return, and the two typed errors raise. Rows: `CONN-SPARK-MCP-SPARK4-R001`.
- **SC-002**: `tests/test_api_client.py` asserts each catalog read and its hint. Rows: `CONN-SPARK-MCP-SPARK4-R002`.
- **SC-003**: `tests/test_rest_operations.py` asserts one tool per read and the server choice. Rows: `CONN-SPARK-MCP-SPARK4-R003`.
- **SC-004**: `tests/test_api_client.py`: the disabled refusal, the recorded run and both cancel forms. Rows: `CONN-SPARK-MCP-SPARK4-R004`.
- **SC-005**: The wheel-readiness receipt lists exactly `pyspark-client`. Rows: `CONN-SPARK-MCP-SPARK4-R005`.

## Assumptions

- The `spark-mcp` package keeps the entry point and module layout named in the rows.
- The shared SDK supplies the transport, authentication, retry and certification helpers the rows reuse.

## Traceability

| Label | Rows | Design section | Test ID | Evidence |
|---|---|---|---|---|
| FR-001 | `CONN-SPARK-MCP-SPARK4-R001` | plan.md Production caller chain | T-001 | tests/test_api_client.py::test_sql_returns_rows_and_columns |
| FR-002 | `CONN-SPARK-MCP-SPARK4-R002` | plan.md Production caller chain | T-002 | tests/test_catalog_reads.py::test_each_catalog_read_carries_the_read_only_hint |
| FR-003 | `CONN-SPARK-MCP-SPARK4-R003` | plan.md Production caller chain | T-003 | tests/test_rest_operations.py::test_history_server_is_selected_by_server |
| FR-004 | `CONN-SPARK-MCP-SPARK4-R004` | plan.md Production caller chain | T-004 | tests/test_api_client.py::test_submit_transform_refuses_when_disabled |
| FR-005 | `CONN-SPARK-MCP-SPARK4-R005` | plan.md Production caller chain | T-005 | tests/test_source_build_allowlist.py::test_only_pyspark_client_builds_from_source |
