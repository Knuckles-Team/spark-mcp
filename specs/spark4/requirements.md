# CONN-SPARK-MCP-SPARK4 — Requirements

Governing spec: [spec.md](spec.md).

Each row is one requirement. The generator sets each row's state in `status.json`.

| ID | Requirement | Acceptance |
|---|---|---|
| `CONN-SPARK-MCP-SPARK4-R001` | **Spark Connect read queries.** `query_table`, `count_rows`, `summarize_table` and `explain_query` in `spark_mcp/api/api_client_spark.py` must run through one Spark Connect session. A failed query must raise a typed job failure. An unreachable server must raise a typed unreachable error. | `tests/test_api_client.py`: rows and columns return, and the two typed errors raise. |
| `CONN-SPARK-MCP-SPARK4-R002` | **Catalog reads.** `list_catalogs`, `list_databases`, `list_tables`, `list_columns` and `list_functions` must read the Spark 4 catalog. Each tool carries the read-only hint. | `tests/test_api_client.py` asserts each catalog read and its hint. |
| `CONN-SPARK-MCP-SPARK4-R003` | **Monitoring REST reads.** `spark_mcp/api/api_client_rest.py` must serve one tool per monitoring REST read: version, applications, jobs, stages, executors and SQL executions. A tool may target the driver or the history server. | `tests/test_rest_operations.py` asserts one tool per read and the server choice. |
| `CONN-SPARK-MCP-SPARK4-R004` | **Governed transform submit.** A SQL transform submit must refuse while transforms are disabled. An enabled submit must record its run. Cancel must stop all runs or the runs of one tag. | `tests/test_api_client.py`: the disabled refusal, the recorded run and both cancel forms. |
| `CONN-SPARK-MCP-SPARK4-R005` | **Source-build dependency.** `pyproject.toml` must name `pyspark-client` in `[tool.pipelines] allow-source-build`, per PIPE-RELEASE-R010. No other dependency may build from source. | The wheel-readiness receipt lists exactly `pyspark-client`. |
