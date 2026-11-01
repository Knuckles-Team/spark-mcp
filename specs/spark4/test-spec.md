# CONN-SPARK-MCP-SPARK4 — Test specification

Governing spec: [spec.md](spec.md).

| Test ID | Requirement | Proving test | Expected observation |
|---|---|---|---|
| T-001 | `CONN-SPARK-MCP-SPARK4-R001` | `tests/test_api_client.py::test_sql_returns_rows_and_columns` | `tests/test_api_client.py`: rows and columns return, and the two typed errors raise. |
| T-002 | `CONN-SPARK-MCP-SPARK4-R002` | `tests/test_catalog_reads.py::test_each_catalog_read_carries_the_read_only_hint` (planned) | `tests/test_api_client.py` asserts each catalog read and its hint. |
| T-003 | `CONN-SPARK-MCP-SPARK4-R003` | `tests/test_rest_operations.py::test_history_server_is_selected_by_server` | `tests/test_rest_operations.py` asserts one tool per read and the server choice. |
| T-004 | `CONN-SPARK-MCP-SPARK4-R004` | `tests/test_api_client.py::test_submit_transform_refuses_when_disabled` | `tests/test_api_client.py`: the disabled refusal, the recorded run and both cancel forms. |
| T-005 | `CONN-SPARK-MCP-SPARK4-R005` | `tests/test_source_build_allowlist.py::test_only_pyspark_client_builds_from_source` (planned) | The wheel-readiness receipt lists exactly `pyspark-client`. |

## Negative and boundary cases

Each proving test above includes the refusal or error case that its row names. A vendor outage or an unauthorised call must return the SDK typed error.

## Quality and release proof

Run `uv run pytest tests` and `pre-commit run --all-files` from the repository root. A planned test is added by the code change that lands the row.
