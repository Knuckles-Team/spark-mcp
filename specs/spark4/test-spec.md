# CONN-SPARK-MCP-SPARK4 — Test specification

Governing spec: [spec.md](spec.md).

| Test ID | Requirement | Expected observation |
|---|---|---|
| T-R001 | CONN-SPARK-MCP-SPARK4-R001 | `tests/test_api_client.py`: rows and columns return, and the two typed errors raise. |
| T-R002 | CONN-SPARK-MCP-SPARK4-R002 | `tests/test_api_client.py` asserts each catalog read and its hint. |
| T-R003 | CONN-SPARK-MCP-SPARK4-R003 | `tests/test_rest_operations.py` asserts one tool per read and the server choice. |
| T-R004 | CONN-SPARK-MCP-SPARK4-R004 | `tests/test_api_client.py`: the disabled refusal, the recorded run and both cancel forms. |
| T-R005 | CONN-SPARK-MCP-SPARK4-R005 | The wheel-readiness receipt lists exactly `pyspark-client`. |
