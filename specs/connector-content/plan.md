# CONN-SPARK-MCP-CONNECTOR-CONTENT — Design and implementation plan

Governing spec: [spec.md](spec.md).

## Architecture

`pyproject.toml` declares `spark-mcp` as the connector identity. `create_mcp_server` resolves the packaged content of `spark_mcp` from it.
