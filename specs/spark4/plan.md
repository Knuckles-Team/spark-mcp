# CONN-SPARK-MCP-SPARK4 — Design and implementation plan

Governing spec: [spec.md](spec.md).

Component: Spark connector
Code: ../../spark_mcp/api/api_client_spark.py ../../spark_mcp/api/api_client_rest.py

`spark_mcp/api/api_client_spark.py` holds the Spark Connect client. `spark_mcp/api/api_client_rest.py` holds the monitoring REST client. The MCP server registers one tool per public method through the SDK (SDK-CONNECTOR-CONTROL-R020). The `pyspark-client` dependency builds from source under PIPE-RELEASE-R010.
