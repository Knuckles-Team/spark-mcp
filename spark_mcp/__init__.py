"""spark-mcp: Spark Connect (gRPC) API + MCP Server + A2A Server."""

from scion_sdk.connector import Connector

from spark_mcp.api_client import Api
from spark_mcp.auth import get_client

connector = Connector(
    api=Api,
    get_client=get_client,
    tools="spark_mcp.mcp_server",
    instructions="Spark Connect (gRPC) MCP Server — session SQL execution, versioned Transform submission/rerun against the shared Iceberg lakehouse catalog (scope=lakekeeper, explicit), catalog reads, OpenLineage config/status reporting, and a Wire-First KG ingest tool for the SparkApplication->SparkJob->Transform->TransformRun->DatasetVersion chain. Never shells out to `kubectl exec spark-sql` as a fallback.",
)
