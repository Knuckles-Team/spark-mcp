"""Import-time smoke test — every module in the package must import cleanly."""


def test_startup():
    import spark_mcp.api_client  # noqa: F401
    import spark_mcp.auth  # noqa: F401
    import spark_mcp.kg_ingest  # noqa: F401
    import spark_mcp.mcp.mcp_spark  # noqa: F401
    import spark_mcp.mcp_server  # noqa: F401
    import spark_mcp.models  # noqa: F401
