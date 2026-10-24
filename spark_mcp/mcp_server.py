"""Main FastMCP server and tool registration for spark-mcp."""

from spark_mcp.kg_ingest import register_ingest_tools  # noqa: F401

# SDK discovery registers the register_<tag>_tools functions bound in this module.
__all__ = ["register_ingest_tools"]
