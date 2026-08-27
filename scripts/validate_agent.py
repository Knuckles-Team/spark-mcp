#!/usr/bin/env python3
"""Validate spark-mcp end-to-end through the actual MCP tool-call path.

Unlike a script that calls the API client's Python methods directly, this builds
the real FastMCP server instance (``get_mcp_instance()``) and drives it through
``fastmcp.Client`` over the in-memory transport — the same call path an MCP client
(Claude, the multiplexer) actually uses (tool discovery + ``call_tool``), not a
shortcut around it. Requires a reachable Spark Connect endpoint
(``SPARK_CONNECT_URL``, default ``sc://spark-connect.apps.svc:15002``) — this is a
LIVE validation run, not a mock. If the cluster DNS name is not resolvable from
this shell, point ``SPARK_CONNECT_URL`` at a ``kubectl port-forward``ed
``sc://127.0.0.1:15002`` instead.
"""

import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


async def main() -> int:
    try:
        import setuptools  # noqa: F401  (primes distutils shim for py3.12+)
        from fastmcp import Client

        from spark_mcp.mcp_server import get_mcp_instance
    except ImportError as e:
        print(f"Import failed: {type(e).__name__}: {e}")
        print("Please install dependencies via `pip install .[mcp]`")
        return 1

    print("Building spark-mcp FastMCP server instance...")
    mcp, _args, _middlewares = get_mcp_instance()

    async with Client(mcp) as client:
        tools = await client.list_tools()
        names = sorted(t.name for t in tools)
        print(f"Discovered {len(names)} tools.")
        spark_tools = [n for n in names if n.startswith("spark_")]
        print(f"spark_* tools ({len(spark_tools)}): {spark_tools}")
        if not spark_tools:
            print("FAIL: no spark_* tools discovered")
            return 1

        print("\nCalling spark_sql(query='SELECT 1 AS one')...")
        result = await client.call_tool("spark_sql", {"query": "SELECT 1 AS one"})
        payload = result.data if hasattr(result, "data") else result
        print(json.dumps(payload, indent=2, default=str))

        if not isinstance(payload, dict) or payload.get("row_count") != 1:
            print("FAIL: spark_sql did not return exactly one row for SELECT 1")
            return 1

        print("\nCalling spark_application_status()...")
        status_result = await client.call_tool("spark_application_status", {})
        status_payload = (
            status_result.data if hasattr(status_result, "data") else status_result
        )
        print(json.dumps(status_payload, indent=2, default=str))

    print("\nOK: spark-mcp validated end-to-end through the MCP tool-call path.")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
