"""Vulture whitelist for spark-mcp.

Add entries here, with a comment explaining why vulture is wrong, the moment
a genuine false positive shows up.
"""

# Mock Row.asDict(recursive=...) params (tests/test_api_client.py,
# tests/test_ownership.py) must keep this exact keyword name: the real call
# site (spark_mcp/api/api_client_spark.py) invokes `row.asDict(recursive=True)`.
recursive = None
_ = recursive

# Mock requests.post(...) stub (tests/test_auth_env.py) must keep this exact
# keyword name: the real call site (spark_mcp/auth.py) invokes
# `requests.post(..., timeout=15.0, ...)`.
timeout = None
_ = timeout
