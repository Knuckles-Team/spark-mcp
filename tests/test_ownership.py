"""DEC-CA-07 write-gate fail-closed behavior: an unapproved transform is DENIED.

Named `test_ownership.py` to mirror lakekeeper-mcp's fail-closed-negative-test file
(GOC-78 there; DEC-CA-07's approval gate here) — this is this lane's own explicit
negative test: "submit a Transform manifest with an unapproved action -> denied,
not executed."

Two independent, always-enforced layers are proven here:
  1. The client-side `SPARK_ENABLE_TRANSFORMS` gate (`SparkApi._require_transforms_enabled`,
     the same real mechanism as egeria-mcp's `EGERIA_ENABLE_WRITE` — an explicit
     refusal, not a UI-only guard).
  2. `dispatch_intent`'s `_approval_satisfied_by_session_load` fleet-intent gate
     (DEC-CA-07), proven against `agent_utilities.mcp.tools.intent_tools` directly
     — a caller who never `load_tools`-ed `spark_submit_transform` is refused even
     with a syntactically valid plan.
"""

from __future__ import annotations

import pytest

from spark_mcp.api.api_client_base import SparkApiError
from spark_mcp.api.api_client_spark import SparkApi


class _FakeSession:
    version = "3.5.9"

    def sql(
        self, query
    ):  # pragma: no cover - never reached, transform is refused first
        raise AssertionError("submit_transform must refuse before touching Spark")


def _manifest():
    return {
        "transform": "unapproved_transform",
        "kind": "sql",
        "body": "SELECT 1",
        "inputs": [],
        "output": {"table": "lakehouse.analytics.out", "mode": "append"},
    }


def test_submit_transform_denied_when_writes_disabled():
    """Known-bad demonstration: SPARK_ENABLE_TRANSFORMS unset -> denied, never executed."""
    client = SparkApi(
        remote_url="sc://spark-connect.example:15002", enable_transforms=False
    )
    client._session = _FakeSession()

    with pytest.raises(SparkApiError) as excinfo:
        client.submit_transform(_manifest())

    assert excinfo.value.kind == "disabled"
    # No TransformRun was ever recorded — the refusal happens before submission.
    assert client.list_transform_runs()["count"] == 0


def test_rerun_transform_denied_when_writes_disabled():
    client = SparkApi(
        remote_url="sc://spark-connect.example:15002", enable_transforms=False
    )
    client._session = _FakeSession()

    with pytest.raises(SparkApiError) as excinfo:
        client.rerun_transform("any-run-id")
    assert excinfo.value.kind == "disabled"


def test_submit_transform_allowed_once_enabled():
    """Positive control: the same manifest succeeds once the gate is opened."""

    class _AllowSession(_FakeSession):
        def sql(self, query):
            class _Df:
                columns = ["one"]

                def limit(self, n):
                    return self

                def collect(self):
                    class _Row:
                        def asDict(self, recursive=False):
                            return {"one": 1}

                    return [_Row()]

            return _Df()

    client = SparkApi(
        remote_url="sc://spark-connect.example:15002", enable_transforms=True
    )
    client._session = _AllowSession()

    record = client.submit_transform(_manifest())
    assert record["status"] == "succeeded"


def test_dispatch_intent_approval_gate_refuses_without_session_load():
    """DEC-CA-07's fleet-intent layer: a session that never loaded the exact tool

    is refused even for a syntactically valid plan (BUG-040's fix, the real
    mechanism `dispatch_intent` enforces above this package's own client-side gate).
    """
    from agent_utilities.mcp.tools.intent_tools import (
        _approval_satisfied_by_session_load,
    )

    class _FakeMux:
        def tool_dispatchable(self, tool_name):
            return False  # this session never called load_tools for it

    class _FakeMcp:
        _fleet_mux = _FakeMux()

    assert (
        _approval_satisfied_by_session_load(_FakeMcp(), "spark_submit_transform")
        is False
    )


def test_dispatch_intent_approval_gate_allows_after_session_load():
    from agent_utilities.mcp.tools.intent_tools import (
        _approval_satisfied_by_session_load,
    )

    class _FakeMux:
        def tool_dispatchable(self, tool_name):
            return tool_name == "spark_submit_transform"

    class _FakeMcp:
        _fleet_mux = _FakeMux()

    assert (
        _approval_satisfied_by_session_load(_FakeMcp(), "spark_submit_transform")
        is True
    )
