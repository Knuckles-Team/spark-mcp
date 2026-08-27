"""Fail-closed behavior of the Spark Connect client's transform-submission layer.

An unreachable Spark Connect endpoint, a query error, or a disabled write path must
each raise a typed ``SparkApiError`` with a distinguishing ``kind`` — never degrade
to an empty dict/list (the same class of bug this program has hit before:
ServiceNow PDI returning 200+HTML on every path).
"""

from __future__ import annotations

import pytest

from spark_mcp.api.api_client_base import SparkApiError
from spark_mcp.api.api_client_spark import SparkApi


class _FakeRow:
    def __init__(self, data):
        self._data = data

    def asDict(self, recursive=False):  # noqa: N802 - matches pyspark's Row API
        return dict(self._data)


class _FakeDataFrame:
    def __init__(self, rows, columns):
        self._rows = rows
        self.columns = columns

    def limit(self, n):
        return _FakeDataFrame(self._rows[:n], self.columns)

    def collect(self):
        return [_FakeRow(r) for r in self._rows]


class _FakeSession:
    def __init__(self, sql_responses=None, sql_errors=None):
        self._sql_responses = sql_responses or {}
        self._sql_errors = sql_errors or {}
        self.version = "3.5.9"

    def sql(self, query):
        if query in self._sql_errors:
            raise self._sql_errors[query]
        rows, columns = self._sql_responses.get(query, ([], []))
        return _FakeDataFrame(rows, columns)

    def interruptAll(self):  # noqa: N802
        return ["op-1"]

    def interruptTag(self, tag):  # noqa: N802
        return [f"op-{tag}"]


def _client_with_session(session, **kwargs):
    client = SparkApi(remote_url="sc://spark-connect.example:15002", **kwargs)
    client._session = session
    return client


def test_sql_returns_rows_and_columns():
    session = _FakeSession(sql_responses={"SELECT 1": ([{"one": 1}], ["one"])})
    client = _client_with_session(session)

    result = client.sql("SELECT 1")

    assert result["rows"] == [{"one": 1}]
    assert result["row_count"] == 1
    assert result["columns"] == ["one"]


def test_sql_error_raises_typed_job_failed():
    session = _FakeSession(sql_errors={"SELECT bad": RuntimeError("boom")})
    client = _client_with_session(session)

    with pytest.raises(SparkApiError) as excinfo:
        client.sql("SELECT bad")
    assert excinfo.value.kind == "job_failed"


def test_get_session_import_failure_raises_typed_unreachable(monkeypatch):
    """If pyspark itself cannot be imported, that is an 'unreachable' condition,

    never a silent None session — same typed-error discipline as a real gRPC
    connection failure.
    """
    client = SparkApi(remote_url="sc://nonexistent.invalid:15002")

    import builtins

    real_import = builtins.__import__

    def _fake_import(name, *args, **kwargs):
        if name == "pyspark.sql.connect.session":
            raise ImportError("no pyspark in this environment")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)

    with pytest.raises(SparkApiError) as excinfo:
        client.get_session()
    assert excinfo.value.kind == "unreachable"


def test_cancel_all_and_by_tag():
    session = _FakeSession()
    client = _client_with_session(session)

    assert client.cancel(None) == {"interrupted_operation_ids": ["op-1"], "tag": None}
    assert client.cancel("mytag") == {"interrupted_operation_ids": ["op-mytag"], "tag": "mytag"}


def test_submit_transform_refuses_when_disabled():
    client = _client_with_session(_FakeSession(), enable_transforms=False)

    manifest = {
        "transform": "t1",
        "kind": "sql",
        "body": "SELECT 1",
        "inputs": [],
        "output": {"table": "lakehouse.analytics.out", "mode": "append"},
    }
    with pytest.raises(SparkApiError) as excinfo:
        client.submit_transform(manifest)
    assert excinfo.value.kind == "disabled"


def test_submit_transform_sql_records_run_when_enabled():
    session = _FakeSession(sql_responses={"SELECT 1": ([{"one": 1}], ["one"])})
    client = _client_with_session(session, enable_transforms=True)

    manifest = {
        "transform": "t1",
        "kind": "sql",
        "body": "SELECT 1",
        "inputs": [],
        "output": {"table": "lakehouse.analytics.out", "mode": "append"},
    }
    record = client.submit_transform(manifest)

    assert record["status"] == "succeeded"
    assert record["row_count"] == 1
    result = client.list_transform_runs()
    assert result["count"] == 1
    assert result["runs"][0]["run_id"] == record["run_id"]


def test_rerun_transform_reuses_original_pinned_inputs():
    session = _FakeSession(sql_responses={"SELECT 1": ([{"one": 1}], ["one"])})
    client = _client_with_session(session, enable_transforms=True)

    manifest = {
        "transform": "t1",
        "kind": "sql",
        "body": "SELECT 1",
        "inputs": [{"table": "lakehouse.analytics.src", "as_of_version": "42"}],
        "output": {"table": "lakehouse.analytics.out", "mode": "append"},
    }
    first = client.submit_transform(manifest)
    second = client.rerun_transform(first["run_id"])

    assert second["rerun_of"] == first["run_id"]
    assert second["inputs"] == first["inputs"]


def test_rerun_transform_unknown_run_id_raises():
    client = _client_with_session(_FakeSession(), enable_transforms=True)
    with pytest.raises(SparkApiError):
        client.rerun_transform("does-not-exist")


def test_lineage_events_not_implemented():
    client = _client_with_session(_FakeSession())
    with pytest.raises(SparkApiError) as excinfo:
        client.lineage_events()
    assert excinfo.value.kind == "disabled"
