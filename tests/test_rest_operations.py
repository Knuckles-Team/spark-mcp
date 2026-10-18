"""Spark monitoring REST operations: paths, params, server choice, typed errors."""

from __future__ import annotations

import httpx
import pytest

from spark_mcp.api.api_client_base import SparkApiError
from spark_mcp.api_client import Api


def _api(handler) -> Api:
    return Api(
        remote_url="sc://unused:15002",
        ui_url="http://driver:4040",
        history_url="http://history:18080",
        rest_transport=httpx.MockTransport(handler),
    )


def test_list_jobs_hits_the_driver_with_status_filter() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=[{"jobId": 1}, {"jobId": 2}])

    result = _api(handler).list_jobs("app-1", status="running")
    assert result == {
        "server": "driver",
        "items": [{"jobId": 1}, {"jobId": 2}],
        "count": 2,
    }
    assert (
        str(seen[0].url)
        == "http://driver:4040/api/v1/applications/app-1/jobs?status=running"
    )


def test_history_server_is_selected_by_server() -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return httpx.Response(200, json={"spark": "4.1.3"})

    assert _api(handler).get_version(server="history")["item"] == {"spark": "4.1.3"}
    assert seen == ["http://history:18080/api/v1/version"]


def test_stage_task_list_maps_camel_case_params() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=[])

    _api(handler).list_stage_tasks("a", 3, 0, length=5, sort_by="-runtime")
    assert seen[0].url.path == "/api/v1/applications/a/stages/3/0/taskList"
    assert dict(seen[0].url.params) == {"length": "5", "sortBy": "-runtime"}


def test_http_error_is_typed_not_empty() -> None:
    api = _api(lambda request: httpx.Response(404, text="no such app"))
    with pytest.raises(SparkApiError) as caught:
        api.get_application("missing")
    assert caught.value.status_code == 404
    assert caught.value.kind == "job_failed"


def test_unreachable_is_typed() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    with pytest.raises(SparkApiError) as caught:
        _api(handler).list_applications()
    assert caught.value.kind == "unreachable"


def test_kill_stage_posts_to_the_driver_ui() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(302)

    assert _api(handler).kill_stage(7) == {"stage_id": 7, "kill_requested": True}
    assert seen[0].method == "POST"
    assert str(seen[0].url) == "http://driver:4040/stages/stage/kill/?id=7"


def test_verbs_drive_annotations() -> None:
    assert Api.list_applications.__http_method__ == "GET"
    assert Api.query_table.__http_method__ == "GET"
    assert Api.kill_job.__http_method__ == "DELETE"
    assert Api.sql.__http_method__ == "POST"
