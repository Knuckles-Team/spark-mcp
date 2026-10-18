"""Spark monitoring REST operations for spark-mcp.

Spark serves one monitoring REST API (``/api/v1``) from two places:

- the live driver UI of the long-running Spark Connect server (``SPARK_UI_URL``,
  default ``http://spark-runner.apps.svc:4040``) — running applications;
- the Spark History Server (``SPARK_HISTORY_URL``, default
  ``http://spark-history.apps.svc:18080``) — completed applications, replayed
  from event logs.

Both expose the same endpoints, so every operation takes ``server`` ("driver" or
"history") to pick the endpoint; it never changes what the operation does. Each
method is one REST operation and becomes one MCP tool.
"""

from __future__ import annotations

from typing import Any, Literal
from urllib.parse import quote

import httpx

from spark_mcp.api.api_client_base import SparkApiError

Server = Literal["driver", "history"]
JobStatus = Literal["running", "succeeded", "failed", "unknown"]
StageStatus = Literal["active", "complete", "pending", "failed", "skipped"]
AppStatus = Literal["completed", "running"]

_DEFAULT_TIMEOUT_S = 30.0


def _verb(method: str):
    """Declare the HTTP verb the SDK derives tool annotations from."""

    def mark(function):
        function.__http_method__ = method
        return function

    return mark


def _seg(value: str | int) -> str:
    return quote(str(value), safe="")


def _params(**values: Any) -> dict[str, Any]:
    return {
        key: (str(value).lower() if isinstance(value, bool) else value)
        for key, value in values.items()
        if value is not None
    }


class SparkRestBase:
    """HTTP plumbing behind :class:`SparkRestOperations` (never served as tools)."""

    ui_url: str
    history_url: str
    rest_timeout_s: float
    _http: httpx.Client | None

    def _rest_init(
        self,
        *,
        ui_url: str,
        history_url: str,
        rest_timeout_s: float = _DEFAULT_TIMEOUT_S,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.ui_url = ui_url.rstrip("/")
        self.history_url = history_url.rstrip("/")
        self.rest_timeout_s = rest_timeout_s
        self._transport = transport
        self._http = None

    def _client(self) -> httpx.Client:
        if self._http is None:
            self._http = httpx.Client(
                timeout=self.rest_timeout_s, transport=self._transport
            )
        return self._http

    def _base(self, server: Server) -> str:
        if server == "driver":
            return self.ui_url
        if server == "history":
            return self.history_url
        raise SparkApiError(f"unknown Spark REST server {server!r}", kind="job_failed")

    def _request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> httpx.Response:
        try:
            response = self._client().request(method, url, params=params)
        except httpx.HTTPError as exc:
            raise SparkApiError(
                f"Spark REST endpoint unreachable ({url}): {exc}", kind="unreachable"
            ) from exc
        if response.status_code >= 400:
            raise SparkApiError(
                f"Spark REST {method} {url} failed: HTTP {response.status_code}",
                kind="job_failed",
                status_code=response.status_code,
                body=response.text[:2000],
            )
        return response

    def _get(
        self, server: Server, path: str, params: dict[str, Any] | None = None
    ) -> Any:
        response = self._request(
            "GET", f"{self._base(server)}/api/v1{path}", params=params
        )
        try:
            return response.json()
        except ValueError as exc:
            raise SparkApiError(
                f"Spark REST GET {path} returned a non-JSON body", kind="job_failed"
            ) from exc

    def _items(
        self, server: Server, path: str, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        data = self._get(server, path, params)
        items = data if isinstance(data, list) else [data]
        return {"server": server, "items": items, "count": len(items)}

    def _one(
        self, server: Server, path: str, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        data = self._get(server, path, params)
        return {"server": server, "item": data}


class SparkRestOperations(SparkRestBase):
    """One method per Spark monitoring REST operation."""

    @_verb("GET")
    def get_version(self, server: Server = "driver") -> dict[str, Any]:
        """Spark version reported by the driver UI or the History Server."""
        return self._one(server, "/version")

    @_verb("GET")
    def list_applications(
        self,
        server: Server = "driver",
        status: AppStatus | None = None,
        min_date: str | None = None,
        max_date: str | None = None,
        min_end_date: str | None = None,
        max_end_date: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """List applications (dates as yyyy-MM-dd or yyyy-MM-dd'T'HH:mm:ss.SSSz)."""
        return self._items(
            server,
            "/applications",
            _params(
                status=status,
                minDate=min_date,
                maxDate=max_date,
                minEndDate=min_end_date,
                maxEndDate=max_end_date,
                limit=limit,
            ),
        )

    @_verb("GET")
    def get_application(self, app_id: str, server: Server = "driver") -> dict[str, Any]:
        """One application with its attempts."""
        return self._one(server, f"/applications/{_seg(app_id)}")

    @_verb("GET")
    def list_jobs(
        self,
        app_id: str,
        server: Server = "driver",
        status: JobStatus | None = None,
    ) -> dict[str, Any]:
        """List the jobs of one application, optionally filtered by status."""
        return self._items(
            server, f"/applications/{_seg(app_id)}/jobs", _params(status=status)
        )

    @_verb("GET")
    def get_job(
        self, app_id: str, job_id: int, server: Server = "driver"
    ) -> dict[str, Any]:
        """One job of an application."""
        return self._one(server, f"/applications/{_seg(app_id)}/jobs/{_seg(job_id)}")

    @_verb("GET")
    def list_stages(
        self,
        app_id: str,
        server: Server = "driver",
        status: StageStatus | None = None,
        details: bool | None = None,
        with_summaries: bool | None = None,
    ) -> dict[str, Any]:
        """List the stages of one application."""
        return self._items(
            server,
            f"/applications/{_seg(app_id)}/stages",
            _params(status=status, details=details, withSummaries=with_summaries),
        )

    @_verb("GET")
    def list_stage_attempts(
        self,
        app_id: str,
        stage_id: int,
        server: Server = "driver",
        details: bool | None = None,
    ) -> dict[str, Any]:
        """All attempts of one stage."""
        return self._items(
            server,
            f"/applications/{_seg(app_id)}/stages/{_seg(stage_id)}",
            _params(details=details),
        )

    @_verb("GET")
    def get_stage_attempt(
        self,
        app_id: str,
        stage_id: int,
        attempt_id: int,
        server: Server = "driver",
        details: bool | None = None,
    ) -> dict[str, Any]:
        """One attempt of one stage."""
        return self._one(
            server,
            f"/applications/{_seg(app_id)}/stages/{_seg(stage_id)}/{_seg(attempt_id)}",
            _params(details=details),
        )

    @_verb("GET")
    def get_stage_task_summary(
        self,
        app_id: str,
        stage_id: int,
        attempt_id: int,
        server: Server = "driver",
        quantiles: str | None = None,
    ) -> dict[str, Any]:
        """Task metric quantiles for one stage attempt (quantiles like "0.05,0.5,0.95")."""
        return self._one(
            server,
            f"/applications/{_seg(app_id)}/stages/{_seg(stage_id)}/{_seg(attempt_id)}/taskSummary",
            _params(quantiles=quantiles),
        )

    @_verb("GET")
    def list_stage_tasks(
        self,
        app_id: str,
        stage_id: int,
        attempt_id: int,
        server: Server = "driver",
        offset: int | None = None,
        length: int | None = None,
        sort_by: str | None = None,
        status: str | None = None,
    ) -> dict[str, Any]:
        """Tasks of one stage attempt, paged (sort_by like "runtime" or "-runtime")."""
        return self._items(
            server,
            f"/applications/{_seg(app_id)}/stages/{_seg(stage_id)}/{_seg(attempt_id)}/taskList",
            _params(offset=offset, length=length, sortBy=sort_by, status=status),
        )

    @_verb("GET")
    def list_executors(self, app_id: str, server: Server = "driver") -> dict[str, Any]:
        """Active executors (the driver included) of one application."""
        return self._items(server, f"/applications/{_seg(app_id)}/executors")

    @_verb("GET")
    def list_all_executors(
        self, app_id: str, server: Server = "driver"
    ) -> dict[str, Any]:
        """Active and dead executors of one application."""
        return self._items(server, f"/applications/{_seg(app_id)}/allexecutors")

    @_verb("GET")
    def list_storage_rdds(
        self, app_id: str, server: Server = "driver"
    ) -> dict[str, Any]:
        """Persisted RDDs of one application."""
        return self._items(server, f"/applications/{_seg(app_id)}/storage/rdd")

    @_verb("GET")
    def get_storage_rdd(
        self, app_id: str, rdd_id: int, server: Server = "driver"
    ) -> dict[str, Any]:
        """Storage status and distribution of one persisted RDD."""
        return self._one(
            server, f"/applications/{_seg(app_id)}/storage/rdd/{_seg(rdd_id)}"
        )

    @_verb("GET")
    def get_environment(self, app_id: str, server: Server = "driver") -> dict[str, Any]:
        """Runtime, Spark, Hadoop and system properties of one application (redacted by Spark)."""
        return self._one(server, f"/applications/{_seg(app_id)}/environment")

    @_verb("GET")
    def list_sql_executions(
        self,
        app_id: str,
        server: Server = "driver",
        details: bool | None = None,
        plan_description: bool | None = None,
        offset: int | None = None,
        length: int | None = None,
    ) -> dict[str, Any]:
        """SQL executions (queries) of one application."""
        return self._items(
            server,
            f"/applications/{_seg(app_id)}/sql",
            _params(
                details=details,
                planDescription=plan_description,
                offset=offset,
                length=length,
            ),
        )

    @_verb("GET")
    def get_sql_execution(
        self,
        app_id: str,
        execution_id: int,
        server: Server = "driver",
        details: bool | None = None,
        plan_description: bool | None = None,
    ) -> dict[str, Any]:
        """One SQL execution with its plan and metrics."""
        return self._one(
            server,
            f"/applications/{_seg(app_id)}/sql/{_seg(execution_id)}",
            _params(details=details, planDescription=plan_description),
        )

    @_verb("GET")
    def list_processes(self, app_id: str, server: Server = "driver") -> dict[str, Any]:
        """Miscellaneous processes (for example the Connect server) of one application."""
        return self._items(
            server, f"/applications/{_seg(app_id)}/allmiscellaneousprocess"
        )

    @_verb("DELETE")
    def kill_job(self, job_id: int) -> dict[str, Any]:
        """Kill one running job on the live driver (Spark UI kill endpoint)."""
        self._request(
            "POST", f"{self.ui_url}/jobs/job/kill/", params={"id": str(job_id)}
        )
        return {"job_id": job_id, "kill_requested": True}

    @_verb("DELETE")
    def kill_stage(self, stage_id: int) -> dict[str, Any]:
        """Kill one running stage on the live driver (Spark UI kill endpoint)."""
        self._request(
            "POST", f"{self.ui_url}/stages/stage/kill/", params={"id": str(stage_id)}
        )
        return {"stage_id": stage_id, "kill_requested": True}
