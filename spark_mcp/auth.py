"""Identity credentials loader for the Spark Connect client.

**Identity propagation status (measured live, 2026-08-26):** the deployed
``spark-connect`` Service (``spark-connect.apps.svc:15002``) speaks plaintext gRPC —
no TLS, no ``spark.connect.grpc.interceptor.classes`` auth interceptor configured
server-side (confirmed by reading the live ``spark-scripts`` ConfigMap's
``spark-connect-server.sh``). Every in-cluster caller reaches it anonymously today,
the same "no access control yet" gap this program has already named for Lakekeeper's
``authz=allowall`` (CA-54's territory, not this lane's). This module mints a real
Keycloak client-credentials token and CAN attach it as a genuine gRPC bearer
credential (``pyspark``'s ``ChannelBuilder`` ``token=`` param -> real
``grpc.access_token_call_credentials``) — but attaching a token forces a secure
(TLS) channel, which the plaintext endpoint does not support, so this is disabled by
default (``SPARK_CONNECT_ATTACH_IDENTITY_TOKEN=false``) until CA-53 fronts the
Connect port with TLS. Flip both ``SPARK_CONNECT_ATTACH_IDENTITY_TOKEN=true`` and
``SPARK_CONNECT_USE_SSL=true`` once that lands; until then this module still mints
the token (proving the credential path works end-to-end) but ``api_client_spark``
refuses to attach it over a plaintext channel rather than silently downgrading.
"""

from __future__ import annotations

import threading
import time
from typing import Any

import requests
from agent_utilities.base_utilities import get_logger
from agent_utilities.core.config import setting

from spark_mcp.api.api_client_base import SparkApiError
from spark_mcp.api_client import Api

logger = get_logger(__name__)

_EXPIRY_SKEW_S = 15.0
_DEFAULT_TOKEN_TTL_S = 60.0

DEFAULT_SPARK_CONNECT_URL = "sc://spark-connect.apps.svc:15002"


class _TokenCache:
    """Thread-safe, single-credential client-credentials token cache (mirrors

    lakekeeper_mcp.auth._TokenCache — same shape, different service/scope).
    """

    def __init__(
        self,
        *,
        token_url: str,
        client_id: str,
        client_secret: str,
        scope: str,
        verify: Any = True,
    ) -> None:
        self._token_url = token_url
        self._client_id = client_id
        self._client_secret = client_secret
        self._scope = scope
        self._verify = verify
        self._lock = threading.Lock()
        self._token: str | None = None
        self._expires_at: float = 0.0

    def get(self) -> str:
        with self._lock:
            now = time.monotonic()
            if self._token and now < self._expires_at - _EXPIRY_SKEW_S:
                return self._token
            self._mint(now)
            assert self._token is not None
            return self._token

    def _mint(self, now: float) -> None:
        try:
            response = requests.post(
                self._token_url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self._client_id,
                    "client_secret": self._client_secret,
                    "scope": self._scope,
                },
                timeout=15.0,
                verify=self._verify,
            )
        except requests.RequestException as exc:
            raise SparkApiError(
                f"Spark identity OAuth2 token mint failed (network): {exc}",
                kind="unreachable",
            ) from exc

        if response.status_code >= 400:
            raise SparkApiError(
                f"Spark identity OAuth2 token mint failed: HTTP {response.status_code}",
                status_code=response.status_code,
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise SparkApiError(
                "Spark identity OAuth2 token endpoint returned a non-JSON body"
            ) from exc

        token = payload.get("access_token")
        if not token:
            raise SparkApiError(
                "Spark identity OAuth2 token response carried no access_token"
            )
        ttl = float(
            payload.get("expires_in", _DEFAULT_TOKEN_TTL_S) or _DEFAULT_TOKEN_TTL_S
        )
        self._token = token
        self._expires_at = now + ttl


_cache_lock = threading.Lock()
_cache: _TokenCache | None = None


def _token_cache() -> _TokenCache:
    global _cache
    with _cache_lock:
        if _cache is not None:
            return _cache
        token_url = setting("SPARK_OAUTH_TOKEN_URL", "") or (
            setting("SPARK_KEYCLOAK_URL", "https://keycloak.arpa").rstrip("/")
            + f"/realms/{setting('SPARK_KEYCLOAK_REALM', 'homelab')}"
            + "/protocol/openid-connect/token"
        )
        client_id = setting("SPARK_SERVICE_CLIENT_ID", "spark-service")
        client_secret = setting("SPARK_SERVICE_CLIENT_SECRET", "")
        if not client_secret:
            raise SparkApiError(
                "SPARK_SERVICE_CLIENT_SECRET is not configured — cannot mint a "
                "Spark identity OAuth2 token"
            )
        _cache = _TokenCache(
            token_url=token_url,
            client_id=client_id,
            client_secret=client_secret,
            scope=setting("SPARK_OAUTH_SCOPE", "lakekeeper"),
        )
        return _cache


def get_identity_token() -> str:
    """Return a cached, valid identity token, minting/refreshing as needed."""
    return _token_cache().get()


def get_client() -> Api:
    """Build a Spark Connect API client from the environment.

    Honors ``SPARK_CONNECT_URL`` (default the live in-cluster endpoint),
    ``SPARK_SUBMIT_TIMEOUT_S`` (default 300s, this lane's documented budget),
    ``SPARK_ENABLE_TRANSFORMS`` (write gate for spark_submit_transform/
    spark_rerun_transform — same real client-side refusal mechanism as
    egeria-mcp's ``EGERIA_ENABLE_WRITE``), and the identity-attach toggles
    documented in this module's docstring.
    """
    attach_identity = str(
        setting("SPARK_CONNECT_ATTACH_IDENTITY_TOKEN", "false")
    ).lower() in (
        "1",
        "true",
        "yes",
    )
    use_ssl = str(setting("SPARK_CONNECT_USE_SSL", "false")).lower() in (
        "1",
        "true",
        "yes",
    )
    enable_transforms = str(setting("SPARK_ENABLE_TRANSFORMS", "false")).lower() in (
        "1",
        "true",
        "yes",
    )
    return Api(
        remote_url=setting("SPARK_CONNECT_URL", DEFAULT_SPARK_CONNECT_URL),
        submit_timeout_s=float(setting("SPARK_SUBMIT_TIMEOUT_S", "300")),
        enable_transforms=enable_transforms,
        attach_identity_token=attach_identity,
        identity_token_provider=get_identity_token if attach_identity else None,
        use_ssl=use_ssl,
        lakehouse_catalog=setting("SPARK_LAKEHOUSE_CATALOG", "lakehouse"),
        lakehouse_scope=setting("SPARK_LAKEHOUSE_SCOPE", "lakekeeper"),
    )
