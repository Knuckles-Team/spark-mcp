"""Auth env-var honoring for the Spark Connect client + identity-token cache."""

import pytest

import spark_mcp.auth as auth_module
from spark_mcp.api.api_client_base import SparkApiError


@pytest.fixture(autouse=True)
def _reset_token_cache(monkeypatch):
    """Every test gets a fresh module-level token cache."""
    monkeypatch.setattr(auth_module, "_cache", None)
    yield
    monkeypatch.setattr(auth_module, "_cache", None)


def test_get_client_honors_spark_connect_url(monkeypatch):
    monkeypatch.setenv("SPARK_CONNECT_URL", "sc://spark-connect.example:15002")

    client = auth_module.get_client()

    assert client.remote_url == "sc://spark-connect.example:15002"


def test_get_client_falls_back_to_default_url(monkeypatch):
    monkeypatch.delenv("SPARK_CONNECT_URL", raising=False)

    client = auth_module.get_client()

    assert client.remote_url == auth_module.DEFAULT_SPARK_CONNECT_URL


def test_get_client_defaults_transforms_disabled(monkeypatch):
    monkeypatch.delenv("SPARK_ENABLE_TRANSFORMS", raising=False)

    client = auth_module.get_client()

    assert client.enable_transforms is False


def test_get_client_enables_transforms_when_set(monkeypatch):
    monkeypatch.setenv("SPARK_ENABLE_TRANSFORMS", "true")

    client = auth_module.get_client()

    assert client.enable_transforms is True


def test_get_client_defaults_identity_attach_disabled(monkeypatch):
    monkeypatch.delenv("SPARK_CONNECT_ATTACH_IDENTITY_TOKEN", raising=False)
    monkeypatch.delenv("SPARK_CONNECT_USE_SSL", raising=False)

    client = auth_module.get_client()

    assert client.attach_identity_token is False
    assert client.use_ssl is False


def test_get_client_honors_lakehouse_scope_override(monkeypatch):
    monkeypatch.setenv("SPARK_LAKEHOUSE_SCOPE", "custom-scope")

    client = auth_module.get_client()

    assert client.lakehouse_scope == "custom-scope"


def test_token_cache_requires_client_secret(monkeypatch):
    monkeypatch.delenv("SPARK_SERVICE_CLIENT_SECRET", raising=False)

    with pytest.raises(SparkApiError, match="SPARK_SERVICE_CLIENT_SECRET"):
        auth_module.get_identity_token()


def test_token_mint_sends_client_credentials_grant(monkeypatch):
    monkeypatch.setenv("SPARK_SERVICE_CLIENT_SECRET", "secret")
    captured = {}

    class _FakeResponse:
        status_code = 200

        def json(self):
            return {"access_token": "tok-123", "expires_in": 300}

    def _fake_post(url, data=None, timeout=None, verify=None):
        captured["url"] = url
        captured["data"] = data
        return _FakeResponse()

    monkeypatch.setattr(auth_module.requests, "post", _fake_post)

    token = auth_module.get_identity_token()

    assert token == "tok-123"
    assert captured["data"]["grant_type"] == "client_credentials"
    assert captured["data"]["scope"] == "lakekeeper"


def test_token_cache_reuses_unexpired_token(monkeypatch):
    monkeypatch.setenv("SPARK_SERVICE_CLIENT_SECRET", "secret")
    calls = {"n": 0}

    class _FakeResponse:
        status_code = 200

        def json(self):
            calls["n"] += 1
            return {"access_token": f"tok-{calls['n']}", "expires_in": 300}

    monkeypatch.setattr(auth_module.requests, "post", lambda *a, **k: _FakeResponse())

    first = auth_module.get_identity_token()
    second = auth_module.get_identity_token()

    assert first == second == "tok-1"
    assert calls["n"] == 1
