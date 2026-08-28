# Configuration

| Variable | Default | Notes |
|----------|---------|-------|
| `SPARK_CONNECT_URL` | `sc://spark-connect.apps.svc:15002` | Spark Connect gRPC endpoint |
| `SPARK_SUBMIT_TIMEOUT_S` | `300` | Submission timeout budget (no measured baseline for typical job duration yet) |
| `SPARK_LAKEHOUSE_CATALOG` | `lakehouse` | Iceberg catalog name (already registered server-side) |
| `SPARK_LAKEHOUSE_SCOPE` | `lakekeeper` | Must stay explicit — never the shared-client default `catalog` |
| `SPARK_ENABLE_TRANSFORMS` | `False` | Write gate for `spark_submit_transform`/`spark_rerun_transform` |
| `SPARK_CONNECT_ATTACH_IDENTITY_TOKEN` | `False` | Requires `SPARK_CONNECT_USE_SSL=True` too — see architecture.md |
| `SPARK_CONNECT_USE_SSL` | `False` | The deployed endpoint is plaintext gRPC today |
| `SPARK_SERVICE_CLIENT_ID` / `SPARK_SERVICE_CLIENT_SECRET` | — | Keycloak client-credentials, minted even when attach is disabled |
| `SPARK_OAUTH_SCOPE` | `lakekeeper` | OAuth2 scope for the identity token mint |
| `SPARK_OAUTH_TOKEN_URL` | derived from `SPARK_KEYCLOAK_URL`/`SPARK_KEYCLOAK_REALM` | Full override |
| `SPARK_KEYCLOAK_URL` | `http://localhost:8080` | |
| `SPARK_KEYCLOAK_REALM` | `homelab` | |
| `SPARKTOOL` | `True` | Tool-surface toggle |
| `INGESTTOOL` | `True` | Tool-surface toggle |
| `MCP_TOOL_MODE` | `intent` | Standard fleet MCP tool-exposure mode |

See `.env.example` for the full template.
