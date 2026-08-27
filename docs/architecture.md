# Architecture

## Transport: Spark Connect (gRPC), not REST

Unlike most of the fleet's connectors (`lakekeeper_mcp`, `jena_mcp`), Spark Connect is
a gRPC protocol — `spark_mcp/api/api_client_spark.py`'s `SparkApi` wraps
`pyspark.sql.connect.session.SparkSession.builder.remote("sc://spark-connect.apps.svc:15002")`
rather than an HTTP `requests.Session`. `spark_mcp/api/api_client_base.py` carries
only the one thing every fleet client needs regardless of transport: a typed,
non-degrading `SparkApiError` with a `kind` (`unreachable` / `job_failed` /
`session_evicted` / `disabled`) so a caller can distinguish "Spark Connect is down"
from "the query itself failed" from "the write gate is closed" — never collapsing
all three into a silent empty result.

## Server-side catalog registration

The deployed `spark-runner` pod's Connect server registers the `lakehouse` Iceberg
REST catalog at `spark-submit` startup (`services/spark`'s `spark-scripts`
ConfigMap, key `spark-connect-server.sh`) — `spark.sql.catalog.lakehouse.type=rest`,
`.scope=lakekeeper` (explicit), pointed at the same Lakekeeper REST URI/warehouse/
credentials Trino's own catalog uses. Catalog registration is a static, driver-startup
Spark conf, not something a Connect *client* can change at runtime — this package's
own `.config("spark.sql.catalog.lakehouse.scope", "lakekeeper")` builder call is
defense-in-depth documentation of the invariant, not the enforcement point.

**Proven live** (2026-08-26), over a `kubectl port-forward` to
`spark-connect.apps.svc:15002`:

```
>>> spark.sql("SELECT * FROM lakehouse.analytics.trino_verify").show()
+---+-----+---+
| id| name|val|
+---+-----+---+
|  1|alpha|1.1|
|  2| beta|2.2|
|  3|gamma|3.3|
+---+-----+---+
```

The same table Trino's sibling lane already proved reading/writing — cross-engine
Iceberg interop confirmed end-to-end through this package's client.

## No Spark History Server

`services/spark/AGENTS.md` documents that no Spark History Server is deployed — job
logs/UI are lost once a job's process exits. `spark_list_transform_runs`/
`spark_application_status` therefore read this package's OWN in-process
`TransformRun` ledger (`SparkApi._runs`), never a reconstruction of Spark's own
(absent) history. This is process-local: a restart of the MCP server loses the
ledger. Persisting it is a follow-up (KG ingestion via `spark_ingest_run` is the
durable path — ingest a run before it can be lost to a restart).

## Transform execution

`spark_submit_transform` accepts a `TransformManifest` (`transform`, `kind: sql|
pyspark`, `body`, `inputs[{table, as_of_version}]`, `output{table, mode}`):

- `kind="sql"`: every declared input is textually rewritten to
  `table VERSION AS OF <as_of_version>` before `spark.sql(body)` runs — deterministic
  reruns, never an implicit "latest" read on a declared rerun.
- `kind="pyspark"`: `body` is evaluated as a single DataFrame-API expression against
  a controlled namespace (`spark`, plus each input bound to its table's trailing
  name component) — a deliberately CONSTRAINED subset of "submit an arbitrary
  PySpark job," not full `spark-submit` job packaging. Full job submission is out of
  this lane's scope (`services/spark/AGENTS.md`'s own "revisit with the operator
  instead" boundary).

Both paths are gated by `SPARK_ENABLE_TRANSFORMS` (client-side refusal, same real
mechanism as `egeria-mcp`'s `EGERIA_ENABLE_WRITE`) beneath DEC-CA-07's fleet-intent
approval layer (`dispatch_intent` -> `_approval_satisfied_by_session_load`).

## Identity propagation — a genuine, documented gap

`pyspark`'s `ChannelBuilder` supports a `token=` URI param that attaches a real gRPC
bearer credential (`grpc.access_token_call_credentials`). This package's `auth.py`
mints a real Keycloak client-credentials token and can attach it — but setting a
token forces a secure (TLS) channel, and the deployed `spark-connect` Service speaks
plaintext gRPC (confirmed by reading the live `spark-scripts` ConfigMap: no TLS conf,
no `spark.connect.grpc.interceptor.classes` auth interceptor). So identity-token
attach is disabled by default (`SPARK_CONNECT_ATTACH_IDENTITY_TOKEN=false`) and
`api_client_spark.py` REFUSES to attach a token over a plaintext channel rather than
silently downgrading it to a no-op. Every in-cluster caller reaches Spark Connect
anonymously today — the same class of gap already named for Lakekeeper's
`authz=allowall` (CA-54's territory).

## KG ingestion

`kg_ingest.py` follows `egeria_mcp`'s pattern: `ingest_entities`/mapper functions,
producing `:SparkApplication` -[`hasJob`]-> `:SparkJob` -[`runs`]-> `:TransformRun`
-[`executesTransform`]-> `:Transform`, plus `:TransformRun` -[`producedVersion`/
`consumedVersion`]-> `:DatasetVersion`. Node ids: `spark:<Class>:<externalId>`.
