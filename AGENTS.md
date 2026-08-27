# spark-mcp - AGENTS

> Claude Code loads this file via `CLAUDE.md` (`@AGENTS.md` import) — the two stay
> in sync. Edit **this** file, not `CLAUDE.md`.

## Project Structure
- `spark_mcp/`: Main server code (Spark Connect client, MCP tool groups, KG ingest, skills)
- `tests/`: Test suite
- `docs/`: Architecture documentation

## Tech Stack
- Python 3.12+
- agent-utilities >= 2.0.0, <3.0.0
- Model Context Protocol (MCP)
- `pyspark[connect]` — a thin gRPC CLIENT against a live Spark Connect server; this
  package never runs a Spark driver/worker itself.

## Commands
- `pytest`: Run tests
- `pre-commit run --all-files`: Lint code
- `python -m agent_utilities.mcp.check_env_var_drift --check`: env-var drift gate (must be 0)

## Domain notes (read before touching `auth.py` or `api_client_spark.py`)
- **Spark Connect IS live**, confirmed 2026-08-26 against
  `spark-connect.apps.svc:15002` (the `spark-runner` pod's Connect server) via
  `kubectl port-forward` + a real `pyspark.sql.connect` session:
  `SELECT * FROM lakehouse.analytics.trino_verify` returned the same 3 rows
  (`alpha`/`beta`/`gamma`) Trino's sibling lane already proved reading/writing —
  cross-engine Iceberg interop confirmed end-to-end. `designs/PROOFS.md`'s
  "no Spark Connect server deployed yet" framing (and the lane file's own
  "unverified") are now STALE — CA-53 landed it.
- `bin/spark-shell --remote` does **not** work on the vanilla Spark image (no
  Connect branch without a `-Pconnect` build) — always use `pyspark.sql.connect`
  (the Python client), never the shell. Confirmed by this lane, not merely asserted.
- The `lakehouse` Iceberg REST catalog is registered SERVER-SIDE at spark-submit
  startup (`services/spark` ConfigMap `spark-scripts`, key
  `spark-connect-server.sh`) with `scope=lakekeeper` explicit — the identical
  `invalid_scope` failure mode Trino/Lakekeeper hit if this is ever dropped
  (`services/spark/AGENTS.md`). This client cannot override that static conf at
  runtime; its own `.config(...)` call is defense-in-depth documentation, not the
  enforcement point.
- **Python 3.12 needs `setuptools` imported before `pyspark`** — `pyspark` 3.5.9's
  `pandas.utils.require_minimum_pandas_version` still calls
  `from distutils.version import LooseVersion`, and stdlib `distutils` was removed
  in 3.12. `import setuptools` primes its bundled distutils shim
  (`_distutils_hack`) before the first `pyspark` import; this package's
  `api_client_spark.py` does this explicitly rather than relying on import order
  at the call site. `setuptools>=68` must be present at runtime — it is NOT
  declared as an explicit dependency in `pyproject.toml` today (pulled in
  transitively via the build toolchain in every environment tested); if a future
  environment lacks it, add `setuptools>=68` to `dependencies`.
- **Identity propagation is a genuine, documented gap, not a design choice**:
  `pyspark`'s `ChannelBuilder` supports a `token=` param (real gRPC
  `access_token_call_credentials`), but attaching one forces a TLS channel, and
  the deployed `spark-connect` Service is plaintext gRPC (no TLS conf, no
  `spark.connect.grpc.interceptor.classes` auth interceptor — confirmed by reading
  the live ConfigMap). Every in-cluster caller reaches Spark Connect anonymously
  today. `auth.py` mints a real Keycloak token and CAN attach it
  (`SPARK_CONNECT_ATTACH_IDENTITY_TOKEN=true` + `SPARK_CONNECT_USE_SSL=true`), but
  refuses to attach it over a plaintext channel rather than silently downgrading —
  this needs CA-53 (or a follow-up) to front the Connect port with TLS before it
  can do anything today. Same class of gap as Lakekeeper's `authz=allowall`.
- **No Spark History Server exists** — `spark_list_transform_runs`/
  `spark_application_status` read this package's own in-process `TransformRun`
  ledger (`SparkApi._runs`), never Spark's own (absent) job history. The ledger is
  process-local; a restart loses it. `spark_ingest_run` is the durable path.
- `spark_submit_transform`'s `kind="pyspark"` path evaluates `body` as ONE
  DataFrame-API expression against a controlled namespace (`spark` + named input
  DataFrames) — a deliberately constrained subset of "submit an arbitrary PySpark
  job," not full `spark-submit` packaging (out of this lane's scope; see
  `services/spark/AGENTS.md`'s own "revisit with the operator instead" boundary).
- Every write path (`spark_submit_transform`/`spark_rerun_transform`) is gated
  client-side by `SPARK_ENABLE_TRANSFORMS` (refuses, not silently executes, when
  unset) — the same real mechanism as `egeria-mcp`'s `EGERIA_ENABLE_WRITE`
  (`api_client_spark.py::_require_transforms_enabled`), beneath DEC-CA-07's
  fleet-intent approval layer (`dispatch_intent` ->
  `_approval_satisfied_by_session_load`, `intent_tools.py`).

## ActionSpec / DEC-CA-07 status (as of this package's initial build, 2026-08-26)
`CA-32` (the `ActionSpec` schema extension adding `parameters`/`target_resource`/
`conflict_policy`/`requires_approval`/`approval_class`) has **not** merged onto
`agent-utilities` `main` yet — confirmed by reading
`agent_utilities/knowledge_graph/ontology/connector_manifest.py` (still the
three-field `{id, name, description}` shape). This package's `connector_manifest.yml`
therefore carries only the boilerplate two-field `actions:` entries every generated
manifest gets (`epistemic-answer`, `run_graph_flow`) — the rich typed-Action
declarations for `spark_submit_transform`/`spark_rerun_transform`/`spark_cancel` are
deferred until CA-32 lands, and recorded in `connector_manifest.yml`'s own
`review_todos`. All three tools are fully implemented and callable now; only their
typed-Action manifest declaration awaits CA-32. Each tool documents, in its own
docstring, that the client-side `SPARK_ENABLE_TRANSFORMS` gate + `dispatch_intent`'s
session-load approval check are the REAL, already-enforced gates in the meantime.

## connector_manifest.yml gate status
`connector_manifest.yml` and the full capability certification bundle
(`spark_mcp/ontology/{certification.json,shapes/connector.shacl.ttl,mappings/
source.yaml,fixtures/records.json,migrations/manifest.json}`) are generated via
agent-utilities's real generator scripts (`generate_connector_manifests.py` /
`generate_connector_capability_bundles.py`), same shape as every other connector in
the fleet — never hand-typed. `scripts/update_ontology_lock.py` requires a trusted
release-signing key (`ONTOLOGY_RELEASE_SIGNING_PRIVATE_KEY_REF`) not configured in
this environment — the `ontology.lock` entry itself is left to the fleet-wide
registration coordinator, per this lane's brief; not attempted here.

Separately, `http://knuckles.team/kg/spark` is not yet in `agent-utilities`'s
`REGISTERED_FEDERATED_IRIS` whitelist (`ontology_federation.py`) — the same
onboarding step every new connector goes through, also left to the coordinator.

## ⛔ Keep the Repository Root Pristine — No Scratch / Temp / Debug Files

**The repository ROOT must contain only canonical project files** (packaging,
config, docs, lockfiles). The only hidden directories allowed at root are
`.git/`, `.github/`, and `.specify/` (plus a local, git-ignored `.venv/`).

**NEVER write any of the following — anywhere in the repo, and ESPECIALLY at the root:**
- One-off / debug / migration scripts: `fix_*.py`, `migrate_*.py`, `refactor_*.py`,
  `replace_*.py`, `update_*.py`, `debug_*.py`, or `test_*.py` **at the root**
  (real tests live in `tests/` only).
- Databases / data dumps: `*.db`, `*.db-wal`, `*.sqlite*`, `*.corrupted`.
- Logs / command output: `*.log`, scratch `*.txt`, `*.orig`, `*.rej`, `*.bak`.
- Build artifacts: `*.tsbuildinfo`, compiled binaries, coverage files.
- AI agent scratch directories: `.agent/`, `.agents/`, `.agent_data/`, `.tmp/`,
  `.hypothesis/`, or any per-tool cache committed to git.
- Any file that is NOT production source, a test in `tests/`, documentation, or
  a recognized config/lockfile.

**Where scratch goes instead:** `~/workspace/scratch/` (experiments),
`~/workspace/reports/` (command output); tests go in `tests/` (pytest).
Before finishing a task, run `git status` and confirm no stray root files were added.

## Working Discipline — think, simplify, stay surgical, verify
- **Think before coding.** State assumptions explicitly; surface options rather
  than silently picking one.
- **Simplicity first.** Minimum code that solves the stated problem.
- **Stay surgical.** Every changed line traces to the task.
- **Verify against a goal.** Prove behavior with a real call against the live
  Spark Connect deployment, not a mock alone.

## Quality Bar — Leave the Codebase Clean (REQUIRED)
Run `pre-commit run --all-files` and drive it fully green before committing.
Do not silence checks (`# noqa`, `# type: ignore`, `SKIP=`, `--no-verify`) to
force green.

## Working with Git Worktrees (multi-session)
This is a small, individually-owned package repo. Check `git worktree list`
before assuming a shared-worktree convention applies — if single-worktree,
committing on a topic branch in place is fine (confirm the branch first).
