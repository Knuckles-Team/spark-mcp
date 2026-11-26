---
name: spark-kg-ingestion
skill_type: skill
description: >-
  Push one Spark Connect TransformRun into the medulla Knowledge Graph as typed OWL nodes via the spark-mcp MCP server's Wire-First ingest tool — SparkApplication, SparkJob, Transform, TransformRun, and the DatasetVersions it consumed/produced. Use when the agent must make a Transform execution queryable/joinable in the KG alongside Lakekeeper's own catalog nodes and Trino's lineage.
license: MIT
tags: [spark, spark-connect, kg, ingest, mcp]
metadata:
  author: Knuckles-Team
  version: '0.2.0'
---
# Spark KG Ingestion

Scope: Push one Spark Connect TransformRun into the medulla Knowledge Graph as typed OWL nodes via the spark-mcp MCP server's Wire-First ingest tool — SparkApplication, SparkJob, Transform, TransformRun, and the DatasetVersions it consumed/produced. Use when the agent must make a Transform execution queryable/joinable in the KG alongside Lakekeeper's own catalog nodes and Trino's lineage. Do NOT use for running/inspecting a Transform directly (use spark-transform-operations).

Pushes one recorded `TransformRun` (this package's own ledger — there is no Spark
History Server in this deployment to backfill from) into the medulla engine
as a typed OWL chain, through the required `native_ingest` (Wire-First /
`ApplyChangeEnvelope`) authority — never a bespoke write path.

## When to use
- Make a Transform execution's job/run/lineage state queryable/joinable in the KG.
- Record a `:TransformRun` node after `spark_submit_transform`/`spark_rerun_transform`
  succeeds (or fails — a failed run is still ingestable; `status`/`error` are
  first-class fields).

## When NOT to use
- Running/inspecting a Transform directly → `spark-transform-operations`.
- Materializing OpenLineage `prov:Activity` nodes from the Kafka event stream —
  CA-25's territory, not this tool.

## Prerequisites & environment
Same as `spark-transform-operations`. No additional KG-side credentials — ingestion
runs through the process-owned `GraphComputeEngine` authority.

## Steps
1. Confirm the run exists with `spark_list_transform_runs`. Verify the target `run_id` appears with its `status`.
2. Call `spark_ingest_run(run_id)` for that run. Verify the response reports the ingested entity counts and no `NativeIngestError`.
3. Check the chain: query the KG for the `:TransformRun` node id `spark:TransformRun:<run_id>`. Verify it links to its `:Transform` and `:SparkApplication`.
4. When inputs were pinned with `as_of_version`, verify `consumedVersion` edges exist for each pinned input.

## Failure recovery
| Failure | Cause | Recovery |
|---------|-------|----------|
| Run not found error | `run_id` is not in this package's ledger | Call `spark_list_transform_runs` and use a listed `run_id` |
| `NativeIngestError` | Engine authority unavailable | Ask the operator to restore the engine, then call `spark_ingest_run` with the same `run_id` (nothing was partially committed) |
| No `consumedVersion` edge | Input had no pinned `as_of_version` | Expected for a first run; resubmit with `spark_submit_transform` pinning `as_of_version` if lineage is required |

## Tool

`spark_ingest_run(run_id)` — produces:

```
:SparkApplication -[hasJob]-> :SparkJob -[runs]-> :TransformRun
:TransformRun -[executesTransform]-> :Transform
:TransformRun -[producedVersion]-> :DatasetVersion   (output table's new snapshot)
:TransformRun -[consumedVersion]-> :DatasetVersion   (each pinned input, if as_of_version was set)
```

Node ids follow `spark:<Class>:<externalId>`. Batches at ≤500 entities per
`native_ingest.ingest_entities` call (egeria-mcp's convention). Never partially
commits — `NativeIngestError` propagates rather than silently acking a partial batch.

## Failure modes to expect
- `run_id` not found in this package's ledger raises, never returns an empty chain.
- The engine authority being unavailable raises `NativeIngestError`, not a quiet no-op.
- An input without a pinned `as_of_version` produces no `consumedVersion` edge for
  that input (nothing to pin) — this is expected for a first, non-reproducible run.
