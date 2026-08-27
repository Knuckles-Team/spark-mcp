---
name: spark-kg-ingestion
skill_type: skill
description: >-
  Push one Spark Connect TransformRun into the epistemic-graph Knowledge Graph as
  typed OWL nodes via the spark-mcp MCP server's Wire-First ingest tool —
  SparkApplication, SparkJob, Transform, TransformRun, and the DatasetVersions it
  consumed/produced. Use when the agent must make a Transform execution
  queryable/joinable in the KG alongside Lakekeeper's own catalog nodes and Trino's
  lineage. Do NOT use for running/inspecting a Transform directly (use
  spark-transform-operations).
license: MIT
tags: [spark, spark-connect, kg, ingest, mcp]
metadata:
  author: Genius
  version: '0.1.0'
---
# Spark KG Ingestion

Pushes one recorded `TransformRun` (this package's own ledger — there is no Spark
History Server in this deployment to backfill from) into the epistemic-graph engine
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
