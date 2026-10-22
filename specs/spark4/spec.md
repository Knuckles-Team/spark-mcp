# CONN-SPARK-MCP-SPARK4 — Spark 4 queries and monitoring connector

Owner: spark-mcp.

## Purpose and user stories

An agent reads data and job state from a Spark 4 cluster. The connector runs read-only queries through Spark Connect. It reads application, job, stage and executor state from the monitoring REST API.

## Requirements and acceptance

The rows are in [requirements.md](requirements.md).

## Why a new spec

The repository had no local spec. The 0.4.0 release added the Spark Connect client and the monitoring REST client without rows.

## Scope and interfaces

In scope: Spark Connect queries, catalog reads, monitoring REST reads and the governed transform submit. Out of scope: cluster provisioning and the connector contract of the SDK.
