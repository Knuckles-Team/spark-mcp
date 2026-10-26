# Changelog

All notable changes to `spark-mcp` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.4] - 2026-10-24

### Changed
- Central connector migration: requires `agent-connector-sdk>=0.5.4`; server bootstrap, version, containers, ontology artifacts, security scaffold and pre-commit config are provided centrally.

## [Unreleased]

## [0.4.5] - 2026-10-26

### Added
- Standard spec-delivery Pages dashboard and the `spark-mcp-development` delivery skill.

## [0.4.1] - 2026-10-23

### Changed
- First published release of the 0.4 line (0.4.0 was tagged in the changelog but never published).

## [0.4.0] - 2026-10-18

### Changed
- Spark 4: `pyspark-client` 4.1 (Spark Connect) on Python 3.14 with numpy 2,
  pandas and pyarrow; the 3.12 readiness exception is gone.

### Added
- DataFrame and catalog operations over Spark Connect: query_table, count_rows,
  summarize_table, explain_query, list_catalogs, list_databases, list_tables,
  list_columns, list_functions, get_current_catalog.
- Monitoring REST operations against the driver UI and the History Server:
  applications, jobs, stages (attempts, task summary, task list), executors,
  storage RDDs, environment, SQL executions, processes, version, and kill_job /
  kill_stage on the live driver.

## [0.1.0] - 2026-08-26

### Added
- Initial release: MCP server, A2A agent, and API client for Spark Connect
  (gRPC) integration — SQL/PySpark execution against a live Spark Connect
  session, versioned Transform manifest submission/tracking against the
  shared Iceberg lakehouse catalog, and lineage/application status reporting.
- Connector capability certification bundle (`connector_manifest.yml`,
  ontology, KG ingestion).
- Standard fleet scaffolding (CI workflows, pre-commit, docs site, packaging
  metadata, pytest.ini) to bring the repo to parity with the rest of the
  agent-packages fleet.

### Changed
- Regenerated `connector_manifest.yml` + `certification.json` from the
  committed tree.
