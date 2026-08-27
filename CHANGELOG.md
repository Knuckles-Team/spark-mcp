# Changelog

All notable changes to `spark-mcp` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
