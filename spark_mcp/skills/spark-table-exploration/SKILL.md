---
name: spark-table-exploration
skill_type: skill
description: >-
  Explore the Spark catalog and Iceberg tables through the spark-mcp MCP server
  — list catalogs, databases, tables, columns and functions, describe and
  summarize a table, count rows, run a bounded table query, and explain a
  query plan before running it. Use when the agent must find a table, inspect its
  shape, or read a sample without writing SQL. Do NOT use to run governed
  transforms (use spark-transform-operations) or to read job and stage metrics
  (use spark-history-monitoring).
license: MIT
tags: [spark, catalog, iceberg, tables, mcp]
metadata:
  author: Knuckles-Team
  version: '0.1.0'
---
# Spark Table Exploration

Read-only catalog and table inspection over the Spark Connect session.

## When to use
- Find a table: `spark_list_catalogs`, `spark_get_current_catalog`, `spark_list_databases`, `spark_list_tables`, `spark_list_lakekeeper_tables`.
- Inspect shape: `spark_describe`, `spark_list_columns`, `spark_list_functions`.
- Sample and measure: `spark_query_table` (bounded by `limit`), `spark_count_rows`, `spark_summarize_table`.
- Check cost first: `spark_explain_query` with `mode` set to `simple`, `extended`, `codegen`, `cost` or `formatted`.

## When NOT to use
- Writing or transforming data: `spark-transform-operations`.
- Job, stage or executor metrics: `spark-history-monitoring`.

## Steps
1. `spark_get_current_catalog`, then `spark_list_databases` with an optional `pattern`.
2. `spark_list_tables` for the database, then `spark_describe` on the table.
3. `spark_summarize_table` for statistics or `spark_query_table` with `columns`, `where`, `order_by` and a small `limit`.
4. For anything heavier, run `spark_explain_query` first and confirm the plan.

## Failure modes
- A table that does not exist raises a typed error, never an empty result.
- `spark_query_table` always applies a row limit; ask for more only when needed.
