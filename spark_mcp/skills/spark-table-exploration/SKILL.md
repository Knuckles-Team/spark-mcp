---
name: spark-table-exploration
skill_type: skill
description: >-
  Explore the Spark catalog and Iceberg tables through the spark-mcp MCP server — list catalogs, databases, tables, columns and functions, describe and summarize a table, count rows, run a bounded table query, and explain a query plan before running it. Use when the agent must find a table, inspect its shape, or read a sample without writing SQL.
license: MIT
tags: [spark, catalog, iceberg, tables, mcp]
metadata:
  author: Knuckles-Team
  version: '0.1.0'
---
# Spark Table Exploration

Scope: Explore the Spark catalog and Iceberg tables through the spark-mcp MCP server — list catalogs, databases, tables, columns and functions, describe and summarize a table, count rows, run a bounded table query, and explain a query plan before running it. Use when the agent must find a table, inspect its shape, or read a sample without writing SQL. Do NOT use to run governed transforms (use spark-transform-operations) or to read job and stage metrics (use spark-history-monitoring).

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
1. Call `spark_get_current_catalog` and `spark_list_databases` (optional `pattern`). Verify the database you need appears in the list.
2. Call `spark_list_tables` for that database. Verify the table name is present.
3. Call `spark_describe` on the table. Verify the columns and types match what the task needs.
4. Sample with `spark_query_table` (`columns`, `where`, `order_by`, small `limit`) or measure with `spark_summarize_table`. Verify the returned row count is within the `limit`.
5. For anything heavier, call `spark_explain_query` with `mode` set to `formatted`. Verify the plan before running the query.

## Failure recovery
| Failure | Cause | Recovery |
|---------|-------|----------|
| Typed table-not-found error | Wrong catalog, database or table name | Call `spark_list_catalogs`, `spark_list_databases` and `spark_list_tables` again and copy the exact names |
| Fewer rows than expected | The row `limit` was applied | Raise `limit` in `spark_query_table`, or use `spark_count_rows` for the exact total |
| Connection error | Spark Connect session unreachable | Call `spark_get_version` to test the session, and ask the operator to check the connect URL setting |
