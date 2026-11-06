---
name: spark-history-monitoring
skill_type: skill
description: >-
  Read Spark application, job, stage, task, executor, SQL execution and storage
  metrics from the Spark REST API through the spark-mcp MCP server. Use when the
  agent must find why a Spark job is slow or failed, compare stage task
  distributions, check executor health, or read a SQL execution plan. Do NOT use
  to cancel or kill running work (use spark-job-control) or to run SQL (use
  spark-transform-operations).
license: MIT
tags: [spark, monitoring, history, metrics, mcp]
metadata:
  author: Knuckles-Team
  version: '0.1.0'
---
# Spark History Monitoring

Read-only views of the Spark REST API for the driver or the history server (the `server` argument).

## Tools
| Tool | Purpose |
|------|---------|
| `spark_list_applications` / `spark_get_application` | Applications and their attempts |
| `spark_get_version` / `spark_get_environment` | Spark version and effective configuration |
| `spark_list_jobs` / `spark_get_job` | Jobs and their status |
| `spark_list_stages` / `spark_list_stage_attempts` / `spark_get_stage_attempt` | Stages and attempts |
| `spark_get_stage_task_summary` / `spark_list_stage_tasks` | Task time distribution and task rows |
| `spark_list_executors` / `spark_list_all_executors` / `spark_list_processes` | Executor and process health |
| `spark_list_sql_executions` / `spark_get_sql_execution` | SQL executions and plans |
| `spark_list_storage_rdds` / `spark_get_storage_rdd` | Cached RDD storage |

## Steps
1. `spark_list_applications` and pick the app id.
2. `spark_list_jobs` with `status` set to failed or running, then `spark_get_job`.
3. Follow the job to its stages with `spark_list_stages`, then `spark_get_stage_task_summary` to see skew.
4. Check `spark_list_executors` for lost executors and `spark_get_sql_execution` for the plan.

## Failure modes
- An unknown app id or stage id raises a typed not-found error.
- The history server and the driver expose different data; set `server` explicitly.
