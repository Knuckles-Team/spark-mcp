---
name: spark-history-monitoring
skill_type: skill
description: >-
  Read Spark application, job, stage, task, executor, SQL execution and storage metrics from the Spark REST API through the spark-mcp MCP server. Use when the agent must find why a Spark job is slow or failed, compare stage task distributions, check executor health, or read a SQL execution plan.
license: MIT
tags: [spark, monitoring, history, metrics, mcp]
metadata:
  author: Knuckles-Team
  version: '0.1.0'
---
# Spark History Monitoring

Scope: Read Spark application, job, stage, task, executor, SQL execution and storage metrics from the Spark REST API through the spark-mcp MCP server. Use when the agent must find why a Spark job is slow or failed, compare stage task distributions, check executor health, or read a SQL execution plan. Do NOT use to cancel or kill running work (use spark-job-control) or to run SQL (use spark-transform-operations).

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
1. Find the application with `spark_list_applications` (set `server` explicitly). Verify the response contains the app id you expect before continuing.
2. List the failing or running jobs with `spark_list_jobs` (filter on `status`) and open one with `spark_get_job`. Verify the job status and stage ids are present in the output.
3. Walk the job to its stages with `spark_list_stages` and read `spark_get_stage_task_summary`. Verify the task time quantiles show whether one stage has skew.
4. Check executor health with `spark_list_executors`. Verify no executor is reported lost or excluded.
5. Read the plan with `spark_list_sql_executions` and `spark_get_sql_execution`. Verify the execution id matches the job under study.

## Failure recovery
| Failure | Cause | Recovery |
|---------|-------|----------|
| Typed not-found error | Wrong app id or stage id | List again with `spark_list_applications` or `spark_list_stages` and copy the id from that output |
| Empty or unexpected data | Driver and history server expose different data | Set the `server` argument explicitly to the driver or the history server and call the same tool |
| Executors missing from the list | Application already finished | Call `spark_list_all_executors` to include dead executors |
