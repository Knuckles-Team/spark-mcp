---
name: spark-job-control
skill_type: skill
description: >-
  Cancel or kill running Spark work through the spark-mcp MCP server — cancel
  operations by tag, kill a job or a stage, and check the application status. Use
  when the agent must stop a runaway query or stage after reading its metrics. Do
  NOT use to read metrics (use spark-history-monitoring) or to submit transforms
  (use spark-transform-operations).
license: MIT
tags: [spark, jobs, cancel, mcp]
metadata:
  author: Knuckles-Team
  version: '0.1.0'
---
# Spark Job Control

Destructive controls; every one needs explicit `confirm` and the operator's approval.

## Tools
- `spark_application_status` reports the session and application state.
- `spark_cancel` cancels operations carrying a `tag`.
- `spark_kill_job` kills a job by `job_id`.
- `spark_kill_stage` kills a stage by `stage_id`.

## Steps
1. Read the target with `spark_application_status`. Verify the application is running and note the `job_id`, `stage_id` or `tag` to stop (use the spark-history-monitoring skill to identify it).
2. If the work was tagged, call `spark_cancel` with the `tag` and `confirm` set to true. Verify the response reports the cancelled operations.
3. Otherwise stop one job with `spark_kill_job` (`job_id`, `confirm` true) or one stage with `spark_kill_stage` (`stage_id`, `confirm` true). Verify the call returns without error.
4. Read `spark_application_status` again. Verify the job or stage no longer shows as running.

## Failure recovery
| Failure | Cause | Recovery |
|---------|-------|----------|
| Call refused | `confirm` not set or the operator has not approved | Obtain the operator's approval, then call the same tool with `confirm` set to true |
| Not-found error | Job or stage already finished, or wrong id | Call `spark_application_status` and re-read the active ids; skip the kill if the work is done |
| Cancel affects nothing | No operation carries that `tag` | Use `spark_kill_job` with the `job_id` from the monitoring skill instead |
