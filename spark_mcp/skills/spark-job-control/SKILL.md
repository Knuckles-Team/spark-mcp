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
1. Read the job or stage with the monitoring skill and confirm it is the one to stop.
2. Prefer `spark_cancel` by tag over a kill when the work was tagged.
3. Call the kill tool with `confirm` set, then read the status again.

## Failure modes
- Without `confirm` the call is refused.
- Killing an already finished job returns a not-found error.
