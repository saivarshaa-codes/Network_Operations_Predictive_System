---
name: pipeline-troubleshooting
description: Diagnoses pipeline failures, rejected telemetry rows, stale warehouse partitions, and ingestion audit anomalies.
---

# Pipeline Troubleshooting Skill

## When to Activate
Activate when a user reports:
- Pipeline failure or unhealthy status
- Quarantine files under `data/rejected/`
- High null counts or data freshness warnings

## Diagnostic Workflow
1. Check `GET /pipeline/status` for `rows_rejected`, `nulls_handled`, and `reasons`.
2. Map failures against the failure handling matrix (`docs/de8_failure_handling_matrix.md`).
3. Differentiate between transient upstream CSV formatting and systemic Spark aggregation failure.
4. Report root cause, operational blast radius, and recovery steps.
