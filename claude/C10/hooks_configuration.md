# Project Hooks Configuration (Phase 7 - C10)

This document specifies the event-driven hooks enforcing engineering discipline during developer and agent activities.

---

## 1. Hook Trigger Matrix

| Hook Name | Lifecycle Event | Pattern / Target | Action Taken | Rationale |
| :--- | :--- | :--- | :--- | :--- |
| **`post_edit_grain_leakage`** | `Post-Edit` | `spark/**`, `ML/**` | Runs the canonical grain uniqueness test and temporal leakage test immediately. | Grain duplicates and temporal data leakage are silent, compounding bugs that invalidate downstream warehouse tables and models. |
| **`pre_edit_pipeline_guard`** | `Pre-Edit` | `airflow/**`, `config/**` | Blocks execution unless explicit human confirmation token is supplied. | Uncontrolled changes to orchestration schedules or DAG connections can disrupt production telemetry pipelines. |

---

## 2. Hooks vs. CI Boundaries

| Responsibility | Enforced in Hooks (Local Edit-Time) | Enforced in CI (Pull Request / Merge) |
| :--- | :--- | :--- |
| **Data Grain Invariant** | Yes (Fast, local check on edited files) | Yes (Full regression test on warehouse) |
| **Feature Leakage** | Yes (Immediate validation) | Yes (End-to-end dataset validation) |
| **Full PySpark Test Suite** | No (Too slow for edit loop) | Yes (Distributed test matrix) |
| **Airflow DAG Cycle Check** | Pre-edit confirmation | Full DAG load testing in isolated container |
| **Style & Formatting** | Fast pre-commit linter | Automated PR blocking checks |
