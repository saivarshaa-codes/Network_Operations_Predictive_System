# Project Permissions and Security Model (Phase 7 - C6)

This document formalizes the **Least Privilege Security Architecture** governing autonomous coding agents and developer tooling across the Network Operations Predictive Intelligence System.

---

## 1. Classification Matrix: Allow / Ask / Deny

| Operation Category | Classification | Guardrail Rationale |
| :--- | :--- | :--- |
| **Source Reading & Static Linting** | `ALLOW` | Safe, read-only analysis over `api/`, `spark/`, `ML/`, `frontend/`, `docs/`, and test runs. |
| **Automated Test Execution** | `ALLOW` | Non-destructive execution of test suites (`pytest tests/`). |
| **Dependency & Package Updates** | `ASK` | Prevents undetected package drift, breaking versions, or unauthorized external network fetches. |
| **Pipeline & Airflow Configurations** | `ASK` | Changes to DAG schedules or task chains directly affect hourly telemetry pipelines. |
| **Database Migrations & Warehouse Builds** | `ASK` | Recompiling or altering schemas requires NOC DBA confirmation. |
| **Raw Data Mutating / Writing** | `DENY` | Raw telemetry under `data/raw/` is legally and contractually immutable. |
| **Data Deletion** | `DENY` | Deleting files under `data/` risks unrecoverable data loss. |
| **Secrets & Credentials Access** | `DENY` | Secrets in `.env`, credentials, or private certificates must never be read by agents. |
| **Destructive SQL / Commands** | `DENY` | `DROP TABLE`, `TRUNCATE`, and force git commands are blocked unconditionally. |

---

## 2. Managed Team Settings
In collaborative engineering environments, these boundaries are pinned in `permissions_policy.json` and mirrored in `.claude/settings.json` so every team member adheres to identical safety boundaries.
