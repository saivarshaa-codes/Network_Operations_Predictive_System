# Project Slash Commands Specification (Phase 7 - C7)

This document specifies the repeatable operational commands available to NOC engineers and platform developers.

---

## 1. Command Catalog

| Command | Classification | Primary Inputs | Tools/Endpoints Called | Expected Output Shape |
| :--- | :--- | :--- | :--- | :--- |
| **`/check-pipeline`** | Operations (NOC) | None | `GET /pipeline/status` | Concise operational health summary: healthy/degraded status, row count published, rejection counts, freshness rating, active warnings. |
| **`/explain-grid`** | Operations (NOC) | `grid_id: int` | `GET /network/grid/{id}`, `GET /network/grid/{id}/features`, `GET /network/grid/{id}/location`, `POST /network/predict-risk` | Four-section operational briefing: `SEVERITY`, `EVIDENCE`, `INTERPRETATION`, `NEXT CHECKS`. |
| **`/review-anomaly`** | Operations (NOC) | `grid_id: int` | `GET /network/alerts`, `POST /network/predict-risk`, `GET /network/grid/{id}/features` | Multi-signal reconciliation: compares NP3 rule alerts against ML3 classifier and ML4 anomaly scores, explaining any divergence. |
| **`/test-api`** | Engineering (Dev) | `suite: str = "all"` | `pytest tests/test_ingestion.py`, FastAPI TestClient checks | Test execution summary: total passed, failed, skipped, with failure tracebacks and execution latency. |
| **`/network-health`** | Engineering (Dev) | `table: str = "fact_network_activity"` | SQL Warehouse query: duplicate check on `(grid_id, timestamp)` | Pass/Fail report asserting canonical grain uniqueness. |

---

## 2. Command Details & Operational Discipline
- **`/network-health`**: Fails immediately if any grid cell possesses multiple rows for the same timestamp. This is the canonical grain invariant.
- **`/explain-grid`**: Enforces all project terminology constraints—activity values are proportional indices, never counts or megabytes; zero congestion assertions.
