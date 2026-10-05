# Reusable Network Operations Skills Specification (Phase 7 - C8)

This directory packages repeated NOC engineering instructions and domain constraints into reusable Claude Skills.

---

## 1. Packaged Skills

### A. `network-anomaly-analysis`
- **Activation Triggers**: Queries asking why a grid cell is flagged, what an anomaly or risk score means, or whether a telemetry pattern is abnormal.
- **Required Evidence**:
  - `grid_id`
  - `current_activity` (proportional measure)
  - `baseline_activity`
  - `anomaly_score` or `risk_score`
  - `pipeline_status`
- **Strict Behavior**:
  - **Refusal on Incomplete Evidence**: If any required field is missing (e.g. bare grid ID without metrics or missing anomaly score), the skill explicitly refuses to assign a severity, stating that evidence is insufficient.
  - **Format**: Returns the mandatory 4-section output (`SEVERITY`, `EVIDENCE`, `INTERPRETATION`, `NEXT CHECKS`).
  - **Terminology Guardrails**: Activity values are proportional telemetry indices; zero congestion claims.

---

### B. `pipeline-troubleshooting`
- **Activation Triggers**: Queries regarding pipeline failures, stale analytics warehouse data, quarantined CSV files, or high rejection rates.
- **Investigation Matrix**:
  - Inspects CSV schema conformance at landing (`ingestion.py`).
  - Checks Spark null-handling logs and country-code aggregation.
  - Validates SQLite warehouse freshness against `AS_OF`.

---

### C. `telecom-data-quality`
- **Activation Triggers**: Queries auditing dataset integrity, schema changes, or database migrations.
- **Enforcement Rules**:
  - Asserts canonical grain uniqueness on `(grid_id, timestamp)`.
  - Asserts non-negative activity measures ($x \ge 0$).
  - Validates that GeoJSON joins use `properties.cellId` exclusively.
