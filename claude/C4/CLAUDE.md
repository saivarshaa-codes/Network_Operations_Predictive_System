# Network Operations Predictive Intelligence — CLAUDE.md

This file defines the engineering architecture, operational rules, and terminology constraints for Claude Code and all agentic workflows operating in this repository.

---

## Non-Negotiable Project Rules

### 1. Canonical Schema & Grain Separation
- **Raw Landing Grain**: Multiple rows per grid cell per hour, disaggregated by country code (`country_code`).
- **Analytics Warehouse Grain**: Exactly **one row per (grid_id, hourly_timestamp)** after country-code aggregation.
- Ingestion and Spark pipelines must enforce that no duplicate `(grid_id, timestamp)` pairs exist in the analytics layer.

### 2. Country-Code Aggregation Standard
- Telemetry across diverse international country codes must be summed into unified activity metrics before publishing to the analytics layer or machine learning pipelines.
- The country code dimension is an ingestion-time partition, not an analytics-grain dimension.

### 3. Activity Measures Terminology
- Activity values are **non-negative proportional activity measures** (indices).
- **NEVER** describe activity measures as literal call counts, SMS counts, message counts, or megabytes/gigabytes.

### 4. Congestion Terminology Constraint
- **NEVER claim or assert network congestion.**
- The platform telemetry measures relative activity volumes; it contains **no capacity, latency, packet loss, or radio throughput metrics**.
- Elevated activity or high anomaly scores indicate an **operational attention signal**, not confirmed physical failure or congestion.

### 5. Geographic Joins on `properties.cellId`
- Spatial joins against `data/reference/milano-grid.geojson` must ALWAYS match on `properties.cellId`.
- **NEVER** join on the zero-based GeoJSON feature array index `id`, which introduces a systematic spatial offset.

### 6. The `AS_OF` Temporal Convention
- The system operates over historical batch intervals where "now" is non-trivially defined by the `AS_OF` timestamp parameter.
- All rolling baselines, feature windows, and hotspot queries must calculate temporal windows strictly relative to `AS_OF`, excluding any data points timestamped $> \text{AS\_OF}$ to prevent data leakage.

---

## Architectural Boundaries

- **Data Immutability**: `data/raw/` is strictly immutable. Never delete, overwrite, or mutate raw CSV files.
- **Computation Allocation**: Heavy transformations, spatial joins, and temporal aggregations belong in Spark and SQL—never inside LLM prompt contexts or thin API wrappers.
- **Pipeline Trustworthiness**: Operational assertions must always verify `GET /pipeline/status` before presenting conclusions to engineers.
