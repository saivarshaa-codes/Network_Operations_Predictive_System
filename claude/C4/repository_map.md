# Repository Map: Network Operations Predictive Intelligence System

This map documents the end-to-end architecture, directory responsibilities, and data lineage across the platform.

---

## 1. Directory Responsibilities

| Directory | Core Responsibility | Key Assets |
| :--- | :--- | :--- |
| **`data/`** | Multi-stage data tier: `landing/`, `raw/` (immutable), `rejected/` (quarantine), `reference/` (`milano-grid.geojson`), `processed/` and `analytics/` (Parquet). | `milano-grid.geojson`, `network_ops.db` |
| **`ingestion/`** | Daily CSV file detection, quarantine routing, schema validation, and audit logging. | `ingestion.py` |
| **`Phase1/`** | Python Data Engineering baseline: parsing, cleaning, baseline calculation, and rule-based alerts. | `usage_processor.py`, `network_alerts.py` |
| **`spark/` & `Phase2/`** | Distributed PySpark telemetry processing: null handling, country-code aggregation to hourly grid grain, GeoJSON spatial join, and analytical Parquet generation. | `spark_ingestion.py`, `network_activity_cleaning.py`, `network_activity_aggregations.py`, `geospatial_enrichment.py`, `telecom_pipeline.py` |
| **`airflow/`** | Workflow orchestration DAGs coordinating ingest -> spark -> warehouse load -> quality check -> ML scoring. | `network_pipeline_dag.py`, `ingestion_dag.py`, `spark_processing_dag.py` |
| **`warehouse/`** | Analytical SQLite database compilation from curated Parquet datasets. | `build_warehouse.py` |
| **`api/`** | High-performance FastAPI REST layer serving summaries, grid drills, hotspots, alerts, ML predictions, and pipeline status. | `main.py`, `service.py`, `models.py`, `database.py` |
| **`ML/`** | Feature engineering (`ML2`), risk classification training (`ML3`), anomaly scoring (`ML4`), and batch scoring (`ML6`). | `feature_engineering.py`, `train_risk_classifier.py`, `compute_anomalies.py`, `batch_score.py` |
| **`frontend/`** | React + Leaflet lightweight NOC operations dashboard displaying citywide maps, hotspot tables, and predictive risk indicators. | `src/App.jsx`, `src/components/`, `public/reference/` |
| **`claude/`** | Phase 7 AI-Assisted Operations suite: insight generator, tool-using assistant, incident investigation, MCP server, hooks, and autonomous agents. | `C1` to `C16` |
| **`docs/`** | Operational contracts, data contracts, failure handling matrix, runbooks. | `runbook.md`, `API5_CONTRACT.md`, `de8_failure_handling_matrix.md` |
| **`tests/`** | End-to-end unit, integration, grain uniqueness, and feature leakage tests. | `test_ingestion.py`, `api/test_api1.py`, `ML2/test_feature_engineering.py` |

---

## 2. End-to-End Data Lineage
```
[Landing CSVs: data/landing/]
            │
            ▼ (ingestion/ingestion.py)
[Raw Immutable: data/raw/] ──(Validation failure)──► [Quarantine: data/rejected/]
            │
            ▼ (spark/telecom_pipeline.py & Phase2)
   1. Clean nulls & invalid records
   2. Aggregate country codes -> Canonical Grain: (grid_id, hourly_timestamp)
   3. Spatial Join with data/reference/milano-grid.geojson on properties.cellId
            │
            ▼
[Processed Parquet: outputs/sp7_pipeline/ & data/processed/]
            │
            ▼ (warehouse/build_warehouse.py)
[Analytics Warehouse: data/warehouse/network_ops.db]
            │
            ├──────────────────────────────────────────┐
            ▼                                          ▼
[ML Pipeline: ML/ML2 -> ML3 -> ML4 -> ML6]    [FastAPI Service Layer: api/main.py]
  - 24h rolling feature vectors                  - GET /network/summary
  - Risk classification & Anomaly scoring        - GET /network/grid/{grid_id}
  - Batch scored tables                          - GET /network/hotspots
            │                                    - GET /pipeline/status
            │                                    - POST /network/predict-risk
            ▼                                          ▲
   [Scoring Updates in DB] ────────────────────────────┘
                                                       │
                                                       ▼
                                   [React NOC Dashboard & Claude AI Agents]
```
