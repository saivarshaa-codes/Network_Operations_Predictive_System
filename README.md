# Network Operations & Predictive Intelligence System

An end-to-end telecom operational intelligence platform bridging distributed data engineering, analytical data warehousing, RESTful microservices, interactive geospatial visualization, leak-free machine learning, and autonomous agentic investigation workflows.

The system operationalizes open telecommunications activity telemetry through a six-stage operational lifecycle:

$$\text{\bf Observe} \longrightarrow \text{\bf Process} \longrightarrow \text{\bf Engineer} \longrightarrow \text{\bf Predict} \longrightarrow \text{\bf Explain} \longrightarrow \text{\bf Act}$$

---

## 1. Project Overview & Operational Storyline

* **Observe**: Ingests multi-stream Call Detail Record (CDR) telemetry covering SMS, voice calls, and internet activity across geographic grid squares. Validates incoming schemas and isolates malformed records to quarantine storage.
* **Process**: Distributed PySpark jobs clean missing values, aggregate country-code records into unified hourly cell activity, and enrich records with geospatial boundaries via Milan grid GeoJSON.
* **Engineer**: Relational star-schema analytical warehouse (`dim_time`, `dim_grid`, `fact_network_activity`) computes backward-looking rolling features (activity growth, 24h averages, variability, internet share) with zero forward temporal leakage.
* **Predict**: Machine learning classifiers and statistical baseline models score grid cells to forecast next-hour operational attention probabilities.
* **Explain**: Anthropic Claude agentic layers investigate anomalies, correlate spatial neighbor telemetry, and synthesize multi-agent findings without hallucinating hardware congestion.
* **Act**: Interactive React NOC dashboard, custom operator slash commands, Model Context Protocol (MCP) tool servers, and headless CLI investigators empower network engineers to triage operational risks.

---

## 2. System Architecture

```
[Raw CDR Telemetry / Ingestion Lake]
   │  (Schema validation, quarantine routing)
   ▼
[PySpark Distributed ETL Engine]
   │  (Country-code aggregation, GeoJSON spatial join on properties.cellId)
   ▼
[SQLite Analytical Star Warehouse (network_ops.db)]
   ├── dim_time (hourly temporal dimension)
   ├── dim_grid (10,000 geographic grid cells)
   ├── fact_network_activity (hourly operational traffic grain)
   └── network_feature_table (leak-free rolling ML features)
   │
   ├──────────────────────────────┬──────────────────────────────┐
   ▼                              ▼                              ▼
[FastAPI Service Layer]    [Machine Learning]         [Claude AI Agent Layer]
(REST Endpoints API1-API6) (Decision Tree / Baseline) (C1-C16 Tool Dispatch & MCP)
   │                              │                              │
   └──────────────────────────────┴──────────────────────────────┘
                                  ▼
                     [React 18 NOC Dashboard]
                   (Leaflet Map & Grid Explorer)
```

### Core Technologies
* **Data Processing & ETL**: Python 3.12, PySpark 4.2.0, pandas 3.0.5
* **Orchestration**: Apache Airflow (`airflow/dags/network_pipeline_dag.py`)
* **Analytics Warehouse**: SQLite 3 star schema (`data/warehouse/network_ops.db`)
* **Backend API**: FastAPI 0.110.0, Pydantic, Uvicorn
* **Frontend UI**: React 18.2.0, Vite v8.2.2, Leaflet.js, TailwindCSS
* **Machine Learning**: scikit-learn 1.9.0 (DecisionTreeClassifier, IQR/Z-score baselines)
* **Agentic Engineering**: Anthropic Python SDK (`claude-sonnet-4-6`), Model Context Protocol (MCP)

---

## 3. Repository Structure

```
.
├── Phase1/                # Pure Python data profiling, baseline & rule alerts (NP1-NP3)
├── Phase2/                # PySpark exploratory scripts & geospatial enrichment (SP1-SP6)
├── airflow/               # Airflow DAGs for pipeline orchestration (DE7)
├── api/                   # FastAPI backend service layer, routes, and models (API1-API6)
│   ├── database.py        # Read-only SQLite connection provider
│   ├── main.py            # FastAPI application entrypoint & middleware
│   ├── models.py          # Pydantic schema contracts
│   ├── service.py         # Analytical queries & ML model integration
│   ├── test_api1.py       # Unit tests for /network/summary
│   └── test_api2.py       # Unit tests for /network/grid/{id}
├── claude/                # Claude AI Engineering modules (C1-C16)
│   ├── C1-C3/             # Incident narratives, autonomous tool agent, context compaction
│   ├── C4-C6/             # CLAUDE.md guardrails, plan mode, permission safety engine
│   ├── C7-C11/            # Slash commands, skills packaging, multi-agent supervisor, rollbacks
│   └── C12-C16/           # MCP server, team plugins, headless CLI, CI reviews, token optimization
├── data/
│   ├── landing/           # Ingest drop zone for raw CDR batch files
│   ├── raw/               # Verified raw records
│   ├── rejected/          # Quarantine storage for malformed/corrupt files
│   ├── reference/         # milano-grid.geojson reference geometry
│   └── warehouse/         # Production SQLite database (network_ops.db)
├── docs/                  # Architecture diagrams & failure handling matrices
├── frontend/              # React 18 / Vite NOC operations dashboard (RE1-RE5)
├── ingestion/             # Raw ingestion framework & machine-readable pipeline status
├── ML/                    # Machine learning pipeline (ML1-ML6)
│   ├── ML1-ML2/           # Proxy target generation & leak-free feature engineering
│   ├── ML3-ML4/           # DecisionTree training/evaluation & anomaly baseline
│   └── ML5-ML6/           # High-throughput batch scoring & Top-20 attention reports
├── outputs/               # Parquet dataset exports & summary reports
├── spark/                 # Production PySpark ETL pipeline (SP7)
└── tests/                 # Data engineering unit tests (DE1-DE2)
```

---

## 4. Prerequisites & Environment

* **Operating System**: Windows, Linux, or macOS
* **Python**: Python 3.12.x
* **Node.js**: Node.js v20.x or higher, npm 10+
* **Java**: Java 8, 11, or 17 (required for Apache Spark)
* **Anthropic API Key**: Configured in `.env` for Phase 7 Claude modules:
  ```env
  ANTHROPIC_API_KEY=your-api-key-here
  ```

---

## 5. Setup & Installation

### 1. Python Environment Setup
```bash
# Clone the repository
git clone <repository-url>
cd "Network Operations Predictive System"

# Create and activate virtual environment
python -m venv venv
# On Windows:
.\venv\Scripts\activate
# On Linux/macOS:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Frontend Installation & Build
```bash
cd frontend
npm install
npm run build
cd ..
```

---

## 6. Pipeline Execution Sequence

1. **Ingest Raw Data & Quarantine Checks**:
   ```bash
   python ingestion/raw_ingestion.py
   ```
2. **Execute PySpark Distributed Aggregation**:
   ```bash
   python spark/telecom_pipeline.py
   ```
3. **Build Analytical Star Schema Warehouse**:
   ```bash
   python warehouse/build_warehouse.py
   ```
4. **Extract Leak-Free ML Features**:
   ```bash
   python ML/ML2/feature_engineering.py
   ```
5. **Train & Evaluate Decision Tree Risk Model**:
   ```bash
   python ML/ML3/train_model.py
   ```
6. **Generate Top-20 Operational Attention Batch Report**:
   ```bash
   python ML/ML6/attention_report.py
   ```

---

## 7. Starting the Application Services

### 1. Launch FastAPI Backend
```bash
uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
```
API Documentation and interactive Swagger UI will be available at:  
`http://localhost:8000/docs`

### 2. Launch React NOC Dashboard
```bash
cd frontend
npm run dev
```
Dashboard interface will be accessible at:  
`http://localhost:5173`

---

## 8. API Service & Endpoints

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/network/summary` | Global network KPIs, total activity, active grid count, and `AS_OF` timestamp. |
| `GET` | `/network/grid/{grid_id}` | 24-hour historical activity time series for a single grid cell. |
| `GET` | `/network/hotspots` | Top N grid cells ranked by total activity. |
| `GET` | `/network/alerts` | Active rule-based anomaly alerts (spikes, drops, high activity). |
| `GET` | `/network/grid/{grid_id}/features` | Latest leak-free ML feature vector for a grid cell. |
| `GET` | `/network/predict-risk` | Model-inferred operational attention probability and risk level. |
| `GET` | `/pipeline/status` | Ingestion status, warehouse health, and data freshness metrics. |
| `GET` | `/network/grid/{grid_id}/location` | Geographic centroid coordinates and polygon geometry. |
| `GET` | `/network/grid/{grid_id}/neighbours` | Surrounding 8 topological neighbor cells. |

---

## 9. Machine Learning Purpose & Governance

* **Operational Attention Framing**: The ML risk model forecasts the likelihood that a cell will require operator triage in the next hour due to an extreme departure from historical baselines.
* **Not a Fault Detector**: A high risk score represents an **operational attention signal**, *not* a confirmed equipment failure.
* **Leakage Prevention**: All features are computed strictly from historical windows ($T-24$ to $T-1$). Forward leakage tests mathematically verify that future traffic changes have zero impact on past features.

---

## 10. Claude AI Engineering Layer

The repository implements 16 advanced Claude capabilities (C1–C16) grounded in `CLAUDE.md`:
* **Evidence-Based Reasoning**: Claude accesses telemetry exclusively through structured tools (`get_pipeline_status`, `get_network_summary`) and never invents unobserved network states.
* **Context Window Optimization**: Compresses raw telemetry records by 89%–99%, reducing LLM token consumption while retaining anomaly signal density.
* **Model Context Protocol (MCP)**: Exposes a standard MCP tool server (`claude/C12/network_mcp_server.py`) mirroring production FastAPI schemas.
* **Multi-Agent Orchestration**: Hierarchical supervisor synthesizes reports across ingestion, anomaly, spatial, and governance specialists while surfacing dissenting data staleness caveats.

---

## 11. Testing & Validation

Run the automated test suites:

```bash
# 1. Run Data Ingestion & Idempotency Tests (DE1-DE2)
python -m unittest tests/test_ingestion.py

# 2. Run API Summary Tests (API1)
python -m unittest api/test_api1.py

# 3. Run API Grid Activity Tests (API2)
python -m unittest api/test_api2.py

# 4. Run ML Temporal Leakage Regression Test (ML2)
python ML/ML2/test_feature_engineering.py

# 5. Run Claude Team Plugin & Rule Safeguard Tests (C13)
python claude/C13/team_plugin.py

# 6. Verify Frontend Production Build (RE5)
cd frontend && npm run build && cd ..
```

---

## 12. Important Telecom Data Semantics

To preserve strict domain validity, all components adhere to the project data contract:
1. **Activity Measures**: Values in the Milan dataset are **unitless proportional activity indices**, NOT literal SMS/call counts and NOT megabytes or bandwidth.
2. **Grid Geometry**: `grid_id` (1–10,000) represents a square geographic cell in Milan. Geospatial joins must strictly match on `feature.properties.cellId` in GeoJSON.
3. **No Congestion Claims**: The dataset contains interaction activity but **no network capacity, latency, packet drop, or radio utilization metrics**. The platform never claims "network congestion" or "cell tower saturation"; it flags **elevated operational attention** for engineering review.
