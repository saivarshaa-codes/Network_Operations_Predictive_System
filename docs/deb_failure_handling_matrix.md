# DE8 — Network Pipeline Failure Handling Matrix

| # | Fault Type | Expected Action | Control / Module | Reason |
|---|---|---|---|---|
| 1 | Missing daily file | WARN / CONTINUE | ingestion/ingestion.py | Missing input should be visible without corrupting existing data |
| 2 | Duplicate file | CONTINUE / SKIP | ingestion/ingestion.py | Idempotent routing prevents duplicate ingestion |
| 3 | Malformed timestamp | REJECT / QUARANTINE | ingestion/ingestion.py | Invalid timestamps cannot safely enter downstream processing |
| 4 | Negative activity value | REJECT / QUARANTINE | ingestion/ingestion.py | Negative network activity is invalid input |
| 5 | Unexpected or missing column | REJECT / QUARANTINE | ingestion/ingestion.py | Schema mismatch must be stopped before processing |
| 6 | Partially corrupt file | REJECT / QUARANTINE | ingestion/ingestion.py | Corrupt input must not contaminate the raw/analytics pipeline |
| 7 | Spark job failure | RETRY → FAIL | airflow/dags/network_pipeline_dag.py | Transient Spark failures are retried; persistent failure stops the pipeline |

## Implemented Controls

1. **Schema validation** — invalid or incomplete schemas are rejected before processing.
2. **Quarantine routing** — rejected files are copied to `data/rejected/`.
3. **Idempotent ingestion** — an already-routed file is recorded as `SKIPPED` instead of being duplicated.
4. **Airflow retry** — the Spark processing task retries once after failure.
5. **Fail-fast processing** — a failed Spark subprocess raises `AirflowException`.

## Safe Rerun

The pipeline can be rerun after a previous execution. The warehouse builder recreates the SQLite warehouse rather than appending duplicate fact records.

After rerunning, verify:

- pipeline status is `SUCCESS`
- `AS_OF` is present
- published row count is correct
- zero duplicate `(grid_id, timestamp)` keys exist in `hourly_grid_summary`

