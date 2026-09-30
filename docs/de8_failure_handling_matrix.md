# DE8 — Failure Handling Matrix

| Fault Type                      | Expected Action     | Implemented Control                                                   | Evidence                              |
| ------------------------------- | ------------------- | --------------------------------------------------------------------- | ------------------------------------- |
| 1. Missing daily file           | FAIL                | Pipeline validation checks expected input availability                | Pipeline status = FAILED              |
| 2. Duplicate file               | CONTINUE / WARN     | Ingestion detects already-routed destination and returns SKIPPED      | `ingestion.py` → `SKIPPED`            |
| 3. Duplicate ingestion attempt  | CONTINUE            | Idempotent routing prevents duplicate copy                            | Existing file remains unchanged       |
| 4. Malformed timestamp          | REJECT / QUARANTINE | Minimum-quality validation rejects malformed records/files            | `data/rejected/` + status             |
| 5. Negative activity value      | REJECT / QUARANTINE | Minimum-quality validation rejects negative activity                  | `data/rejected/` + status             |
| 6. Unexpected or missing column | REJECT              | Schema validation checks required columns                             | `ingestion.py` + status               |
| 7. Partially corrupt file       | REJECT / QUARANTINE | File-quality validation prevents bad data entering raw/processed flow | `data/rejected/` + status             |
| 8. Spark job failure            | RETRY → FAIL        | Airflow `spark_process` has `retries=1`                               | Airflow task status + pipeline status |

## Implemented Controls Demonstrated

1. **Schema validation** — prevents files with missing/unexpected required columns from entering the pipeline.
2. **Quarantine/routing** — invalid files are moved to `data/rejected/` instead of entering `data/raw/`.
3. **Idempotent ingestion** — reprocessing an already-routed file produces `SKIPPED` rather than duplicating it.
4. **Airflow retry** — Spark processing is configured with one retry before final failure.

## Safe Rerun

A previously processed file is submitted again.

Expected behaviour:

* No duplicate copy is created.
* Ingestion reports `SKIPPED`.
* Warehouse is rebuilt safely.
* Analytics row count remains correct.
* `(grid_id, timestamp)` remains unique.

## DE7 Pipeline Status Evidence

Each pipeline execution produces a machine-readable status record containing:

* `run_id`
* overall `status`
* per-task status
* row counts
* `AS_OF`
* warehouse information
* failure information when applicable

