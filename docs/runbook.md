# DE7 — Troubleshooting Map

| Failure | Module / Location | What to Check |
|---|---|---|
| Incoming file not detected | ingestion/ingestion.py | data/landing/ and filename pattern |
| Schema validation failure | ingestion/ingestion.py | required CSV columns |
| Minimum quality failure | ingestion/ingestion.py | timestamps, negative activity, malformed data |
| File routing failure | ingestion/ingestion.py | data/raw/ and data/rejected/ |
| Spark processing failure | spark/telecom_pipeline.py | input files, schema, Spark transformation, reference GeoJSON |
| Warehouse loading failure | warehouse/build_warehouse.py | processed Parquet, SQLite database, dimension/fact loading |
| Quality check failure | airflow/dags/network_pipeline_dag.py | row counts, duplicate fact keys, AS_OF, warehouse |
| Notification failure | airflow/dags/network_pipeline_dag.py | pipeline status JSON |

