# Proposed Missing Tests and Documentation

This document records high-priority tests and operational documents identified during the repository audit for Phase 7 (C4).

---

## 1. Missing Core Tests

### A. Data Grain Invariance Test
- **Risk**: Pipeline bugs could emit duplicate `(grid_id, timestamp)` rows if country-code aggregation is bypassed or malformed.
- **Proposed Test**: `tests/test_grain_uniqueness.py`
  - Query `hourly_grid_summary` in `network_ops.db`.
  - Assert `COUNT(*) == COUNT(DISTINCT grid_id || '_' || timestamp)`.
  - Fail immediately if any grid cell has multiple records in the same hour.

### B. Feature Leakage (Temporal Causality) Test
- **Risk**: Engineered features in `ML2` could compute statistics using timestamps at or after `feature_timestamp`, corrupting operational prediction.
- **Proposed Test**: `tests/test_temporal_leakage.py`
  - Validate that for any feature vector with timestamp $T$, all underlying raw activity timestamps $t$ satisfy $t < T$.
  - Assert zero future telemetry is incorporated in rolling 24h baseline.

### C. Pipeline Unhealthy Status API Gating Test
- **Risk**: API endpoints could return confident responses when underlying data is corrupted or stale without signaling the operator.
- **Proposed Test**: `tests/test_pipeline_health_gating.py`
  - Mock `/pipeline/status` to return `healthy: false` and `freshness: VERY_STALE`.
  - Verify that downstream assistants and consumer clients receive explicit uncertainty markers.

### D. GeoJSON CellID Join Fidelity Test
- **Risk**: Joining on zero-based GeoJSON array index instead of `properties.cellId` leads to spatial offset of 1 grid cell across the entire city.
- **Proposed Test**: `tests/test_geojson_join.py`
  - Assert that `grid_location` coordinates for grid `4821` match the centroid calculated specifically from the GeoJSON feature whose `properties.cellId == 4821`.

---

## 2. Missing Operational Documentation

1. **`docs/data_contract.md`**: Formal specification of schema types, allowed value ranges (non-negative activity measures), and partition hierarchies.
2. **`docs/api_contracts.md`**: Comprehensive OpenAPI specification documenting query parameter formats and error responses.
3. **`docs/model_card.md`**: Documentation of ML3 classifier training parameters, class weights, precision/recall trade-offs, and ML4 anomaly score calculation.
