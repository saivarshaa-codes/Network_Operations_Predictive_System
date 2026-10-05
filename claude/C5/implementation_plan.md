# Implementation Plan: Surging Grids against Baseline (Phase 7 - C5)

**Feature**: Top grids whose activity increased most sharply against their baseline in the current reporting window.  
**Mode**: Explore → Plan → Approve → Execute  
**Architectural Decision**: Additive API enhancement reusing ML2 / Phase 1 baseline logic.

---

## 1. Architectural Exploration & Layer Allocation

### Why Compute in the API Layer (with SQLite index backing)?
- **Options Evaluated**:
  1. *Spark Pipeline*: Batch computes once per scheduled run; too heavy for interactive UI drill-downs with variable thresholds.
  2. *Frontend Client-Side*: Forces fetching thousands of grid timeseries to compute growth in browser memory (violates context & payload rules).
  3. *API / Warehouse Query (Recommended)*: Computes surging grids dynamically over the pre-aggregated `hourly_grid_summary` table in SQLite, reusing existing baseline algorithms.

---

## 2. Baseline Logic Reuse
- In `Phase1/usage_processor.py` and `ML/ML2/feature_engineering.py`, baseline activity is calculated over a rolling 24-hour window excluding the current interval:
  $$\text{baseline} = \frac{1}{N} \sum_{t=T-24}^{T-1} \text{total\_activity}_t$$
  $$\text{growth} = \frac{\text{current\_activity} - \text{baseline}}{\max(\text{baseline}, 1.0)}$$
- We reuse this exact formulation rather than introducing an inconsistent third formula.

---

## 3. Additive API Contract

**Endpoint**: `GET /network/surging-grids`  
**Query Parameters**:
- `limit: int = 10` (Default 10, max 100)
- `min_baseline: float = 50.0` (Filters out quiet rural cells where minor noise creates misleading % surges)
- `as_of: str | None = None` (Reporting window end)

**Response Shape**:
```json
{
  "as_of": "2013-11-07T23:00:00",
  "surging_grids": [
    {
      "grid_id": 4821,
      "current_activity": 254.97,
      "baseline_activity": 182.10,
      "growth_ratio": 0.40,
      "centroid_lat": 45.4595,
      "centroid_lon": 9.0728,
      "rank": 1
    }
  ]
}
```

---

## 4. UI Placement & Non-Breaking Verification
- Belongs on the **React Overview Page** (`frontend/src/App.jsx`) as an operational alert card: *"Top Surging Grids against Baseline"*.
- **Backwards Compatibility**: Additive endpoint does not touch `/network/summary` or `/network/hotspots`, guaranteeing existing consumers continue uninterrupted.
