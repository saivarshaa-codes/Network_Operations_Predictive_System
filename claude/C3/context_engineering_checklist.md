# Telecom Context-Engineering Checklist

This checklist defines the operational rules for compiling LLM context packages for Network Operations Center (NOC) investigations.

---

## 1. Context Selection & Granularity
- [ ] **No Raw Row Dumping**: Never inject raw event streams or unaggregated timeseries into LLM prompts. Large volumes dilute attention and inflate token costs.
- [ ] **Aggregated Metrics Only**: Provide pre-computed summaries (mean, min, max, std dev, spike frequencies) computed by Spark/SQL.
- [ ] **Canonical Grain Respect**: Ensure evidence conforms to the canonical grain (one grid cell per hourly timestamp after country-code aggregation).
- [ ] **Geographic Bounding**: When spatial context is needed, restrict neighbouring grid cells to immediate centroids (using `properties.cellId`).

---

## 2. Pipeline Health & Trustworthiness Verification
- [ ] **Pre-Flight Pipeline Check**: Every operational prompt must query `GET /pipeline/status` before asserting conclusions.
- [ ] **Staleness Gating**: If analytics data is `STALE` or `VERY_STALE` relative to `AS_OF`, explicitly record this in `UNCERTAINTY`.
- [ ] **Rejection Accounting**: Note any quarantined or rejected CSV rows in the ingestion audit logs.
- [ ] **Null Handling Transparency**: Surface any imputation or zero-filling applied during data cleaning.

---

## 3. Strict Terminology & Domain Boundaries
- [ ] **Activity Measures**: Activity values are unitless proportional telemetry measures—never call counts, SMS counts, or MB.
- [ ] **No Congestion Claims**: Never claim or imply "network congestion" without capacity, throughput, or latency telemetry.
- [ ] **Attention Signal vs Fault**: Treat ML risk scores and anomaly scores as priority signals for human attention, not confirmed faults.
- [ ] **Explicit Insufficiency**: When required telemetry fields are missing, state that evidence is insufficient rather than estimating.

---

## 4. Response Structuring
- [ ] **Required Structure**: Follow the standardized 3-section format:
  1. `CURRENT EVIDENCE`: Verifiable metrics for the evaluated interval.
  2. `HISTORICAL EVIDENCE`: Historical frequency, baseline comparisons, and past spikes.
  3. `UNCERTAINTY`: Explicit missing data, pipeline limitations, and bounds of inference.
