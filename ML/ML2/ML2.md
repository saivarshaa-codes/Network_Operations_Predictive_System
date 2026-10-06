# ML2 — Engineer Network Activity Features

## 1. Objective

Create a small, understandable feature set from the accumulated network activity history to support prediction of whether a grid will meet the defined elevated-activity condition in the next hourly interval.

Features must be computed strictly from information available up to the feature timestamp.

---

## 2. Feature Window Convention

For a prediction about `t+1`, every feature is computed using only the trailing window ending at `t`, inclusive of `t`, and nothing after `t`.

Therefore:

* `feature_timestamp = t`
* Feature data must satisfy `timestamp <= feature_timestamp`
* The future interval `t+1` is never used as a feature
* The target for the prediction is evaluated separately at `t+1`

### Window Design

The recent feature window is the trailing 24 hours:

`t-23` through `t`

The prior comparison window is the preceding 24 hours:

`t-47` through `t-24`

This provides a complete daily cycle for the recent behaviour and an immediately preceding 24-hour period for comparison.

---

## 3. Feature Definitions

### 3.1 `avg_activity`

Mean `total_activity` across the recent 24-hour window.

Purpose: captures the recent activity level of the grid.

---

### 3.2 `activity_growth`

Measures the change between the recent 24-hour activity level and the preceding 24-hour baseline.

Formula:

`(recent_mean - prior_mean) / prior_mean`

If the prior mean is zero, the feature is set to `0.0` to prevent undefined or infinite values.

Purpose: captures whether activity is increasing or decreasing relative to the preceding period.

---

### 3.3 `active_hours`

Count of hours in the recent 24-hour window where:

`total_activity > 0`

Purpose: captures how consistently active the grid has been.

Zero-activity grids are preserved rather than removed.

---

### 3.4 `peak_ratio`

Ratio of the maximum recent activity to the recent average activity.

Formula:

`max_activity / avg_activity`

If the recent average is zero, the feature is set to `0.0`.

Purpose: captures peakiness and whether activity has produced a large peak relative to its normal recent level.

---

### 3.5 `variability`

Standard deviation of `total_activity` across the recent 24-hour window.

Purpose: captures how much activity fluctuates during the recent period.

---

### 3.6 `internet_share`

Ratio of internet activity to total activity over the recent 24-hour window.

Formula:

`sum(internet_activity) / sum(total_activity)`

If total activity is zero, the feature is set to `0.0`.

Purpose: captures the composition of activity represented by internet activity.

---

## 4. Output Table

The engineered features are persisted as:

`network_feature_table`

Required grain:

`grid_id + feature_timestamp`

There must be exactly one feature row for each grid and feature timestamp.

Required feature columns:

* `grid_id`
* `feature_timestamp`
* `avg_activity`
* `activity_growth`
* `active_hours`
* `peak_ratio`
* `variability`
* `internet_share`

---

## 5. Data Source

The feature pipeline uses the warehouse activity data:

`fact_network_activity`

with dimensions resolved through:

* `dim_time`
* `dim_grid`

The warehouse currently contains:

* 10,000 grids
* 168 hourly intervals
* 7 days of accumulated history
* zero duplicate `(grid_key, time_key)` records

The Trainer Guide recommends 14 days of accumulated history. The current implementation proceeds with the available 7-day history and does not fabricate additional historical data.

---

## 6. Leakage Prevention

Leakage prevention is treated as a testable property.

For every feature row:

`all source timestamps <= feature_timestamp`

No feature may use information from:

`feature_timestamp + 1`

or any later timestamp.

The target at `t+1` is used only as the future prediction label and must never enter the feature calculation.

---

## 7. Required Validation

The ML2 implementation must verify:

* Exact required feature names exist.
* `feature_timestamp` represents `t`.
* No duplicate `(grid_id, feature_timestamp)` rows exist.
* No required feature contains NULL values.
* No feature contains NaN or infinity.
* Zero-activity grids are handled safely.
* No feature uses future timestamps.
* Manual feature calculations can be reproduced for a sample grid such as Grid `4821`.

---

## 8. Downstream Use

The resulting `network_feature_table` is the canonical feature output for the predictive pipeline.

It will support:

`network_feature_table → ML3 model training`

and will later support:

`network_feature_table → API4`

The API must consume stored features rather than independently recomputing the feature definitions.

---

## 9. Operational Interpretation

These features describe recent grid behaviour.

They do not directly represent:

* network capacity
* throughput
* latency
* packet loss
* congestion
* confirmed faults
* service failures

A positive predictive result remains an **operational attention signal** indicating that the grid may warrant investigation.
