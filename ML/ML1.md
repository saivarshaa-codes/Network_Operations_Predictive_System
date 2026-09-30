# ML1 — Operational ML Problem Definition

## 1. Primary ML Problem

### Future High-Activity Risk

The model predicts whether a grid is likely to exhibit **elevated activity in the next hourly interval**, based only on activity behaviour available up to the current time.

The model is intended to provide an operational risk signal that helps a NOC engineer decide which grids should be investigated.

The model does **not** claim that elevated activity represents confirmed network congestion or a network fault.

---

## 2. Prediction Unit

The prediction unit is:

> **One `grid_id` at one hourly prediction interval.**

For a feature timestamp `t`, the model predicts the elevated-activity condition for the next hourly interval, `t+1`.

Therefore:

```text
feature_timestamp = t
target interval   = t+1
```

---

## 3. Feature / Target Boundary

The prediction problem follows a strict temporal boundary.

### Features

Features may use information available through time `t`, inclusive.

```text
Features ≤ t
```

No feature may use information from any timestamp after `t`.

### Target

The target describes the future hourly interval:

```text
Target = condition at t+1
```

Therefore:

```text
                    Prediction Time
                          ↓
t-23 ──────────────── t ─────── t+1
<---- feature history ---->     <target>
```

Information from `t+1` is used only to construct the training label. It must never be provided as a model feature.

---

## 4. Target Definition

The target represents whether the next hourly interval exhibits elevated activity relative to the grid's recent activity baseline.

For each `grid_id` and feature timestamp `t`:

### Step 1 — Calculate the prior 24-hour baseline

Use the trailing 24 hourly observations ending at `t`:

```text
t-23, t-22, ..., t-1, t
```

The baseline is the **median total activity** across these 24 hours.

```text
prior_24h_baseline(t)
    = median(total_activity from t-23 through t)
```

### Step 2 — Observe the future activity

The actual activity at the next hourly interval is:

```text
total_activity(t+1)
```

### Step 3 — Assign the target

```text
target = 1
```

when:

```text
total_activity(t+1)
>
1.5 × prior_24h_baseline(t)
```

Otherwise:

```text
target = 0
```

The `1.5×` multiplier defines the elevated-activity threshold used for the training target.

---

## 5. Label Type

The target is a **training proxy label** based on elevated activity behaviour.

The dataset does not contain verified labels for:

* network faults
* network congestion
* capacity breaches
* throughput degradation
* packet loss
* latency problems
* service failures

Therefore, a positive target means:

> **The grid exhibited elevated activity in the future hourly interval according to the defined training threshold.**

It does not mean that a network fault or congestion actually occurred.

---

## 6. Business Action

The model's prediction supports operational prioritization.

When the model predicts elevated future activity risk, the recommended business action is:

> **Investigate the grid.**

The model helps prioritize where operational attention may be useful.

It does not determine the cause of the activity and does not independently confirm a network problem.

---

## 7. Explicit Non-Goals

The model is **not** intended to claim or predict:

* confirmed network congestion
* capacity exceeded
* insufficient network capacity
* throughput degradation
* packet loss
* latency problems
* confirmed network faults
* service failure

The project does not contain the network-capacity or service-quality measurements required to make these claims.

---

## 8. Leakage Protection

Leakage prevention is a core requirement of the ML problem definition.

### Temporal feature boundary

All model features must be constructed using information available at or before `t`.

```text
FEATURES ≤ t
```

No feature may contain information from:

```text
t+1 or later
```

### Future target boundary

The target is determined using the actual activity at `t+1`.

```text
TARGET = activity at t+1
```

This future value is used only during label construction.

It must never appear in the feature set.

### Training and evaluation boundary

Final model evaluation must preserve chronological order.

Training observations must precede test observations in time.

Random train/test mixing must not be used for the final evaluation because it can allow information from later periods to influence model development or produce an unrealistically optimistic estimate of future performance.

---

## 9. Prediction Timeline

The complete prediction problem can be represented as:

```text
Historical observations

t-23  t-22  ...  t-2  t-1   t
  |     |          |    |    |
  └──────── 24-hour history ─┘
                  |
                  ↓
             Build features
                  |
                  ↓
              ML Model
                  |
                  ↓
       Predict elevated activity
              at t+1
                  |
                  ↓
            NOC Action
            INVESTIGATE
```

The actual `t+1` activity is only known after the prediction time and is therefore used retrospectively to create the training target.

---

## 10. Relationship to NP3

NP3 provides a transparent rule-based signal for elevated activity at the current interval.

The ML phase extends this concept into a forward-looking prediction.

### NP3

> **Is activity high now?**

### ML

> **Is activity likely to be high next hour?**

Therefore, the ML model complements rather than replaces the existing rule-based alert layer.

NP3 provides a transparent current-state signal, while ML provides a predictive risk signal for the next hourly interval.

---

## 11. Feature and Target Sheet

| Component                   | Definition                                                       |
| --------------------------- | ---------------------------------------------------------------- |
| Prediction unit             | `grid_id` + hourly interval                                      |
| Feature timestamp           | `t`                                                              |
| Prediction horizon          | Next hourly interval                                             |
| Target interval             | `t+1`                                                            |
| Feature boundary            | Information available through `t`                                |
| Historical baseline         | Trailing 24 hours: `t-23` through `t`                            |
| Baseline statistic          | Median `total_activity`                                          |
| Elevated-activity threshold | `1.5 × prior_24h_baseline(t)`                                    |
| Positive target             | `total_activity(t+1) > 1.5 × baseline(t)`                        |
| Negative target             | `total_activity(t+1) ≤ 1.5 × baseline(t)`                        |
| Label type                  | Training proxy                                                   |
| Business action             | Investigate                                                      |
| Forbidden claim             | Confirmed congestion, capacity breach, fault, or service failure |
| Final evaluation            | Chronological / time-ordered                                     |

---

## 12. Final One-Line Problem Statement

> **Using recent hourly activity behaviour available through time `t`, predict whether a grid will meet the defined elevated-activity condition at `t+1`, so that a NOC engineer can prioritize the grid for investigation.**
