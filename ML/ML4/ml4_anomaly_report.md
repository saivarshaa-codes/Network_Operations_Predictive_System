# ML4 Anomaly Baseline Evaluation Report

## Overview

- Baseline: historical median by grid and hour of day.
- High anomaly threshold: +50% deviation.
- Low anomaly threshold: -50% deviation.
- Anomaly score: absolute percentage deviation.
- Direction: high, low, or normal.

## Historical Bucket Validation

- Grid/hour buckets: 240,000
- Minimum observations per bucket: 4
- Maximum observations per bucket: 7
- Average observations per bucket: 7.00

## Three-Way Comparison

| Mechanism | Flagged Count |
|---|---:|
| NP3 | 13,540 |
| ML3 | 17,411 |
| ML4 high anomaly | 34,793 |
| All three | 0 |
| NP3 + ML3 only | 0 |
| NP3 + ML4 only | 2,202 |
| ML3 + ML4 only | 1,240 |
| NP3 only | 11,338 |
| ML3 only | 16,171 |
| ML4 only | 31,351 |
| None | 1,617,692 |

## Agreement

- ML3 vs ML4 agreement: 0.9704
- NP3 vs ML4 agreement: 0.9739

## Disagreement Interpretation

- NP3-only cases indicate activity that satisfies the current-day high-activity rule but is not identified as an ML3 future risk or ML4 historical high anomaly.
- ML3-only cases indicate future high-activity risk without a current NP3 alert or current ML4 high historical anomaly.
- ML4-only cases indicate current activity that is unusual for the grid's historical hour-of-day behaviour without a current NP3 alert or ML3 future-risk prediction.

## Operational Interpretation

NP3 identifies current elevated activity using its rule-based baseline.

ML3 predicts future elevated activity using recent activity behaviour available through the prediction timestamp.

ML4 identifies current activity that deviates materially from the historical hour-of-day behaviour of the same grid.

The three mechanisms answer different operational questions, so disagreement is informative rather than automatically a defect.

A positive signal is an operational attention signal for investigation. It is not evidence of a confirmed network fault.

## Output Files

- Anomaly scores: `/mnt/d/Network Operations Predictive System/ML/ML4/network_anomaly_scores.csv`
- Three-way comparison: `/mnt/d/Network Operations Predictive System/ML/ML4/ml4_three_way_comparison.csv`