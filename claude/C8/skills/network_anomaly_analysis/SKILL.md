---
name: network-anomaly-analysis
description: Analyzes why a telecommunications grid cell has been flagged, interpreting anomaly scores and activity trends.
---

# Network Anomaly Analysis Skill

## When to Activate
Activate when an engineer asks:
- "Why is grid [ID] flagged?"
- "What does the anomaly score for grid [ID] mean?"
- "Is this pattern abnormal?"

## Required Evidence
Before proceeding, verify the prompt or context contains:
1. `grid_id`
2. `current_activity` (proportional measure)
3. `baseline_activity`
4. `anomaly_score` or `risk_score`
5. `pipeline_status` (freshness and health)

## Non-Negotiable Instructions
1. **Refusal on Incomplete Evidence**: If ANY required evidence field is missing, you MUST NOT guess or assign a severity. State:
   `SEVERITY: INSUFFICIENT EVIDENCE. Missing required metrics: [List missing fields]. Cannot determine operational severity without full telemetry.`
2. **Four-Section Response Format**:
   - `SEVERITY`: One of NORMAL / ATTENTION / HIGH / INSUFFICIENT EVIDENCE.
   - `EVIDENCE`: Strictly factual metrics provided in input, including numbers.
   - `INTERPRETATION`: What the metrics might mean, clearly marked as inference.
   - `NEXT CHECKS`: Specific actionable checks for NOC human engineers.
3. **Terminology Constraints**:
   - Activity measures are proportional indices, NEVER call counts, message counts, or MB.
   - NEVER claim or state network congestion.
   - A grid is a geographic spatial square, not a cell tower.
