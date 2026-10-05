# Checkpoint & Safe Rollback Report: Anomaly Threshold Experiment (Phase 7 - C11)

**Experiment**: Lowering Anomaly Threshold to Flag ~2x Grids  
**Status**: **ROLLED BACK TO CHECKPOINT-001**  
**Decision Rationale**: Lowering the threshold doubled alert volume (+108%), degraded precision, inundated operators with noise, and diluted agreement with established operational alerts.

---

## 1. Quantitative Before-and-After Evaluation

| Metric | Baseline (Checkpoint-001) | Experimental (Modified Threshold) | Post-Rollback (Restored State) |
| :--- | :--- | :--- | :--- |
| **Anomaly Sensitivity Threshold** | $0.65$ | $0.35$ | $0.65$ |
| **Citywide Flagged Cells** | 12 cells | 25 cells (+108%) | 12 cells |
| **Agreement with NP3 Rule Alerts**| 83.3% | 48.0% (-35.3%) | 83.3% |
| **Top-20 Attention Composition** | High-growth urban cells | Diluted with rural background variance | Restored to core operational priorities |
| **NOC Fatigue Assessment** | Manageable queue | Operator fatigue / high false-positive rate | Restored |

---

## 2. Rollback Verification
- Snapshot `checkpoint-001.json` captured pre-experiment configuration and operational state.
- Automated rollback restored parameter values in the active runtime.
- System query confirmed that alert volume returned exactly to 12 cells.
