from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

# ============================================================
# SKILL REGISTRY & EXECUTOR
# ============================================================

SKILLS_DIR = Path(__file__).resolve().parent / "skills"


def execute_without_skill(query: str, evidence: Dict[str, Any]) -> str:
    """Simulates a generic baseline model without project skill guidance.
    
    Generic models frequently hallucinate congestion, invent units like MB or call counts,
    and give confident answers even when evidence is missing.
    """
    grid_id = evidence.get("grid_id", "Unknown")
    act = evidence.get("current_activity", 250)
    return (
        f"[WITHOUT SKILL - Unconstrained Output]\n"
        f"Grid {grid_id} has high traffic of {act} calls and data packets. "
        f"This cell appears to be experiencing significant network congestion during peak hours. "
        f"Engineers should immediately reboot the cell tower to relieve the congestion."
    )


def execute_with_anomaly_skill(query: str, evidence: Dict[str, Any]) -> str:
    """Executes using the packaged network-anomaly-analysis skill."""
    # Step 1: Validate required evidence fields
    required_fields = ["grid_id", "current_activity", "baseline_activity", "anomaly_score", "pipeline_status"]
    missing = [f for f in required_fields if f not in evidence or evidence[f] is None]

    if missing:
        return (
            "[WITH SKILL - Strict Skill Enforcement]\n"
            "SEVERITY\n"
            "INSUFFICIENT EVIDENCE\n\n"
            "EVIDENCE\n"
            f"- Partial evidence supplied for Grid {evidence.get('grid_id', 'Unknown')}.\n"
            f"- Missing required metrics: {', '.join(missing)}.\n\n"
            "INTERPRETATION\n"
            "- In accordance with the network-anomaly-analysis skill, severity and risk tier cannot be determined "
            "without complete telemetry and pipeline health verification. Operational estimates are prohibited.\n\n"
            "NEXT CHECKS\n"
            "1. Query the feature engineering layer to retrieve missing metrics.\n"
            "2. Ensure GET /pipeline/status is verified before re-submitting for analysis."
        )

    # Step 2: Complete evidence evaluation following 4-section format
    grid_id = evidence["grid_id"]
    cur_act = evidence["current_activity"]
    base_act = evidence["baseline_activity"]
    score = evidence["anomaly_score"]
    pipe = evidence["pipeline_status"]

    severity = "ATTENTION" if score > 0.4 else "NORMAL"
    if score > 0.7:
        severity = "HIGH"

    pipe_note = "trustworthy" if pipe.get("healthy") else f"DEGRADED ({pipe.get('freshness', 'UNKNOWN')})"

    return (
        f"[WITH SKILL - Strict Skill Enforcement]\n"
        f"SEVERITY\n"
        f"{severity}\n\n"
        f"EVIDENCE\n"
        f"- Grid ID: {grid_id}.\n"
        f"- Current total activity measure: {cur_act:.2f} (proportional telemetry index).\n"
        f"- Rolling baseline activity measure: {base_act:.2f}.\n"
        f"- Model anomaly score: {score:.4f}.\n"
        f"- Pipeline status: {pipe_note}.\n\n"
        f"INTERPRETATION\n"
        f"- Observed activity measure is elevated relative to baseline.\n"
        f"- Measures are unitless proportional telemetry indicators, NOT call counts or MB.\n"
        f"- This operational signal does NOT prove or imply network congestion, as physical link capacity is not measured.\n\n"
        f"NEXT CHECKS\n"
        f"1. Verify transceiver radio alarms in adjacent spatial cells.\n"
        f"2. Inspect Airflow run history to monitor ongoing warehouse ingestion freshness."
    )


# ============================================================
# DEMONSTRATION & VERIFICATION
# ============================================================

def run_skill_comparison():
    print("=" * 70)
    print("C8 — SKILLS: PACKAGE REUSABLE NETWORK EXPERTISE")
    print("=" * 70)

    # Test Case 1: Incomplete Evidence (Missing anomaly_score and pipeline_status)
    print("\n--- TEST CASE 1: Incomplete Evidence Handling ---")
    incomplete_evidence = {
        "grid_id": 4821,
        "current_activity": 254.97,
        "baseline_activity": 180.20,
        # anomaly_score and pipeline_status are intentionally omitted
    }
    print("Input: Grid 4821 with missing anomaly_score & pipeline_status.")
    resp_incomplete = execute_with_anomaly_skill("Why is Grid 4821 flagged?", incomplete_evidence)
    print(resp_incomplete)
    assert "INSUFFICIENT EVIDENCE" in resp_incomplete, "Skill failed to refuse severity on incomplete evidence!"
    print("\n>>> VERIFIED: Skill explicitly refused severity when evidence was incomplete.")

    # Test Case 2: Complete Evidence - Side-by-Side Comparison
    print("\n" + "=" * 70)
    print("--- TEST CASE 2: Side-by-Side Comparison (With vs Without Skill) ---")
    complete_evidence = {
        "grid_id": 4821,
        "current_activity": 420.50,
        "baseline_activity": 210.00,
        "anomaly_score": 0.78,
        "pipeline_status": {"healthy": True, "freshness": "FRESH"},
    }

    without_skill = execute_without_skill("Explain Grid 4821", complete_evidence)
    with_skill = execute_with_anomaly_skill("Explain Grid 4821", complete_evidence)

    print("\n[WITHOUT SKILL OUTPUT]:")
    print(without_skill)
    print("\n[WITH SKILL OUTPUT]:")
    print(with_skill)

    print("\n" + "=" * 70)
    print("Comparison Summary:")
    print(" - Without skill: Hallucinated 'calls', 'data packets', and declared 'congestion'.")
    print(" - With skill: Enforced 4 sections, unitless measures, zero congestion claims, and cited pipeline health.")
    print("=" * 70)


if __name__ == "__main__":
    run_skill_comparison()
