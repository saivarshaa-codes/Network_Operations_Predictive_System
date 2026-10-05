from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from fastapi.testclient import TestClient

# ============================================================
# CONFIG
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / ".env")

from api.main import app

client = TestClient(app)

disabled_tools: set[str] = set()


# ============================================================
# HEADLESS NOC INVESTIGATION AGENT
# ============================================================

class HeadlessNOCInvestigator:
    """Autonomous headless agent implementing gather -> compare -> assess -> summarize."""

    def __init__(self, simulate_tool_failure: Optional[str] = None):
        self.disabled_tool = simulate_tool_failure

    def _call_tool(self, tool_name: str, fn) -> Any:
        if self.disabled_tool == tool_name:
            raise RuntimeError(f"Tool '{tool_name}' simulated hardware/network outage")
        return fn()

    def investigate(self, grid_id: int) -> Dict[str, Any]:
        evidence_list: List[Dict[str, Any]] = []
        uncertainties: List[str] = []
        recommended_checks: List[str] = []

        # ========================================================
        # STEP 1: GATHER — MANDATORY PRE-FLIGHT: PIPELINE STATUS
        # ========================================================
        pipeline_status = {}
        try:
            pipeline_status = self._call_tool(
                "pipeline_status",
                lambda: client.get("/pipeline/status").json()
            )
            evidence_list.append({
                "claim": "Warehouse ingestion and pipeline execution status",
                "value": pipeline_status.get("status"),
                "source_tool": "pipeline_status",
            })
            evidence_list.append({
                "claim": "Warehouse data freshness rating",
                "value": pipeline_status.get("freshness"),
                "source_tool": "pipeline_status",
            })
        except Exception as exc:
            uncertainties.append(f"CRITICAL: pipeline_status unavailable ({exc}). Underlying data trustworthiness cannot be verified.")

        pipeline_healthy = pipeline_status.get("healthy", False)
        freshness = pipeline_status.get("freshness", "UNKNOWN")
        if not pipeline_healthy or freshness in ["STALE", "VERY_STALE"]:
            reasons = pipeline_status.get("reasons", [])
            uncertainties.append(
                f"PIPELINE DEGRADATION: Data freshness is '{freshness}' with warnings: {'; '.join(reasons)}. "
                "Severity assessment is strictly constrained because telemetry lags current time."
            )

        # ========================================================
        # STEP 2: GATHER — TELEMETRY & ML EVIDENCE
        # ========================================================
        
        # Grid Location
        try:
            loc = self._call_tool(
                "grid_location",
                lambda: client.get(f"/network/grid/{grid_id}/location").json()
            )
            lat = loc.get("centroid_latitude")
            lon = loc.get("centroid_longitude")
            evidence_list.append({
                "claim": f"Grid {grid_id} geographic centroid in Milan",
                "value": f"Lat: {lat:.4f}, Lon: {lon:.4f}",
                "source_tool": "grid_location",
            })
        except Exception as exc:
            uncertainties.append(f"grid_location unavailable ({exc}). Centroid coordinates unknown.")

        # Grid Activity
        cur_act = None
        try:
            act = self._call_tool(
                "grid_activity",
                lambda: client.get(f"/network/grid/{grid_id}").json()
            )
            pts = act.get("data", [])
            if pts:
                cur_act = pts[-1].get("total_activity")
                evidence_list.append({
                    "claim": f"Grid {grid_id} current total activity measure (unitless proportional index)",
                    "value": round(cur_act, 2),
                    "source_tool": "grid_activity",
                })
        except Exception as exc:
            uncertainties.append(f"grid_activity unavailable ({exc}). Current interval activity measure unknown.")

        # Grid Features
        try:
            feat = self._call_tool(
                "grid_features",
                lambda: client.get(f"/network/grid/{grid_id}/features").json()
            )
            growth = feat.get("activity_growth")
            if growth is not None:
                evidence_list.append({
                    "claim": f"Grid {grid_id} 24h rolling activity growth ratio",
                    "value": round(float(growth), 4),
                    "source_tool": "grid_features",
                })
        except Exception as exc:
            uncertainties.append(f"grid_features unavailable ({exc}). Growth ratio unknown; no estimate substituted.")

        # Anomaly / Risk Score
        risk_score = None
        risk_level = "UNKNOWN"
        try:
            risk = self._call_tool(
                "anomaly_score",
                lambda: client.post("/network/predict-risk", json={"grid_id": grid_id}).json()
            )
            risk_score = risk.get("risk_score")
            risk_level = risk.get("risk_level", "NORMAL")
            evidence_list.append({
                "claim": f"Grid {grid_id} ML risk prediction score",
                "value": round(float(risk_score), 4),
                "source_tool": "anomaly_score",
            })
        except Exception as exc:
            uncertainties.append(f"anomaly_score unavailable ({exc}). ML risk score unavailable.")

        # ========================================================
        # STEP 3: COMPARE & ASSESS
        # ========================================================
        # Terminology Rule: High activity is NOT congestion
        uncertainties.append("Telemetry contains no physical capacity, latency, or throughput figures. Congestion cannot be claimed.")

        # Constrained severity determination:
        # If pipeline is unhealthy or critical tools failed, severity cannot be confirmed as HIGH
        if not pipeline_healthy or freshness in ["STALE", "VERY_STALE"]:
            severity = "ATTENTION"
        elif risk_score is not None and risk_score > 0.65:
            severity = "HIGH"
        elif risk_score is not None and risk_score > 0.35:
            severity = "ATTENTION"
        else:
            severity = "NORMAL"

        # ========================================================
        # STEP 4: SUMMARIZE & RECOMMEND CHECKS
        # ========================================================
        if not pipeline_healthy:
            recommended_checks.append("Verify Airflow DAG scheduler and run history to clear warehouse staleness.")
        recommended_checks.append(f"Inspect physical site alarms on transceivers servicing Milan grid cell {grid_id}.")
        recommended_checks.append(f"Check spatial neighbours using /network/grid/{grid_id}/neighbours to evaluate geographic spread.")

        return {
            "grid_id": grid_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "severity": severity,
            "evidence": evidence_list,
            "uncertainty": uncertainties,
            "recommended_checks": recommended_checks,
        }


# ============================================================
# MAIN CLI RUNNER
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Headless NOC Investigation Agent")
    parser.add_argument("--grid-id", type=int, default=4821, help="Grid ID to investigate")
    parser.add_argument("--disable-tool", type=str, default=None, help="Simulate failure of a specific tool")
    parser.add_argument("--output-json", type=str, default=None, help="File path to save JSON brief")
    args = parser.parse_args()

    agent = HeadlessNOCInvestigator(simulate_tool_failure=args.disable_tool)
    brief = agent.investigate(args.grid_id)

    print(json.dumps(brief, indent=2))

    if args.output_json:
        out_path = Path(args.output_json)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(brief, f, indent=2)
        print(f"\nBrief saved to: {out_path}")


if __name__ == "__main__":
    main()
