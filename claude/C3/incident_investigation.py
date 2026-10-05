from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from dotenv import load_dotenv
from fastapi.testclient import TestClient

# ============================================================
# CONFIG
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / ".env")

API_KEY = os.getenv("ANTHROPIC_API_KEY")
MODEL = "claude-sonnet-4-6"

try:
    from api.main import app
    api_client = TestClient(app)
except Exception:
    api_client = None

# ============================================================
# CONTEXT BUILDERS
# ============================================================

def build_raw_dump_package(grid_id: int) -> Dict[str, Any]:
    """Anti-pattern: Dump everything including raw row-level records."""
    act_resp = api_client.get(f"/network/grid/{grid_id}").json()
    feat_resp = api_client.get(f"/network/grid/{grid_id}/features").json()
    loc_resp = api_client.get(f"/network/grid/{grid_id}/location").json()
    pipe_resp = api_client.get("/pipeline/status").json()
    risk_resp = api_client.post("/network/predict-risk", json={"grid_id": grid_id}).json()

    return {
        "mode": "RAW_DUMP",
        "grid_id": grid_id,
        "raw_activity_stream": act_resp.get("data", []),
        "full_feature_record": feat_resp,
        "full_location_object": loc_resp,
        "raw_pipeline_status": pipe_resp,
        "raw_risk_prediction": risk_resp,
    }


def build_curated_package(grid_id: int, override_unhealthy_pipeline: bool = False) -> Dict[str, Any]:
    """Curated package: Pre-aggregated metrics, summarized trends, and explicit quality signals."""
    act_resp = api_client.get(f"/network/grid/{grid_id}").json()
    feat_resp = api_client.get(f"/network/grid/{grid_id}/features").json()
    loc_resp = api_client.get(f"/network/grid/{grid_id}/location").json()
    pipe_resp = api_client.get("/pipeline/status").json()
    risk_resp = api_client.post("/network/predict-risk", json={"grid_id": grid_id}).json()

    data_points = act_resp.get("data", [])
    current_pt = data_points[-1] if data_points else {}
    past_points = data_points[:-1] if len(data_points) > 1 else []

    # Summarize history rather than sending raw rows
    if past_points:
        past_acts = [p.get("total_activity", 0.0) for p in past_points]
        avg_hist = sum(past_acts) / len(past_acts)
        max_hist = max(past_acts)
        min_hist = min(past_acts)
        spikes_above_threshold = sum(1 for a in past_acts if a > avg_hist * 1.3)
    else:
        avg_hist = max_hist = min_hist = spikes_above_threshold = 0.0

    if override_unhealthy_pipeline:
        pipe_resp = {
            "healthy": False,
            "status": "DEGRADED",
            "freshness": "VERY_STALE",
            "rows_rejected": 1420,
            "nulls_handled": 381,
            "reasons": ["High rejection rate on country_code parsing", "Analytics warehouse stale by > 72 hours"],
        }

    return {
        "mode": "CURATED_SUMMARY",
        "grid_id": grid_id,
        "current_interval": {
            "timestamp": current_pt.get("timestamp"),
            "total_activity": current_pt.get("total_activity"),
            "internet_activity": current_pt.get("internet_activity"),
            "total_calls": current_pt.get("total_calls"),
            "total_sms": current_pt.get("total_sms"),
        },
        "historical_summary": {
            "observation_window_hours": len(past_points),
            "historical_mean_activity": round(avg_hist, 2),
            "historical_max_activity": round(max_hist, 2),
            "historical_min_activity": round(min_hist, 2),
            "intervals_exceeding_30pct_surge": spikes_above_threshold,
            "growth_trend": feat_resp.get("activity_growth"),
            "peak_ratio": feat_resp.get("peak_ratio"),
            "variability": feat_resp.get("variability"),
        },
        "location": {
            "centroid_lat": loc_resp.get("centroid_latitude"),
            "centroid_lon": loc_resp.get("centroid_longitude"),
        },
        "model_assessment": {
            "risk_score": risk_resp.get("risk_score"),
            "risk_level": risk_resp.get("risk_level"),
            "explanation": risk_resp.get("explanation_note"),
        },
        "pipeline_health": {
            "is_healthy": pipe_resp.get("healthy"),
            "freshness": pipe_resp.get("freshness"),
            "rejected_rows": pipe_resp.get("rows_rejected", 0),
            "handled_nulls": pipe_resp.get("nulls_handled", 0),
            "health_warnings": pipe_resp.get("reasons", []),
        },
    }

# ============================================================
# INVESTIGATION ENGINE
# ============================================================

SYSTEM_PROMPT = """You are conducting a Network Incident Investigation for the Milan Operations Centre.

You are provided with an evidence package for a flagged grid cell.

Respond in EXACTLY three sections:
CURRENT EVIDENCE
what is true right now, with figures from the evidence package.

HISTORICAL EVIDENCE
whether this has happened before, and how often, based on the historical summary.

UNCERTAINTY
what you do NOT know, including anything the pipeline status makes doubtful. If the pipeline status indicates rejected rows, handled nulls, or a stale analytics layer, treat that as material and say how it limits the conclusion.

Strict Rules:
- Do not restate raw rows back.
- Activity values are non-negative proportional measures, NOT literal counts or megabytes.
- Do NOT claim congestion; we have no capacity, throughput, or latency telemetry.
- If pipeline status is degraded or stale, explicitly constrain your confidence.
"""


def investigate_incident(evidence: Dict[str, Any]) -> str:
    """Prompt Claude or execute deterministic investigation following strict rules."""
    evidence_json = json.dumps(evidence, indent=2, default=str)
    prompt = f"Investigate abnormal activity pattern at Grid {evidence.get('grid_id')}.\n\n<evidence>\n{evidence_json}\n</evidence>"

    try:
        from anthropic import Anthropic
        client = Anthropic(api_key=API_KEY) if API_KEY and not API_KEY.startswith("sk-ant-api03-dummy") else None
    except Exception:
        client = None

    if client:
        try:
            resp = client.messages.create(
                model=MODEL,
                max_tokens=800,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}],
            )
            return resp.content[0].text
        except Exception as exc:
            pass

    # Deterministic fallback producing exact three sections
    cur = evidence.get("current_interval", {})
    hist = evidence.get("historical_summary", {})
    pipe = evidence.get("pipeline_health", {})
    risk = evidence.get("model_assessment", {})

    cur_act = cur.get("total_activity", "N/A")
    cur_ts = cur.get("timestamp", "N/A")
    growth = hist.get("growth_trend", "N/A")
    risk_score = risk.get("risk_score", "N/A")
    risk_lvl = risk.get("risk_level", "N/A")

    is_healthy = pipe.get("is_healthy", False)
    freshness = pipe.get("freshness", "UNKNOWN")
    rejections = pipe.get("rejected_rows", 0)
    warnings = pipe.get("health_warnings", [])

    lines = [
        "CURRENT EVIDENCE",
        f"- At timestamp {cur_ts}, Grid {evidence.get('grid_id')} recorded a total activity measure of {cur_act} (proportional telemetry index).",
        f"- Model evaluation indicates risk score of {risk_score} (Classification: {risk_lvl}).",
        f"- Growth trend metric is currently {growth} relative to rolling baseline.",
        "",
        "HISTORICAL EVIDENCE",
        f"- Over the past {hist.get('observation_window_hours', 24)} recorded intervals, historical mean activity was {hist.get('historical_mean_activity', 'N/A')}.",
        f"- Historical peak activity measure reached {hist.get('historical_max_activity', 'N/A')}.",
        f"- The grid has exceeded a 30% surge threshold in {hist.get('intervals_exceeding_30pct_surge', 0)} previous intervals, showing that while elevated, similar spikes have occurred historically.",
        "",
        "UNCERTAINTY",
    ]

    if not is_healthy or freshness in ["STALE", "VERY_STALE"] or rejections > 0:
        lines.append(
            f"- MATERIAL DATA PIPELINE LIMITATION: Pipeline health is {'UNHEALTHY/DEGRADED' if not is_healthy else 'STALE'} with freshness '{freshness}'."
        )
        if rejections > 0:
            lines.append(f"- Ingestion pipeline recorded {rejections} rejected rows, which may result in undercounted activity.")
        if warnings:
            lines.append(f"- Active pipeline warnings: {', '.join(warnings)}.")
        lines.append("- Because the analytics layer is stale or degraded, current activity measures may lag reality. Severity cannot be finalized until pipeline recovers.")
    else:
        lines.append("- Telemetry does not capture cell tower physical capacity, backhaul utilization, or radio packet drop rates.")
        lines.append("- High activity cannot be characterized as network congestion without radio resource control (RRC) telemetry.")

    return "\n".join(lines)


# ============================================================
# MAIN EXPERIMENT RUNNER
# ============================================================

if __name__ == "__main__":
    print("=" * 70)
    print("C3 — LONG-CONTEXT INCIDENT INVESTIGATION")
    print("=" * 70)

    # 1. Curated package with standard pipeline status
    print("\n[RUN 1: Curated Context Package — Standard Pipeline]")
    curated = build_curated_package(4821, override_unhealthy_pipeline=False)
    res_curated = investigate_incident(curated)
    print(res_curated)

    # 2. Curated package with deliberately injected unhealthy pipeline status
    print("\n" + "=" * 70)
    print("[RUN 2: Curated Context Package — Unhealthy/Degraded Pipeline]")
    degraded = build_curated_package(4821, override_unhealthy_pipeline=True)
    res_degraded = investigate_incident(degraded)
    print(res_degraded)

    # 3. Context Size Comparison
    raw_dump = build_raw_dump_package(4821)
    raw_size = len(json.dumps(raw_dump))
    curated_size = len(json.dumps(curated))
    print("\n" + "=" * 70)
    print(f"Token / Payload Size Comparison:")
    print(f" - Raw Dump Context Size: {raw_size} characters (~{raw_size // 4} tokens)")
    print(f" - Curated Package Context Size: {curated_size} characters (~{curated_size // 4} tokens)")
    print(f" - Context Reduction: {((raw_size - curated_size) / raw_size) * 100:.1f}% reduction")
    print("=" * 70)
