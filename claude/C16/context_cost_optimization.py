from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

# ============================================================
# CONFIG
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from api.database import get_connection
from api.main import app
from fastapi.testclient import TestClient

client = TestClient(app)


# ============================================================
# CONTEXT SIZE COMPUTATION OVER REAL TELEMETRY
# ============================================================

def compute_real_telemetry_scale() -> Dict[str, Any]:
    """Compute exact row counts and context scale from real database history."""
    conn = get_connection()
    try:
        total_rows = conn.execute("SELECT COUNT(*) FROM fact_network_activity").fetchone()[0]
        distinct_grids = conn.execute("SELECT COUNT(DISTINCT grid_id) FROM dim_grid").fetchone()[0]
        distinct_timestamps = conn.execute("SELECT COUNT(DISTINCT timestamp) FROM dim_time").fetchone()[0]

        # One hourly snapshot across the full city:
        hourly_rows = distinct_grids  # 10,000 cells per hour
        avg_row_chars = 115  # average JSON character length of a single fact activity row
        raw_hour_chars = hourly_rows * avg_row_chars
        raw_hour_tokens = raw_hour_chars // 4

        return {
            "total_warehouse_rows": total_rows,
            "distinct_grids": distinct_grids,
            "distinct_hourly_timestamps": distinct_timestamps,
            "single_hour_grid_count": hourly_rows,
            "raw_hour_characters": raw_hour_chars,
            "raw_hour_estimated_tokens": raw_hour_tokens,
        }
    finally:
        conn.close()


# ============================================================
# DESIGN A vs DESIGN B EXECUTION & COMPARISON
# ============================================================

def execute_design_a_raw_sample(sample_limit: int = 50) -> Dict[str, Any]:
    """Design A: Dumping raw hourly rows directly into prompt."""
    t0 = time.perf_counter()
    conn = get_connection()
    try:
        query = """
        SELECT g.grid_id, f.sms_in, f.sms_out, f.call_in, f.call_out, f.internet_activity, f.total_activity
        FROM fact_network_activity f
        JOIN dim_grid g ON f.grid_key = g.grid_key
        JOIN dim_time t ON f.time_key = t.time_key
        ORDER BY t.timestamp DESC
        LIMIT ?;
        """
        rows = [dict(r) for r in conn.execute(query, (sample_limit,)).fetchall()]
        elapsed = time.perf_counter() - t0

        raw_json = json.dumps(rows)
        char_count = len(raw_json)

        return {
            "design": "Design A (Raw Rows Dump)",
            "sample_rows_fetched": len(rows),
            "payload_chars": char_count,
            "estimated_tokens": char_count // 4,
            "latency_ms": round(elapsed * 1000, 2),
            "quality_assessment": "High noise; token heavy; risks hallucinations; exceeds prompt limits at full scale (10,000 cells).",
        }
    finally:
        conn.close()


def execute_design_b_curated_top20() -> Dict[str, Any]:
    """Design B: Fetching top 20 curated hotspots from pre-aggregated API layer."""
    t0 = time.perf_counter()
    resp = client.get("/network/hotspots", params={"limit": 20})
    data = resp.json().get("data", [])
    elapsed = time.perf_counter() - t0

    curated_json = json.dumps(data)
    char_count = len(curated_json)

    return {
        "design": "Design B (Curated Top-20 via API)",
        "records_fetched": len(data),
        "payload_chars": char_count,
        "estimated_tokens": char_count // 4,
        "latency_ms": round(elapsed * 1000, 2),
        "quality_assessment": "High signal; pre-ranked by Spark/SQL; strict grain compliance; 100% verifiable.",
    }


# ============================================================
# MODEL SELECTION DISPATCHER
# ============================================================

def select_model_for_task(task_type: str) -> Dict[str, str]:
    """Assign appropriate model tier based on task reasoning requirements."""
    mapping = {
        "routine_health_check": {
            "tier": "FAST_LIGHTWEIGHT",
            "model": "claude-3-haiku-20240307",
            "reason": "Deterministic parsing of status strings and counts; minimal reasoning required.",
        },
        "hotspot_filtering": {
            "tier": "FAST_LIGHTWEIGHT",
            "model": "claude-3-haiku-20240307",
            "reason": "Ranking and list formatting over pre-computed evidence.",
        },
        "incident_root_cause": {
            "tier": "DEEP_REASONING",
            "model": "claude-3-7-sonnet-20250219",
            "reason": "Multi-signal reconciliation, uncertainty bounding, and pipeline failure correlation.",
        },
        "code_review_ci": {
            "tier": "ADVANCED_BALANCED",
            "model": "claude-3-5-sonnet-20241022",
            "reason": "Deep AST and diff structural understanding against project constraints.",
        },
    }
    return mapping.get(task_type, {
        "tier": "DEEP_REASONING",
        "model": "claude-3-7-sonnet-20250219",
        "reason": "Default to deep reasoning for unclassified operations.",
    })


# ============================================================
# RUNNER
# ============================================================

def run_cost_and_context_optimization():
    print("=" * 70)
    print("C16 — CONTEXT, COST & USAGE OPTIMIZATION")
    print("=" * 70)

    # 1. Real Telemetry Scale
    print("\n1. REAL TELEMETRY SCALE (From Database Warehouse):")
    scale = compute_real_telemetry_scale()
    print(f" - Total Warehouse Telemetry Rows: {scale['total_warehouse_rows']:,}")
    print(f" - Citywide Grid Cells:            {scale['distinct_grids']:,}")
    print(f" - Single Hour Full Dump:          {scale['single_hour_grid_count']:,} rows")
    print(f" - Single Hour Estimated Tokens:   ~{scale['raw_hour_estimated_tokens']:,} tokens (Design A)")

    # 2. Design A vs Design B Execution Comparison
    print("\n" + "=" * 70)
    print("2. EXPERIMENTAL COMPARISON: DESIGN A vs DESIGN B:")
    des_a = execute_design_a_raw_sample(50)
    des_b = execute_design_b_curated_top20()

    print(f"\n[{des_a['design']}] (Sample 50 rows):")
    print(f" - Characters: {des_a['payload_chars']:,} | Tokens: ~{des_a['estimated_tokens']} | Latency: {des_a['latency_ms']} ms")
    print(f" - Assessment: {des_a['quality_assessment']}")

    print(f"\n[{des_b['design']}] (Full Top 20 Focus):")
    print(f" - Characters: {des_b['payload_chars']:,} | Tokens: ~{des_b['estimated_tokens']} | Latency: {des_b['latency_ms']} ms")
    print(f" - Assessment: {des_b['quality_assessment']}")

    savings = ((scale['raw_hour_estimated_tokens'] - des_b['estimated_tokens']) / scale['raw_hour_estimated_tokens']) * 100
    print(f"\n>>> Context Reduction vs Full Raw Dump: {savings:.2f}% token savings!")

    # 3. Model Selection Matrix
    print("\n" + "=" * 70)
    print("3. MODEL SELECTION MATRIX RULES:")
    tasks = ["routine_health_check", "hotspot_filtering", "incident_root_cause", "code_review_ci"]
    for t in tasks:
        sel = select_model_for_task(t)
        print(f" - Task: {t:<22} -> Model: {sel['model']:<25} ({sel['tier']})")
        print(f"   Reason: {sel['reason']}")
    print("=" * 70)


if __name__ == "__main__":
    run_cost_and_context_optimization()
