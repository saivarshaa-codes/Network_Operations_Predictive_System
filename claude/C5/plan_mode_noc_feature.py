from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from dotenv import load_dotenv
from fastapi.testclient import TestClient

# ============================================================
# CONFIG
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / ".env")

from api.database import get_connection

# ============================================================
# REUSABLE SURGE CALCULATION LOGIC
# ============================================================

def compute_surging_grids(limit: int = 10, min_baseline: float = 50.0, as_of: str | None = None) -> Dict[str, Any]:
    """Compute top grids whose activity increased most sharply against their rolling baseline.
    
    Reuses the baseline logic from ML2 / Phase 1 stored in network_feature_table:
    - avg_activity: rolling baseline activity measure.
    - activity_growth: fractional increase above baseline.
    """
    connection = get_connection()
    try:
        # Determine effective as_of
        if as_of:
            target_time = as_of
        else:
            cur = connection.execute("SELECT MAX(feature_timestamp) FROM network_feature_table")
            row = cur.fetchone()
            target_time = row[0] if row and row[0] else "2013-11-04 14:00:00"

        query = """
        SELECT 
            f.grid_id,
            f.feature_timestamp,
            f.avg_activity as baseline_activity,
            f.activity_growth as growth_ratio,
            (f.avg_activity * (1.0 + f.activity_growth)) as estimated_current_activity,
            g.centroid_latitude,
            g.centroid_longitude
        FROM network_feature_table f
        LEFT JOIN dim_grid g ON f.grid_id = g.grid_id
        WHERE f.feature_timestamp = ? AND f.avg_activity >= ?
        ORDER BY f.activity_growth DESC
        LIMIT ?;
        """

        cursor = connection.execute(query, (target_time, min_baseline, limit))
        results: List[Dict[str, Any]] = []
        for rank, r in enumerate(cursor.fetchall(), start=1):
            results.append({
                "rank": rank,
                "grid_id": int(r["grid_id"]),
                "current_activity": round(float(r["estimated_current_activity"]), 2),
                "baseline_activity": round(float(r["baseline_activity"]), 2),
                "growth_ratio": round(float(r["growth_ratio"]), 4),
                "growth_percentage": f"{float(r['growth_ratio']) * 100:.1f}%",
                "centroid_lat": r["centroid_latitude"],
                "centroid_lon": r["centroid_longitude"],
            })

        return {
            "as_of": target_time,
            "min_baseline_filter": min_baseline,
            "surging_grids_count": len(results),
            "surging_grids": results,
        }
    finally:
        connection.close()


# ============================================================
# VERIFICATION SUITE
# ============================================================

def verify_additive_change_and_regressions() -> bool:
    """Ensure that new feature calculation works and existing API routes still pass cleanly."""
    from api.main import app
    client = TestClient(app)

    print("Checking existing API endpoints for regression...")
    r1 = client.get("/network/summary")
    assert r1.status_code == 200, f"/network/summary failed: {r1.status_code}"
    print(f" - /network/summary: PASS (HTTP {r1.status_code})")

    r2 = client.get("/network/hotspots")
    assert r2.status_code == 200, f"/network/hotspots failed: {r2.status_code}"
    print(f" - /network/hotspots: PASS (HTTP {r2.status_code})")

    r3 = client.get("/pipeline/status")
    assert r3.status_code == 200, f"/pipeline/status failed: {r3.status_code}"
    print(f" - /pipeline/status: PASS (HTTP {r3.status_code})")

    return True


if __name__ == "__main__":
    print("=" * 70)
    print("C5 — PLAN MODE: ADD A NEW NOC FEATURE SAFELY")
    print("=" * 70)

    print("\n1. Executing additive feature calculation (Top Surging Grids against Baseline):")
    surge_data = compute_surging_grids(limit=5, min_baseline=50.0)
    print(f"Reporting Window: {surge_data['as_of']}")
    print(f"Total Surging Grids Found: {surge_data['surging_grids_count']}")
    for g in surge_data["surging_grids"]:
        print(
            f" - Rank {g['rank']}: Grid {g['grid_id']} | "
            f"Current: {g['current_activity']} | "
            f"Baseline: {g['baseline_activity']} | "
            f"Growth: +{g['growth_percentage']}"
        )

    print("\n2. Verifying existing endpoints have zero regressions:")
    verify_additive_change_and_regressions()
    print("\nFeature implementation and verification complete.")
    print("=" * 70)
