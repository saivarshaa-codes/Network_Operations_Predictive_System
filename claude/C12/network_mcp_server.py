from __future__ import annotations

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

mcp_call_log: List[Dict[str, Any]] = []


def record_call(tool: str, params: Dict[str, Any], result: Any):
    mcp_call_log.append({
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "tool": tool,
        "params": params,
        "result_status": "SUCCESS" if result is not None else "ERROR",
    })


# ============================================================
# MCP SERVER TOOLS (Strictly Thin Wrappers — Zero Business Logic)
# ============================================================

def mcp_network_summary(as_of: Optional[str] = None) -> Dict[str, Any]:
    """MCP Tool: network_summary -> GET /network/summary"""
    params = {"as_of": as_of} if as_of else {}
    resp = client.get("/network/summary", params=params)
    data = resp.json()
    record_call("network_summary", params, data)
    return data


def mcp_grid_activity(grid_id: int, as_of: Optional[str] = None) -> Dict[str, Any]:
    """MCP Tool: grid_activity -> GET /network/grid/{grid_id}"""
    if not isinstance(grid_id, int) or grid_id < 1 or grid_id > 10000:
        raise ValueError(f"Invalid grid_id: {grid_id}. Must be integer between 1 and 10000.")

    params = {"as_of": as_of} if as_of else {}
    resp = client.get(f"/network/grid/{grid_id}", params=params)
    data = resp.json()
    record_call("grid_activity", {"grid_id": grid_id, **params}, data)
    return data


def mcp_grid_features(grid_id: int) -> Dict[str, Any]:
    """MCP Tool: grid_features -> GET /network/grid/{grid_id}/features"""
    if not isinstance(grid_id, int):
        raise ValueError("grid_id must be integer")

    resp = client.get(f"/network/grid/{grid_id}/features")
    data = resp.json()
    record_call("grid_features", {"grid_id": grid_id}, data)
    return data


def mcp_grid_location(grid_id: int) -> Dict[str, Any]:
    """MCP Tool: grid_location -> GET /network/grid/{grid_id}/location"""
    if not isinstance(grid_id, int):
        raise ValueError("grid_id must be integer")

    resp = client.get(f"/network/grid/{grid_id}/location")
    data = resp.json()
    record_call("grid_location", {"grid_id": grid_id}, data)
    return data


def mcp_hotspots(limit: int = 10, as_of: Optional[str] = None) -> Dict[str, Any]:
    """MCP Tool: hotspots -> GET /network/hotspots"""
    params = {"limit": min(max(1, limit), 100)}
    if as_of:
        params["as_of"] = as_of
    resp = client.get("/network/hotspots", params=params)
    data = resp.json()
    record_call("hotspots", params, data)
    return data


def mcp_alerts(limit: int = 10, severity: Optional[str] = None) -> Dict[str, Any]:
    """MCP Tool: alerts -> GET /network/alerts"""
    params = {"limit": limit}
    if severity:
        params["severity"] = severity
    resp = client.get("/network/alerts", params=params)
    data = resp.json()
    record_call("alerts", params, data)
    return data


def mcp_pipeline_status() -> Dict[str, Any]:
    """MCP Tool: pipeline_status -> GET /pipeline/status"""
    resp = client.get("/pipeline/status")
    data = resp.json()
    record_call("pipeline_status", {}, data)
    return data


# ============================================================
# VERIFICATION & QUERY ENGINE
# ============================================================

def verify_tool_matches_api_exactly() -> bool:
    """Acceptance Criteria: Every MCP tool result matches direct API call exactly."""
    endpoints_to_verify = [
        ("network_summary", mcp_network_summary(), client.get("/network/summary").json()),
        ("grid_location", mcp_grid_location(4821), client.get("/network/grid/4821/location").json()),
        ("hotspots", mcp_hotspots(limit=5), client.get("/network/hotspots", params={"limit": 5}).json()),
        ("pipeline_status", mcp_pipeline_status(), client.get("/pipeline/status").json()),
    ]

    all_matched = True
    for name, mcp_out, api_out in endpoints_to_verify:
        match = mcp_out == api_out
        print(f" - Tool '{name}': {'EXACT MATCH (PASS)' if match else 'MISMATCH (FAIL)'}")
        if not match:
            all_matched = False

    return all_matched


def answer_required_questions():
    """Answer the two canonical questions mandated by Lab C12."""
    print("\n" + "=" * 70)
    print("QUERY 1: 'Which grids have the highest anomaly scores?'")
    print("=" * 70)

    # Call MCP hotspots tool (which includes risk_score/anomaly)
    hotspots_data = mcp_hotspots(limit=5)
    print("MCP Tool Called: hotspots(limit=5)")
    for pt in hotspots_data.get("data", []):
        gid = pt.get("grid_id")
        score = pt.get("risk_score")
        act = pt.get("total_activity")
        status = pt.get("status")
        print(f" - Grid {gid} (Rank {pt.get('rank')}): Total Activity = {act:.2f}, Risk Score = {score}, Status = {status}")

    print("\n" + "=" * 70)
    print("QUERY 2: 'Check whether their data pipeline completed successfully.'")
    print("=" * 70)

    # Call MCP pipeline_status tool
    pipe_status = mcp_pipeline_status()
    print("MCP Tool Called: pipeline_status()")
    print(f"Pipeline Overall Status: {pipe_status.get('status')}")
    print(f"Pipeline Healthy: {pipe_status.get('healthy')}")
    print(f"Freshness: {pipe_status.get('freshness')}")
    print(f"Task Breakdown: {pipe_status.get('per_task_status')}")
    print(f"Active Warnings: {pipe_status.get('reasons')}")


if __name__ == "__main__":
    print("=" * 70)
    print("C12 — MODEL CONTEXT PROTOCOL (MCP) NETWORK INTELLIGENCE SERVER")
    print("=" * 70)

    print("\n1. Verifying MCP Tools are strictly thin wrappers matching APIs exactly:")
    exact_match = verify_tool_matches_api_exactly()
    assert exact_match, "MCP Tool output did not match direct API response!"

    print("\n2. Executing Required Operational Queries via MCP:")
    answer_required_questions()

    print("\n" + "=" * 70)
    print(f"Total MCP Tool Calls recorded: {len(mcp_call_log)}")
    for log_item in mcp_call_log:
        print(f" - [{log_item['timestamp']}] Tool: {log_item['tool']} | Result: {log_item['result_status']}")
    print("=" * 70)
