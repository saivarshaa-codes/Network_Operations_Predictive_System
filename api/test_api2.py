from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from api.main import app


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "data" / "warehouse" / "network_ops.db"


client = TestClient(app)


def test_grid_4821_default_returns_24_points():
    response = client.get("/network/grid/4821")

    assert response.status_code == 200

    body = response.json()

    assert body["grid_id"] == "4821"
    assert len(body["data"]) == 24

    timestamps = [
        point["timestamp"]
        for point in body["data"]
    ]

    assert len(timestamps) == len(set(timestamps))


def test_grid_0_returns_404():
    response = client.get("/network/grid/0")

    assert response.status_code == 404


def test_grid_10001_returns_404():
    response = client.get("/network/grid/10001")

    assert response.status_code == 404


def test_grid_response_matches_warehouse():
    response = client.get("/network/grid/4821")

    assert response.status_code == 200

    body = response.json()

    target_timestamp = body["data"][0]["timestamp"]

    connection = sqlite3.connect(
        f"file:{DB_PATH}?mode=ro",
        uri=True,
    )
    connection.row_factory = sqlite3.Row

    row = connection.execute(
        """
        SELECT
            t.timestamp,
            g.grid_id,
            f.sms_in,
            f.sms_out,
            f.call_in,
            f.call_out,
            f.internet_activity,
            f.total_sms,
            f.total_calls,
            f.total_activity,
            f.internet_share
        FROM fact_network_activity f
        JOIN dim_time t
            ON f.time_key = t.time_key
        JOIN dim_grid g
            ON f.grid_key = g.grid_key
        WHERE g.grid_id = ?
          AND t.timestamp = ?
        """,
        (
            "4821",
            target_timestamp.replace("T", " "),
        ),
    ).fetchone()

    connection.close()

    assert row is not None

    api_point = body["data"][0]

    assert api_point["sms_in"] == row["sms_in"]
    assert api_point["sms_out"] == row["sms_out"]
    assert api_point["call_in"] == row["call_in"]
    assert api_point["call_out"] == row["call_out"]
    assert api_point["internet_activity"] == row["internet_activity"]
    assert api_point["total_sms"] == row["total_sms"]
    assert api_point["total_calls"] == row["total_calls"]
    assert api_point["total_activity"] == row["total_activity"]
    assert api_point["internet_share"] == row["internet_share"]