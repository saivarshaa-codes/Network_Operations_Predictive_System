from __future__ import annotations

import sqlite3
import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from fastapi.testclient import TestClient

from api.main import app


DB_PATH = PROJECT_ROOT / "data" / "warehouse" / "network_ops.db"


client = TestClient(app)


class TestApi2(unittest.TestCase):
    def test_grid_4821_default_returns_24_points(self):
        response = client.get("/network/grid/4821")

        self.assertEqual(response.status_code, 200)

        body = response.json()

        self.assertEqual(body["grid_id"], "4821")
        self.assertEqual(len(body["data"]), 24)

        timestamps = [
            point["timestamp"]
            for point in body["data"]
        ]

        self.assertEqual(len(timestamps), len(set(timestamps)))

    def test_grid_0_returns_404(self):
        response = client.get("/network/grid/0")

        self.assertEqual(response.status_code, 404)

    def test_grid_10001_returns_404(self):
        response = client.get("/network/grid/10001")

        self.assertEqual(response.status_code, 404)

    def test_grid_response_matches_warehouse(self):
        response = client.get("/network/grid/4821")

        self.assertEqual(response.status_code, 200)

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

        self.assertIsNotNone(row)

        api_point = body["data"][0]

        self.assertEqual(api_point["sms_in"], row["sms_in"])
        self.assertEqual(api_point["sms_out"], row["sms_out"])
        self.assertEqual(api_point["call_in"], row["call_in"])
        self.assertEqual(api_point["call_out"], row["call_out"])
        self.assertEqual(api_point["internet_activity"], row["internet_activity"])
        self.assertEqual(api_point["total_sms"], row["total_sms"])
        self.assertEqual(api_point["total_calls"], row["total_calls"])
        self.assertEqual(api_point["total_activity"], row["total_activity"])
        self.assertEqual(api_point["internet_share"], row["internet_share"])


# Retain standalone function aliases for backward compatibility if called directly
_test_case = TestApi2()
test_grid_4821_default_returns_24_points = lambda: _test_case.test_grid_4821_default_returns_24_points()
test_grid_0_returns_404 = lambda: _test_case.test_grid_0_returns_404()
test_grid_10001_returns_404 = lambda: _test_case.test_grid_10001_returns_404()
test_grid_response_matches_warehouse = lambda: _test_case.test_grid_response_matches_warehouse()


if __name__ == "__main__":
    unittest.main()