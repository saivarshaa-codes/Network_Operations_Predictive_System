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


class TestApi1(unittest.TestCase):
    """Unit tests for API1 — Network Summary endpoint."""

    def test_summary_status_200(self):
        response = client.get("/network/summary")
        self.assertEqual(response.status_code, 200)

    def test_summary_payload_contract(self):
        response = client.get("/network/summary")
        self.assertEqual(response.status_code, 200)
        body = response.json()

        expected_fields = {
            "total_activity",
            "active_grids",
            "peak_hour",
            "top_grid",
            "as_of",
        }
        self.assertTrue(expected_fields.issubset(body.keys()))
        self.assertIsInstance(body["total_activity"], float)
        self.assertIsInstance(body["active_grids"], int)
        self.assertIsInstance(body["top_grid"], str)

    def test_summary_active_grids(self):
        response = client.get("/network/summary")
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["active_grids"], 10000)

    def test_summary_matches_warehouse(self):
        response = client.get("/network/summary")
        self.assertEqual(response.status_code, 200)
        body = response.json()

        connection = sqlite3.connect(
            f"file:{DB_PATH}?mode=ro",
            uri=True,
        )
        connection.row_factory = sqlite3.Row

        row = connection.execute(
            """
            SELECT
                total_activity,
                active_grids,
                peak_hour,
                top_grid,
                as_of
            FROM network_summary
            LIMIT 1
            """
        ).fetchone()

        connection.close()

        self.assertIsNotNone(row)
        self.assertAlmostEqual(body["total_activity"], row["total_activity"], places=2)
        self.assertEqual(body["active_grids"], row["active_grids"])
        self.assertEqual(body["top_grid"], str(row["top_grid"]))


# Backward-compatible function aliases
_test_case = TestApi1()
test_summary_status_200 = lambda: _test_case.test_summary_status_200()
test_summary_payload_contract = lambda: _test_case.test_summary_payload_contract()
test_summary_active_grids = lambda: _test_case.test_summary_active_grids()
test_summary_matches_warehouse = lambda: _test_case.test_summary_matches_warehouse()


if __name__ == "__main__":
    unittest.main()
