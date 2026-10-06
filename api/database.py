from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterator

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "data" / "warehouse" / "network_ops.db"


def get_connection() -> sqlite3.Connection:
    """Open a read-only connection to the analytics warehouse."""
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"Warehouse database not found: {DB_PATH}"
        )

    connection = sqlite3.connect(
        f"file:{DB_PATH}?mode=ro",
        uri=True,
        timeout=30,
    )

    connection.row_factory = sqlite3.Row

    # Allow the API to wait briefly if Airflow is writing.
    connection.execute("PRAGMA busy_timeout = 30000")

    return connection


def get_db() -> Iterator[sqlite3.Connection]:
    connection = get_connection()

    try:
        yield connection
    finally:
        connection.close()