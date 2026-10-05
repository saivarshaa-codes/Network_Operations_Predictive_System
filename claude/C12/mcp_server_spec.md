# Network Intelligence MCP Server Specification (Phase 7 - C12)

This specification defines the Model Context Protocol (MCP) interface exposing platform capabilities to Claude Code and AI clients.

---

## 1. Architectural Guardrails (Thin Wrapper Rule)
- **Zero Business Logic**: The MCP server is strictly a protocol proxy over FastAPI endpoints.
- **No In-Server Calculations**: The MCP layer must NOT calculate percentiles, aggregate timeseries, re-score anomalies, or apply thresholds. If an operational metric is missing, it must be added to FastAPI (`api/`), not MCP.
- **Strict Input Validation**: All parameters (`grid_id`, `limit`, timestamps) are strictly validated before dispatch.

---

## 2. Exposed MCP Tool Catalog

| MCP Tool Name | Target REST Endpoint | Schema Parameters | Description |
| :--- | :--- | :--- | :--- |
| **`network_summary`** | `GET /network/summary` | `as_of: Optional[str]` | High-level citywide telemetry metrics and peak activity timestamp. |
| **`grid_activity`** | `GET /network/grid/{grid_id}` | `grid_id: int`, `as_of: Optional[str]` | Hourly activity metrics for a target grid cell. |
| **`grid_features`** | `GET /network/grid/{grid_id}/features` | `grid_id: int` | 24h rolling ML features for predictive models. |
| **`grid_location`** | `GET /network/grid/{grid_id}/location` | `grid_id: int` | Centroid coordinates and spatial properties in Milan. |
| **`hotspots`** | `GET /network/hotspots` | `limit: int = 10`, `as_of: Optional[str]` | Top active grid cells in the reporting window. |
| **`alerts`** | `GET /network/alerts` | `limit: int = 10`, `severity: Optional[str]` | Static rule-based threshold alerts. |
| **`pipeline_status`** | `GET /pipeline/status` | None | Ingestion status, row counts, and warehouse freshness rating. |
