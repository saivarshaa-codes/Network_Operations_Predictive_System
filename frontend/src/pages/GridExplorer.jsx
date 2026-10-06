import { useEffect, useState } from "react";
import { getGridActivity } from "../services/api";

function GridExplorer({ initialGridId = "" }) {
  const [gridId, setGridId] = useState(initialGridId);
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  async function loadGrid(value) {
    const trimmedValue = String(value).trim();

    if (!trimmedValue) {
      setError("Enter a grid ID.");
      setData(null);
      return;
    }

    setLoading(true);
    setError("");
    setData(null);

    try {
      const result = await getGridActivity(trimmedValue);
      setData(result);
    } catch (err) {
      if (err.message.includes("404")) {
        setError(`Grid ${trimmedValue} was not found.`);
      } else {
        setError(err.message || "Unable to load grid activity.");
      }
    } finally {
      setLoading(false);
    }
  }

  // RE4 → RE3 handoff.
  // When a hotspot/map polygon opens Grid Explorer,
  // automatically load that exact grid.
  useEffect(() => {
    if (!initialGridId) {
      return;
    }

    const value = String(initialGridId);

    setGridId(value);
    loadGrid(value);
  }, [initialGridId]);

  async function handleSearch(event) {
    event.preventDefault();
    await loadGrid(gridId);
  }

  const points = Array.isArray(data)
    ? data
    : data?.data || data?.points || data?.activity || [];

  return (
    <section className="grid-explorer">
      <div className="section-heading">
        <div>
          <span className="eyebrow">GRID INVESTIGATION</span>

          <h1>Grid Explorer</h1>

          <p>
            Drill into recent hourly network activity for an individual Milan
            grid.
          </p>
        </div>
      </div>

      <form className="grid-search" onSubmit={handleSearch}>
        <label htmlFor="grid-id">GRID ID</label>

        <input
          id="grid-id"
          type="text"
          value={gridId}
          onChange={(event) => setGridId(event.target.value)}
          placeholder="e.g. 4821"
        />

        <button type="submit" disabled={loading}>
          {loading ? "LOADING..." : "INVESTIGATE"}
        </button>
      </form>

      {loading && (
        <div className="state-card loading-state">
          <span className="state-dot" />
          Querying network activity...
        </div>
      )}

      {!loading && error && (
        <div className="state-card error-state">
          <div className="state-icon">!</div>

          <div>
            <strong>GRID NOT AVAILABLE</strong>
            <p>{error}</p>
          </div>
        </div>
      )}

      {!loading && data && points.length === 0 && (
        <div className="state-card error-state">
          No activity points were returned for this grid.
        </div>
      )}

      {!loading && points.length > 0 && (
        <>
          <div className="grid-result-header">
            <div>
              <span className="eyebrow">ACTIVE INVESTIGATION</span>

              <h2>Grid {gridId}</h2>
            </div>

            <div className="point-count">
              <strong>{points.length}</strong>
              <span>HOURLY POINTS</span>
            </div>
          </div>

          <div className="series-legend">
            <span className="series-sms">SMS</span>
            <span className="series-calls">CALLS</span>
            <span className="series-internet">INTERNET</span>
            <span className="series-total">TOTAL ACTIVITY</span>
          </div>

          <div className="activity-table-wrapper">
            <table className="activity-table">
              <thead>
                <tr>
                  <th>TIME</th>
                  <th>SMS</th>
                  <th>CALLS</th>
                  <th>INTERNET</th>
                  <th>TOTAL ACTIVITY</th>
                </tr>
              </thead>

              <tbody>
                {points.map((point, index) => (
                  <tr key={point.timestamp || index}>
                    <td className="timestamp">
                      {point.timestamp || point.time}
                    </td>

                    <td className="metric-sms">
                      {formatValue(
                        point.total_sms ??
                          point.sms ??
                          point.sms_activity
                      )}
                    </td>

                    <td className="metric-calls">
                      {formatValue(
                        point.total_calls ??
                          point.calls ??
                          point.call_activity
                      )}
                    </td>

                    <td className="metric-internet">
                      {formatValue(
                        point.internet_activity ??
                          point.internet ??
                          point.internet_share
                      )}
                    </td>

                    <td className="metric-total">
                      {formatValue(
                        point.total_activity ??
                          point.activity
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </section>
  );
}

function formatValue(value) {
  if (value === null || value === undefined) {
    return "—";
  }

  if (typeof value === "number") {
    return value.toLocaleString(undefined, {
      maximumFractionDigits: 1,
    });
  }

  return value;
}

export default GridExplorer;