import { useEffect, useMemo, useState } from "react";
import {
  GeoJSON,
  MapContainer,
  TileLayer,
  Tooltip,
  useMap,
} from "react-leaflet";

import { getAlerts, getHotspots } from "../services/api";
import "leaflet/dist/leaflet.css";

const GEOJSON_URL = "/reference/milano-grid.geojson";

const DEFAULT_CENTER = [45.4642, 9.19];

function statusForGrid(gridId, hotspotMap, alertMap) {
  const id = String(gridId);

  const alert = alertMap.get(id);
  const hotspot = hotspotMap.get(id);

  if (alert) {
    if (
      alert.severity === "ACTIVITY_SPIKE" ||
      alert.severity === "HIGH_ACTIVITY"
    ) {
      return "HIGH";
    }

    return "ATTENTION";
  }

  if (hotspot) {
    return hotspot.rank === 1 ? "HIGH" : "ATTENTION";
  }

  return "NORMAL";
}

function statusClass(status) {
  return status.toLowerCase();
}

/*
 * Fits the map to the actual Milan GeoJSON bounds.
 * This runs when the GeoJSON becomes available.
 */
function MapViewport({ geojson }) {
  const map = useMap();

  useEffect(() => {
    if (!geojson?.features?.length) {
      return;
    }

    const layer = window.L.geoJSON(geojson);
    const bounds = layer.getBounds();

    if (bounds.isValid()) {
      map.fitBounds(bounds, {
        padding: [20, 20],
        maxZoom: 12,
      });
    }
  }, [geojson, map]);

  return null;
}

function HotspotsAlerts({ onOpenGrid }) {
  const [hotspots, setHotspots] = useState([]);
  const [alerts, setAlerts] = useState([]);
  const [geojson, setGeojson] = useState(null);

  const [limit, setLimit] = useState(10);
  const [severity, setSeverity] = useState("");

  const [loading, setLoading] = useState(true);
  const [mapLoading, setMapLoading] = useState(true);
  const [error, setError] = useState("");

  /*
   * IMPORTANT:
   * GeoJSON is fetched exactly ONCE per page mount.
   * Changing limit/severity never triggers this effect.
   */
  useEffect(() => {
    let active = true;

    async function loadGeoJson() {
      try {
        setMapLoading(true);

        const response = await fetch(GEOJSON_URL);

        if (!response.ok) {
          throw new Error(
            `GeoJSON request returned ${response.status}`
          );
        }

        const data = await response.json();

        if (active) {
          setGeojson(data);
        }
      } catch (err) {
        console.error("GeoJSON load failed:", err);

        if (active) {
          setError(
            "Unable to load the Milan geographic reference."
          );
        }
      } finally {
        if (active) {
          setMapLoading(false);
        }
      }
    }

    loadGeoJson();

    return () => {
      active = false;
    };
  }, []);

  /*
   * Operational API data.
   * Re-runs only when the user changes filters.
   */
  useEffect(() => {
    let active = true;

    async function loadOperationalData() {
      try {
        setLoading(true);
        setError("");

        const [hotspotResponse, alertResponse] =
          await Promise.all([
            getHotspots(limit),
            getAlerts(limit, severity),
          ]);

        if (!active) {
          return;
        }

        setHotspots(hotspotResponse.data || []);
        setAlerts(alertResponse.data || []);
      } catch (err) {
        console.error("Operational intelligence load failed:", err);

        if (active) {
          setError(
            "Unable to retrieve hotspot and alert intelligence from the API."
          );
        }
      } finally {
        if (active) {
          setLoading(false);
        }
      }
    }

    loadOperationalData();

    return () => {
      active = false;
    };
  }, [limit, severity]);

  /*
   * Fast lookup maps.
   */
  const hotspotMap = useMemo(
    () =>
      new Map(
        hotspots.map((item) => [
          String(item.grid_id),
          item,
        ])
      ),
    [hotspots]
  );

  const alertMap = useMemo(
    () =>
      new Map(
        alerts.map((item) => [
          String(item.grid_id),
          item,
        ])
      ),
    [alerts]
  );

  const topHotspot = hotspots[0] || null;

  /*
   * Create a lookup of operationally important grids.
   * This is used only for styling/status decisions.
   */
  const operationalIds = useMemo(
    () =>
      new Set([
        ...hotspots.map((item) => String(item.grid_id)),
        ...alerts.map((item) => String(item.grid_id)),
      ]),
    [hotspots, alerts]
  );

  /*
   * Styling:
   *
   * HIGH       = solid border + dense fill + TOP marker where applicable
   * ATTENTION  = dashed border + patterned visual treatment
   * NORMAL     = thin dashed neutral grid
   *
   * This means status is not communicated through colour alone.
   */
  function polygonStyle(feature) {
    const gridId = String(
      feature.properties?.cellId
    );

    const status = statusForGrid(
      gridId,
      hotspotMap,
      alertMap
    );

    const isTop =
      topHotspot &&
      String(topHotspot.grid_id) === gridId;

    if (status === "HIGH") {
      return {
        color: isTop ? "#ffffff" : "#ff6677",
        weight: isTop ? 4 : 2.5,
        fillColor: "#ff6677",
        fillOpacity: isTop ? 0.48 : 0.30,
        dashArray: isTop ? null : "1 2",
      };
    }

    if (status === "ATTENTION") {
      return {
        color: "#ffc46b",
        weight: 2,
        fillColor: "#ffc46b",
        fillOpacity: 0.20,
        dashArray: "6 4",
      };
    }

    return {
      color: "#607780",
      weight: 0.7,
      fillColor: "#607780",
      fillOpacity: 0.045,
      dashArray: "2 5",
    };
  }

  function handlePolygonClick(gridId) {
    if (onOpenGrid) {
      onOpenGrid(String(gridId));
    }
  }

  return (
    <main className="dashboard re4-page">
      {/* =====================================================
          HERO
          ===================================================== */}

      <section className="hero">
        <div>
          <p className="section-label">
            GEOGRAPHIC INTELLIGENCE
          </p>

          <h2>Hotspots &amp; Alerts</h2>

          <p className="hero-copy">
            Prioritized network activity mapped across the
            Milan geographic grid.
          </p>
        </div>

        <div className="asof">
          <span>OPERATIONAL VIEW</span>

          <strong>
            {hotspots[0]?.timestamp
              ? new Date(
                  hotspots[0].timestamp
                ).toLocaleString()
              : "Awaiting API"}
          </strong>
        </div>
      </section>

      {/* =====================================================
          ERROR
          ===================================================== */}

      {error && (
        <section className="state-card error-state">
          <div className="state-icon">!</div>

          <div>
            <strong>
              INTELLIGENCE UNAVAILABLE
            </strong>

            <p>{error}</p>
          </div>
        </section>
      )}

      {/* =====================================================
          CONTROLS
          ===================================================== */}

      <section className="re4-controls">
        <div>
          <span>RANK LIMIT</span>

          <select
            value={limit}
            onChange={(event) =>
              setLimit(Number(event.target.value))
            }
          >
            <option value={5}>5</option>
            <option value={10}>10</option>
            <option value={20}>20</option>
            <option value={50}>50</option>
          </select>
        </div>

        <div>
          <span>ALERT SEVERITY</span>

          <select
            value={severity}
            onChange={(event) =>
              setSeverity(event.target.value)
            }
          >
            <option value="">
              ALL
            </option>

            <option value="ACTIVITY_SPIKE">
              ACTIVITY SPIKE
            </option>

            <option value="HIGH_ACTIVITY">
              HIGH ACTIVITY
            </option>

            <option value="ACTIVITY_DROP">
              ACTIVITY DROP
            </option>
          </select>
        </div>

        <div className="re4-status-legend">
          <span>
            <i className="legend-high" />
            <b>HIGH</b>
            <small>Priority</small>
          </span>

          <span>
            <i className="legend-attention" />
            <b>ATTENTION</b>
            <small>Review</small>
          </span>

          <span>
            <i className="legend-normal" />
            <b>NORMAL</b>
            <small>Baseline</small>
          </span>
        </div>
      </section>

      {/* =====================================================
          RANKING + ALERTS
          ===================================================== */}

      <section className="re4-grid">
        {/* ---------------- RANKING ---------------- */}

        <article className="panel re4-ranking-panel">
          <div className="panel-header">
            <div>
              <p className="section-label">
                PRIORITY QUEUE
              </p>

              <h3>Top activity grids</h3>
            </div>

            <span className="live-badge">
              {loading ? "SYNCING" : "LIVE"}
            </span>
          </div>

          <div className="re4-table-wrapper">
            <table className="re4-table">
              <thead>
                <tr>
                  <th>Rank</th>
                  <th>Grid</th>
                  <th>Activity</th>
                  <th>Timestamp</th>
                  <th>Status</th>
                </tr>
              </thead>

              <tbody>
                {hotspots.map((item) => {
                  const status = statusForGrid(
                    item.grid_id,
                    hotspotMap,
                    alertMap
                  );

                  return (
                    <tr
                      key={item.grid_id}
                      onClick={() =>
                        handlePolygonClick(
                          item.grid_id
                        )
                      }
                    >
                      <td className="rank-cell">
                        #{item.rank}
                      </td>

                      <td className="grid-id-cell">
                        {item.grid_id}
                      </td>

                      <td className="activity-cell">
                        {Number(
                          item.total_activity
                        ).toLocaleString("en-US", {
                          maximumFractionDigits: 1,
                        })}
                      </td>

                      <td>
                        {new Date(
                          item.timestamp
                        ).toLocaleTimeString([], {
                          hour: "2-digit",
                          minute: "2-digit",
                        })}
                      </td>

                      <td>
                        <span
                          className={`status-badge ${statusClass(
                            status
                          )}`}
                        >
                          {status}
                        </span>
                      </td>
                    </tr>
                  );
                })}

                {!hotspots.length && !loading && (
                  <tr>
                    <td
                      colSpan="5"
                      className="empty-table-state"
                    >
                      No hotspot data available.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </article>

        {/* ---------------- ALERTS ---------------- */}

        <article className="panel re4-alert-panel">
          <div className="panel-header">
            <div>
              <p className="section-label">
                RULE-BASED INTELLIGENCE
              </p>

              <h3>Current alerts</h3>
            </div>

            <span className="live-badge">
              {alerts.length}
            </span>
          </div>

          <div className="alert-list">
            {alerts.map((alert, index) => {
              const status =
                alert.severity ===
                  "ACTIVITY_SPIKE" ||
                alert.severity ===
                  "HIGH_ACTIVITY"
                  ? "HIGH"
                  : "ATTENTION";

              return (
                <button
                  key={`${alert.grid_id}-${alert.timestamp}-${index}`}
                  className="alert-row"
                  onClick={() =>
                    handlePolygonClick(
                      alert.grid_id
                    )
                  }
                >
                  <span
                    className={`alert-marker ${statusClass(
                      status
                    )}`}
                  />

                  <span className="alert-content">
                    <strong>
                      GRID {alert.grid_id}
                    </strong>

                    <small>
                      {alert.alert_type}
                    </small>

                    <em>
                      {alert.reason}
                    </em>
                  </span>

                  <span
                    className={`status-badge ${statusClass(
                      status
                    )}`}
                  >
                    {status}
                  </span>
                </button>
              );
            })}

            {!alerts.length && !loading && (
              <div className="empty-state">
                No alerts match the selected
                severity.
              </div>
            )}
          </div>
        </article>
      </section>

      {/* =====================================================
          MILAN MAP
          ===================================================== */}

      <section className="panel re4-map-panel">
        <div className="panel-header">
          <div>
            <p className="section-label">
              MILAN GEOGRAPHIC GRID
            </p>

            <h3>Operational hotspot map</h3>
          </div>

          <div className="map-meta">
            <span>
              {geojson?.features?.length || 0} GRID CELLS
            </span>

            <span>
              {topHotspot
                ? `TOP GRID ${topHotspot.grid_id}`
                : "NO TOP GRID"}
            </span>
          </div>
        </div>

        <div className="map-container">
          {mapLoading && (
            <div className="map-loading">
              <div className="loader" />

              <span>
                LOADING MILAN GRID REFERENCE
              </span>
            </div>
          )}

          <MapContainer
            center={DEFAULT_CENTER}
            zoom={11}
            scrollWheelZoom
            className="milan-map"
          >
            <TileLayer
              attribution="&copy; OpenStreetMap contributors"
              url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
            />

            {geojson && (
              <>
                <MapViewport
                  geojson={geojson}
                />

                <GeoJSON
                    key={`${limit}-${severity}-${hotspots.map((item) => item.grid_id).join(",")}-${alerts.map((item) => `${item.grid_id}-${item.alert_type}`).join(",")}`}
                    data={geojson}
                    style={polygonStyle}
                  onEachFeature={(
                    feature,
                    layer
                  ) => {
                    const gridId = String(
                      feature.properties?.cellId
                    );

                    const hotspot =
                      hotspotMap.get(gridId);

                    const alert =
                      alertMap.get(gridId);

                    const status =
                      statusForGrid(
                        gridId,
                        hotspotMap,
                        alertMap
                      );

                    const isTop =
                      topHotspot &&
                      String(
                        topHotspot.grid_id
                      ) === gridId;

                    const statusText =
                      isTop
                        ? "HIGH • TOP HOTSPOT"
                        : status;

                    layer.bindTooltip(
                      `
                        <strong>GRID ${gridId}</strong><br/>
                        STATUS: ${statusText}
                        ${
                          hotspot
                            ? `<br/>RANK: #${hotspot.rank}
                               <br/>ACTIVITY: ${Number(
                                 hotspot.total_activity
                               ).toFixed(1)}`
                            : ""
                        }
                        ${
                          alert
                            ? `<br/>ALERT: ${alert.alert_type}`
                            : ""
                        }
                        <br/><span>CLICK TO OPEN GRID EXPLORER</span>
                      `,
                      {
                        sticky: true,
                      }
                    );

                    // layer.on({
                    //   click: () =>
                    //     handlePolygonClick(
                    //       gridId
                    //     ),

                    //   mouseover: (event) => {
                    //     event.target.setStyle({
                    //       weight:
                    //         isTop ? 5 : 3,
                    //       fillOpacity:
                    //         isTop
                    //           ? 0.62
                    //           : 0.42,
                    //     });

                    //     event.target.bringToFront();
                    //   },

                    //   mouseout: (event) => {
                    //     event.target.setStyle(
                    //       polygonStyle(
                    //         feature
                    //       )
                    //     );
                    //   },
                    // });
                    layer.on({
                      click: () =>
                        handlePolygonClick(gridId),
                    });
                  }}
                />
              </>
            )}
          </MapContainer>
        </div>

        <div className="map-footer">
          <span>
            CLICK POLYGON → GRID EXPLORER
          </span>

          <span>
            STATIC REFERENCE · FETCHED ONCE
          </span>

          <span>
            milano-grid.geojson
          </span>
        </div>
      </section>
    </main>
  );
}

export default HotspotsAlerts;