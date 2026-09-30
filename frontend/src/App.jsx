import { useEffect, useState } from "react";
import { getNetworkSummary } from "./services/api";
import "./App.css";
import GridExplorer from "./pages/GridExplorer";
import HotspotsAlerts from "./pages/HotspotsAlerts";
import PredictiveRisk from "./pages/PredictiveRisk";

function formatNumber(value) {
  return new Intl.NumberFormat("en-US", {
    maximumFractionDigits: 0,
  }).format(value);
}

// Display the hour exactly as returned by the API.
// This avoids converting the historical Milan dataset timestamp
// through the browser's local timezone.
function formatApiHour(timestamp) {
  if (!timestamp) {
    return "—";
  }

  const match = String(timestamp).match(/T(\d{2}):(\d{2})/);

  return match ? `${match[1]}:${match[2]}` : String(timestamp);
}

function App() {
  const [summary, setSummary] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [activePage, setActivePage] = useState("overview");

  // Used when RE4 map/table opens a specific grid in Grid Explorer.
  const [selectedGrid, setSelectedGrid] = useState("");

  useEffect(() => {
    let active = true;

    async function loadSummary() {
      const startedAt = performance.now();

      try {
        setLoading(true);
        setError("");

        console.log("[Overview] Requesting /network/summary...");

        const data = await getNetworkSummary();

        const elapsed = (
          (performance.now() - startedAt) /
          1000
        ).toFixed(2);

        console.log(
          `[Overview] /network/summary completed in ${elapsed}s`,
          data
        );

        if (active) {
          setSummary(data);
        }
      } catch (err) {
        const elapsed = (
          (performance.now() - startedAt) /
          1000
        ).toFixed(2);

        console.error(
          `[Overview] /network/summary failed after ${elapsed}s`,
          err
        );

        if (active) {
          setError(
            err?.message ||
              "Unable to reach the Network Operations API."
          );
        }
      } finally {
        if (active) {
          setLoading(false);
        }
      }
    }

    loadSummary();

    return () => {
      active = false;
    };
  }, []);

  function openGridExplorer(gridId) {
    setSelectedGrid(String(gridId));
    setActivePage("grid");
  }

  return (
    <div className="app-shell">
      {/* =========================================================
          HEADER
      ========================================================= */}
      <header className="topbar">
        <div className="brand">
          <div className="brand-mark">
            <span />
            <span />
            <span />
          </div>

          <div>
            <div className="eyebrow">
              MILAN // NETWORK INTELLIGENCE
            </div>

            <h1>Network Operations Center</h1>
          </div>
        </div>

        <div className="system-status">
          <span
            className={`status-dot ${error ? "offline" : ""}`}
          />

          <div>
            <strong>
              {error ? "API OFFLINE" : "SYSTEM ONLINE"}
            </strong>

            <small>LIVE DATA LINK</small>
          </div>
        </div>
      </header>

      {/* =========================================================
          NAVIGATION
      ========================================================= */}
      <nav className="navigation">
        <button
          className={`nav-item ${
            activePage === "overview" ? "active" : ""
          }`}
          onClick={() => setActivePage("overview")}
        >
          Overview
        </button>

        <button
          className={`nav-item ${
            activePage === "grid" ? "active" : ""
          }`}
          onClick={() => {
            setSelectedGrid("");
            setActivePage("grid");
          }}
        >
          Grid Explorer
        </button>

        <button
          className={`nav-item ${
            activePage === "alerts" ? "active" : ""
          }`}
          onClick={() => setActivePage("alerts")}
        >
          Alerts
        </button>

        <button
          className={`nav-item ${
            activePage === "risk" ? "active" : ""
          }`}
          onClick={() => setActivePage("risk")}
        >
          Predictive Risk
        </button>

      </nav>

      {/* =========================================================
          RE2 — NETWORK OVERVIEW
      ========================================================= */}
      {activePage === "overview" && (
        <main className="dashboard">
          <section className="hero">
            <div>
              <p className="section-label">NETWORK OVERVIEW</p>

              <h2>Operational picture</h2>

              <p className="hero-copy">
                Milan telecom activity intelligence across the monitored
                grid.
              </p>
            </div>

            <div className="asof">
              <span>REPORTING WINDOW</span>

              {/* Use the timestamp supplied by the API directly.
                  Do not use the browser clock/timezone. */}
              <strong>
                {summary?.as_of || "Awaiting API"}
              </strong>
            </div>
          </section>

          {/* =====================================================
              LOADING STATE
          ===================================================== */}
          {loading && (
            <section className="state-card loading-state">
              <div className="loader" />

              <div>
                <strong>CONNECTING TO NETWORK API</strong>

                <p>
                  Retrieving current network summary…
                </p>
              </div>
            </section>
          )}

          {/* =====================================================
              ERROR STATE
          ===================================================== */}
          {error && !loading && (
            <section className="state-card error-state">
              <div className="state-icon">!</div>

              <div>
                <strong>NETWORK DATA UNAVAILABLE</strong>

                <p>{error}</p>
              </div>
            </section>
          )}

          {/* =====================================================
              RE2 DATA
          ===================================================== */}
          {summary && !loading && !error && (
            <>
              {/* =================================================
                  FOUR REQUIRED NOC KPI CARDS
              ================================================= */}
              <section className="kpi-grid">
                {/* Total Activity */}
                <article className="kpi-card primary">
                  <div className="kpi-top">
                    <span>Total Activity</span>
                    <span className="kpi-code">ACT-01</span>
                  </div>

                  <strong>
                    {formatNumber(summary.total_activity)}
                  </strong>

                  <small>
                    Current reporting window
                  </small>
                </article>

                {/* Active Grids */}
                <article className="kpi-card">
                  <div className="kpi-top">
                    <span>Active Grids</span>
                    <span className="kpi-code">GRID-01</span>
                  </div>

                  <strong>
                    {formatNumber(summary.active_grids)}
                  </strong>

                  <small>
                    Monitored geographic cells
                  </small>
                </article>

                {/* Peak Hour */}
                <article className="kpi-card">
                  <div className="kpi-top">
                    <span>Peak Hour</span>
                    <span className="kpi-code">TIME-01</span>
                  </div>

                  <strong>
                    {formatApiHour(summary.peak_hour)}
                  </strong>

                  <small>
                    Highest observed activity
                  </small>
                </article>

                {/* Top Grid */}
                <article className="kpi-card">
                  <div className="kpi-top">
                    <span>Top Grid</span>
                    <span className="kpi-code">GRID-02</span>
                  </div>

                  <strong>
                    #{summary.top_grid}
                  </strong>

                  <small>
                    Highest current activity
                  </small>
                </article>
              </section>

              {/* =================================================
                  SUPPORTING OVERVIEW PANELS
              ================================================= */}
              <section className="main-grid">
                <article className="panel activity-panel">
                  <div className="panel-header">
                    <div>
                      <p className="section-label">
                        INTELLIGENCE LAYER
                      </p>

                      <h3>Network activity signal</h3>
                    </div>

                    <span className="live-badge">
                      LIVE
                    </span>
                  </div>

                  <div className="signal-visual">
                    <div className="signal-grid">
                      {Array.from({ length: 42 }).map(
                        (_, index) => (
                          <span
                            key={index}
                            className={
                              index % 7 === 0
                                ? "signal-hot"
                                : ""
                            }
                          />
                        )
                      )}
                    </div>

                    <div className="signal-caption">
                      <span>
                        10,000 geographic cells monitored
                      </span>

                      <span>
                        Hourly resolution
                      </span>
                    </div>
                  </div>
                </article>

                <article className="panel health-panel">
                  <div className="panel-header">
                    <div>
                      <p className="section-label">
                        DATA CONNECTION
                      </p>

                      <h3>System health</h3>
                    </div>
                  </div>

                  <div className="health-row">
                    <span className="health-indicator" />

                    <div>
                      <strong>FastAPI</strong>

                      <small>
                        Network intelligence service
                      </small>
                    </div>

                    <b>CONNECTED</b>
                  </div>

                  <div className="health-row">
                    <span className="health-indicator" />

                    <div>
                      <strong>Warehouse</strong>

                      <small>
                        Curated network data
                      </small>
                    </div>

                    <b>READY</b>
                  </div>

                  <div className="health-footer">
                    <span>AS OF</span>

                    {/* Again, display API value directly */}
                    <strong>
                      {summary.as_of}
                    </strong>
                  </div>
                </article>
              </section>
            </>
          )}
        </main>
      )}

      {/* =========================================================
          RE3 — GRID EXPLORER
      ========================================================= */}
      {activePage === "grid" && (
        <GridExplorer initialGridId={selectedGrid} />
      )}

      {/* =========================================================
          RE4 — HOTSPOTS + ALERTS + MILAN MAP
      ========================================================= */}
      {activePage === "alerts" && (
        <HotspotsAlerts
          onOpenGrid={openGridExplorer}
        />
      )}

      {/* =========================================================
          RE5 — PREDICTIVE RISK
      ========================================================= */}
      {activePage === "risk" && <PredictiveRisk />}

      {/* =========================================================
          FOOTER
      ========================================================= */}
      <footer>
        <span>
          NETWORK OPERATIONS &amp; PREDICTIVE INTELLIGENCE
        </span>

        <span>
          {activePage === "overview" &&
            "RE2 // NETWORK OVERVIEW"}

          {activePage === "grid" &&
            "RE3 // GRID EXPLORER"}

          {activePage === "alerts" &&
            "RE4 // HOTSPOTS // ALERTS // MILAN MAP"}

          {activePage === "risk" &&
            "RE5 // PREDICTIVE RISK"}

        </span>
      </footer>
    </div>
  );
}

export default App;