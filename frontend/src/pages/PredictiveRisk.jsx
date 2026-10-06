import { useState } from "react";
import { getNetworkInsight, predictRisk } from "../services/api";
import "./PredictiveRisk.css";

function PredictiveRisk() {
  const [gridId, setGridId] = useState("");
  const [timestamp, setTimestamp] = useState("");
  const [prediction, setPrediction] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [insight, setInsight] = useState(null);
  const [insightLoading, setInsightLoading] = useState(false);
  const [insightError, setInsightError] = useState("");

 function convertUserTimestampToISO(value) {
  const match = value.match(
    /^(\d{2})-(\d{2})-(\d{4})\s+(\d{2}):(\d{2})$/
  );

  if (!match) {
    throw new Error(
      "Timestamp must use the format DD-MM-YYYY HH:MM."
    );
  }

  const [, day, month, year, hour, minute] = match;

  const dayNumber = Number(day);
  const monthNumber = Number(month);
  const yearNumber = Number(year);
  const hourNumber = Number(hour);
  const minuteNumber = Number(minute);

  if (
    monthNumber < 1 ||
    monthNumber > 12 ||
    dayNumber < 1 ||
    dayNumber > 31 ||
    hourNumber < 0 ||
    hourNumber > 23 ||
    minuteNumber < 0 ||
    minuteNumber > 59
  ) {
    throw new Error("Enter a valid timestamp.");
  }

  // ML3 stores timestamps using the dataset's Milan/local time representation.
  // Do NOT convert through the browser timezone.
  // Example:
  // 03-11-2013 17:00 -> 2013-11-03T17:00:00
  return `${year}-${month}-${day}T${hour}:${minute}:00`;
}

  async function handlePredict(event) {
    event.preventDefault();

    const gridValue = gridId.trim();
    const timestampValue = timestamp.trim();

    if (!gridValue) {
      setError("Enter a grid ID.");
      setPrediction(null);
      return;
    }

    let isoTimestamp = "";

    if (timestampValue) {
      try {
        isoTimestamp = convertUserTimestampToISO(timestampValue);
      } catch (err) {
        setError(err.message);
        setPrediction(null);
        return;
      }
    }

    setLoading(true);
    setError("");
    setPrediction(null);
    setInsight(null);
    setInsightError("");

    try {
      const result = await predictRisk(
        gridValue,
        isoTimestamp
      );

      setPrediction(result);
    } catch (err) {
      setError(
        err.message || "Unable to retrieve predictive risk."
      );
    } finally {
      setLoading(false);
    }
  }

  async function handleGenerateInsight() {
    if (!prediction) {
      return;
    }

    setInsightLoading(true);
    setInsightError("");

    try {
      const result = await getNetworkInsight(
        prediction.grid_id,
        prediction.feature_timestamp || ""
      );
      setInsight(result);
    } catch (err) {
      setInsightError(
        err.message || "Unable to retrieve Claude network insight."
      );
    } finally {
      setInsightLoading(false);
    }
  }

  function parseInsightText(rawText) {
    if (!rawText) return null;
    const lines = rawText.split("\n");
    const sections = {
      severity: "",
      evidence: [],
      interpretation: [],
      nextChecks: [],
    };
    let current = null;

    for (const rawLine of lines) {
      const trimmed = rawLine.trim();
      const upper = trimmed.toUpperCase();
      if (upper === "SEVERITY") {
        current = "severity";
        continue;
      }
      if (upper === "EVIDENCE") {
        current = "evidence";
        continue;
      }
      if (upper === "INTERPRETATION") {
        current = "interpretation";
        continue;
      }
      if (upper === "NEXT CHECKS") {
        current = "nextChecks";
        continue;
      }

      if (current === "severity" && !sections.severity && trimmed) {
        sections.severity = trimmed;
      } else if (current === "evidence" && trimmed) {
        sections.evidence.push(trimmed);
      } else if (current === "interpretation" && trimmed) {
        sections.interpretation.push(trimmed);
      } else if (current === "nextChecks" && trimmed) {
        sections.nextChecks.push(trimmed);
      }
    }

    return sections;
  }

  function formatScore(score) {
    if (score === null || score === undefined) {
      return "—";
    }

    return Number(score).toFixed(2);
  }

  function getRiskLevel(level) {
    return String(level || "UNKNOWN").toUpperCase();
  }

  function getRiskPosition(score) {
    if (score === null || score === undefined) {
      return 0;
    }

    const numericScore = Number(score);

    if (Number.isNaN(numericScore)) {
      return 0;
    }

    return Math.min(Math.max(numericScore * 100, 0), 100);
  }

  const parsedSections = parseInsightText(insight?.insight);

  return (
    <main className="dashboard predictive-risk-page">

      {/* =========================================================
          PAGE HEADER
      ========================================================= */}
      <section className="risk-hero">
        <div className="risk-hero-main">
          <div className="risk-status-line">
            <span className="risk-status-dot" />
            PREDICTIVE INTELLIGENCE ONLINE
          </div>

          <p className="section-label">
            RE5 // MODEL INTELLIGENCE
          </p>

          <h2>Predictive Risk</h2>

          <p className="risk-hero-copy">
            Evaluate a network grid using the deployed predictive
            risk service and review its model-generated attention signal.
          </p>
        </div>

        <div className="model-status-card">
          <span>MODEL SERVICE</span>

          <strong>
            {prediction?.model_version || "STUB-v1"}
          </strong>

          <small>
            {prediction
              ? "Prediction returned"
              : "Ready for prediction"}
          </small>
        </div>
      </section>

      {/* =========================================================
          REQUEST PANEL
      ========================================================= */}
      <section className="panel risk-request-panel">
        <div className="risk-request-heading">
          <div>
            <p className="section-label">PREDICTION REQUEST</p>
            <h3>Analyze network grid</h3>
          </div>

          <span className="request-code">
            POST /network/predict-risk
          </span>
        </div>

        <form
          className="risk-request-form"
          onSubmit={handlePredict}
        >
          {/* REQUIRED GRID ID */}
          <div className="risk-input-group">
            <label htmlFor="risk-grid-id">
              NETWORK GRID ID
            </label>

            <input
              id="risk-grid-id"
              type="text"
              value={gridId}
              onChange={(event) =>
                setGridId(event.target.value)
              }
              placeholder="4821"
              autoComplete="off"
            />
          </div>

          {/* OPTIONAL TIMESTAMP */}
          <div className="risk-input-group">
            <label htmlFor="risk-timestamp">
              TIMESTAMP{" "}
              <span className="optional-label">
                OPTIONAL
              </span>
            </label>

            <input
              id="risk-timestamp"
              type="text"
              value={timestamp}
              onChange={(event) =>
                setTimestamp(event.target.value)
              }
              placeholder="03-11-2013 17:00"
              autoComplete="off"
            />
          </div>

          <button
            type="submit"
            className="risk-predict-button"
            disabled={loading}
          >
            <span className="button-icon">
              {loading ? "…" : "↗"}
            </span>

            {loading ? "RUNNING MODEL" : "PREDICT RISK"}
          </button>
        </form>

        <div className="request-helper">
          <span>INPUT</span>
          <strong>Grid ID</strong>

          <span className="helper-divider" />

          <span>TIME</span>
          <strong>Optional timestamp</strong>

          <span className="helper-divider" />

          <span>OUTPUT</span>
          <strong>Risk score + level</strong>

          <span className="helper-divider" />

          <span>MODEL</span>
          <strong>Version displayed below</strong>
        </div>
      </section>

      {/* =========================================================
          ERROR
      ========================================================= */}
      {error && (
        <section className="risk-error">
          <div className="risk-error-icon">!</div>

          <div>
            <strong>PREDICTION UNAVAILABLE</strong>
            <p>{error}</p>
          </div>
        </section>
      )}

      {/* =========================================================
          EMPTY STATE
      ========================================================= */}
      {!prediction && !loading && !error && (
        <section className="risk-empty-state">
          <div className="empty-scan">
            <span />
            <span />
            <span />
          </div>

          <div>
            <p className="section-label">MODEL READY</p>

            <h3>Awaiting network grid</h3>

            <p>
              Enter a grid ID above to request a predictive
              attention signal. A timestamp may also be provided
              when a specific prediction time is required.
            </p>
          </div>

          <div className="empty-example">
            <span>EXAMPLE GRID</span>
            <strong>4821</strong>
          </div>
        </section>
      )}

      {/* =========================================================
          RESULTS
      ========================================================= */}
      {prediction && (
        <section className="risk-results">

          {/* =====================================================
              MODEL OUTPUT
          ===================================================== */}
          <article className="panel model-output-card">
            <div className="result-card-header">
              <div>
                <p className="section-label">MODEL OUTPUT</p>
                <h3>Predictive attention signal</h3>
              </div>

              <div className="model-version">
                <span>MODEL</span>
                <strong>{prediction.model_version}</strong>
              </div>
            </div>

            <div className="score-area">

              <div className="score-main">
                <span>RISK SCORE</span>

                <strong>
                  {formatScore(prediction.risk_score)}
                </strong>

                <small>
                  Model-generated predictive signal
                </small>
              </div>

              <div className="risk-level-display">
                <span>RISK LEVEL</span>

                <strong>
                  {getRiskLevel(prediction.risk_level)}
                </strong>

                <small>
                  Grid {prediction.grid_id}
                </small>
              </div>

            </div>

            {/* Risk scale */}
            <div className="risk-scale-section">
              <div className="risk-scale-header">
                <span>LOWER ATTENTION</span>
                <span>HIGHER ATTENTION</span>
              </div>

              <div className="risk-scale">
                <div className="risk-scale-track" />

                <div
                  className="risk-scale-marker"
                  style={{
                    left: `${getRiskPosition(
                      prediction.risk_score
                    )}%`,
                  }}
                >
                  <span />
                </div>
              </div>

              <div className="risk-scale-values">
                <span>0.00</span>
                <span>0.25</span>
                <span>0.50</span>
                <span>0.75</span>
                <span>1.00</span>
              </div>
            </div>

            {/* Metadata */}
            <div className="prediction-meta">

              <div>
                <span>GRID ID</span>
                <strong>{prediction.grid_id}</strong>
              </div>

              <div>
                <span>RISK LEVEL</span>
                <strong>
                  {getRiskLevel(prediction.risk_level)}
                </strong>
              </div>

              <div>
                <span>MODEL VERSION</span>
                <strong>{prediction.model_version}</strong>
              </div>

              {prediction.feature_timestamp && (
              <div>
                <span>FEATURE TIMESTAMP</span>
                <strong>{prediction.feature_timestamp}</strong>
              </div>
            )}

            </div>

            {/* Interpretation */}
            <div className="interpretation-box">
              <div className="interpretation-icon">i</div>

              <div>
                <strong>INTERPRETATION</strong>

                <p>
                  This score is a predictive attention signal.
                  It should not be interpreted as a confirmed
                  network fault.
                </p>
              </div>
            </div>
          </article>

          {/* =====================================================
              AI EXPLANATION
          ===================================================== */}
          <article className="panel ai-explanation-card">

            <div className="result-card-header">
              <div>
                <p className="section-label">REASONING LAYER</p>
                <h3>Explain with AI</h3>
              </div>

              <span className={insight ? "future-badge active" : "future-badge"}>
                {insight ? "CLAUDE ONLINE" : "INTELLIGENCE READY"}
              </span>
            </div>

            {insight ? (
              <div className="ai-insight-content">
                <div className="ai-insight-header">
                  <div className="ai-insight-severity-row">
                    <span className="ai-section-label">ASSESSED SEVERITY</span>
                    <span className={`ai-severity-badge severity-${(insight.severity || "attention").toLowerCase()}`}>
                      {insight.severity || "ATTENTION"}
                    </span>
                  </div>
                  <span className="ai-insight-model">{insight.model_version}</span>
                </div>

                {parsedSections?.interpretation?.length > 0 && (
                  <div className="ai-section">
                    <span className="ai-section-title">OPERATIONAL INTERPRETATION</span>
                    <p className="ai-section-body">
                      {parsedSections.interpretation.join(" ")}
                    </p>
                  </div>
                )}

                {parsedSections?.evidence?.length > 0 && (
                  <div className="ai-section">
                    <span className="ai-section-title">OBSERVED EVIDENCE</span>
                    <ul className="ai-evidence-list">
                      {parsedSections.evidence.map((item, idx) => (
                        <li key={idx}>{item.replace(/^[-*•]\s*/, "")}</li>
                      ))}
                    </ul>
                  </div>
                )}

                {parsedSections?.nextChecks?.length > 0 && (
                  <div className="ai-section">
                    <span className="ai-section-title">RECOMMENDED NEXT CHECKS</span>
                    <ol className="ai-checks-list">
                      {parsedSections.nextChecks.map((item, idx) => (
                        <li key={idx}>{item.replace(/^\d+[\.\)]\s*/, "")}</li>
                      ))}
                    </ol>
                  </div>
                )}

                <div className="ai-refresh-row">
                  <button
                    type="button"
                    className="ai-refresh-button"
                    onClick={handleGenerateInsight}
                    disabled={insightLoading}
                  >
                    <span>↻</span> {insightLoading ? "REFRESHING..." : "RE-ANALYZE WITH CLAUDE"}
                  </button>
                </div>
              </div>
            ) : (
              <div className="ai-placeholder">

                <div className="ai-orbit">
                  <div className="ai-core">AI</div>
                </div>

                <p className="ai-eyebrow">
                  OPERATIONAL REASONING
                </p>

                <h4>
                  Evidence-grounded operational reasoning
                </h4>

                <p className="ai-description">
                  Synthesize model risk signals, rolling telemetry baselines,
                  and diurnal activity patterns into actionable NOC investigation guidance.
                </p>

                <button
                  type="button"
                  className="ai-button active-ready"
                  onClick={handleGenerateInsight}
                  disabled={insightLoading || !prediction}
                >
                  <span>✦</span>
                  {insightLoading ? "ANALYZING EVIDENCE WITH CLAUDE..." : "EXPLAIN WITH AI"}
                </button>

                {insightError && (
                  <div className="ai-error-box">
                    <span>!</span>
                    <small>{insightError}</small>
                  </div>
                )}

                <small>
                  Direct server-side Claude intelligence integration.
                </small>
              </div>
            )}

            <div className="ai-boundary">
              <span>MODEL</span>
              <strong>Score &amp; level</strong>

              <span className="boundary-arrow">→</span>

              <span>AI</span>
              <strong>Reasoning &amp; explanation</strong>
            </div>
          </article>

        </section>
      )}
    </main>
  );
}

export default PredictiveRisk;