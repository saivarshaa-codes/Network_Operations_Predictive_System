const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;

async function request(endpoint, options = {}) {
  const controller = new AbortController();

  const timeoutId = setTimeout(() => {
    controller.abort();
  }, 180000);

  try {
    const response = await fetch(`${API_BASE_URL}${endpoint}`, {
      ...options,
      signal: controller.signal,
      headers: {
        "Content-Type": "application/json",
        ...(options.headers || {}),
      },
    });

    if (!response.ok) {
      let detail = "";

      try {
        const errorBody = await response.json();
        detail = errorBody?.detail || "";
      } catch {
        // Ignore non-JSON error responses.
      }

      throw new Error(
        detail || `Network API returned ${response.status}`
      );
    }

    return await response.json();
  } catch (error) {
    if (error.name === "AbortError") {
      throw new Error(
        "Network API request timed out after 180 seconds."
      );
    }

    throw error;
  } finally {
    clearTimeout(timeoutId);
  }
}

export function getNetworkSummary() {
  return request("/network/summary");
}

export function getGridActivity(gridId) {
  return request(`/network/grid/${gridId}`);
}

export function getHotspots(limit = 10) {
  return request(`/network/hotspots?limit=${limit}`);
}

export function getAlerts(limit = 10, severity = "") {
  const params = new URLSearchParams({
    limit: String(limit),
  });

  if (severity) {
    params.set("severity", severity);
  }

  return request(`/network/alerts?${params.toString()}`);
}

export function predictRisk(gridId, timestamp = "") {
  const body = {
    grid_id: String(gridId),
  };

  if (timestamp) {
    body.timestamp = timestamp;
  }

  return request("/network/predict-risk", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function getNetworkInsight(gridId, timestamp = "") {
  const body = {
    grid_id: Number(gridId),
  };

  if (timestamp) {
    body.timestamp = timestamp;
  }

  return request("/network/insight", {
    method: "POST",
    body: JSON.stringify(body),
  });
}