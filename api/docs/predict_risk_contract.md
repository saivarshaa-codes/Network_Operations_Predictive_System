# Network Risk Prediction API Contract

## 1. Endpoint

**Method:** `POST`

**Path:** `/network/predict-risk`

**Purpose:**
Return the ML3 risk assessment for a requested network grid. The endpoint supports the latest available prediction by default and can optionally select a prediction for a specific timestamp.

---

## 2. Request

### Required field

| Field     | Type    | Required | Description             |
| --------- | ------- | -------: | ----------------------- |
| `grid_id` | integer |      Yes | Network grid identifier |

### Optional field

| Field       | Type              | Required | Description                                                     |
| ----------- | ----------------- | -------: | --------------------------------------------------------------- |
| `timestamp` | ISO-8601 datetime |       No | Select a prediction for a specific historical feature timestamp |

### Example — latest prediction

```json
{
  "grid_id": 1
}
```

### Example — specific timestamp

```json
{
  "grid_id": 10,
  "timestamp": "2013-11-06T18:00:00"
}
```

---

## 3. Successful Response

**HTTP Status:** `200 OK`

```json
{
  "grid_id": "10",
  "risk_score": 0.8179477744729879,
  "risk_level": "HIGH",
  "model_version": "ML3-v1",
  "feature_timestamp": "2013-11-06T18:00:00",
  "explanation_note": "High future high-activity risk. Investigate the grid."
}
```

### Response fields

| Field               | Type     | Description                                       |
| ------------------- | -------- | ------------------------------------------------- |
| `grid_id`           | string   | Requested network grid                            |
| `risk_score`        | float    | ML3 risk score between 0 and 1                    |
| `risk_level`        | string   | Operational risk band: `LOW`, `MEDIUM`, or `HIGH` |
| `model_version`     | string   | Version of the ML model used                      |
| `feature_timestamp` | datetime | Timestamp represented by the returned prediction  |
| `explanation_note`  | string   | Operational explanation of the prediction         |

---

## 4. Risk Levels

|      Risk score | Risk level |
| --------------: | ---------- |
|        `< 0.40` | `LOW`      |
| `0.40 – < 0.80` | `MEDIUM`   |
|       `>= 0.80` | `HIGH`     |

A high risk indicates an **operational attention signal for future high activity**. It does not represent a confirmed network fault.

---

## 5. Validation Errors

If the required `grid_id` field is missing, the API returns:

**HTTP Status:** `422 Unprocessable Entity`

FastAPI/Pydantic provides a readable validation response identifying the missing field.

Example invalid request:

```json
{}
```

---

## 6. Not Found

If no prediction exists for the requested grid/timestamp combination:

**HTTP Status:** `404 Not Found`

The response contains a readable error message identifying the missing prediction.

---

## 7. Data Source

The current ML5 implementation uses the trained ML3 prediction artifact and the generated ML3 prediction output.

Current model version:

```text
ML3-v1
```

Current prediction output:

```text
ML/ML3/outputs/risk_predictions.csv
```

The API selects the latest prediction for a grid when no timestamp is supplied, or the matching prediction when a timestamp is supplied.

---

## 8. Backward Compatibility

The original grid-only request remains supported:

```json
{
  "grid_id": 1
}
```

The optional `timestamp` field is additive and does not make changes to existing grid-only requests.

---

## 9. API5 Acceptance Criteria

| Criterion                                                                      | Status |
| ------------------------------------------------------------------------------ | ------ |
| `model_version` is present and clearly identifies the model version            | PASS   |
| Missing required field returns HTTP `422` with readable validation information | PASS   |
| Prediction API contract is documented under `docs/`                            | PASS   |

**API5 contract status: COMPLETE**
