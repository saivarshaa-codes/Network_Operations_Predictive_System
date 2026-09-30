# API5 — Prediction Endpoint Contract

## Endpoint

`POST /network/predict-risk`

## Purpose

Provides a network risk prediction for a grid.

The current implementation is a stub so that React and Claude integrations can be developed before the ML5 model is available.

## Request

### Required field

| Field     | Type    | Required | Description                                  |
| --------- | ------- | -------- | -------------------------------------------- |
| `grid_id` | integer | Yes      | Network grid identifier, valid range 1–10000 |

### Example

```json
{
  "grid_id": 4821
}
```

## Response

| Field              | Type   | Description                                    |
| ------------------ | ------ | ---------------------------------------------- |
| `grid_id`          | string | Requested grid identifier                      |
| `risk_score`       | float  | Risk score                                     |
| `risk_level`       | string | Risk classification                            |
| `model_version`    | string | Version of the prediction implementation/model |
| `explanation_note` | string | Explanation or implementation note             |

### Current Stub Response

```json
{
  "grid_id": "4821",
  "risk_score": 0.5,
  "risk_level": "MEDIUM",
  "model_version": "STUB-v1",
  "explanation_note": "Prediction implementation is currently a stub."
}
```

## Validation

* Missing required `grid_id` returns HTTP 422 with a readable validation error.
* Grid IDs outside 1–10000 return HTTP 404.
* The API does not perform ML feature engineering or model inference in the stub implementation.

## ML5 Integration Contract

ML5 must replace the stub implementation without changing:

* HTTP method
* endpoint path
* request field names
* request field types
* response field names
* response field types

The `model_version` field must identify the real model after ML5 integration.

The endpoint contract is therefore stable for React, Claude, and downstream consumers.
