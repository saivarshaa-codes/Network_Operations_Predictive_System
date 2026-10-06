from __future__ import annotations

import json
import os
from pathlib import Path

from anthropic import Anthropic
from dotenv import load_dotenv


# ============================================================
# CONFIG
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

load_dotenv(PROJECT_ROOT / ".env")

API_KEY = os.getenv("ANTHROPIC_API_KEY")

if not API_KEY:
    raise RuntimeError(
        "ANTHROPIC_API_KEY is not set. "
        "Add it to the project .env file."
    )

MODEL = "claude-sonnet-4-6"

client = Anthropic(api_key=API_KEY)


# ============================================================
# SYSTEM PROMPT
# ============================================================

SYSTEM_PROMPT = """
You are a Network Operations Centre insight assistant.

Your job is to explain a machine-generated network activity signal
using ONLY the structured evidence provided by the application.

You are NOT the ML model.
You do NOT calculate the risk or anomaly score.
You do NOT invent missing evidence.

Important project rules:

1. Activity values are activity measures.
   They are NOT literal call counts, SMS counts, message counts,
   or megabytes.

2. Do NOT claim network congestion.
   The available evidence does not contain capacity, throughput,
   latency, or utilization measurements.

3. A risk or anomaly score is an operational attention signal,
   not proof of a confirmed network fault.

4. Separate observed evidence from interpretation.

5. Never invent a number.

6. If important evidence is missing or insufficient to determine
   severity, explicitly say that the evidence is insufficient and
   identify what additional evidence would be needed.

Return EXACTLY these four sections:

SEVERITY
One of:
NORMAL
ATTENTION
HIGH

EVIDENCE
Only facts directly supported by the supplied evidence.
Include relevant numbers.

INTERPRETATION
Explain what the evidence might mean.
Clearly distinguish inference from observed evidence.

NEXT CHECKS
Give practical checks that a human NOC engineer should perform next.

Keep the response concise and operational.
"""


# ============================================================
# CLAUDE CALL
# ============================================================

def generate_network_insight(evidence: dict) -> str:

    evidence_json = json.dumps(
        evidence,
        indent=2,
        ensure_ascii=False
    )

    user_prompt = f"""
Analyze this curated grid evidence:

<grid_evidence>
{evidence_json}
</grid_evidence>

Use only the evidence above.

Remember:
- Do not invent missing values.
- Do not describe activity measures as literal counts or MB.
- Do not claim congestion.
- Treat the anomaly/risk score as an operational attention signal.
- If the evidence is insufficient, explicitly state that.
"""

    response = client.messages.create(
        model=MODEL,
        max_tokens=700,
        system=SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": user_prompt
            }
        ]
    )

    return response.content[0].text


# ============================================================
# TEST EVIDENCE
# ============================================================

if __name__ == "__main__":

    evidence = {
        "grid_id": 4821,
        "timestamp": "2013-11-07T17:00:00",
        "current_activity": 823651800.12,
        "baseline_activity": 610000000.00,
        "activity_growth": 0.35,
        "peak_ratio": 1.42,
        "variability": 0.18,
        "internet_share": 0.82,
        "anomaly_score": 0.71,
        "anomaly_direction": "high"
    }

    print("=" * 70)
    print("C1 — CLAUDE NETWORK INSIGHT")
    print("=" * 70)
    print(f"Model: {MODEL}")
    print()

    result = generate_network_insight(evidence)

    print(result)
    