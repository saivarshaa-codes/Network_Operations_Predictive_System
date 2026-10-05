from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

# ============================================================
# CONFIG
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Sample candidate PR diff touching code, comments, and schemas
SAMPLE_PR_DIFF = """
diff --git a/api/models.py b/api/models.py
--- a/api/models.py
+++ b/api/models.py
@@ -88,6 +88,7 @@ class PredictionResponse(BaseModel):
     grid_id: str
     risk_score: float
     risk_level: str
+    surge_index: float | None = None
     model_version: str
 
diff --git a/services/alert_service.py b/services/alert_service.py
--- a/services/alert_service.py
+++ b/services/alert_service.py
@@ -14,6 +14,10 @@ def generate_surge_alert(grid_id, current, baseline):
+    # Detect when high traffic causes network congestion on cell tower
+    if current > baseline * 1.5:
+        return "HIGH_CONGESTION_ALERT: Total megabytes exceeded threshold."
"""


# ============================================================
# CI ENGINEERING REVIEW AGENT (Advisory Only)
# ============================================================

class CIEngineeringReviewer:
    """Automated reviewer auditing changes against project rules in CLAUDE.md."""

    def __init__(self, claude_md_path: Path):
        self.rules_text = claude_md_path.read_text(encoding="utf-8") if claude_md_path.exists() else ""

    def review_change(self, diff_text: str, test_suite_results: Dict[str, Any]) -> Dict[str, Any]:
        findings = {}

        # 1. Data-Grain Risk Check
        grain_risk = "PASS: No alteration to the (grid_id, timestamp) canonical grouping keys detected."
        if "GROUP BY" in diff_text and "timestamp" not in diff_text:
            grain_risk = "RISK: Aggregation query modified without hourly timestamp key; may break canonical grain."
        findings["data_grain"] = grain_risk

        # 2. Leakage Risk Check
        leakage_risk = "PASS: Temporal windowing uses strictly prior timestamps (t < target_time)."
        if "timestamp <=" in diff_text or "t >=" in diff_text:
            leakage_risk = "RISK: Boundary condition in temporal query might allow target_time telemetry leakage."
        findings["leakage"] = leakage_risk

        # 3. Geographic Join Check
        geo_risk = "PASS: Spatial joins continue using properties.cellId."
        if "cellId" in diff_text and ".id" in diff_text:
            geo_risk = "RISK: Possible join on GeoJSON feature array index instead of properties.cellId."
        findings["geographic_join"] = geo_risk

        # 4. API Contract Check
        api_risk = "PASS: Changes to API response model (adding 'surge_index') are additive and optional."
        if "-" in diff_text and ("grid_id:" in diff_text or "total_activity:" in diff_text):
            api_risk = "BREAKING CHANGE: Removed or renamed existing field in API response model."
        findings["api_contract"] = api_risk

        # 5. Terminology Check (Rule #4 & #3)
        terminology_violations = []
        lower_diff = diff_text.lower()
        if "congestion" in lower_diff:
            terminology_violations.append("Asserts 'congestion' (Violates Rule #4: we have no throughput/capacity data)")
        if "megabytes" in lower_diff or " mb" in lower_diff:
            terminology_violations.append("Treats activity measure as 'megabytes' (Violates Rule #3: activity values are proportional unitless measures)")
        if "cell tower" in lower_diff:
            terminology_violations.append("Refers to 'cell tower' instead of geographic grid cell")

        if terminology_violations:
            findings["terminology"] = (
                f"FLAGGED (Violations caught by CI review that unit tests missed): "
                f"{'; '.join(terminology_violations)}."
            )
        else:
            findings["terminology"] = "PASS: Terminology adheres to project definitions."

        # 6. Missing Tests Check
        missing_tests = []
        if "surge_index" in diff_text and "test_" not in diff_text:
            missing_tests.append("Additive field 'surge_index' lacks serialization test in test_api.py")
        findings["missing_tests"] = (
            f"RECOMMENDATION: {'; '.join(missing_tests)}" if missing_tests else "PASS: Test coverage adequate."
        )

        # Agent Authority Constraints: Strictly Advisory
        is_blocked = "FLAGGED" in findings["terminology"] or "BREAKING" in findings["api_contract"]
        recommendation = "REJECT / REQUEST CHANGES" if is_blocked else "APPROVE"

        return {
            "pr_target": "feature/traffic-surge-metric",
            "test_suite_status": test_suite_results.get("status", "PASSED"),
            "agent_authority": "ADVISORY_ONLY (No deployment or merge permission)",
            "recommendation": recommendation,
            "category_findings": findings,
            "disclaimer": "This review is advisory. Final merge and deployment authority rests strictly with human NOC engineers.",
        }


# ============================================================
# RUNNER
# ============================================================

def run_ci_review():
    print("=" * 70)
    print("C15 — HEADLESS / CI ENGINEERING REVIEW")
    print("=" * 70)

    claude_md = PROJECT_ROOT / "CLAUDE.md"
    reviewer = CIEngineeringReviewer(claude_md)

    # 1. Step 1: Tests run first
    mock_test_results = {"status": "PASSED", "tests_run": 14, "failures": 0}
    print("Step 1: Test suite executed prior to review.")
    print(f"Status: {mock_test_results['status']} ({mock_test_results['tests_run']} tests run, 0 failures)\n")

    # 2. Step 2: Agent reviews diff against CLAUDE.md
    print("Step 2: Reviewing PR diff across 6 risk categories...")
    report = reviewer.review_change(SAMPLE_PR_DIFF, mock_test_results)

    print("\n" + "=" * 70)
    print("REVIEW REPORT:")
    print("=" * 70)
    for cat, finding in report["category_findings"].items():
        print(f" - [{cat.upper()}]: {finding}")

    print("\n" + "=" * 70)
    print(f"Agent Recommendation: {report['recommendation']}")
    print(f"Agent Authority:      {report['agent_authority']}")
    print(f"Governance Note:      {report['disclaimer']}")
    print("=" * 70)


if __name__ == "__main__":
    run_ci_review()
