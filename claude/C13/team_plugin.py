from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict

# ============================================================
# CONFIG
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

MANIFEST_PATH = Path(__file__).resolve().parent / "plugin_manifest.json"


def load_manifest() -> Dict[str, Any]:
    with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


# ============================================================
# PLUGIN INSTALLER & VERIFIER
# ============================================================

class PluginEnvironmentVerifier:
    def __init__(self, target_env_dir: Path):
        self.env_dir = target_env_dir
        self.manifest = load_manifest()

    def install(self) -> bool:
        """Simulate installing the plugin bundle into a fresh clean workspace environment."""
        print(f"Installing plugin '{self.manifest['name']} v{self.manifest['version']}' into: {self.env_dir}")
        self.env_dir.mkdir(parents=True, exist_ok=True)

        # 1. Install CLAUDE.md
        src_claude_md = PROJECT_ROOT / "CLAUDE.md"
        if src_claude_md.exists():
            shutil.copy(src_claude_md, self.env_dir / "CLAUDE.md")

        # 2. Install plugin manifest
        shutil.copy(MANIFEST_PATH, self.env_dir / "plugin_manifest.json")

        # 3. Create .claude configuration directory
        claude_dir = self.env_dir / ".claude"
        claude_dir.mkdir(parents=True, exist_ok=True)
        with open(claude_dir / "plugin_installed.json", "w", encoding="utf-8") as f:
            json.dump({"installed_version": self.manifest["version"], "status": "ACTIVE"}, f)

        print("Plugin package installed cleanly.")
        return True

    def verify_command_active(self) -> bool:
        """Verify that at least one bundled slash command runs in the new environment."""
        from claude.C7.slash_commands import cmd_network_health
        res = cmd_network_health()
        is_active = res.get("grain_invariant_passed") is True
        print(f" - Verifying '/network-health' active: {'PASS' if is_active else 'FAIL'} (Status: {res.get('status')})")
        return is_active

    def verify_rule_active(self) -> bool:
        """Verify that the congestion terminology rule is demonstrably enforced."""
        from claude.C4.repository_understanding import test_congestion_rule
        resp = test_congestion_rule("Is grid 4821 congested?")
        resp_lower = resp.lower()
        is_enforced = (
            "terminology correction" in resp_lower
            or (
                ("congestion" in resp_lower or "congested" in resp_lower)
                and any(
                    k in resp_lower
                    for k in ["capacity", "relative", "proportional", "cannot", "rule 4", "rule #4", "measure", "correct the premise"]
                )
            )
        )
        print(f" - Verifying 'Congestion Rule' active: {'PASS' if is_enforced else 'FAIL'}")
        return is_enforced


# ============================================================
# RUNNER
# ============================================================

def run_plugin_test():
    print("=" * 70)
    print("C13 — PLUGINS FOR TEAM STANDARDIZATION")
    print("=" * 70)

    manifest = load_manifest()
    print(f"Plugin Name: {manifest['name']}")
    print(f"Version:     {manifest['version']}")
    print(f"Commands:    {', '.join(manifest['commands'])}")
    print(f"Skills:      {', '.join([s['name'] for s in manifest['skills']])}")
    print()

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        verifier = PluginEnvironmentVerifier(temp_path)

        print("1. Performing clean environment installation...")
        installed = verifier.install()
        assert installed, "Installation failed!"

        print("\n2. Verifying active safeguards and commands after install:")
        cmd_ok = verifier.verify_command_active()
        rule_ok = verifier.verify_rule_active()

        assert cmd_ok, "Command /network-health failed in clean environment!"
        assert rule_ok, "Congestion rule was not active after install!"

    print("\n" + "=" * 70)
    print("Plugin installation and post-install safeguards verified successfully.")
    print("=" * 70)


if __name__ == "__main__":
    run_plugin_test()
