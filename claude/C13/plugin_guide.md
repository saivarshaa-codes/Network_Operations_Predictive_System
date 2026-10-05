# Network Engineering Claude Plugin: Architecture & Governance (Phase 7 - C13)

This plugin bundles the complete set of project standards, guardrails, skills, and tools into a single installable team package.

---

## 1. Bundled Assets Manifest

- **Project Rules**: `CLAUDE.md` containing all 6 non-negotiable data and terminology rules.
- **Project Skills**:
  - `network-anomaly-analysis`
  - `pipeline-troubleshooting`
  - `telecom-data-quality`
- **Slash Commands**:
  - `/check-pipeline`
  - `/explain-grid`
  - `/review-anomaly`
  - `/test-api`
  - `/network-health`
- **Event Hooks**:
  - `post_edit_grain_leakage` (monitors `spark/**` and `ML/**`)
  - `pre_action_pipeline_guard` (monitors `airflow/**`)
- **Approved MCP Tool Definitions**:
  - `network_summary`, `grid_activity`, `grid_features`, `grid_location`, `hotspots`, `alerts`, `pipeline_status`.

---

## 2. Versioning and Ownership Strategy

### Semantic Versioning (SemVer)
- **`v1.0.0`**: Initial unified release of Phase 7 platform standards.
- **PATCH (`1.0.x`)**: Non-breaking adjustments to prompt templates, documentation links, or skill descriptions.
- **MINOR (`1.x.0`)**: Additive features (e.g. new slash command `/network-surging-grids`, new non-breaking MCP tools).
- **MAJOR (`x.0.0`)**: Changes to non-negotiable project rules, changes to canonical grains, or breaking API contract updates.

### Ownership & Governance
- **Owner**: Network Operations & Platform Engineering Lead (`noc-architecture-guild@telecom.internal`).
- **Review Requirement**: Any PR modifying `CLAUDE.md` rules, permissions, or hooks requires approval from at least one Principal Data Engineer and one NOC Operations Lead.
- **Distribution**: Pinned via git submodule or central internal plugin registry to guarantee synchronized environments across engineering workstations.
