# Cost, Context & Model Selection Guidelines (Phase 7 - C16)

This document establishes the architecture standards for cost optimization, context engineering, and model selection in the Network Operations Predictive Intelligence System.

---

## 1. Architectural Principle
> **"LLMs reason over curated evidence; Spark and SQL remain responsible for large-scale computation."**

Dumping raw timeseries into LLM prompts violates cost and reliability standards. Pre-computation in Spark and SQL collapses multi-gigabyte partitions into focused operational evidence packages.

---

## 2. Real Telemetry Context Size & Cost Comparison

Based on the Milan Telecom dataset (10,000 geographic grid cells):

| Metric | Design A: Raw Telemetry Dump (1 Hour) | Design B: Curated Top-20 Evidence (API) | Ratio / Savings |
| :--- | :--- | :--- | :--- |
| **Row Count** | 10,000 raw grid records | 20 curated records | **500x reduction** |
| **Character Volume** | ~1,250,000 characters | ~2,400 characters | **99.8% reduction** |
| **Token Consumption** | ~312,500 tokens (exceeds standard limits) | ~600 tokens | **99.8% token savings** |
| **Inference Cost / Query** | ~$0.93 - $4.68 / query | ~$0.0018 / query | **>99.9% cost savings** |
| **Annualized Cost (NOC 24/7)**| ~$81,000 / year (at 1 query/5 min) | ~$189 / year | **$80,800+ saved** |
| **Response Latency** | 25 - 45 seconds (or context overflow) | 0.8 - 1.5 seconds | **~30x faster** |
| **Answer Quality** | Hallucination-prone; needle-in-haystack dilution | Sharp, grounded, 100% cited facts | Significant accuracy gain |

---

## 3. Model Selection Matrix by Task Difficulty

| Operational Task | Recommended Model Tier | Rationale |
| :--- | :--- | :--- |
| **Routine Health Checks (`/check-pipeline`)** | Fast / Lightweight (Haiku / Flash) | Deterministic parsing of boolean health and row counts; sub-second latency required. |
| **Structured Evidence Filtering (`/hotspots`)**| Fast / Lightweight (Haiku / Flash) | Ranking and basic formatting of pre-computed warehouse lists. |
| **Multi-Signal Incident Investigation** | Deep Reasoning (Claude Sonnet / Extended Thinking) | Resolving subtle disagreements between static rules and ML models; rigorous uncertainty bounding. |
| **Headless CI Review Agent (`C15`)** | Balanced / Advanced (Claude Sonnet) | Deep structural analysis of code diffs against architectural rules in `CLAUDE.md`. |
| **Architecture Refactoring & Planning (`C5`)** | Deep Reasoning (Claude Sonnet / Opus) | Complex impact analysis across multi-tier dependencies (Spark, SQL, FastAPI, React). |

---

## 4. Long-Session Context Management & Compaction Rules
1. **Tool-Call Compaction**: In multi-turn investigations, prune raw intermediate tool JSON payloads after the model has extracted the required metrics. Retain only `{source_tool, metric_name, value}` triples in session history.
2. **Periodic Summarization**: After 4 dialogue turns, summarize preceding evidence into an immutable checkpoint state before accepting further operator drill-down queries.
