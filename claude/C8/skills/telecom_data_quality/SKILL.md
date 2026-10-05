---
name: telecom-data-quality
description: Audits dataset grain invariants, non-negative bounds, and spatial GeoJSON join integrity across the telecom pipeline.
---

# Telecom Data Quality Skill

## Core Invariants Enforced
1. **Canonical Grain Uniqueness**:
   - Exactly one record per `(grid_id, timestamp)` in the analytics layer.
   - Any duplicate indicates an incomplete or broken country-code aggregation.
2. **Non-Negative Activity**:
   - Activity measures must satisfy $x \ge 0.0$. Negative values are corrupt.
3. **GeoJSON Key Alignment**:
   - Joins against `data/reference/milano-grid.geojson` must use `properties.cellId` exclusively.
   - Flag any joins targeting feature array indexes.
