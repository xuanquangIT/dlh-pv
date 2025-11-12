# Quick Reference - Outlier Detection & Data Quality

## Files Created

1. **EDA Notebook** 
   - Location: `src/pv_lakehouse/etl/scripts/notebooks/eda_outlier_detection.ipynb`
   - Purpose: Comprehensive exploratory analysis with visualizations
   - Run: `jupyter notebook src/pv_lakehouse/etl/scripts/notebooks/eda_outlier_detection.ipynb`

2. **Outlier Handler Module**
   - Location: `src/pv_lakehouse/etl/silver/outlier_handler.py`
   - Purpose: Reusable utilities for quality validation
   - Import: `from pv_lakehouse.etl.silver.outlier_handler import *`

3. **Documentation**
   - Location: `doc/OUTLIER_DETECTION_GUIDE.md`
   - Purpose: Comprehensive implementation guide

---

## Quick Start (3 Minutes)

### Step 1: Run EDA Analysis

```bash
cd /home/pvlakehouse/dlh-pv
jupyter notebook src/pv_lakehouse/etl/scripts/notebooks/eda_outlier_detection.ipynb
```

**What you'll see:**
- Data shape and distribution
- Physical bounds violations
- Temporal anomalies
- Statistical outliers (IQR, 3-sigma)
- Diurnal pattern validation (solar-specific)
- Quality visualization charts
- Summary recommendations

### Step 2: Review Findings

Key metrics from the notebook:
```
ENERGY DATA:
  Total: 1,000,000 rows
  ✅ GOOD: 950,000 (95%)
  ⚠️ CAUTION: 40,000 (4%)
  ❌ REJECT: 10,000 (1%)

Quality issues breakdown:
  - STATISTICAL_OUTLIER: 35,000
  - NIGHT_HIGH_ENERGY: 5,000
  - OUT_OF_BOUNDS: 10,000
```

### Step 3: Choose Implementation Strategy

**Option A: Flag Only (Recommended First)**
```python
from pv_lakehouse.etl.silver.outlier_handler import *

# Add quality columns
result = add_physical_bounds_flag(result)
result = add_temporal_flags(result, 'date_hour')
result = add_composite_quality_flag(result, 'date_hour', 'energy_mwh')

# Keep all records, include quality columns
return result
```

**Option B: Filter Bad Data**
```python
# Exclude REJECT, keep GOOD+CAUTION
result = filter_by_quality(result, keep_good=True, keep_caution=True)
```

---

## Common Issues & Fixes

| Issue | Cause | Fix |
|-------|-------|-----|
| High rejection rate (>5%) | Incorrect bounds | Review PHYSICAL_BOUNDS in notebook |
| Night energy flagged | Solar equipment | Expected - use CAUTION level |
| Zero energy all day | System downtime | Valid - check maintenance logs |
| Negative energy | Sensor error | REJECT - out of bounds |
| Missing data clusters | API failures | Check ingest logs |

---

## Quality Flag Levels

| Flag | Use | Action |
|------|-----|--------|
| ✅ GOOD | Production, reporting | Use as-is |
| ⚠️ CAUTION | Analysis, research | Flag for review |
| ❌ REJECT | Exclude from analysis | Remove or investigate |

---

## Validation Checklist

- [ ] Run EDA notebook and review statistics
- [ ] Check facility-specific bounds are correct
- [ ] Validate against known good data
- [ ] Review examples of REJECT records
- [ ] Decide rejection strategy (strict/moderate/permissive)
- [ ] Implement in Silver loader
- [ ] Test on sample data
- [ ] Monitor rejection rates over time

---

## Key Metrics (By Dataset)

### Energy Data ☀️
- **Physical bounds**: 0 ≤ energy ≤ ∞ MWh
- **Night hours**: 22:00 - 06:00 (should be ~0)
- **Peak hours**: 10:00 - 15:00 (should be high)
- **Expected retention**: 90-99% GOOD records

### Weather Data 🌤️
- **Radiation**: 0-1500 W/m²
- **Temperature**: -50 to +60°C
- **Wind speed**: 0-60 m/s
- **Expected retention**: 95-99% GOOD records

### Air Quality Data 💨
- **PM2.5**: 0-500 μg/m³
- **PM10**: 0-500 μg/m³
- **UV Index**: 0-15
- **Expected retention**: 98-99% GOOD records

---

## Next Steps

1. **Immediate** (Today)
   - [ ] Run EDA notebook
   - [ ] Review quality statistics
   - [ ] Share findings with team

2. **Short-term** (This week)
   - [ ] Integrate outlier_handler into Silver loaders
   - [ ] Add quality_flag columns to Silver tables
   - [ ] Test on 1 week of data
   - [ ] Monitor for issues

3. **Long-term** (This month)
   - [ ] Define facility-specific bounds
   - [ ] Set up quality monitoring dashboard
   - [ ] Document data quality SLA
   - [ ] Create data quality alerts

---

## Support

- **EDA Questions**: See `eda_outlier_detection.ipynb`
- **Implementation Help**: See `doc/OUTLIER_DETECTION_GUIDE.md`
- **Code Examples**: See `src/pv_lakehouse/etl/silver/outlier_handler.py`

---

**Created**: 2025-11-12
**Version**: 1.0
