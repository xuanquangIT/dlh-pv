# Summary: Silver Layer - Outlier Detection & Data Quality Implementation

## What Has Been Created

### 1. 📊 EDA Notebook (Exploratory Data Analysis)
**File**: `src/pv_lakehouse/etl/scripts/notebooks/eda_outlier_detection.ipynb`

**Contents**:
- Import dependencies and load Bronze layer data
- Descriptive statistics (mean, median, std, min, max)
- Physical bounds validation for each variable
- Temporal anomaly detection (timestamps, duplicates, gaps)
- Distribution analysis and statistical outliers (IQR, Z-score, 3-sigma)
- Time-of-day pattern validation (solar diurnal cycles)
- Data quality visualization (histograms, box plots, time series with outliers)
- Outlier flagging with composite quality scores
- Summary report with recommendations

**How to use**:
```bash
cd /home/pvlakehouse/dlh-pv
jupyter notebook src/pv_lakehouse/etl/scripts/notebooks/eda_outlier_detection.ipynb
```

---

### 2. 🔧 Outlier Handler Module
**File**: `src/pv_lakehouse/etl/silver/outlier_handler.py`

**Key Functions**:
- `add_physical_bounds_flag()` - Validate against min/max ranges
- `add_temporal_flags()` - Check for null/future timestamps
- `add_diurnal_pattern_flags()` - Validate solar generation patterns
- `add_statistical_outlier_flags()` - Detect IQR or Z-score outliers
- `add_composite_quality_flag()` - Combine all validations
- `filter_by_quality()` - Filter by quality level
- `get_quality_summary()` - Get quality statistics

**Import Example**:
```python
from pv_lakehouse.etl.silver.outlier_handler import (
    add_physical_bounds_flag,
    add_temporal_flags,
    add_composite_quality_flag,
)
```

---

### 3. 📚 Documentation Files

#### A. Comprehensive Guide
**File**: `doc/OUTLIER_DETECTION_GUIDE.md`

Contains:
- Physical bounds reference table (for all variables)
- Quality flag levels explained (GOOD, CAUTION, REJECT)
- Quality issue codes (OUT_OF_BOUNDS, INVALID_TIMESTAMP, etc.)
- Diurnal pattern rules for solar
- Statistical methods (Tukey's IQR, Z-score)
- 3 implementation strategies (strict/moderate/permissive)
- Code examples for each dataset type
- Monitoring and alerting guidance
- Common issues and troubleshooting

#### B. Quick Reference
**File**: `doc/OUTLIER_DETECTION_QUICK_REFERENCE.md`

Contains:
- 3-minute quick start guide
- Common issues & fixes table
- Quality flag levels at a glance
- Key metrics by dataset
- Next steps (immediate/short-term/long-term)
- Validation checklist

---

### 4. 💡 Example Implementations
**File**: `src/pv_lakehouse/etl/silver/examples_with_quality.py`

Contains 3 example loaders:
1. `SilverHourlyEnergyLoaderWithQuality` - Energy with diurnal validation
2. `SilverHourlyWeatherLoaderWithQuality` - Weather with moderate bounds
3. `SilverHourlyAirQualityLoaderWithQuality` - Air quality with lenient thresholds

Each example shows:
- How to integrate quality validation functions
- Appropriate bounds for each dataset
- Quality summary logging
- Warning thresholds

---

## Quality Validation Framework

### Physical Bounds (Domain Knowledge)

**Energy**: 0 ≤ energy ≤ ∞ MWh
- No negative values allowed
- Night generation anomaly if > 0.1 MWh (22:00-06:00)

**Weather**:
- Temperature: -50 to +60°C
- Radiation: 0-1500 W/m²
- Wind: 0-60 m/s
- Pressure: 800-1100 hPa

**Air Quality**:
- PM2.5, PM10: 0-500 μg/m³
- UV Index: 0-15

### Temporal Validation

- ✅ Timestamps must be non-null
- ✅ Timestamps must be ≤ current time
- ✅ No duplicate facility-hour pairs
- ✅ No gaps > 24 hours (investigate)

### Statistical Validation

**Tukey's IQR Method**:
```
Outliers = values outside [Q1 - 1.5×IQR, Q3 + 1.5×IQR]
Catches ~0.7% of normal data
```

**Z-Score Method (3-sigma)**:
```
Outliers = |Z| > 3.0 (99.7% confidence)
```

### Solar-Specific Validation (Diurnal)

```
Night (22:00-06:00): energy should be ~0
  ├─ Flag HIGH if > 0.1 MWh
  └─ Suggests equipment malfunction

Peak (10:00-15:00): energy should be HIGH
  ├─ Flag LOW if < 1.0 MWh
  └─ May indicate cloud cover or downtime
```

---

## Quality Flags

| Flag | Meaning | Action |
|------|---------|--------|
| ✅ GOOD | Passes all checks | Use in production |
| ⚠️ CAUTION | Anomalies detected but within bounds | Flag for review |
| ❌ REJECT | Out of bounds or invalid | Exclude from production |

---

## Implementation Strategies

### Strategy 1: Flag Only (Recommended First)
```python
# Add quality columns, keep all records
result = add_physical_bounds_flag(result)
result = add_temporal_flags(result, 'date_hour')
result = add_composite_quality_flag(result, 'date_hour', 'energy_mwh')
return result  # Include quality_flag and quality_issues columns
```
- **Retention**: 100%
- **Use for**: Understanding data provenance
- **Benefit**: Can adjust filtering later without re-running

### Strategy 2: Moderate Filtering
```python
# Exclude REJECT, keep GOOD+CAUTION
result = filter_by_quality(result, keep_good=True, keep_caution=True)
return result
```
- **Retention**: 95-99%
- **Use for**: Analytics, dashboards
- **Benefit**: Balance between data completeness and quality

### Strategy 3: Strict Filtering
```python
# Keep only GOOD records
result = filter_by_quality(result, keep_good=True)
return result
```
- **Retention**: 85-95%
- **Use for**: Financial reporting, critical metrics
- **Benefit**: Highest confidence in data quality

---

## Expected Results

### Energy Data (Solar)
```
Total records: 1,000,000
  ✅ GOOD: 920,000 (92%)
  ⚠️ CAUTION: 60,000 (6%)
  ❌ REJECT: 20,000 (2%)

Top issues:
  - STATISTICAL_OUTLIER: 45,000
  - NIGHT_HIGH_ENERGY: 15,000
  - OUT_OF_BOUNDS: 20,000
```

### Weather Data
```
Total records: 1,500,000
  ✅ GOOD: 1,470,000 (98%)
  ⚠️ CAUTION: 20,000 (1.3%)
  ❌ REJECT: 10,000 (0.7%)

(Higher quality - fewer outliers than energy)
```

### Air Quality Data
```
Total records: 1,500,000
  ✅ GOOD: 1,485,000 (99%)
  ⚠️ CAUTION: 12,000 (0.8%)
  ❌ REJECT: 3,000 (0.2%)

(Highest quality - most consistent sensors)
```

---

## Quick Start (5 Minutes)

### 1. Run the EDA Notebook
```bash
jupyter notebook src/pv_lakehouse/etl/scripts/notebooks/eda_outlier_detection.ipynb
```

### 2. Review Findings
- Check quality statistics for your data
- Understand which records are flagged and why
- Review visualization charts

### 3. Choose Implementation Strategy
- **Option A**: Add quality columns (recommended first)
- **Option B**: Filter based on quality (moderate)
- **Option C**: Strict filtering (production use)

### 4. Implement in Silver Loader
```python
from pv_lakehouse.etl.silver.outlier_handler import *

# Add validations
result = add_physical_bounds_flag(result)
result = add_temporal_flags(result, 'date_hour')
result = add_composite_quality_flag(result, 'date_hour', 'energy_mwh')

# Log quality metrics
summary = get_quality_summary(result)
print(f"Quality: {summary['GOOD_pct']:.2f}% GOOD records")

return result
```

### 5. Monitor Quality Over Time
Track rejection rates by facility and date to catch data quality issues early.

---

## Files Summary

| File | Purpose | When to Use |
|------|---------|-------------|
| `eda_outlier_detection.ipynb` | Analyze data quality | Initial assessment |
| `outlier_handler.py` | Reusable validation functions | In all Silver loaders |
| `examples_with_quality.py` | Example implementations | Reference for integration |
| `OUTLIER_DETECTION_GUIDE.md` | Comprehensive documentation | Implementation guidance |
| `OUTLIER_DETECTION_QUICK_REFERENCE.md` | Quick start guide | First time users |

---

## Next Steps

### Immediate (Today)
- [ ] Review this summary
- [ ] Run EDA notebook on your data
- [ ] Share findings with team

### This Week
- [ ] Integrate outlier_handler into one Silver loader (e.g., energy)
- [ ] Test on 1 week of data
- [ ] Monitor quality metrics
- [ ] Document any facility-specific adjustments needed

### This Month
- [ ] Integrate into all Silver loaders
- [ ] Set up quality monitoring dashboard
- [ ] Define data quality SLA
- [ ] Create alerting rules for quality degradation

---

## Support & Resources

- **EDA Analysis**: `src/pv_lakehouse/etl/scripts/notebooks/eda_outlier_detection.ipynb`
- **Implementation Guide**: `doc/OUTLIER_DETECTION_GUIDE.md`
- **Quick Reference**: `doc/OUTLIER_DETECTION_QUICK_REFERENCE.md`
- **Code Examples**: `src/pv_lakehouse/etl/silver/examples_with_quality.py`
- **Utility Functions**: `src/pv_lakehouse/etl/silver/outlier_handler.py`

---

**Created**: 2025-11-12
**Version**: 1.0
**Status**: Ready to use ✅
