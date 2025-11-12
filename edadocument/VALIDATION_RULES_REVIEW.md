# Validation Rules Review & Analysis

## Current Validation Issues

### 1. Energy Loader - TUKEY_LOWER = -53.25 ❌
**Problem**: Energy generation cannot be negative!
- Current: `-53.25 to 88.75 MWh` (Tukey IQR)
- Issue: Negative lower bound is unrealistic for solar generation
- Recommendation: Change to `0.0 to 88.75 MWh`

**Reason**: Tukey IQR method on real data included some anomalies:
- Q1 = 0 MWh (25th percentile)
- Q3 = 35.5 MWh (75th percentile)
- IQR = 35.5 MWh
- Tukey lower = Q1 - 1.5*IQR = 0 - 53.25 = -53.25 ❌

**Fix**: Cap lower bound at 0 since energy must be non-negative

---

### 2. Weather Loader - Radiation Bounds ✅
**Current**: `0.0 to 1000.0 W/m²` (Updated from 1500)
**Status**: Good
**Rationale**:
- Extraterrestrial: 1361 W/m²
- Atmospheric max: ~1000 W/m² (realistic)
- This is correct

---

### 3. Air Quality Loader - Bounds ✅
**Current**: Various, e.g., `pm2_5: (0.0, 500.0)` μg/m³
**Status**: Good for most parameters
**Note**: AQI calculation already validates range 0-500

---

## Validation Logic Flow

### Energy Loader Validation:
```
1. Physical bounds: energy_mwh >= 0
   - REJECT if violated
   
2. Temporal: timestamp is not null
   - REJECT if violated
   
3. Night anomaly: energy > 0.1 MWh between 22:00-06:00
   - CAUTION if violated
   
4. Statistical outlier: energy > 88.75 MWh
   - CAUTION if violated
```

### Weather Loader Validation:
```
1. Bounds check: All columns within ranges
   - REJECT if any violated
   
2. Night radiation: > 50 W/m² between 22:00-06:00
   - REJECT if violated
   
3. Night radiation caution: 10-50 W/m² at night
   - CAUTION if violated
   
4. Sunrise spike: > 500 W/m² at 6am
   - REJECT if violated
```

### Air Quality Loader Validation:
```
1. Bounds check: All pollutants within ranges
   - CAUTION if violated
   
2. AQI range: 0-500
   - CAUTION if violated
```

---

## Recommended Fixes

### Energy Loader - PRIORITY: HIGH
**Change TUKEY_LOWER from -53.25 to 0.0**

Rationale:
- Solar generation cannot be negative
- Tukey IQR includes unrealistic bounds
- Should use physical bounds (>= 0) not statistical bounds for lower bound

### Weather Loader - PRIORITY: MEDIUM
**Consider tightening radiation bounds**
- Current upper: 1000 W/m²
- Realistic peak: 950 W/m²
- Could use 950 to catch more anomalies

### Overall - PRIORITY: MEDIUM
**Standardize quality flag logic across all loaders**
- GOOD: All checks pass
- CAUTION: Minor issues (statistical anomalies, borderline values)
- REJECT: Physical/temporal violations

---

## Expected Impact After Fixes

### Before Fix:
```
Energy:
- GOOD: 93.48%
- CAUTION: 6.52%
- REJECT: 0%

Issue: Tukey lower bound of -53.25 never triggered REJECT
```

### After Fix (Expected):
```
Energy:
- GOOD: ~93-94% (same, most records are positive)
- CAUTION: ~6-7% (same)
- REJECT: 0% (unchanged, fix doesn't add rejections)

Benefit: Cleaner logic, more defensible
```

---

## Action Plan

1. Fix `TUKEY_LOWER` from -53.25 → 0.0 in energy loader
2. Drop Silver tables
3. Re-run all 3 loaders with --mode full
4. Compare metrics:
   - Quality distributions
   - Correlation scores
   - Anomaly patterns
5. Verify improvements
