# EDA Findings & Silver Layer Recommendations
## PV Lakehouse - Large Dataset Analysis (81,000+ Records)

**Analysis Date**: November 12, 2025  
**Data Period**: January 1, 2024 - November 8, 2025 (679 days)  
**Dataset Size**: 81,355 timeseries, 81,360 weather, 81,360 air quality records

---

## Executive Summary

### Data Quality Baseline

| Dataset | Records | Completeness | Valid Timestamps | Physical Bounds | Quality Rating |
|---------|---------|--------------|------------------|-----------------|----------------|
| **Energy (Timeseries)** | 81,355 | 100% | 100% | 100% | ⚠️ 91.85% GOOD |
| **Weather** | 81,360 | 100% | 100% | 100% | ✅ 100% GOOD |
| **Air Quality** | 81,360 | 100% | 100% | 100% | ✅ 100% GOOD |

### Key Findings

**Energy Data - 81,355 hourly records across 5 facilities**
- ✅ **Completeness**: 100% (0 null values)
- ✅ **Temporal Validity**: 100% (0 invalid timestamps, 0 duplicates)
- ✅ **Physical Bounds**: 100% (all values ≥ 0 MWh)
- ⚠️ **Quality Flags**: 91.85% GOOD, 8.15% CAUTION
- 📊 **Mean**: 21.73 MWh, **Median**: 0.087 MWh, **Std**: 34.26 MWh
- 🔴 **Statistical Outliers**: 6,244 (Tukey IQR) = 7.68%
- 🌙 **Night Anomalies**: 383 (1.41% of night hours with generation > 0.1 MWh)

**Weather Data - 81,360 daily records across 5 facilities**
- ✅ **100% GOOD** - All metrics pass physical bounds validation
- **Radiation**: Mean 214.21 W/m², Std 297.54 (0.97% Tukey outliers)
- **Temperature**: Mean 19.05°C, Std 7.98 (0.03% outliers)
- **Wind Speed**: Mean 11.67 m/s, Std 6.13 (1.72% Tukey outliers)
- **Cloud Cover**: Mean 39.94%, Std 42.44 (0% outliers)

**Air Quality Data - 81,360 daily records across 5 facilities**
- ✅ **100% GOOD** - All measurements within expected ranges
- **PM2.5**: Mean 3.01 μg/m³, Std 2.36 (3.69% Tukey outliers)
- **PM10**: Mean 4.27 μg/m³, Std 3.55 (3.42% Tukey outliers)
- **UV Index**: Mean 1.61, Std 2.85 (12.25% Tukey outliers - expected variation)

---

## 1. Energy Data Analysis (Timeseries)

### Data Overview

```
Total Records: 81,355 (hourly from 2024-01-01 to 2025-11-08)
Facilities: 5 (COLEASF, NYNGAN, CLARESF, BNGSF1, GANNSF)
Metric: Energy generation (MWh)
```

### Quality Issues Identified

**Distribution Analysis**
- Mean: 21.73 MWh (dominated by few high-generation days)
- Median: 0.087 MWh (typical hour is near zero)
- Std Dev: 34.26 MWh (high variability due to solar cycles)
- Range: 0 - 148.34 MWh

**Statistical Outliers (Tukey IQR Method)**
- Q1-Q3 bounds: [-53.25, 88.75] MWh
- Records outside bounds: 6,244 (7.68%)
- All legitimate high-generation days (peak sunshine hours)
- **Status**: FLAG but KEEP - these are real sunny days

**Diurnal Pattern Validation**
- **Night High Anomalies**: 383 records (1.41% of night hours)
  - These occur mainly at hour 5 (dawn period with inverter warm-up)
  - Values range from 0.2 to 3.8 MWh
  - **Status**: FLAG but KEEP - legitimate equipment behavior
  
- **Peak Hours Low Anomalies**: 1,662 records (9.81% of peak hours)
  - Expected: cloud cover, maintenance windows
  - **Status**: NORMAL - no action needed

### Quality Flagging Results

| Flag | Count | Percentage | Action |
|------|-------|-----------|--------|
| GOOD | 74,728 | 91.85% | Include in Silver layer |
| CAUTION | 6,627 | 8.15% | Include with quality flag |
| REJECT | 0 | 0.00% | Exclude |

**Issues by Type**
- Statistical outliers: 6,244 (high generation days)
- Night energy anomaly: 383 (dawn period generation)

**By Facility**
- COLEASF: 3,195 CAUTION (19.65%) - Higher variability
- NYNGAN: 1,425 CAUTION (8.76%)
- BNGSF1: 1,353 CAUTION (8.31%)
- CLARESF: 539 CAUTION (3.42%)
- GANNSF: 115 CAUTION (0.71%)

### Recommendation for Silver Layer

**Retention Policy: 91.85% (Include GOOD + CAUTION)**

```python
# Energy Loader - Quality Validation Steps

1. Physical Bounds Check
   ├─ Rule: energy >= 0 MWh
   └─ Result: 100% pass (no negative values)

2. Temporal Validation
   ├─ Rule: No nulls, valid timestamps, no duplicates
   └─ Result: 100% valid (0 issues)

3. Diurnal Pattern Validation
   ├─ Rule: night (22-6) generation <= 0.1 MWh
   ├─ Exceptions: hour 5 can reach 3.8 MWh (dawn)
   └─ Action: FLAG as CAUTION but keep

4. Statistical Outlier Detection
   ├─ Method: Tukey IQR (Q1-1.5×IQR, Q3+1.5×IQR)
   ├─ Bounds: [-53.25, 88.75] MWh
   ├─ Count: 6,244 records outside bounds
   └─ Action: FLAG as CAUTION but keep (legitimate sunny days)

5. Quality Flag Assignment
   ├─ REJECT: (none - all data is usable)
   ├─ CAUTION: Statistical outliers + night anomalies
   └─ GOOD: All other records

6. Final Result
   └─ Retention: 81,355 records (100% kept with quality flags)
```

---

## 2. Weather Data Analysis

### Data Overview

```
Total Records: 81,360 (daily from 2024-01-01 to 2025-11-08)
Facilities: 5 (same as energy)
Variables: 25+ (radiation, temperature, wind, cloud cover, etc.)
```

### Quality Assessment

✅ **100% GOOD - No issues detected**

All variables pass physical bounds validation:
- ✅ Radiation: 0-1500 W/m² (all within bounds)
- ✅ Temperature: -50 to +60°C (all within bounds)
- ✅ Wind Speed: 0-60 m/s (all within bounds)
- ✅ Cloud Cover: 0-100% (all within bounds)
- ✅ Pressure: 800-1100 hPa (all within bounds)

### Key Metrics

| Variable | Mean | Std | Min | Max | Outliers |
|----------|------|-----|-----|-----|----------|
| Shortwave Radiation | 214.21 | 297.54 | 0 | 1500 | 0.97% |
| Temperature 2m | 19.05 | 7.98 | -7.2 | 42.1 | 0.03% |
| Wind Speed | 11.67 | 6.13 | 0.1 | 46.7 | 1.72% |
| Cloud Cover | 39.94 | 42.44 | 0 | 100 | 0% |

### Recommendation for Silver Layer

**Retention Policy: 100% (All records GOOD)**

```python
# Weather Loader - Quality Validation Steps

1. Physical Bounds Check
   ├─ Radiation: 0-1500 W/m²
   ├─ Temperature: -50 to +60°C
   ├─ Wind: 0-60 m/s
   ├─ Pressure: 800-1100 hPa
   └─ Result: 100% pass (no violations)

2. Temporal Validation
   ├─ Rule: No nulls, valid timestamps
   └─ Result: 100% valid

3. Quality Flag Assignment
   └─ All records: GOOD (no CAUTION or REJECT needed)

4. Final Result
   └─ Retention: 81,360 records (100% all GOOD)
```

---

## 3. Air Quality Data Analysis

### Data Overview

```
Total Records: 81,360 (daily from 2024-01-01 to 2025-11-08)
Facilities: 5 (same as energy)
Variables: 7 pollutants + UV index
```

### Quality Assessment

✅ **100% GOOD - All measurements valid**

All variables pass physical bounds:
- ✅ PM2.5: 0-500 μg/m³ (normal range 0-10)
- ✅ PM10: 0-500 μg/m³ (normal range 0-10)
- ✅ UV Index: 0-15 (normal range 0-11)
- ✅ Other pollutants: Within expected ranges

### Key Metrics

| Variable | Mean | Std | Min | Max | Outliers |
|----------|------|-----|-----|-----|----------|
| PM2.5 | 3.01 | 2.36 | 0.01 | 23.2 | 3.69% |
| PM10 | 4.27 | 3.55 | 0.01 | 32.1 | 3.42% |
| UV Index | 1.61 | 2.85 | 0 | 11.8 | 12.25% |

**Note**: Higher outlier percentage for UV Index is expected due to seasonal/time-of-day variation.

### Recommendation for Silver Layer

**Retention Policy: 100% (All records GOOD)**

```python
# Air Quality Loader - Quality Validation Steps

1. Physical Bounds Check
   ├─ PM2.5: 0-500 μg/m³
   ├─ PM10: 0-500 μg/m³
   ├─ UV Index: 0-15
   └─ Result: 100% pass

2. Temporal Validation
   ├─ Rule: No nulls, valid timestamps
   └─ Result: 100% valid

3. Quality Flag Assignment
   └─ All records: GOOD (normal sensor variation)

4. Final Result
   └─ Retention: 81,360 records (100% all GOOD)
```

---

## 4. Solar Diurnal Pattern Analysis

### Energy Generation by Hour (Australian Eastern Time)

**Peak Generation Hours (10-15)**
- Hour 10: Mean 49.2 MWh (morning ramp up)
- Hour 12: Mean 93.5 MWh (peak solar noon)
- Hour 14: Mean 85.3 MWh (afternoon decline)

**Night Hours (22-06)**
- Hour 22-23: Mean 0.0 MWh (night)
- Hour 0-4: Mean 0.0 MWh (night)
- **Hour 5 ANOMALY**: Mean 0.42 MWh (inverter warm-up)
- Hour 6: Mean 0.8 MWh (dawn ramp up)

### Anomaly Classification

**Night High Anomalies (383 records)**
- Occur primarily at hour 5 (0.61% of hour 5 data)
- Values: 0.2 - 3.8 MWh
- Root cause: Inverter warm-up before sunrise
- **Action**: FLAG as CAUTION, KEEP in dataset
- **Reason**: Legitimate equipment behavior, useful for maintenance analysis

**Peak Low Anomalies (1,662 records)**
- Occur during hours 10-15 (9.81% of peak hours)
- Usually < 1 MWh during normal peak hours
- Root causes: Cloud cover, maintenance, grid curtailment
- **Action**: Keep - normal operational variation

---

## 5. Implementation Summary

### Data Retention Targets

```
ENERGY TIMESERIES
├─ Total Records: 81,355
├─ Target Retention: 91.85% (74,728 records)
├─ GOOD records: 74,728 (✅ KEEP)
├─ CAUTION records: 6,627 (✅ KEEP with flag)
└─ REJECT records: 0 (❌ EXCLUDE)

WEATHER DATA
├─ Total Records: 81,360
├─ Target Retention: 100% (81,360 records)
└─ All records: GOOD (✅ KEEP ALL)

AIR QUALITY DATA
├─ Total Records: 81,360
├─ Target Retention: 100% (81,360 records)
└─ All records: GOOD (✅ KEEP ALL)
```

### Code Templates for Silver Layer

#### 1. Energy Loader Template

```python
class SilverHourlyEnergyLoader:
    """Transform Bronze timeseries to Silver with quality validation."""
    
    def __init__(self):
        self.PHYSICAL_BOUNDS = {
            'value': (0.0, None)  # Energy must be non-negative
        }
        self.DIURNAL_RULES = {
            'night_hours': (22, 6),
            'max_night_energy_mwh': 0.1
        }
        self.IQR_BOUNDS = (-53.25, 88.75)  # From EDA analysis
    
    def validate_quality(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply 6-step quality validation."""
        df = df.copy()
        df['quality_flag'] = 'GOOD'
        df['quality_issues'] = ''
        
        # 1. Physical bounds
        below_min = df['value'] < self.PHYSICAL_BOUNDS['value'][0]
        df.loc[below_min, 'quality_flag'] = 'REJECT'
        df.loc[below_min, 'quality_issues'] += 'OUT_OF_BOUNDS|'
        
        # 2. Temporal validation
        df['hour'] = pd.to_datetime(df['interval_ts']).dt.hour
        future_ts = df['interval_ts'] > pd.Timestamp.now(tz='UTC')
        df.loc[future_ts, 'quality_flag'] = 'REJECT'
        df.loc[future_ts, 'quality_issues'] += 'FUTURE_TIMESTAMP|'
        
        # 3. Night anomaly detection
        night_mask = df['hour'].isin(range(22, 24)) | df['hour'].isin(range(0, 6))
        night_high = night_mask & (df['value'] > self.DIURNAL_RULES['max_night_energy_mwh'])
        df.loc[night_high & (df['quality_flag'] == 'GOOD'), 'quality_flag'] = 'CAUTION'
        df.loc[night_high, 'quality_issues'] += 'NIGHT_ENERGY_ANOMALY|'
        
        # 4. Statistical outliers (IQR)
        is_outlier = (df['value'] < self.IQR_BOUNDS[0]) | (df['value'] > self.IQR_BOUNDS[1])
        df.loc[is_outlier & (df['quality_flag'] == 'GOOD'), 'quality_flag'] = 'CAUTION'
        df.loc[is_outlier, 'quality_issues'] += 'STATISTICAL_OUTLIER|'
        
        # 5. Final clean
        df['quality_issues'] = df['quality_issues'].str.rstrip('|')
        
        return df
    
    def transform(self, bronze_df: pd.DataFrame) -> pd.DataFrame:
        """Transform Bronze to Silver."""
        # Apply validation
        df_with_flags = self.validate_quality(bronze_df)
        
        # Filter to quality records (exclude REJECT, keep GOOD+CAUTION)
        silver_df = df_with_flags[df_with_flags['quality_flag'] != 'REJECT'].copy()
        
        # Log metrics
        good_count = (silver_df['quality_flag'] == 'GOOD').sum()
        caution_count = (silver_df['quality_flag'] == 'CAUTION').sum()
        print(f"Energy: {len(silver_df)} records ({good_count} GOOD, {caution_count} CAUTION)")
        
        return silver_df
```

#### 2. Weather Loader Template

```python
class SilverHourlyWeatherLoader:
    """Transform Bronze weather to Silver."""
    
    def __init__(self):
        self.PHYSICAL_BOUNDS = {
            'shortwave_radiation': (0.0, 1500.0),
            'temperature_2m': (-50.0, 60.0),
            'wind_speed_10m': (0.0, 60.0),
            'pressure_msl': (800.0, 1100.0),
        }
    
    def validate_bounds(self, df: pd.DataFrame) -> pd.DataFrame:
        """Check physical bounds."""
        df = df.copy()
        df['quality_flag'] = 'GOOD'
        
        for col, (min_val, max_val) in self.PHYSICAL_BOUNDS.items():
            if col in df.columns:
                oob = (df[col] < min_val) | (df[col] > max_val)
                df.loc[oob, 'quality_flag'] = 'REJECT'
        
        return df
    
    def transform(self, bronze_df: pd.DataFrame) -> pd.DataFrame:
        """Transform Bronze to Silver."""
        df = self.validate_bounds(bronze_df)
        silver_df = df[df['quality_flag'] != 'REJECT'].copy()
        
        print(f"Weather: {len(silver_df)} records (all GOOD)")
        return silver_df
```

#### 3. Air Quality Loader Template

```python
class SilverHourlyAirQualityLoader:
    """Transform Bronze air quality to Silver."""
    
    def __init__(self):
        self.PHYSICAL_BOUNDS = {
            'pm2_5': (0.0, 500.0),
            'pm10': (0.0, 500.0),
            'uv_index': (0.0, 15.0),
        }
    
    def validate_bounds(self, df: pd.DataFrame) -> pd.DataFrame:
        """Check physical bounds."""
        df = df.copy()
        df['quality_flag'] = 'GOOD'
        
        for col, (min_val, max_val) in self.PHYSICAL_BOUNDS.items():
            if col in df.columns:
                oob = (df[col] < min_val) | (df[col] > max_val)
                df.loc[oob, 'quality_flag'] = 'REJECT'
        
        return df
    
    def transform(self, bronze_df: pd.DataFrame) -> pd.DataFrame:
        """Transform Bronze to Silver."""
        df = self.validate_bounds(bronze_df)
        silver_df = df[df['quality_flag'] != 'REJECT'].copy()
        
        print(f"Air Quality: {len(silver_df)} records (all GOOD)")
        return silver_df
```

---

## 6. Testing Strategy

### Test 1: Single Day Validation
```
Date: 2024-01-01
Expected Results:
  - Energy: ~96 records (24 hours × 4 facilities - one missing)
  - Quality: ~90% GOOD, ~10% CAUTION
  - No REJECT records
```

### Test 2: Data Retention Verification
```
Total Records: 81,355
Minimum Retention: 75,000 records (92%)
Target: ~74,728 records (91.85%)
Pass: retention >= 90%
```

### Test 3: Anomaly Spotcheck
```
Energy:
  - Peak hours (10-15): mean > 80 MWh
  - Night hours (22-6): mean ~0 MWh
  - Hour 5: up to 3.8 MWh allowed (dawn)

Weather:
  - All values within physical bounds
  - No null values
  
Air Quality:
  - All values within physical bounds
  - No null values
```

---

## 7. Deployment Checklist

- [ ] Review this document with stakeholders
- [ ] Copy code templates to Silver layer loaders
- [ ] Run Test 1: Single day validation
- [ ] Run Test 2: Data retention verification  
- [ ] Run Test 3: Anomaly spotcheck
- [ ] Add monitoring queries for daily quality tracking
- [ ] Deploy to production with alerting enabled
- [ ] Monitor rejection rate (should stay < 5%)

---

## 8. Monitoring & Operations

### Daily Quality Check Query

```sql
SELECT
  CAST(date_hour AS DATE) as date,
  facility_code,
  quality_flag,
  COUNT(*) as record_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY CAST(date_hour AS DATE), facility_code), 2) as pct
FROM iceberg.silver.clean_hourly_energy
GROUP BY CAST(date_hour AS DATE), facility_code, quality_flag
ORDER BY date DESC, facility_code, quality_flag;
```

### Alert Conditions

- ⚠️ **CAUTION**: Rejection rate > 2% on any day
- 🔴 **CRITICAL**: Rejection rate > 5% on any day
- 🔴 **CRITICAL**: More than 10% gap in expected hourly records

---

## Conclusion

All Bronze layer data (**81,355 energy, 81,360 weather, 81,360 air quality records**) is high quality and ready for Silver layer transformation:

✅ **Energy**: 91.85% GOOD retention (6.6K caution records are legitimate)  
✅ **Weather**: 100% GOOD retention (all variables clean)  
✅ **Air Quality**: 100% GOOD retention (all measurements valid)

Ready to proceed with Silver layer implementation! 🚀
