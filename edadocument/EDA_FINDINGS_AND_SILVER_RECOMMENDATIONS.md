# EDA Analysis - Data Quality Findings & Silver Layer Recommendations

## Executive Summary

Analysis of Bronze layer data (Oct 1 - Nov 8, 2025) reveals **high-quality data** with minimal anomalies:
- **Energy**: 89.97% GOOD, 10.03% CAUTION (mostly statistical outliers from high generation days)
- **Weather**: Clean temporal data, normal distributions  
- **Air Quality**: Normal variations, sensor-expected patterns

---

## Key Findings

### 1. ENERGY DATA (Timeseries) - 4,675 Records

**Quality Status**: ✅ EXCELLENT
- **Total Records**: 4,675 (5 facilities × ~935 records each)
- **Date Range**: Oct 1, 2025 - Nov 8, 2025 (39 days)
- **Null Values**: 0 (100% complete)
- **Future Timestamps**: 0 (all valid)
- **Duplicates**: 0 (no key violations)

**Distribution Analysis**:
- **Mean**: 18.80 MWh
- **Median**: 2.08 MWh  
- **Std Dev**: 31.41 MWh
- **Min**: 0.0 MWh
- **Max**: 147.74 MWh

**Outlier Detection** (IQR Method):
- **Tukey Bounds**: [-39.58, 65.97] MWh
- **Statistical Outliers**: 434 (9.28%) - mostly legitimate peak generation days
- **Z-score Outliers (>3σ)**: 127 (2.72%)

**Diurnal Pattern Analysis** (Solar Specific):
- **Night High Anomalies**: 35 records (2.25%) - mostly hour 5 (early morning), values 0.11-0.43 MWh
  - Likely: Inverter warm-up, measurement noise, or equipment testing
  - **Action**: Flag but not exclude - legitimate equipment behavior
  
- **Peak Low Anomalies**: 152 records (15.59%) - noon hours with generation <1.0 MWh
  - Likely: Cloud cover, scheduled maintenance, or weather variations
  - **Action**: Expected behavior, no issue

**Facilities Profile**:
```
BNGSF1    935 records
CLARESF   935 records
COLEASF   935 records
GANNSF    935 records
NYNGAN    935 records
```

---

### 2. WEATHER DATA (Daily) - 4,680 Records

**Quality Status**: ✅ EXCELLENT
- **Null Values**: 0 (all complete)
- **Future Dates**: 0
- **Duplicates**: 0

**Key Variables - Normal Distributions**:

| Variable | Mean | Std Dev | Tukey Outliers | Notes |
|----------|------|---------|----------------|-------|
| Shortwave Radiation | 258.63 W/m² | 330.12 | 0 (0%) | Clean |
| Temperature | 20.17°C | 6.82 | 0 (0%) | Normal seasonal |
| Wind Speed | 12.33 m/s | 6.99 | 50 (1.07%) | Expected variability |
| Cloud Cover | 47.09% | 41.70 | 0 (0%) | Normal |

**Status**: All variables within physical bounds. No data quality issues detected.

---

### 3. AIR QUALITY DATA (Daily) - 4,680 Records

**Quality Status**: ✅ GOOD
- **Null Values**: 0 (all complete)
- **Temporal Validity**: Perfect

**Key Pollutants**:

| Pollutant | Mean | Std Dev | Tukey Outliers | Notes |
|-----------|------|---------|----------------|-------|
| PM2.5 | 3.05 μg/m³ | 2.44 | 143 (3.06%) | Low values, normal variation |
| PM10 | 4.23 μg/m³ | 3.50 | 131 (2.80%) | Low values, normal variation |
| UV Index | 1.88 | 2.88 | 395 (8.44%) | Expected seasonal variation |

**Status**: All values within expected ranges for Australian solar locations. No concerning patterns.

---

## Silver Layer - Recommendations

### 1. **Energy Loader** (`clean_hourly_energy`)

**Current Implementation**: Check and improve with:

```python
# Add quality columns
energy_df['quality_flag'] = 'GOOD'  # Default
energy_df['quality_issues'] = ''

# Physical bounds (all energy >= 0)
energy_df.loc[energy_df['value'] < 0, 'quality_flag'] = 'REJECT'

# Temporal validation
energy_df['interval_ts'] = pd.to_datetime(energy_df['interval_ts'], utc=True)
invalid_ts = energy_df['interval_ts'].isna() | (energy_df['interval_ts'] > pd.Timestamp.now(tz='UTC'))
energy_df.loc[invalid_ts, 'quality_flag'] = 'REJECT'

# Diurnal pattern validation
energy_df['hour'] = energy_df['interval_ts'].dt.hour
night_high = energy_df['hour'].isin(range(22, 24)) | energy_df['hour'].isin(range(0, 6))
night_high &= energy_df['value'] > 0.1
energy_df.loc[night_high & (energy_df['quality_flag'] == 'GOOD'), 'quality_flag'] = 'CAUTION'

# Statistical outlier detection (IQR)
Q1 = energy_df['value'].quantile(0.25)
Q3 = energy_df['value'].quantile(0.75)
IQR = Q3 - Q1
lower = Q1 - 1.5 * IQR
upper = Q3 + 1.5 * IQR
is_outlier = (energy_df['value'] < lower) | (energy_df['value'] > upper)
energy_df.loc[is_outlier & (energy_df['quality_flag'] == 'GOOD'), 'quality_flag'] = 'CAUTION'

# Filtering strategy
# Option 1: Keep all but tag quality
# Option 2: Exclude REJECT records  
# Option 3: Exclude REJECT + CAUTION for strict compliance

result = energy_df[energy_df['quality_flag'] == 'GOOD']  # Strict: 89.97% retained
```

**Expected Results**:
- ✅ 89.97% GOOD records (4,206)
- ⚠️ 10.03% CAUTION records (469) - flag for monitoring
- ❌ 0 REJECT records (no out-of-bounds data)
- Data retention: **90% with high confidence**

---

### 2. **Weather Loader** (`clean_hourly_weather`)

**Recommendations**:
- ✅ Data quality is excellent - minimal changes needed
- Add null checks for each variable
- Add physical bounds validation (temp: -50 to 60°C, radiation: 0-1500 W/m², etc.)
- Keep CAUTION flagging for wind speed outliers (already normal)

```python
# Key bounds to enforce
bounds = {
    'shortwave_radiation': (0, 1500),
    'temperature_2m': (-50, 60),
    'wind_speed_10m': (0, 60),
    'cloud_cover': (0, 100),
}

# Validate all
for col, (min_val, max_val) in bounds.items():
    oob = (weather_df[col] < min_val) | (weather_df[col] > max_val)
    weather_df.loc[oob, 'quality_flag'] = 'REJECT'
```

**Expected Results**:
- ✅ 100% GOOD records with proper validation
- 0 REJECT records
- Data retention: **100%**

---

### 3. **Air Quality Loader** (`clean_hourly_air_quality`)

**Recommendations**:
- Add null checks (currently 0, good!)
- Add physical bounds for pollutants
- Allow some variability (sensors can drift)
- Monitor PM2.5 spikes (health indicator)

```python
# Air quality bounds (generous for sensor variability)
bounds = {
    'pm2_5': (0, 500),
    'pm10': (0, 500),
    'uv_index': (0, 15),
}

# Moderate filtering (CAUTION for investigation, not REJECT)
air_df['quality_flag'] = 'GOOD'

for col, (min_val, max_val) in bounds.items():
    oob_low = air_df[col] < min_val
    oob_high = air_df[col] > max_val
    air_df.loc[oob_high, 'quality_flag'] = 'CAUTION'  # Flag spikes for review
```

**Expected Results**:
- ✅ 96-97% GOOD records  
- ⚠️ 3-4% CAUTION records (normal sensor variability)
- 0 REJECT records
- Data retention: **100% (all data usable)**

---

## Code Changes Summary

### Files to Update:

1. **`src/pv_lakehouse/etl/silver/hourly_energy.py`**
   - Add quality_flag column
   - Add diurnal validation
   - Consider rejection strategy (recommend: keep 90% GOOD)

2. **`src/pv_lakehouse/etl/silver/hourly_weather.py`**
   - Add physical bounds checks
   - Keep all data (100% retention)

3. **`src/pv_lakehouse/etl/silver/hourly_air_quality.py`**
   - Add physical bounds checks
   - Keep all data (100% retention)

---

## Next Steps

### Immediate (This Week):
1. ✅ Review EDA findings above
2. ⏳ Update energy loader with quality flagging (see code above)
3. ⏳ Test on 1 day of data, validate quality thresholds

### Short Term (This Month):
4. ⏳ Update weather and air quality loaders
5. ⏳ Deploy to Silver layer tables
6. ⏳ Monitor quality metrics in production

### Ongoing:
7. ⏳ Track rejection rates by facility and date
8. ⏳ Alert if rejection rate spikes > 5% (data quality issue)
9. ⏳ Quarterly review of thresholds

---

## Data Quality Baseline

**Established Nov 12, 2025**

| Metric | Energy | Weather | Air Quality |
|--------|--------|---------|-------------|
| Completeness | 100% | 100% | 100% |
| Temporal Validity | 100% | 100% | 100% |
| Physical Bounds | 100% | 100% | 100% |
| Statistical Quality | 89.97% GOOD | 99% GOOD | 96% GOOD |
| Recommended Retention | 90% | 100% | 100% |

✅ All Bronze layers are production-ready for Silver transformation!
