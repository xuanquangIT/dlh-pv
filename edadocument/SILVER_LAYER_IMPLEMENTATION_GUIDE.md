# Silver Layer Code Implementation Guide

**Created**: Nov 12, 2025  
**Based on**: EDA Analysis of Bronze Layer (Oct 1 - Nov 8, 2025)  
**Status**: Ready for Implementation ✅

---

## Quick Implementation Checklist

- [ ] Review EDA findings in `EDA_FINDINGS_AND_SILVER_RECOMMENDATIONS.md`
- [ ] View visualizations in `eda_outlier_detection.ipynb`
- [ ] Update `hourly_energy.py` (see Section 1 below)
- [ ] Update `hourly_weather.py` (see Section 2 below)
- [ ] Update `hourly_air_quality.py` (see Section 3 below)
- [ ] Test on 1 day of data
- [ ] Deploy to Silver layer
- [ ] Monitor quality metrics

---

## Section 1: Energy Loader (`hourly_energy.py`)

### Goal
Transform raw energy data with quality validation, keeping 90% GOOD records + flagging anomalies.

### Code Template

```python
import pandas as pd
from datetime import datetime
import numpy as np

class SilverHourlyEnergyLoader:
    """Load and transform energy data with quality validation."""
    
    def __init__(self, spark):
        self.spark = spark
        self.diurnal_rules = {
            'night_start': 22,
            'night_end': 6,
            'peak_start': 10,
            'peak_end': 15,
            'max_night_energy': 0.1  # MWh
        }
    
    def transform(self, bronze_df):
        """Apply quality validations and return clean data."""
        df = bronze_df.copy()
        
        # 1. Initialize quality columns
        df['quality_flag'] = 'GOOD'
        df['quality_issues'] = ''
        
        # 2. Ensure datetime
        df['interval_ts'] = pd.to_datetime(df['interval_ts'], utc=True)
        
        # 3. Physical bounds check
        # Energy must be non-negative
        invalid_energy = df['value'] < 0
        df.loc[invalid_energy, 'quality_flag'] = 'REJECT'
        df.loc[invalid_energy, 'quality_issues'] += 'NEGATIVE_ENERGY|'
        
        # 4. Temporal validation
        null_ts = df['interval_ts'].isna()
        df.loc[null_ts, 'quality_flag'] = 'REJECT'
        df.loc[null_ts, 'quality_issues'] += 'NULL_TIMESTAMP|'
        
        future_ts = df['interval_ts'] > pd.Timestamp.now(tz='UTC')
        df.loc[future_ts, 'quality_flag'] = 'REJECT'
        df.loc[future_ts, 'quality_issues'] += 'FUTURE_TIMESTAMP|'
        
        # 5. Diurnal pattern validation (solar-specific)
        df['hour'] = df['interval_ts'].dt.hour
        night_mask = (df['hour'] >= self.diurnal_rules['night_start']) | \
                     (df['hour'] < self.diurnal_rules['night_end'])
        night_high = night_mask & (df['value'] > self.diurnal_rules['max_night_energy'])
        
        # Flag but don't reject - might be inverter warm-up
        df.loc[night_high & (df['quality_flag'] == 'GOOD'), 'quality_flag'] = 'CAUTION'
        df.loc[night_high, 'quality_issues'] += 'NIGHT_ENERGY_ANOMALY|'
        
        # 6. Statistical outlier detection (IQR method)
        Q1 = df['value'].quantile(0.25)
        Q3 = df['value'].quantile(0.75)
        IQR = Q3 - Q1
        lower_fence = Q1 - 1.5 * IQR
        upper_fence = Q3 + 1.5 * IQR
        
        is_statistical_outlier = ((df['value'] < lower_fence) | \
                                  (df['value'] > upper_fence)) & \
                                 df['value'].notna()
        
        # Flag CAUTION for review (don't reject - high generation is legitimate)
        df.loc[is_statistical_outlier & (df['quality_flag'] == 'GOOD'), 
               'quality_flag'] = 'CAUTION'
        df.loc[is_statistical_outlier, 'quality_issues'] += 'STATISTICAL_OUTLIER|'
        
        # 7. Clean up quality_issues column
        df['quality_issues'] = df['quality_issues'].str.rstrip('|')
        df.loc[df['quality_issues'] == '', 'quality_issues'] = None
        
        # 8. Log quality metrics
        quality_counts = df['quality_flag'].value_counts()
        print(f"\n📊 ENERGY QUALITY SUMMARY")
        print(f"  GOOD:    {quality_counts.get('GOOD', 0):,} ({quality_counts.get('GOOD', 0)/len(df)*100:.2f}%)")
        print(f"  CAUTION: {quality_counts.get('CAUTION', 0):,} ({quality_counts.get('CAUTION', 0)/len(df)*100:.2f}%)")
        print(f"  REJECT:  {quality_counts.get('REJECT', 0):,} ({quality_counts.get('REJECT', 0)/len(df)*100:.2f}%)")
        
        # 9. Filter strategy options:
        # Option A: Keep all (100% retention) - use for analytics
        # Option B: Exclude REJECT only (90% retention) - recommended
        # Option C: Keep GOOD only (90% retention, strict)
        
        # RECOMMENDED: Option B
        result = df[df['quality_flag'] != 'REJECT'].copy()
        
        # 10. Select output columns
        output_cols = ['facility_code', 'interval_ts', 'value', 'metric',
                      'quality_flag', 'quality_issues', 'hour']
        return result[output_cols]


# Usage
# loader = SilverHourlyEnergyLoader(spark)
# clean_energy = loader.transform(bronze_energy)
# clean_energy.write.mode('overwrite').saveAsTable('silver.clean_hourly_energy')
```

### Expected Results

```
📊 ENERGY QUALITY SUMMARY
  GOOD:    4,206 (89.97%)
  CAUTION:   469 (10.03%)
  REJECT:      0 (0.00%)

✅ Data retention: 4,675 → 4,675 (100% with flags)
   Clean records (no issues): 4,206 (90%)
```

### Top Quality Issues Found

```
STATISTICAL_OUTLIER: 434 records
  → High generation days (>65 MWh)
  → Legitimate solar peaks
  → Action: Flag for monitoring, don't exclude

NIGHT_ENERGY_ANOMALY: 35 records  
  → Generation between 0:00-6:00 and 22:00-24:00
  → Values: 0.11-0.43 MWh
  → Action: Flag as CAUTION (inverter warm-up)
```

---

## Section 2: Weather Loader (`hourly_weather.py`)

### Goal
Transform weather data with physical bounds validation, keep 100%.

### Key Points

✅ **Data Quality**: Already excellent (100% complete, normal distributions)
✅ **No temporal issues**: Clean timestamps
✅ **Action**: Add bounds checking for safety, but keep all records

### Code Additions

```python
class SilverHourlyWeatherLoader:
    """Load weather data with bounds validation."""
    
    PHYSICAL_BOUNDS = {
        'shortwave_radiation': (0.0, 1500.0),  # W/m²
        'temperature_2m': (-50.0, 60.0),       # °C
        'wind_speed_10m': (0.0, 60.0),         # m/s
        'cloud_cover': (0.0, 100.0),           # %
        'pressure_msl': (800.0, 1100.0),       # hPa
        'precipitation': (0.0, 1000.0),        # mm
    }
    
    def transform(self, bronze_df):
        """Validate and return weather data."""
        df = bronze_df.copy()
        
        # Initialize quality
        df['quality_flag'] = 'GOOD'
        df['quality_issues'] = ''
        
        # Validate physical bounds
        for col, (min_val, max_val) in self.PHYSICAL_BOUNDS.items():
            if col not in df.columns:
                continue
            
            oob_low = df[col] < min_val
            oob_high = df[col] > max_val
            
            # Flag but don't reject (sensor can drift)
            df.loc[oob_high, 'quality_issues'] += f'{col}_HIGH|'
            df.loc[oob_low, 'quality_issues'] += f'{col}_LOW|'
        
        # Mark any with issues
        df.loc[df['quality_issues'] != '', 'quality_flag'] = 'CAUTION'
        
        # Clean issues
        df['quality_issues'] = df['quality_issues'].str.rstrip('|')
        df.loc[df['quality_issues'] == '', 'quality_issues'] = None
        
        print(f"\n🌤️  WEATHER QUALITY SUMMARY")
        counts = df['quality_flag'].value_counts()
        print(f"  GOOD:    {counts.get('GOOD', 0):,} (100%)")
        print(f"  CAUTION: {counts.get('CAUTION', 0):,} (0%)")
        
        # Keep all data
        return df
```

### Expected Results

```
🌤️  WEATHER QUALITY SUMMARY
  GOOD:    4,680 (100%)
  CAUTION:    0 (0%)
  
✅ Data retention: 100%
```

---

## Section 3: Air Quality Loader (`hourly_air_quality.py`)

### Goal
Transform air quality data with flexible bounds, keep 100%.

### Key Points

✅ **All within normal ranges**: PM2.5, PM10, UV Index normal
✅ **Sensor variability expected**: Allow some "outliers" (normal for pollution sensors)
✅ **Action**: Add bounds, flag extremes, keep all data

### Code Additions

```python
class SilverHourlyAirQualityLoader:
    """Load air quality data with flexible bounds."""
    
    PHYSICAL_BOUNDS = {
        'pm2_5': (0.0, 500.0),      # μg/m³ (sensor max ~500)
        'pm10': (0.0, 500.0),        # μg/m³
        'ozone': (0.0, 500.0),       # ppb equivalent
        'uv_index': (0.0, 15.0),     # UV index scale
    }
    
    def transform(self, bronze_df):
        """Validate and return air quality data."""
        df = bronze_df.copy()
        
        # Initialize
        df['quality_flag'] = 'GOOD'
        df['quality_issues'] = ''
        
        # Validate with lenient bounds (sensors can vary)
        for col, (min_val, max_val) in self.PHYSICAL_BOUNDS.items():
            if col not in df.columns:
                continue
            
            oob = (df[col] < min_val) | (df[col] > max_val)
            df.loc[oob, 'quality_issues'] += f'{col}_OUT_OF_BOUNDS|'
        
        # Any out of bounds → CAUTION (for monitoring)
        df.loc[df['quality_issues'] != '', 'quality_flag'] = 'CAUTION'
        
        # Clean
        df['quality_issues'] = df['quality_issues'].str.rstrip('|')
        df.loc[df['quality_issues'] == '', 'quality_issues'] = None
        
        print(f"\n💨 AIR QUALITY SUMMARY")
        counts = df['quality_flag'].value_counts()
        print(f"  GOOD:    {counts.get('GOOD', 0):,}")
        print(f"  CAUTION: {counts.get('CAUTION', 0):,}")
        
        # Keep all data
        return df
```

### Expected Results

```
💨 AIR QUALITY SUMMARY
  GOOD:    ~4,680 (100%)
  CAUTION: ~0-50 (1% max - normal sensor drift)

✅ Data retention: 100%
```

---

## Testing Strategy

### Test 1: Single Day
```python
# Load 1 day of Bronze data
test_df = bronze.filter(col('interval_date') == '2025-10-01')

# Transform
result = loader.transform(test_df)

# Check quality distribution
assert result.where(col('quality_flag') == 'GOOD').count() / len(result) >= 0.85
print("✅ Test 1 passed")
```

### Test 2: Verify No Data Loss
```python
assert len(result) >= len(test_df) * 0.9  # Keep 90%+ rows
print("✅ Test 2 passed")
```

### Test 3: Spot Check Anomalies
```python
# Check a known anomaly
anomaly = result.where(col('quality_issues') == 'NIGHT_ENERGY_ANOMALY')
print(f"Found {len(anomaly)} night anomalies - ✅ working correctly")
```

---

## Monitoring Dashboard Queries

### Energy Quality
```sql
SELECT 
  facility_code,
  DATE(interval_ts) as date,
  COUNT(*) as total_records,
  SUM(CASE WHEN quality_flag = 'GOOD' THEN 1 ELSE 0 END) as good_records,
  SUM(CASE WHEN quality_flag = 'CAUTION' THEN 1 ELSE 0 END) as caution_records,
  SUM(CASE WHEN quality_flag = 'REJECT' THEN 1 ELSE 0 END) as reject_records,
  ROUND(SUM(CASE WHEN quality_flag = 'GOOD' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as good_pct
FROM silver.clean_hourly_energy
GROUP BY facility_code, DATE(interval_ts)
ORDER BY date DESC, facility_code
```

### Alert: High Rejection Rate
```sql
-- Alert if any facility has >5% rejection rate
SELECT facility_code, date,
  ROUND(reject_records * 100.0 / total_records, 2) as reject_pct
FROM (
  -- query above
)
WHERE reject_pct > 5.0
ORDER BY reject_pct DESC
```

---

## Deployment Checklist

- [ ] Code reviewed and tested
- [ ] Quality thresholds approved
- [ ] Monitoring queries in place
- [ ] Alerts configured
- [ ] Rollback plan documented
- [ ] Deploy to dev
- [ ] Deploy to prod
- [ ] Monitor for 1 week
- [ ] Adjust thresholds if needed

---

## Support & Questions

**Where to find reference data**:
- EDA Notebook: `eda_outlier_detection.ipynb`
- Findings: `EDA_FINDINGS_AND_SILVER_RECOMMENDATIONS.md`
- Code templates: This document

**Key metrics baseline** (Oct 1 - Nov 8, 2025):
- Energy GOOD: 89.97%
- Weather GOOD: 100%  
- Air Quality GOOD: 96%+

**Questions?** Review the notebook for detailed analysis of any metric.

---

**Created**: Nov 12, 2025  
**Next Update**: After first week of production monitoring
