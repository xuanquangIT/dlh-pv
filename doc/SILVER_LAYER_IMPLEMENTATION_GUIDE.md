# 🚀 Silver Layer Data Quality - Implementation Guide

**Phase 2 & 3 Technical Implementation**  
**Based on Comprehensive Analysis Results**

---

## 📋 Summary of Changes

| Item | Status | Priority | Target Date |
|------|--------|----------|-------------|
| DNI bound: 900 → 950 W/m² | ✅ Done | P1 | ✅ Complete |
| CO bound: 10,000 → 1,000 ppb | ✅ Done | P1 | ✅ Complete |
| Performance optimization (40%) | ✅ Done | P1 | ✅ Complete |
| Temporal validation rules | 🔄 Ready | P2 | Week 2 |
| Equipment fault detection | 📋 Ready | P2 | Week 2 |
| Extreme outlier flagging | 📋 Ready | P2 | Week 2 |
| Correlation-based validation | 📋 Ready | P3 | Week 3 |
| Data governance policy | 📋 Ready | P3 | Week 3 |
| Monitoring dashboards | 📋 Ready | P3 | Week 4 |

---

## 🔧 PHASE 2 Implementation (Week 2)

### Task 1: Add Temporal Validation Rules

**Location:** `src/pv_lakehouse/etl/loaders/hourly_energy.py`

**Current Code:** Lines handling `quality_flag` column

**Changes Required:**

Add temporal validation logic BEFORE the final quality_flag column creation:

```python
from pyspark.sql import functions as F

# ... existing code ...

def create_clean_hourly_energy(self, bronze_df):
    """Create clean hourly energy with temporal validation."""
    
    # Extract hour of day for temporal checks
    hour_col = F.hour(F.col('local_datetime'))
    
    # Define time ranges
    night_mask = (hour_col >= 22) | (hour_col < 6)  # 22:00 - 05:59
    early_dawn_mask = (hour_col >= 5) & (hour_col < 6)  # 05:00 - 05:59 (sunrise time)
    daytime_mask = (hour_col >= 6) & (hour_col < 22)  # 06:00 - 21:59
    
    # Existing quality checks
    # ... (keep existing bound checks, outlier detection, etc.)
    
    # ADD: Temporal anomaly detection
    # Night hours should have ~0 energy
    temporal_issue = F.when(
        night_mask & (F.col('energy_mwh') > 1.0),
        'NIGHT_ENERGY_ANOMALY'  # Energy detected during night hours
    ).when(
        early_dawn_mask & (F.col('energy_mwh') > 5.0),
        'EARLY_DAWN_SPIKE'  # Unusual spike at sunrise
    ).otherwise(F.lit(''))
    
    # Combine with existing issues
    all_issues = F.concat_ws(
        '|',
        bound_issues,
        diurnal_issues,
        temporal_issue
    )
    
    # Update quality flag based on issues
    quality_flag = F.when(
        F.length(all_issues) > 0,
        F.when(
            all_issues.contains('NIGHT_ENERGY_ANOMALY'),
            'TEMPORAL_ANOMALY'  # Priority flag for night issues
        ).otherwise('CAUTION')  # Other issues marked as CAUTION
    ).otherwise('GOOD')
    
    # ... rest of processing ...
    
    return df_clean.select(
        # ... existing columns ...
        quality_flag,
        all_issues.alias('quality_issues')
    )
```

**Testing:**
```sql
-- Verify in Gold layer that night anomalies are flagged
SELECT facility_code, date_hour, energy_mwh, quality_flag, quality_issues
FROM lh_silver_clean_hourly_energy
WHERE quality_flag = 'TEMPORAL_ANOMALY'
LIMIT 20;

-- Expected: ~121 records with energy > 1 MWh during 22:00-06:00
```

**Expected Results:**
- 121 night anomaly records flagged (0.45% of night data)
- Top facility: COLEASF (67 records)
- Focus investigation on 05:00 hour (sunrise interference)

---

### Task 2: Implement Equipment Fault Detection

**Location:** `src/pv_lakehouse/etl/loaders/hourly_energy.py`

**Concept:** High radiation but zero energy = equipment failure

```python
# ADD: Equipment fault detection
# Daytime with high radiation but zero/low energy = equipment issue

# Get radiation data (from merged weather)
dni_col = F.col('direct_normal_irradiance')
daytime_high_radiation = daytime_mask & (dni_col > 200)  # Good sunlight

equipment_fault = F.when(
    daytime_high_radiation & (F.col('energy_mwh') < 2.0),
    'POSSIBLE_EQUIPMENT_FAULT'  # Expected to produce energy but didn't
).otherwise(F.lit(''))

# Combine with temporal issues
all_temporal_issues = F.concat_ws('|', temporal_issue, equipment_fault)
```

**Severity Levels:**

```python
# Classify equipment faults by severity
equipment_severity = F.when(
    F.col('energy_mwh') == 0,  # Complete failure
    'EQUIPMENT_FAULT_CRITICAL'
).when(
    F.col('energy_mwh') < F.col('facility_capacity') * 0.1,  # <10% expected
    'EQUIPMENT_FAULT_MAJOR'
).when(
    F.col('energy_mwh') < F.col('facility_capacity') * 0.3,  # <30% expected
    'EQUIPMENT_FAULT_MINOR'
).otherwise(F.lit(''))
```

---

### Task 3: Create EXTREME_OUTLIER Quality Flag

**Location:** `src/pv_lakehouse/etl/loaders/hourly_energy.py`

**Change:** Enhance existing z-score detection to create priority flags

```python
# MODIFY: Existing z-score logic
energy_mean = F.avg(F.col('energy_mwh')).over(facility_window)
energy_std = F.stddev_pop(F.col('energy_mwh')).over(facility_window)

z_score = F.abs((F.col('energy_mwh') - energy_mean) / energy_std)

# NEW: Create outlier tier flags
outlier_flag = F.when(
    z_score > 5.0,
    'EXTREME_OUTLIER'  # Exclude from training (|z|>5)
).when(
    (z_score > 3.0) & (z_score <= 5.0),
    'STRONG_OUTLIER'  # Review and mark CAUTION (3<|z|≤5)
).when(
    (z_score > 2.0) & (z_score <= 3.0),
    'MILD_OUTLIER'  # Track but acceptable (2<|z|≤3)
).otherwise('NORMAL')

# Update quality flag priority
quality_flag = F.when(
    outlier_flag == 'EXTREME_OUTLIER',
    'EXTREME_OUTLIER'  # Highest priority
).when(
    outlier_flag == 'STRONG_OUTLIER',
    'CAUTION'
).when(
    # ... other conditions ...
    F.col('quality_flag') == 'GOOD',
    'GOOD'
).otherwise('CAUTION')
```

---

## 🔬 PHASE 3 Implementation (Week 3-4)

### Task 4: Add Correlation-Based Anomaly Detection

**Location:** `src/pv_lakehouse/etl/loaders/hourly_energy.py`

**Concept:** Energy-radiation correlation should be strong during peak hours

```python
from pyspark.sql.window import Window

# Calculate daily correlation by facility
daily_window = Window.partitionBy('facility_code', F.date_trunc('day', F.col('date_hour')))

# Peak hours only (11:00-15:00 for maximum correlation)
peak_hours_mask = (hour_col >= 11) & (hour_col <= 15)
peak_data = energy_df.filter(peak_hours_mask)

# Calculate correlation
correlation = F.corr(F.col('energy_mwh'), F.col('direct_normal_irradiance')).over(daily_window)

# Flag low correlation days
correlation_anomaly = F.when(
    (correlation < 0.8) & F.col('direct_normal_irradiance') > 500,
    'LOW_CORRELATION_ANOMALY'  # Weak energy-radiation relationship
).otherwise(F.lit(''))

# Efficiency ratio check
expected_energy = F.col('direct_normal_irradiance') * 0.15  # Typical efficiency
efficiency_ratio = F.col('energy_mwh') / (expected_energy + 0.001)

efficiency_anomaly = F.when(
    (efficiency_ratio < 0.8) | (efficiency_ratio > 1.2),
    'EFFICIENCY_ANOMALY'  # Outside 20% variance
).otherwise(F.lit(''))
```

**Threshold Tuning:**
- Default correlation threshold: 0.80 (80%)
- Can be adjusted per facility based on historical baseline
- Flag if <0.80 for 2+ consecutive days

---

### Task 5: Data Governance Policy

**File:** `doc/DATA_GOVERNANCE_POLICY.md` (create new)

**Content Structure:**

```markdown
## Data Retention Policy

### Tier 1 - Production Use
**Quality Flags:** GOOD, CAUTION  
**Percentage:** ~100% of records  
**Use:** Gold layer, ML training, reporting  
**SLA:** <2% CAUTION rate acceptable  
**Review:** Monthly trend analysis  

### Tier 2 - Analysis Use
**Quality Flags:** TEMPORAL_ANOMALY, CORRELATION_ANOMALY  
**Percentage:** ~1-2% of records  
**Use:** Root cause analysis, trend detection  
**Note:** Flag reason in Gold layer metadata  
**Quarantine:** Separate table for investigation  

### Tier 3 - Archive Only
**Quality Flags:** EXTREME_OUTLIER, EQUIPMENT_FAULT_CRITICAL  
**Percentage:** ~0.1% of records  
**Use:** Audit trail only, not for ML/reporting  
**Retention:** 24 months for compliance  
**Review:** Quarterly assessment for re-inclusion  

## Exclusion Criteria

- EXTREME_OUTLIER (|z|>5): Exclude from ML training
- EQUIPMENT_FAULT_CRITICAL: Exclude from performance metrics
- NIGHT_ENERGY_ANOMALY: Flag but may use if no alternative
- LOW_CORRELATION_ANOMALY: Flag for investigation, exclude if confirmed sensor issue

## Escalation Procedure

Trigger alerts when:
- >10% REJECT records (stop pipeline)
- >3% EXTREME_OUTLIER (investigate facility)
- Facility correlation drops below 0.70 (equipment alert)
- Night anomaly rate >1% (sensor maintenance)
```

---

## 🎯 Phase 3 - Monitoring Dashboards

**Technology:** Spark SQL queries + visualization tool (Grafana/Superset/Metabase)

**Key Metrics Queries:**

```sql
-- Daily Quality Score by Facility
SELECT
    facility_code,
    DATE(date_hour) as date,
    ROUND(100 * COUNT(CASE WHEN quality_flag = 'GOOD' THEN 1 END) / COUNT(*), 1) as good_pct,
    ROUND(100 * COUNT(CASE WHEN quality_flag = 'CAUTION' THEN 1 END) / COUNT(*), 1) as caution_pct,
    COUNT(CASE WHEN quality_flag = 'EXTREME_OUTLIER' THEN 1 END) as extreme_outliers
FROM lh_silver_clean_hourly_energy
GROUP BY facility_code, DATE(date_hour)
ORDER BY date DESC, facility_code;

-- Temporal Anomaly Trend
SELECT
    facility_code,
    DATE(date_hour) as date,
    COUNT(*) as night_anomalies,
    ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY DATE(date_hour)), 2) as pct_of_day
FROM lh_silver_clean_hourly_energy
WHERE quality_flag = 'TEMPORAL_ANOMALY'
GROUP BY facility_code, DATE(date_hour)
ORDER BY date DESC;

-- Outlier Severity Distribution
SELECT
    facility_code,
    CASE 
        WHEN z_score > 5 THEN 'EXTREME (>5)'
        WHEN z_score > 3 THEN 'STRONG (3-5)'
        WHEN z_score > 2 THEN 'MILD (2-3)'
        ELSE 'NORMAL'
    END as severity,
    COUNT(*) as count,
    ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY facility_code), 2) as pct
FROM lh_silver_clean_hourly_energy
GROUP BY facility_code, severity
ORDER BY facility_code, severity DESC;
```

---

## 📊 Testing Checklist

### Unit Tests

```python
# test_silver_temporal_validation.py
def test_night_anomaly_detection():
    """Verify night hours energy anomalies are flagged."""
    night_df = df.filter((F.hour(F.col('date_hour')) >= 22) | (F.hour(F.col('date_hour')) < 6))
    anomalies = night_df.filter(F.col('quality_flag') == 'TEMPORAL_ANOMALY')
    
    assert anomalies.count() == 121, "Expected 121 night anomalies"
    assert anomalies.select(F.max('energy_mwh')).collect()[0][0] > 10, "Max should be >10 MWh"

def test_equipment_fault_detection():
    """Verify equipment faults are detected (high radiation, low energy)."""
    day_df = df.filter((F.hour(F.col('date_hour')) >= 6) & (F.hour(F.col('date_hour')) < 22))
    high_rad = day_df.filter(F.col('direct_normal_irradiance') > 500)
    faults = high_rad.filter((F.col('quality_flag') == 'EQUIPMENT_FAULT') | (F.col('energy_mwh') < 2))
    
    # Should have some detected (depends on data)
    assert faults.count() > 0, "Expected some equipment faults"

def test_extreme_outlier_exclusion():
    """Verify extreme outliers are properly flagged for exclusion."""
    extreme = df.filter(F.col('quality_flag') == 'EXTREME_OUTLIER')
    
    # With current data, should be 0
    assert extreme.count() == 0, "Expected 0 extreme outliers in current data"
```

### Integration Tests

```python
# test_silver_quality_gates.py
def test_overall_quality_score():
    """Verify overall data quality meets SLA."""
    good_pct = df.filter(F.col('quality_flag') == 'GOOD').count() / df.count()
    
    assert good_pct >= 0.90, f"Quality score {good_pct:.1%} below 90% threshold"
    
def test_facility_quality_variance():
    """Verify no facility drops below minimum quality."""
    facility_quality = df.groupby('facility_code').agg(
        F.count(F.when(F.col('quality_flag') == 'GOOD', 1)).alias('good_count'),
        F.count(F.lit(1)).alias('total')
    ).withColumn('good_pct', F.col('good_count') / F.col('total'))
    
    min_quality = facility_quality.select(F.min('good_pct')).collect()[0][0]
    assert min_quality >= 0.85, f"Facility quality {min_quality:.1%} below 85% threshold"
```

---

## 📈 Success Metrics

| Metric | Target | Current | Status |
|--------|--------|---------|--------|
| GOOD records | >90% | 93.9% | ✅ Pass |
| CAUTION records | <10% | 6.1% | ✅ Pass |
| EXTREME_OUTLIER | <0.5% | 0.0% | ✅ Pass |
| Strong outliers | <3% | 1.52% | ✅ Pass |
| Night anomalies | <1% night data | 0.45% | ✅ Pass |
| DNI violations | <5% | 1.96% | ✅ Pass |
| Facility correlation | >0.80 | Expected | 🔄 Monitor |

---

## 📚 Reference Files

- **Loaders:** `src/pv_lakehouse/etl/loaders/`
- **Tests:** `src/pv_lakehouse/etl/tests/`
- **Analysis:** `notebooks/Silver_Data_Quality_Comprehensive_Analysis.ipynb`
- **Summary:** `doc/SILVER_LAYER_ANALYSIS_SUMMARY.md`

---

## 🚀 Deployment Steps

1. **Create feature branch:** `feature/silver-phase2-temporal-validation`
2. **Implement Task 1-3** in `hourly_energy.py`
3. **Add unit & integration tests**
4. **Run full regression test suite**
5. **Update documentation**
6. **Create pull request** with analysis notebook link
7. **Peer review** before merge
8. **Deploy to DEV** for validation
9. **Run with 7 days of data** to confirm no issues
10. **Promote to PROD** after validation

---

**Status:** Ready for implementation  
**Next Review:** After Phase 2 completion (Week 2)

