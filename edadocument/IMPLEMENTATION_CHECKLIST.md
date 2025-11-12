# Implementation Checklist - Outlier Detection

## Phase 1: Analysis & Review (Immediate - Today)

- [ ] Read `OUTLIER_DETECTION_IMPLEMENTATION.md` (overview)
- [ ] Read `doc/OUTLIER_DETECTION_QUICK_REFERENCE.md` (quick start)
- [ ] Run EDA notebook:
  ```bash
  jupyter notebook src/pv_lakehouse/etl/scripts/notebooks/eda_outlier_detection.ipynb
  ```
- [ ] Review statistics and visualizations
- [ ] Document current data quality baseline
- [ ] Share findings with team
- [ ] Identify top 5 data quality issues

**Owner**: Data Analyst / Data Engineer
**Deliverable**: Data quality report + findings document

---

## Phase 2: Single Dataset Integration (This Week)

### 2.1 Choose Dataset
- [ ] Start with **Energy data** (most critical)
  - *Reason*: Most direct impact on business metrics
  - *Complexity*: High (diurnal patterns)
  - *Expected rejection*: 2-5%
- [ ] Alternative: Weather data
  - *Reason*: Moderate complexity, good for learning
  - *Expected rejection*: 0.5-1%
- [ ] Alternative: Air Quality data
  - *Reason*: Least critical, simplest rules
  - *Expected rejection*: 0.2-0.5%

### 2.2 Update Loader
- [ ] Copy example from `examples_with_quality.py`
- [ ] Integrate outlier_handler functions:
  - [ ] `add_physical_bounds_flag()`
  - [ ] `add_temporal_flags()`
  - [ ] `add_diurnal_pattern_flags()` (if applicable)
  - [ ] `add_statistical_outlier_flags()`
  - [ ] `add_composite_quality_flag()`
- [ ] Add logging:
  - [ ] Call `get_quality_summary()` after transform
  - [ ] Log quality metrics to console/logs
  - [ ] Alert if rejection rate > 5%

### 2.3 Test Implementation
- [ ] Load 1 day of Bronze data
- [ ] Run transform manually (in notebook or test)
- [ ] Verify quality_flag column has correct values:
  - [ ] Some records marked GOOD
  - [ ] Some records marked CAUTION (optional)
  - [ ] Some records marked REJECT (if violations exist)
- [ ] Check quality_issues column describes issues
- [ ] Validate record counts:
  - [ ] Total = GOOD + CAUTION + REJECT
  - [ ] Retention rate matches expectations

### 2.4 Deploy to Silver
- [ ] Update loader in production:
  ```bash
  cp src/pv_lakehouse/etl/silver/hourly_energy.py \
     src/pv_lakehouse/etl/silver/hourly_energy.py.backup
  # Edit hourly_energy.py with quality validations
  ```
- [ ] Run full load on 1 week of data:
  ```bash
  spark-submit src/pv_lakehouse/etl/silver/cli.py hourly_energy --mode full
  ```
- [ ] Verify Silver table:
  - [ ] Records have quality_flag column
  - [ ] quality_issues column populated
  - [ ] Correct row count
- [ ] Document any facility-specific adjustments needed

**Owner**: Data Engineer
**Deliverable**: Updated loader + test results

---

## Phase 3: Complete Integration (This Month)

### 3.1 Weather Data
- [ ] Integrate outlier_handler into weather loader
- [ ] Use moderate validation rules (allow more gaps)
- [ ] Test and deploy to Silver

### 3.2 Air Quality Data
- [ ] Integrate outlier_handler into air quality loader
- [ ] Use lenient validation rules (sensor maintenance windows)
- [ ] Test and deploy to Silver

### 3.3 Quality Monitoring
- [ ] Create query to track quality metrics by facility:
  ```sql
  SELECT 
    facility_code,
    DATE(date_hour) as date,
    COUNT(*) as total_records,
    SUM(CASE WHEN quality_flag = 'GOOD' THEN 1 ELSE 0 END) as good_records,
    SUM(CASE WHEN quality_flag = 'CAUTION' THEN 1 ELSE 0 END) as caution_records,
    SUM(CASE WHEN quality_flag = 'REJECT' THEN 1 ELSE 0 END) as reject_records,
    ROUND(100.0 * SUM(CASE WHEN quality_flag = 'GOOD' THEN 1 ELSE 0 END) / COUNT(*), 2) as good_pct
  FROM lh.silver.clean_hourly_energy
  GROUP BY facility_code, DATE(date_hour)
  ORDER BY date DESC, facility_code
  ```
- [ ] Create dashboard in BI tool (Grafana/Tableau)
- [ ] Set up alerts:
  - [ ] Alert if facility rejection rate > 5%
  - [ ] Alert if rejection rate increases > 2% day-over-day
  - [ ] Alert if facility has all REJECT records

### 3.4 Documentation
- [ ] Document facility-specific bounds (if adjusted)
- [ ] Document quality SLA:
  - [ ] Target retention rate (e.g., 95% GOOD)
  - [ ] Maximum acceptable rejection rate
  - [ ] Incident response procedures
- [ ] Create runbook for investigating quality issues
- [ ] Update data dictionary with quality_flag field

**Owner**: Data Engineer + Data Analyst
**Deliverable**: Monitoring dashboard + SLA document

---

## Phase 4: Optimization & Tuning (Ongoing)

- [ ] Monitor quality trends by facility
- [ ] Identify systematic issues:
  - [ ] Facility A always has high night energy (check inverter)
  - [ ] Weather data always null on weekends (check ingest)
  - [ ] Seasonal patterns (dust storms, extreme heat)
- [ ] Adjust bounds if needed:
  - [ ] Update PHYSICAL_BOUNDS in outlier_handler.py
  - [ ] Re-run affected data
  - [ ] Document rationale for changes
- [ ] Review CAUTION records quarterly:
  - [ ] Are they valid anomalies or errors?
  - [ ] Should we change CAUTION to REJECT?
  - [ ] Do we need new validation rules?
- [ ] Performance optimization:
  - [ ] Benchmark quality validation overhead
  - [ ] Optimize statistical outlier calculation if slow
  - [ ] Consider parallel processing for multiple facilities

**Owner**: Data Engineer / Analytics Team
**Cadence**: Monthly review

---

## Testing Checklist

### Unit Tests
- [ ] `add_physical_bounds_flag()` correctly flags OOB values
- [ ] `add_temporal_flags()` correctly identifies null/future timestamps
- [ ] `add_diurnal_pattern_flags()` correctly identifies night/peak anomalies
- [ ] `add_statistical_outlier_flags()` correctly identifies outliers
- [ ] `add_composite_quality_flag()` correctly combines flags
- [ ] `filter_by_quality()` correctly filters records
- [ ] `get_quality_summary()` correctly calculates statistics

### Integration Tests
- [ ] Loader runs without errors on 1 day of data
- [ ] Quality columns added correctly
- [ ] Row count preserved (all records kept)
- [ ] GOOD + CAUTION + REJECT = total rows
- [ ] quality_issues column non-empty for non-GOOD records

### Regression Tests
- [ ] Silver table schema unchanged (backward compatible)
- [ ] Query performance acceptable
- [ ] Downstream processes handle quality_flag column
- [ ] BI dashboards still work with new columns

### Data Quality Tests
- [ ] No NULL values in quality_flag column
- [ ] No NULL values in quality_issues for REJECT/CAUTION
- [ ] quality_flag only contains valid values (GOOD/CAUTION/REJECT)
- [ ] No unexpected patterns in quality by facility
- [ ] Retention rate within expected range

---

## Rollback Plan

If issues are found:

1. **Immediate** (< 1 hour)
   - [ ] Stop new jobs
   - [ ] Revert to previous loader version:
     ```bash
     cp src/pv_lakehouse/etl/silver/hourly_energy.py.backup \
        src/pv_lakehouse/etl/silver/hourly_energy.py
     ```
   - [ ] Re-run failed load with original loader

2. **Short-term** (1-24 hours)
   - [ ] Investigate root cause
   - [ ] Fix issue in outlier_handler or loader
   - [ ] Test thoroughly in dev environment
   - [ ] Re-deploy with fix

3. **Long-term**
   - [ ] Add unit test to prevent regression
   - [ ] Document lesson learned
   - [ ] Update runbook

---

## Sign-Off

| Phase | Owner | Approval | Date |
|-------|-------|----------|------|
| Phase 1: Analysis | Data Analyst | Manager | ___ |
| Phase 2: Energy | Data Engineer | Tech Lead | ___ |
| Phase 3: Weather | Data Engineer | Tech Lead | ___ |
| Phase 3: Air Quality | Data Engineer | Tech Lead | ___ |
| Phase 3: Monitoring | Data Analyst | Manager | ___ |
| Phase 4: Ongoing | Data Team | Quarterly | ___ |

---

## Resources

| Document | Purpose |
|----------|---------|
| `OUTLIER_DETECTION_IMPLEMENTATION.md` | Overview & rationale |
| `doc/OUTLIER_DETECTION_GUIDE.md` | Detailed implementation guide |
| `doc/OUTLIER_DETECTION_QUICK_REFERENCE.md` | Quick reference |
| `src/pv_lakehouse/etl/scripts/notebooks/eda_outlier_detection.ipynb` | EDA analysis |
| `src/pv_lakehouse/etl/silver/outlier_handler.py` | Core utilities |
| `src/pv_lakehouse/etl/silver/examples_with_quality.py` | Code examples |

---

## Questions & Troubleshooting

### Q: The rejection rate is too high (>10%)
**A**: 
1. Review PHYSICAL_BOUNDS - may be too strict
2. Check for systematic issues (sensor failure, data encoding)
3. Run EDA notebook to visualize distributions
4. Adjust bounds based on actual data ranges

### Q: How do I add facility-specific bounds?
**A**:
```python
FACILITY_BOUNDS = {
    'NYNGAN': {'energy_mwh': (0.0, 200.0)},
    'GANNSF': {'energy_mwh': (0.0, 100.0)},
}

facility_code = df.select('facility_code').first()[0]
bounds = FACILITY_BOUNDS.get(facility_code, PHYSICAL_BOUNDS)
result = add_physical_bounds_flag(result, bounds=bounds)
```

### Q: Should I exclude CAUTION records?
**A**: Depends on use case:
- **Production metrics**: Exclude (strict)
- **Analysis/Research**: Include with flag
- **Dashboards**: Include but highlight

### Q: How often should I review quality?
**A**: 
- Daily: Automated alerts (rejection rate spike)
- Weekly: Spot check by facility
- Monthly: Trend analysis and tuning
- Quarterly: Major review & documentation

---

**Created**: 2025-11-12
**Last Updated**: 2025-11-12
**Version**: 1.0
