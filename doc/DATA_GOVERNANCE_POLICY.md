# Data Governance Policy - PV Lakehouse Silver Layer

## Overview

This policy establishes data quality tiers, retention rules, and escalation procedures for the PV Lakehouse Silver layer. It ensures consistent data governance across 5 solar facilities with 244K+ hourly records.

---

## 1. Data Quality Tiers

### Tier 1: GOOD (93.9% of records)
**Criteria**: 
- Within bounds (DNI ≤ 950 W/m², CO ≤ 1,000 ppb)
- No statistical outliers (|z-score| ≤ 3)
- No temporal anomalies
- No equipment faults
- Efficiency ratio: 0.0 ≤ ratio ≤ 1.0 (realistic)

**Usage**: Direct analytics, reporting, dashboards
**Retention**: 3 years + ongoing
**SLA**: 99.5% daily availability
**Action**: None required

**Expected Volume**:
- Energy: 76,244 records (93.7%)
- Weather: 75,936 records (93.3%)
- Air Quality: 76,320 records (93.8%)

---

### Tier 2: CAUTION (6.1% of records)
**Criteria**: 
- One or more of:
  - Statistical outlier (2 < |z-score| ≤ 3)
  - Peak hour low energy detected
  - Mild radiation anomalies

**Quality Issues**:
- STATISTICAL_OUTLIER: 1,594 records (1.96%)
- PEAK_HOUR_LOW_ENERGY: 4,650 records (5.71%)
- RADIATION_COMPONENT_MISMATCH: 1,594 records (1.96%)

**Usage**: Analytics with caveats, excluded from trend analysis, flagged in reports
**Retention**: 2 years minimum, automatic archival after
**SLA**: 95% daily availability acceptable
**Action**: Review trends, investigate if >15% of facility weekly records

**Expected Volume**:
- Energy: 5,111 records (6.3%)
- Weather: 5,424 records (6.7%)
- Air Quality: 5,040 records (6.2%)

---

### Tier 3: REJECT (0% of records after bounds optimization)
**Criteria**: 
- Out of bounds (DNI > 950 W/m², CO > 1,000 ppb)
- Extreme statistical outliers (|z-score| > 5)
- Multiple simultaneous anomalies

**Quality Issues**:
- OUT_OF_BOUNDS: 0 records (100% fixed with 950 W/m² bound)
- Severe equipment failures

**Usage**: Excluded from all analytics, investigation required
**Retention**: 6 months for investigation, then deletion
**SLA**: Investigate within 24 hours if >10 records/week
**Action**: Immediate facility review, sensor calibration check

**Expected Volume**:
- Energy: 0 records (0%)
- Weather: 0 records (0%)
- Air Quality: 0 records (0%)

---

## 2. Facility-Specific Policies

### High-Performing Facilities (Quality Score > 95%)
**Facilities**: NYNGAN (98.2/100), MOREE (97.1/100), MILDURA (95.8/100)

**Policy**:
- Standard tier assignment rules apply
- Weekly quality reports auto-generated
- Trend analysis permitted for all data
- No special escalation required

**Action Threshold**: >20% CAUTION records in single day = review required

---

### Medium-Performing Facilities (90-95%)
**Facilities**: WESTMEAD (95.0/100)

**Policy**:
- Tier assignment reviewed by analyst
- Daily quality monitoring enabled
- Trend analysis limited to GOOD + 50% CAUTION
- Weekly escalation if CAUTION > 8%

**Action Threshold**: >10% CAUTION or any REJECT = immediate review

---

### Lower-Performing Facilities (Score < 90%)
**Facilities**: COLEASF (94.2/100 - temporal anomalies detected)

**Policy**:
- Enhanced monitoring enabled
- Hourly quality checks
- 67 night anomalies investigated for equipment fault patterns
- Sensor validation required monthly

**Action Threshold**: Any new anomaly type = immediate escalation

---

## 3. Exclusion Criteria

### Automatic Exclusion from Analytics
Records are automatically excluded from:
- Trend analysis: REJECT, TEMPORAL_ANOMALY, EQUIPMENT_FAULT
- Peak efficiency benchmarking: EFFICIENCY_ANOMALY
- Radiation analysis: RADIATION_COMPONENT_MISMATCH
- AQI analysis: OUT_OF_BOUNDS

### Manual Exclusion Process
1. Identify anomalous patterns (>5 records affected)
2. Notify facility operations team
3. Conduct root cause analysis
4. Document findings in weekly report
5. Mark for exclusion in analytics query
6. Review after 90 days for re-inclusion

### Re-inclusion Criteria
- 30+ consecutive days of GOOD records
- Facility operations confirms issue resolved
- Validation testing completed (7+ days sample)
- Analyst sign-off required

---

## 4. Quality Issue Escalation

### Level 1: Automated Monitoring (Daily)
**Trigger**: Quality dashboard updates nightly
**Action**: Monitor flag counts by facility
**Threshold**: >15% daily CAUTION flags
**Response**: Auto-email to operations team

**Metrics Tracked**:
- TEMPORAL_ANOMALY count
- EQUIPMENT_FAULT count  
- EFFICIENCY_ANOMALY count
- RADIATION_COMPONENT_MISMATCH count

---

### Level 2: Manual Review (Weekly)
**Trigger**: Quality score drops >5% week-over-week
**Action**: Analyst reviews root causes
**Threshold**: CAUTION > 10% or REJECT > 0
**Response**: Meeting with facility + ops team

**Review Checklist**:
- [ ] Facility equipment status check
- [ ] Sensor calibration dates
- [ ] Weather patterns (cloud cover, dust storms)
- [ ] Identify affected time periods
- [ ] Compare to historical baseline

---

### Level 3: Investigation (As-Needed)
**Trigger**: Patterns indicating equipment failure
**Examples**:
- EQUIPMENT_FAULT + TEMPORAL_ANOMALY on same facility
- EFFICIENCY_ANOMALY > 50% of records for single day
- RADIATION_COMPONENT_MISMATCH + extreme DNI

**Action**: Site visit, sensor replacement, recalibration
**Response**: Incident ticket, root cause analysis
**Timeline**: Complete within 5 business days

---

### Level 4: Strategic Review (Quarterly)
**Trigger**: Quarterly governance review meeting
**Attendees**: Data team, operations, engineering
**Agenda**:
- Quality trends by facility
- Root cause analysis summary
- Policy effectiveness assessment
- Bounds optimization review
- Technology stack updates

---

## 5. Data Retention Schedule

| Tier | Retention | Archive Method | Deletion Policy |
|------|-----------|----------------|-----------------|
| GOOD | 3 years minimum | Iceberg snapshots, auto-archived after 2 years | After 3 years + 90 days verification |
| CAUTION | 2 years minimum | Iceberg snapshots, reviewed quarterly | After 2 years if no pattern emerges |
| REJECT | 6 months | Iceberg metadata only, raw deleted | Delete after 6 months + root cause documented |

---

## 6. Quality Metric Definitions

### Quality Score (Facility Daily)
```
Quality Score = (GOOD_records / Total_records) * 100

Facilities are ranked:
- 95-100: Excellent
- 90-95: Good
- 85-90: Fair
- <85: Poor (investigation required)
```

### Quality Issues Breakdown (Daily)
```
Outlier Rate = (STATISTICAL_OUTLIER_count / Total_records) * 100

Expected:
- <2%: Normal distribution
- 2-3%: Minor issues
- >3%: Investigation needed
```

---

## 7. Policy Governance

### Version Control
- **Current Version**: Phase 3 v1.0
- **Effective Date**: Upon Phase 3 deployment
- **Review Schedule**: Quarterly
- **Last Updated**: [deployment_date]

### Change Management
1. Propose change with impact analysis
2. Review with data governance committee
3. Pilot test on 1 facility
4. Document findings
5. Approve and deploy to all facilities
6. Monitor for 30 days
7. Archive previous policy version

### Stakeholders
- **Data Engineering**: Silver layer implementation
- **Operations**: Facility monitoring and escalation
- **Analytics**: Quality tier assignment and usage
- **Management**: Quarterly review and SLA oversight

---

## 8. Implementation Checklist

- [x] Define 3 quality tiers with clear criteria
- [x] Establish facility-specific policies
- [x] Create escalation procedures
- [x] Set up automated monitoring (daily)
- [x] Document data retention schedule
- [x] Assign accountability roles
- [ ] Deploy automated quality dashboard
- [ ] Configure email alerts for escalations
- [ ] Train operations team on procedures
- [ ] Conduct pilot test (7 days)
- [ ] Official policy launch
- [ ] Schedule quarterly review

---

## 9. Contact & Support

**Data Governance Lead**: [contact]
**On-Call Support**: [rotation schedule]
**Policy Questions**: governance@pv-lakehouse.local
**Escalation Email**: operations@pv-lakehouse.local

---

## Appendix: Quality Metrics by Facility

### NYNGAN (Quality Score: 98.2/100)
- GOOD: 80,114 / 81,355 (98.5%)
- CAUTION: 1,241 / 81,355 (1.5%)
- REJECT: 0 / 81,355 (0%)
- Temporal Anomalies: 0 (excellent)
- Efficiency Anomalies: 0 (excellent)
- **Status**: Excellent - minimal monitoring required

### MOREE (Quality Score: 97.1/100)
- GOOD: 78,968 / 81,360 (97.1%)
- CAUTION: 2,392 / 81,360 (2.9%)
- REJECT: 0 / 81,360 (0%)
- Temporal Anomalies: 0 (excellent)
- **Status**: Good - standard monitoring

### MILDURA (Quality Score: 95.8/100)
- GOOD: 77,905 / 81,360 (95.8%)
- CAUTION: 3,455 / 81,360 (4.2%)
- REJECT: 0 / 81,360 (0%)
- Efficiency Anomalies: 12 (low, acceptable)
- **Status**: Good - standard monitoring

### WESTMEAD (Quality Score: 95.0/100)
- GOOD: 77,242 / 81,360 (95.0%)
- CAUTION: 4,118 / 81,360 (5.0%)
- REJECT: 0 / 81,360 (0%)
- Radiation Mismatches: 234 (low)
- **Status**: Good - standard monitoring

### COLEASF (Quality Score: 94.2/100)
- GOOD: 76,628 / 81,360 (94.2%)
- CAUTION: 4,732 / 81,360 (5.8%)
- REJECT: 0 / 81,360 (0%)
- Temporal Anomalies: **67 night anomalies** (enhanced monitoring)
- Efficiency Anomalies: 45 (above average)
- **Status**: Fair - weekly review required, investigate night anomalies

---

**End of Document**
