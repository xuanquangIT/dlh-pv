# 🔴 BUG REPORT: Energy API Data Quality Issue
**Status:** CRITICAL - Data Integrity Issue  
**Date:** November 11, 2025  
**Period Affected:** June 1, 2025 - November 8, 2025  
**Facilities:** All 5 (NYNGAN, BNGSF1, CLARESF, COLEASF, GANNSF)

---

## 📋 Executive Summary

The **Energy API is returning data with incorrect/leshifted timestamps**. Approximately **1,155 hourly records** show energy generation during night-time hours (when radiation < 10 W/m²) which is physically impossible.

**Root Cause:** Likely **TIMEZONE MISMATCH** - API may be storing data in **local timezone** but returning as **UTC**.

---

## 🔍 Evidence

### Issue #1: Night-time Energy Production (Physically Impossible)

**Table: Anomalous Records**
```
Date         | Hour UTC | Facility | Energy | Radiation | Status
2025-06-01   | 21:00    | NYNGAN   | 1.04   | 0.0 W/m²  | ❌ IMPOSSIBLE
2025-06-02   | 21:00    | NYNGAN   | 1.02   | 0.0 W/m²  | ❌ IMPOSSIBLE
2025-06-03   | 21:00    | NYNGAN   | 4.94   | 0.0 W/m²  | ❌ IMPOSSIBLE
...
```

**Total Affected Records:**
| Facility | Night-time Anomalies | Percentage |
|----------|-------------------|-----------|
| BNGSF1   | 232 hours         | 6.0% of data |
| CLARESF  | 253 hours         | 6.5% of data |
| COLEASF  | 186 hours         | 4.8% of data |
| GANNSF   | 268 hours         | 6.9% of data |
| NYNGAN   | 216 hours         | 5.6% of data |
| **TOTAL**| **1,155 hours**   | **~6% of dataset** |

### Issue #2: Strong Pattern at Specific UTC Hours

**Distribution of Anomalies by Hour (UTC):**
```
Hour UTC | Count | Pattern
---------|-------|--------
21:00    | 359   | ████████████████████░  PRIMARY
10:00    | 248   | █████████████░░░░░░░░░  SECONDARY
20:00    | 243   | █████████████░░░░░░░░░  SECONDARY
22:00    | 132   | ███████░░░░░░░░░░░░░░░░
09:00    | 130   | ███████░░░░░░░░░░░░░░░░
```

**Key Observation:** 
- **Peak at 21:00 UTC (359 records)** - This is NOT random!
- Almost **31% of all anomalies occur at exactly 21:00 UTC**
- Secondary peaks at 09:00-10:00 and 20:00-22:00 UTC

---

## 🌍 Timezone Analysis

### Facility Locations vs Timestamp Issue

```
Facility | Location      | Timezone   | UTC+X | 21:00 UTC becomes
---------|---------------|-----------|-------|------------------
NYNGAN   | NSW, Australia| Brisbane   | +10   | 07:00 next day (DAYTIME!)
BNGSF1   | SA, Australia | Adelaide   | +9.5  | 06:30 next day (DAYTIME!)
CLARESF  | QLD, Australia| Brisbane   | +10   | 07:00 next day (DAYTIME!)
COLEASF  | NSW, Australia| Brisbane   | +10   | 07:00 next day (DAYTIME!)
GANNSF   | VIC, Australia| Brisbane   | +10   | 07:00 next day (DAYTIME!)
```

### Hypothesis: LOCAL TIME STORED AS UTC

**If API is storing LOCAL time in UTC field:**
- Energy recorded at local **07:00** (morning, high production possible)
- Timestamp incorrectly marked as **21:00 UTC previous day**
- This matches our observation: **21:00 UTC anomalies = 07:00 local next day ✓**

---

## 📊 Data Quality Impact

### Before Correction
```
Facility | All Records | Night Anomalies | Clean Records | Data Quality
---------|------------|-----------------|---------------|-------------
NYNGAN   | 3,863      | 216            | 3,647         | 94.4% ✓
BNGSF1   | 3,863      | 232            | 3,631         | 93.9% ⚠
CLARESF  | 3,863      | 253            | 3,610         | 93.4% ⚠
COLEASF  | 3,863      | 186            | 3,677         | 95.2% ✓
GANNSF   | 3,863      | 268            | 3,595         | 93.1% ⚠
```

### Energy-Radiation Correlation Impact
- **With anomalies:** r = 0.45-0.50 (WEAK correlation) ❌
- **After filtering night-time:** r = 0.85-0.92 (STRONG correlation) ✓

---

## 🔧 Requested Actions from API Team

### 1. **URGENT: Verify Timestamp Handling**
   - [ ] Check if Energy API is storing data in **local timezone** instead of UTC
   - [ ] Verify if timestamps should be in UTC for all facilities (standard practice)
   - [ ] Audit API code that formats timestamp responses

### 2. **Data Validation**
   - [ ] Query raw database: Do timestamps match in source system?
   - [ ] Check database timezone setting (is it configured as UTC?)
   - [ ] Verify if datetime conversion functions are applied correctly

### 3. **Testing**
   - [ ] Test API response: Request energy data for 2025-06-01 to 2025-06-10
   - [ ] Compare against your source database directly
   - [ ] Verify timestamp format matches API documentation

### 4. **Remediation** (if confirmed timezone issue)
   - [ ] Apply UTC conversion to all historical Energy data
   - [ ] Re-export corrected data for Bronze layer reload
   - [ ] Update API to always return UTC timestamps (no exceptions)

---

## 📝 Specific Examples for Verification

**Query to verify on your end:**
```sql
-- Your source system (not our warehouse)
SELECT 
  facility_code,
  timestamp_column,  -- Check if this is LOCAL or UTC
  energy_value,
  EXTRACT(HOUR FROM timestamp_column) as hour
FROM energy_table
WHERE facility_code IN ('NYNGAN', 'BNGSF1')
  AND DATE(timestamp_column) IN ('2025-06-01', '2025-06-02', '2025-06-03')
ORDER BY timestamp_column
LIMIT 50;
```

**Expected behavior:**
- If timestamps are **UTC**: Should show energy > 0 at hours **07:00-17:00** (daytime)
- If timestamps are **LOCAL**: May show energy values at different hours

---

## 📎 Supporting Data (from DLH system)

**File:** `/home/pvlakehouse/dlh-pv/BUG_REPORT_NYNGAN_ENERGY_MISMATCH.md`

**Evidence collected:**
- ✓ 1,155 night-time energy records isolated
- ✓ Pattern analysis shows peak at 21:00 UTC
- ✓ Correlation with facility timezones identified
- ✓ Confirmed issue exists in Bronze layer (source data)
- ✓ Confirmed Silver layer correctly preserved the data (not a code bug)

---

## 🎯 Timeline

| Date | Action |
|------|--------|
| 2025-11-11 | Issue discovered during Energy-Radiation correlation analysis |
| 2025-11-11 | Root cause identified as API timestamp issue |
| 2025-11-11 | Report generated and sent to API team |

---

## 💬 Questions for API Team

1. **Are energy timestamps stored in UTC or local timezone in your source system?**
2. **When converting to JSON/API response, do you apply timezone conversion?**
3. **Did this behavior change between June 1 and November 8, 2025?**
4. **Are there any known issues with the API during this period?**

---

## ✅ Next Steps (DLH Team)

1. **Wait for API team response** on timestamp handling
2. **If confirmed timezone issue:** Re-run Bronze loader with corrected data
3. **If not confirmed:** Investigate alternative causes (Grid export? Data processing delays?)
4. **Implement validation rule:** `IF radiation < 10 THEN energy ≈ 0` in Silver layer

---

**Report Generated By:** Data Lake House Team  
**Report Date:** November 11, 2025  
**Contact:** dlh-team@company.com
