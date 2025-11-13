
📋 TIER 1 RECOMMENDATIONS (High Priority - Implement Immediately):

1️⃣  NIGHTTIME ENERGY EXCLUSION
   ├─ Problem: 195 records with energy > 0.5 MWh during 22:00-06:00
   ├─ Action: Flag all nighttime energy generation as TEMPORAL_ANOMALY
   ├─ Threshold: energy_mwh = 0 for hours 22:00-05:59
   └─ Expected impact: Clean up ~0.5% of records

2️⃣  EFFICIENCY RATIO VALIDATION  
   ├─ Problem: Some facilities show >100% efficiency (energy > capacity)
   ├─ Action: Flag when (energy_mwh / facility_capacity) > 1.0 OR > 0.5 during peak (11:00-15:00)
   ├─ Threshold: efficiency_ratio > 1.0 = EFFICIENCY_ANOMALY
   └─ Expected impact: Catch equipment failures early

3️⃣  EQUIPMENT FAULT DETECTION
   ├─ Problem: Daytime underperformance not detected
   ├─ Action: Flag when energy < 0.5 MWh during clear sky (06:00-18:00, shortwave > 500)
   ├─ Threshold: energy_mwh < 0.5 AND shortwave > 500 AND hour in [6..18]
   └─ Expected impact: Identify 195 maintenance issues/facility/year

4️⃣  EXTREME OUTLIER REMOVAL
   ├─ Problem: 0 records with |z-score| > 5 (extreme deviations)
   ├─ Action: Exclude from analytics or investigate root cause
   ├─ Rules: Mark as REJECT if z-score > 5 or violates physical bounds
   └─ Expected impact: Remove <0.1% of data, improve statistical reliability

📋 TIER 2 RECOMMENDATIONS (Medium Priority - Implement Phase 3):

5️⃣  RADIATION COMPONENT MISMATCH
   ├─ Problem: Some records show high DNI but low shortwave (sensor drift)
   ├─ Action: Flag when DNI > 500 AND shortwave < (DNI * 0.3)
   ├─ Threshold: ratio_threshold = 0.3 for clear sky conditions
   └─ Expected impact: Detect ~1,594 potential sensor issues

6️⃣  CORRELATION-BASED VALIDATION
   ├─ Problem: Energy-radiation correlation varies by facility (range: 0.85-0.95)
   ├─ Action: Flag records where correlation drops below facility baseline - 0.10
   ├─ Method: Rolling 30-day correlation window by facility
   └─ Expected impact: Early warning for equipment degradation

7️⃣  TEMPERATURE CONSISTENCY
   ├─ Problem: Missing temperature validation in current setup
   ├─ Action: Validate temperature range (-20 to 50°C) and flag extremes
   ├─ Threshold: temperature < -20 OR temperature > 50
   └─ Expected impact: Catch sensor calibration drift

📋 TIER 3 RECOMMENDATIONS (Low Priority - Long-term Improvements):

8️⃣  CLOUD COVER IMPACT MODELING
   ├─ Problem: High radiation with 100% cloud cover = data inconsistency
   ├─ Action: Flag when cloud_cover = 100 AND shortwave_radiation > 100
   ├─ Threshold: Logical inconsistency flagged as RADIATION_COMPONENT_MISMATCH
   └─ Expected impact: Improve weather data quality

9️⃣  FACILITY-SPECIFIC BASELINES
   ├─ Problem: Fixed thresholds don't account for facility differences
   ├─ Action: Build facility-specific quality profiles (capacity, location, weather patterns)
   ├─ Method: Calculate P5, P25, P75, P95 by facility and hour of day
   └─ Expected impact: Reduce false positives by ~30%

🔟 MISSING DATA IMPUTATION
   ├─ Problem: No handling for missing records
   ├─ Action: Flag missing hours as data gaps (not interpolate)
   ├─ Method: Explicit MISSING flag in quality_flag
   └─ Expected impact: Transparency about data gaps
