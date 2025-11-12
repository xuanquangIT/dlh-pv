# SQL Queries for Silver Layer Analysis & Improvements

## 1️⃣ FACILITY-SPECIFIC CAPACITY ANALYSIS

```sql
-- Tìm mức năng lực tối đa thực tế của mỗi facility
SELECT 
    facility_code,
    COUNT(*) as total_hours,
    ROUND(MAX(energy_mwh), 2) as max_energy,
    ROUND(PERCENTILE_APPROX(energy_mwh, 0.95), 2) as p95_energy,
    ROUND(PERCENTILE_APPROX(energy_mwh, 0.90), 2) as p90_energy,
    ROUND(AVG(CASE WHEN HOUR(date_hour) BETWEEN 10 AND 14 THEN energy_mwh END), 2) as avg_peak_energy,
    COUNT(CASE WHEN energy_mwh > 100 THEN 1 END) as events_over_100
FROM iceberg.silver.clean_hourly_energy
WHERE quality_flag = 'GOOD'
GROUP BY facility_code
ORDER BY max_energy DESC;

-- Result: Determine facility-specific thresholds
-- COLEASF: 139.9 MWh (highest capacity)
-- Others: 99-109 MWh
```

---

## 2️⃣ NIGHT-TIME HIGH ENERGY INVESTIGATION

```sql
-- Tìm tất cả records năng lượng cao vào ban đêm
SELECT 
    facility_code,
    date_hour,
    HOUR(date_hour) as hour_of_day,
    energy_mwh,
    quality_issues,
    LAG(energy_mwh) OVER (PARTITION BY facility_code ORDER BY date_hour) as prev_energy,
    LEAD(energy_mwh) OVER (PARTITION BY facility_code ORDER BY date_hour) as next_energy
FROM iceberg.silver.clean_hourly_energy
WHERE (HOUR(date_hour) >= 22 OR HOUR(date_hour) < 6)
  AND energy_mwh > 0.5
ORDER BY facility_code, date_hour;

-- Root cause: 
-- - Meter billing adjustments at night
-- - Timestamp offset issues
-- - Duplicate counting
```

---

## 3️⃣ DAYTIME ZERO ENERGY WITH RADIATION

```sql
-- Tìm khoảng thời gian generation = 0 nhưng có mặt trời
SELECT 
    se.facility_code,
    se.date_hour,
    HOUR(se.date_hour) as hour_of_day,
    se.energy_mwh,
    sw.shortwave_radiation,
    sw.cloud_cover,
    se.quality_issues,
    sw.quality_flag as weather_quality
FROM iceberg.silver.clean_hourly_energy se
LEFT JOIN iceberg.silver.clean_hourly_weather sw 
    ON se.facility_code = se.facility_code 
    AND se.date_hour = sw.date_hour
WHERE (HOUR(se.date_hour) BETWEEN 6 AND 18)
  AND se.energy_mwh = 0
  AND sw.shortwave_radiation > 300
ORDER BY se.facility_code, se.date_hour;

-- Action: Mark as CAUTION or REJECT based on duration
```

---

## 4️⃣ PEAK HOUR LOW ENERGY PATTERN

```sql
-- Phân tích năng lực thấp tại giờ peak
SELECT 
    facility_code,
    HOUR(date_hour) as hour,
    COUNT(*) as hours_count,
    ROUND(MIN(energy_mwh), 2) as min_energy,
    ROUND(AVG(energy_mwh), 2) as avg_energy,
    ROUND(PERCENTILE_APPROX(energy_mwh, 0.25), 2) as q1_energy,
    ROUND(PERCENTILE_APPROX(energy_mwh, 0.50), 2) as median_energy,
    ROUND(PERCENTILE_APPROX(energy_mwh, 0.75), 2) as q3_energy,
    COUNT(CASE WHEN energy_mwh < 5 THEN 1 END) as hours_under_5mwh,
    ROUND(100.0 * COUNT(CASE WHEN energy_mwh < 5 THEN 1 END) / COUNT(*), 2) as pct_under_5
FROM iceberg.silver.clean_hourly_energy
WHERE HOUR(date_hour) BETWEEN 10 AND 14
GROUP BY facility_code, HOUR(date_hour)
ORDER BY facility_code, hour;

-- Expected peak: 40-60 MWh
-- Anomaly: < 5 MWh during peak hours
```

---

## 5️⃣ ENERGY-RADIATION DIVERGENCE DEEP DIVE

```sql
-- Tính divergence per facility, per hour để tìm pattern
WITH hourly_stats AS (
    SELECT 
        se.facility_code,
        HOUR(se.date_hour) as hour_of_day,
        COUNT(*) as total_records,
        ROUND(AVG(se.energy_mwh), 2) as avg_energy,
        ROUND(AVG(sw.shortwave_radiation), 2) as avg_radiation,
        ROUND(
            AVG(ABS(
                ((se.energy_mwh - MIN(se.energy_mwh)) / (MAX(se.energy_mwh) - MIN(se.energy_mwh))) -
                ((sw.shortwave_radiation - MIN(sw.shortwave_radiation)) / (MAX(sw.shortwave_radiation) - MIN(sw.shortwave_radiation)))
            )), 4
        ) as avg_divergence
    FROM iceberg.silver.clean_hourly_energy se
    LEFT JOIN iceberg.silver.clean_hourly_weather sw
        ON se.facility_code = sw.facility_code
        AND se.date_hour = sw.date_hour
    WHERE se.energy_mwh > 0
    GROUP BY se.facility_code, HOUR(se.date_hour)
)
SELECT *
FROM hourly_stats
WHERE avg_divergence > 0.3
ORDER BY facility_code, hour_of_day;

-- High divergence at sunrise/sunset = expected
-- High divergence at peak hours = anomaly
```

---

## 6️⃣ QUALITY FLAG DISTRIBUTION

```sql
-- Xem phân bố quality_flag hiện tại
SELECT 
    facility_code,
    quality_flag,
    COUNT(*) as count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY facility_code), 2) as pct
FROM iceberg.silver.clean_hourly_energy
GROUP BY facility_code, quality_flag
ORDER BY facility_code, quality_flag;

-- Target: 95%+ GOOD per facility
```

---

## 7️⃣ TEMPORAL CONSISTENCY CHECK (DST & Gaps)

```sql
-- Tìm gaps trong hourly data
WITH hourly_seq AS (
    SELECT 
        facility_code,
        date_hour,
        LEAD(date_hour) OVER (PARTITION BY facility_code ORDER BY date_hour) as next_hour,
        CAST((unix_timestamp(LEAD(date_hour) OVER (PARTITION BY facility_code ORDER BY date_hour)) - 
              unix_timestamp(date_hour)) / 3600 AS INT) as hours_diff
    FROM iceberg.silver.clean_hourly_energy
)
SELECT 
    facility_code,
    date_hour,
    next_hour,
    hours_diff
FROM hourly_seq
WHERE hours_diff != 1 AND hours_diff IS NOT NULL
ORDER BY facility_code, date_hour;

-- Most likely: DST transitions (2025-04-06 & 2025-10-05 in Australia)
```

---

## 8️⃣ EQUIPMENT MAINTENANCE DETECTION

```sql
-- Tìm periods có 0 generation và xác định maintenance
SELECT 
    facility_code,
    MIN(date_hour) as outage_start,
    MAX(date_hour) as outage_end,
    DATEDIFF(hour, MIN(date_hour), MAX(date_hour)) + 1 as outage_hours,
    COUNT(*) as zero_energy_hours,
    ROUND(AVG(shortwave_radiation), 2) as avg_radiation_during_outage
FROM (
    SELECT 
        se.facility_code,
        se.date_hour,
        se.energy_mwh,
        LAG(se.energy_mwh) OVER (PARTITION BY se.facility_code ORDER BY se.date_hour) as prev_energy,
        sw.shortwave_radiation
    FROM iceberg.silver.clean_hourly_energy se
    LEFT JOIN iceberg.silver.clean_hourly_weather sw
        ON se.facility_code = sw.facility_code AND se.date_hour = sw.date_hour
    WHERE se.energy_mwh = 0
) t
WHERE prev_energy > 0  -- Outage started
GROUP BY facility_code
HAVING outage_hours >= 4  -- At least 4 hours down
ORDER BY outage_hours DESC;

-- Expected: Scheduled maintenance (early morning or planned)
-- Anomaly: Mid-day unexpected shutdowns
```

---

## 9️⃣ MISSING DATA IDENTIFICATION

```sql
-- Tìm missing hours (expected data nhưng không có)
WITH expected_hours AS (
    SELECT 
        facility_code,
        EXPLODE(SEQUENCE(MIN(date_hour), MAX(date_hour), INTERVAL 1 HOUR)) as expected_date_hour
    FROM iceberg.silver.clean_hourly_energy
    GROUP BY facility_code
)
SELECT 
    e.facility_code,
    COUNT(*) as missing_hours,
    COLLECT_LIST(e.expected_date_hour) as missing_dates
FROM expected_hours e
LEFT JOIN iceberg.silver.clean_hourly_energy a
    ON e.facility_code = a.facility_code 
    AND e.expected_date_hour = a.date_hour
WHERE a.date_hour IS NULL
GROUP BY e.facility_code
HAVING COUNT(*) > 0;

-- Action: Flag as REJECT hoặc NULL handling
```

---

## 🔟 WEATHER IMPACT ON ENERGY

```sql
-- Phân tích tác động của cloud_cover lên energy generation
WITH energy_weather AS (
    SELECT 
        se.facility_code,
        se.date_hour,
        HOUR(se.date_hour) as hour_of_day,
        se.energy_mwh,
        sw.shortwave_radiation,
        sw.cloud_cover,
        CASE 
            WHEN sw.cloud_cover <= 20 THEN 'Clear'
            WHEN sw.cloud_cover <= 50 THEN 'Partly Cloudy'
            WHEN sw.cloud_cover <= 80 THEN 'Mostly Cloudy'
            ELSE 'Overcast'
        END as cloud_condition
    FROM iceberg.silver.clean_hourly_energy se
    LEFT JOIN iceberg.silver.clean_hourly_weather sw
        ON se.facility_code = sw.facility_code 
        AND se.date_hour = sw.date_hour
    WHERE HOUR(se.date_hour) BETWEEN 10 AND 14
)
SELECT 
    facility_code,
    cloud_condition,
    COUNT(*) as hours,
    ROUND(AVG(energy_mwh), 2) as avg_energy,
    ROUND(AVG(shortwave_radiation), 2) as avg_radiation,
    ROUND(STDDEV(energy_mwh), 2) as stddev_energy
FROM energy_weather
GROUP BY facility_code, cloud_condition
ORDER BY facility_code, avg_radiation DESC;

-- Expected: Clear sky → highest energy
-- Flag anomalies: Clear sky but low energy
```

---

## RECOMMENDED EXCLUSION RULES

```sql
-- Insert vào quality improvement pipeline
-- Rule 1: Night high energy
UPDATE iceberg.silver.clean_hourly_energy
SET quality_flag = 'CAUTION',
    quality_issues = CONCAT_WS('|', quality_issues, 'NIGHT_HIGH_ENERGY')
WHERE (HOUR(date_hour) >= 22 OR HOUR(date_hour) < 6)
  AND energy_mwh > 1.0;

-- Rule 2: Daytime zero energy with radiation
UPDATE iceberg.silver.clean_hourly_energy
SET quality_flag = 'CAUTION',
    quality_issues = CONCAT_WS('|', quality_issues, 'DAYTIME_ZERO_WITH_RADIATION')
WHERE HOUR(date_hour) BETWEEN 6 AND 18
  AND energy_mwh = 0
  AND shortwave_radiation > 300;

-- Rule 3: Facility-specific high energy (COLEASF only)
UPDATE iceberg.silver.clean_hourly_energy
SET quality_flag = 'CAUTION',
    quality_issues = CONCAT_WS('|', quality_issues, 'HIGH_ENERGY_ANOMALY')
WHERE facility_code = 'COLEASF'
  AND energy_mwh > 140.0;

-- Rule 4: Facility-specific high energy (others)
UPDATE iceberg.silver.clean_hourly_energy
SET quality_flag = 'CAUTION',
    quality_issues = CONCAT_WS('|', quality_issues, 'HIGH_ENERGY_ANOMALY')
WHERE facility_code != 'COLEASF'
  AND energy_mwh > 115.0;
```

