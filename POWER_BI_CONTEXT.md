# Power BI Visualization - Context for ChatGPT

## 📋 PROJECT OVERVIEW

**Project:** Solar Energy Forecasting with Machine Learning (Master's Thesis)
**Model:** RandomForest Regressor for solar energy prediction
**Performance:** R² = 86.5%, MAE = 6.37 MWh
**Technology Stack:** Spark + Iceberg + Trino + MLflow + Power BI

---

## 🎯 VISUALIZATION REQUIREMENTS

I need to create 2 charts in Power BI to analyze my RandomForest regression model:

### **Chart 1: Top 15 Feature Importance (Bar Chart)**
- **Purpose:** Show which features contribute most to prediction accuracy
- **X-axis:** Feature names (e.g., energy_lag_1h, direct_radiation)
- **Y-axis:** Importance percentage (0-100%)
- **Color:** By feature category (LAG Features, Weather, Temporal, Interaction)
- **Filter:** Only show top 15 features (is_top_15 = true)
- **Sort:** Descending by importance percentage

### **Chart 2: Actual vs Predicted Energy (Scatter Plot)**
- **Purpose:** Visualize model accuracy - how close predictions are to actual values
- **X-axis:** Actual energy (MWh)
- **Y-axis:** Predicted energy (MWh)
- **Reference line:** Perfect prediction line (y = x) at 45-degree angle
- **Color:** By facility or by error magnitude
- **Filter:** Remove zero values (nighttime data)
- **Tooltip:** Show facility, timestamp, MAE, MAPE

---

## 📊 DATA TABLES AVAILABLE IN POWER BI

All tables loaded via **Trino connector** from Iceberg Gold layer:

### **Table 1: dim_feature_importance**
**Purpose:** Feature importance metrics from RandomForest model
**Columns:**
- `feature_importance_key` (BIGINT) - Primary key (1-22)
- `feature_name` (VARCHAR) - Feature name (e.g., "energy_lag_1h")
- `feature_category` (VARCHAR) - Category: "LAG Features", "Weather", "Temporal", "Interaction", "Energy Quality"
- `importance_value` (DOUBLE) - Raw importance value (0.0-1.0)
- `importance_percentage` (DOUBLE) - Percentage contribution (0-100%)
- `rank_overall` (INTEGER) - Global ranking (1-22)
- `rank_in_category` (INTEGER) - Ranking within category
- `is_top_15` (BOOLEAN) - TRUE if in top 15 features
- `feature_description` (VARCHAR) - Human-readable description
- `model_version_key` (BIGINT) - Model version reference
- `created_at` (TIMESTAMP) - Record creation time
- `updated_at` (TIMESTAMP) - Last update time

**Sample Data (Top 5):**
```
rank | feature_name      | category      | importance_pct | is_top_15
-----|-------------------|---------------|----------------|----------
1    | energy_lag_1h     | LAG Features  | 30.12          | TRUE
2    | energy_lag_24h    | LAG Features  | 25.70          | TRUE
3    | energy_lag_168h   | LAG Features  | 17.52          | TRUE
4    | direct_radiation  | Weather       | 11.83          | TRUE
5    | hour_radiation_i. | Interaction   | 4.86           | TRUE
```

**Category Summary:**
- **LAG Features** (3 features): 73.34% total importance - Energy from previous hours/days
- **Weather** (4 features): 14.74% total importance - Solar radiation, cloud cover, temperature
- **Interaction** (4 features): 10.64% total importance - Combined effects (radiation×temp, hour×radiation)
- **Temporal** (7 features): 1.22% total importance - Time patterns (hour, day, month, cyclical encoding)
- **Energy Quality** (2 features): 0.02% total importance - Data completeness metrics

---

### **Table 2: fact_solar_forecast_regression**
**Purpose:** Model predictions vs actual values for test set
**Columns:**
- `forecast_id` (BIGINT) - Primary key
- `date_key` (INTEGER) - Date dimension key (format: YYYYMMDD)
- `time_key` (INTEGER) - Time dimension key (format: HHMM)
- `facility_key` (INTEGER) - Solar facility ID (1-10)
- `weather_condition_key` (INTEGER) - Weather condition reference
- `model_version_key` (BIGINT) - Model version reference
- `actual_energy_mwh` (DOUBLE) - **Actual energy produced (MWh)** ← X-axis for scatter plot
- `predicted_energy_mwh` (DOUBLE) - **Model prediction (MWh)** ← Y-axis for scatter plot
- `forecast_error_mwh` (DOUBLE) - Error = Actual - Predicted
- `absolute_percentage_error` (DOUBLE) - MAPE = |Error| / Actual × 100%
- `mae_metric` (DOUBLE) - Mean Absolute Error
- `rmse_metric` (DOUBLE) - Root Mean Squared Error
- `r2_score` (DOUBLE) - R² score (0.865 = 86.5% variance explained)
- `forecast_timestamp` (TIMESTAMP) - Date-time of prediction
- `created_at` (TIMESTAMP) - Record creation time

**Sample Data:**
```
facility | timestamp           | actual_mwh | predicted_mwh | error_mwh | mape_%
---------|---------------------|------------|---------------|-----------|-------
5        | 2025-10-15 12:00:00 | 45.23      | 45.12         | 0.11      | 0.24
3        | 2025-10-08 11:00:00 | 67.89      | 67.98         | -0.09     | 0.13
2        | 2025-10-22 13:00:00 | 52.34      | 51.87         | 0.47      | 0.90
1        | 2025-10-05 10:00:00 | 38.12      | 39.45         | -1.33     | 3.49
```

**Data Characteristics:**
- **Total Records:** 8,060 predictions (test set = 5% of data)
- **Time Range:** 2025-09-26 to 2025-11-01 (37 days)
- **Facilities:** 10 solar plants (each has ~806 predictions)
- **Zero Values:** Nighttime hours (actual_energy_mwh = 0) - should be filtered out for scatter plot
- **Peak Hours:** 10:00-14:00 (highest energy production)
- **Average MAE:** 6.37 MWh across all facilities
- **Average MAPE:** ~8-12% for daytime predictions

---

## 🔧 POWER BI CONNECTION DETAILS

**Data Source:** Trino (SQL query engine)
- **Server:** localhost:8080
- **Catalog:** iceberg
- **Schema:** gold
- **Authentication:** None (local development)

**Tables Imported:**
1. `iceberg.gold.dim_feature_importance` (22 rows)
2. `iceberg.gold.fact_solar_forecast_regression` (8,060 rows)

---

## 🎨 VISUALIZATION SPECIFICATIONS

### **Chart 1: Feature Importance Bar Chart**

**Requirements:**
1. **Visual Type:** Clustered Bar Chart (horizontal bars preferred)
2. **Data Preparation:**
   - Filter: `is_top_15 = TRUE` (only 15 features)
   - Sort: By `importance_percentage` descending
   
3. **Mappings:**
   - **Y-axis (Category):** `feature_name`
   - **X-axis (Value):** `importance_percentage`
   - **Legend (Color):** `feature_category`
   - **Tooltip:** Include `feature_description`, `rank_overall`

4. **Formatting:**
   - **Title:** "Top 15 Features - Contribution to Model Accuracy"
   - **X-axis label:** "Importance (%)"
   - **Y-axis label:** "Feature Name"
   - **Color scheme:** 
     - LAG Features: Blue shades
     - Weather: Green shades
     - Interaction: Orange shades
     - Temporal: Purple shades
   - **Data labels:** Show percentage values on bars
   - **Reference line:** Optional - mark 5% threshold

5. **Expected Insights:**
   - Top 3 LAG features dominate (73% combined)
   - Weather features are secondary predictors (15%)
   - Temporal patterns have minimal impact (<2%)

---

### **Chart 2: Actual vs Predicted Scatter Plot**

**Requirements:**
1. **Visual Type:** Scatter Chart with reference line
2. **Data Preparation:**
   - Filter: `actual_energy_mwh > 1` (remove nighttime zeros)
   - Optional: Filter by time range or specific facilities
   
3. **Mappings:**
   - **X-axis:** `actual_energy_mwh`
   - **Y-axis:** `predicted_energy_mwh`
   - **Legend (Color):** `facility_key` OR color by `absolute_percentage_error` ranges
   - **Size:** Optional - by `mae_metric`
   - **Tooltip:** 
     - Facility ID
     - Timestamp (forecast_timestamp)
     - Actual Energy (MWh)
     - Predicted Energy (MWh)
     - Error (forecast_error_mwh)
     - MAPE (absolute_percentage_error)

4. **Reference Line:**
   - **Type:** Diagonal line from origin (0,0) to max value
   - **Formula:** Y = X (perfect prediction line)
   - **Style:** Dashed red line
   - **Label:** "Perfect Prediction"

5. **Formatting:**
   - **Title:** "Model Accuracy: Actual vs Predicted Energy Production"
   - **X-axis label:** "Actual Energy (MWh)"
   - **Y-axis label:** "Predicted Energy (MWh)"
   - **X-axis range:** 0 to max(actual_energy_mwh) + 5 MWh
   - **Y-axis range:** 0 to max(predicted_energy_mwh) + 5 MWh
   - **Aspect ratio:** Equal scales (1:1) for proper visual assessment
   - **Grid lines:** Enabled for easier reading

6. **Color Options:**
   - **Option A - By Facility:** Different color per facility (1-10)
   - **Option B - By Error Magnitude:**
     - Excellent: MAPE < 5% (Green)
     - Good: MAPE 5-10% (Yellow)
     - Fair: MAPE 10-20% (Orange)
     - Poor: MAPE > 20% (Red)

7. **Expected Insights:**
   - Points clustered near diagonal line = good predictions
   - R² = 0.865 means 86.5% of points follow the trend
   - Outliers (far from line) = model struggles with specific conditions

---

## 📐 CHART INTERPRETATION GUIDE

### **Feature Importance Chart:**
- **What it shows:** Which input variables drive predictions
- **Key finding:** Historical energy (LAG features) is the strongest predictor
- **Business value:** Guides feature engineering - focus on LAG and Weather features

### **Actual vs Predicted Chart:**
- **What it shows:** How accurate the model is
- **Perfect prediction:** All points on diagonal line
- **Good model:** Points clustered tightly around line (current R² = 0.865)
- **Systematic error:** Points consistently above/below line
- **Random error:** Points scattered evenly around line

---

## 🔍 ADDITIONAL CONTEXT

### **Feature Descriptions:**

**LAG Features (Time Series Predictors):**
- `energy_lag_1h`: Energy from 1 hour ago (captures immediate trends)
- `energy_lag_24h`: Energy from same hour yesterday (daily patterns)
- `energy_lag_168h`: Energy from same hour last week (weekly patterns)

**Weather Features (Environmental Predictors):**
- `direct_radiation`: Direct solar radiation (W/m²) - strongest weather predictor
- `shortwave_radiation`: Total incoming solar radiation (W/m²)
- `cloud_cover`: Cloud coverage percentage (%) - inversely related to energy
- `temperature_2m`: Air temperature at 2m height (°C) - affects panel efficiency

**Interaction Features (Combined Effects):**
- `hour_radiation_interaction`: Hour of day × Solar radiation (captures time-dependent solar angles)
- `radiation_temp_interaction`: Solar radiation × Temperature (panel efficiency losses at high temps)
- `cloud_radiation_ratio`: Cloud-adjusted radiation = (100 - cloud%) × radiation / 100
- `completeness_radiation_interaction`: Data quality × Radiation (accounts for measurement reliability)

**Temporal Features (Time Patterns):**
- `hour_of_day`: 0-23 hour (captures daily cycle)
- `hour_sin`, `hour_cos`: Cyclical encoding of hour (smooth transitions midnight ↔ 1am)
- `month`, `month_sin`, `month_cos`: Seasonal patterns
- `day_of_week`, `is_weekend`: Weekly patterns (less important for solar)

---

## ✅ QUESTIONS TO ASK CHATGPT

**Copy-paste this prompt to ChatGPT:**

---

I'm working on a solar energy forecasting project using RandomForest regression (R² = 86.5%, MAE = 6.37 MWh). I've already loaded 2 tables from my Gold layer into Power BI via Trino connector:

1. **dim_feature_importance** (22 rows) - Contains feature importance metrics
2. **fact_solar_forecast_regression** (8,060 rows) - Contains actual vs predicted values

I need step-by-step guidance to create 2 charts:

**Chart 1: Top 15 Feature Importance (Bar Chart)**
- Show features ranked by importance percentage
- Color by category (LAG Features, Weather, Temporal, Interaction)
- Filter to show only top 15 (is_top_15 = TRUE)
- Sort descending by importance_percentage

**Chart 2: Actual vs Predicted Energy (Scatter Plot)**
- X-axis: actual_energy_mwh
- Y-axis: predicted_energy_mwh
- Add diagonal reference line (Y = X) for "perfect prediction"
- Filter out nighttime data (actual_energy_mwh > 1)
- Color by facility_key or error magnitude

Please provide:
1. Step-by-step instructions for each chart (which visual to select, how to drag fields, formatting)
2. How to add and format the diagonal reference line in Chart 2
3. How to create calculated columns for error categories if needed (MAPE ranges)
4. Best practices for titles, labels, colors, and tooltips
5. How to ensure Chart 2 has equal X/Y scales (1:1 aspect ratio)

**Context:** See attached POWER_BI_CONTEXT.md for full table schemas and sample data.

---

## 📎 FILES TO SHARE WITH CHATGPT

1. **This file:** `POWER_BI_CONTEXT.md` (provides all context)
2. **Optional:** Screenshot of Power BI interface showing imported tables
3. **Optional:** Sample query results from Trino to validate data

---

## 🎯 SUCCESS CRITERIA

**Chart 1 is done when:**
- ✅ Shows exactly 15 features (filtered by is_top_15)
- ✅ Sorted by importance descending (energy_lag_1h at top)
- ✅ Color-coded by category (LAG = blue, Weather = green, etc.)
- ✅ Data labels show percentage values
- ✅ Tooltip includes feature description

**Chart 2 is done when:**
- ✅ Scatter plot with actual on X-axis, predicted on Y-axis
- ✅ Diagonal reference line visible (Y = X)
- ✅ Nighttime data filtered out (only actual > 1 MWh)
- ✅ Points clustered near diagonal line (R² = 0.865 validation)
- ✅ Tooltip shows facility, timestamp, actual, predicted, error, MAPE
- ✅ Equal axis scales (1:1 ratio) for accurate visual assessment

---

## 📊 EXAMPLE DAX FORMULAS (if needed)

**Error Category (Calculated Column):**
```dax
Error_Category = 
SWITCH(
    TRUE(),
    [absolute_percentage_error] < 5, "Excellent (<5%)",
    [absolute_percentage_error] < 10, "Good (5-10%)",
    [absolute_percentage_error] < 20, "Fair (10-20%)",
    "Poor (>20%)"
)
```

**Perfect Prediction Line (for reference):**
```dax
Perfect_Prediction = [actual_energy_mwh]
```

---

## 🚀 QUICK START CHECKLIST

Before asking ChatGPT:
1. ✅ Confirm tables loaded in Power BI (check field list)
2. ✅ Verify row counts: dim_feature_importance = 22, fact_solar_forecast_regression = 8,060
3. ✅ Test filter: is_top_15 = TRUE should show 15 rows
4. ✅ Check data types: importance_percentage and actual/predicted are decimals
5. ✅ Share this POWER_BI_CONTEXT.md file with ChatGPT

Ready to create professional-quality visualizations! 📈
