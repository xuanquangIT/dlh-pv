# ETL Architecture - Bronze & Silver Layers

## Tổng Quan Kiến Trúc

Hệ thống tuân theo **Medallion Architecture** (3 lớp):

```
┌─────────────────────────────────────────────────────────────────┐
│                      API EXTERNAL (Hourly)                      │
│  OpenElectricity  │  Open-Meteo Weather  │  Open-Meteo Air-Q    │
└──────────────┬────────────────────────────────────┬──────────────┘
               │ (Thô, chưa xử lý)                  │
               ▼                                     ▼
    ┌──────────────────────────┐       ┌──────────────────────────┐
    │    BRONZE LAYER          │       │  (Partitioned by date)   │
    │ ──────────────────────── │       └──────────────────────────┘
    │ • raw_facilities         │       Load Mode:
    │ • raw_facility_timeseries│       • backfill (overwrite)
    │ • raw_facility_weather   │       • incremental (merge)
    │ • raw_facility_air_quality
    └──────────────┬───────────┘
                   │ (Transform, Validate, Quality Check)
                   ▼
    ┌──────────────────────────┐
    │    SILVER LAYER          │
    │ ──────────────────────── │
    │ ✅ clean_facility_master  │
    │ ⚠️ clean_hourly_energy    │
    │ ✅ clean_hourly_weather   │
    │ ✅ clean_hourly_air_quality
    └──────────────┬───────────┘
                   │ (GOOD records only)
                   ▼
    ┌──────────────────────────┐
    │    GOLD LAYER            │  (Sắp triển khai)
    │ ──────────────────────── │
    │ 📊 fact_hourly_energy    │
    │ 📊 fact_hourly_weather   │
    │ 📈 dim_facility          │
    └──────────────┬───────────┘
                   │
        ┌──────────┴──────────┐
        ▼                     ▼
    ┌─────────────┐      ┌──────────┐
    │  Power BI   │      │  ML Model│
    │  Dashboard  │      │ Training │
    └─────────────┘      └──────────┘
```

---

## Quick Stats

| Layer | Records | Quality | Status |
|---|---|---|---|
| **Bronze** | 4 tables | Raw data | ✅ ACTIVE |
| **Silver** | 4 tables | 85-100% GOOD | ✅ 97.1/100 READY |
| **Gold** | Pending | N/A | ⏳ COMING |

---

## Bronze Layer (Thu Thập Thô)

📄 **Tài liệu chi tiết**: `BRONZE_LAYER.md`

**4 Bảng Ingestion**:
- `raw_facilities` - Metadata cơ sở (mỗi tuần)
- `raw_facility_timeseries` - Năng lượng theo giờ (mỗi giờ)
- `raw_facility_weather` - Thời tiết từ Open-Meteo (mỗi ngày)
- `raw_facility_air_quality` - Chất lượng không khí (mỗi ngày)

**Cách chạy**:
```bash
python -m pv_lakehouse.etl.bronze load_timeseries
python -m pv_lakehouse.etl.bronze load_weather
python -m pv_lakehouse.etl.bronze load_air_quality
```

---

## Silver Layer (Làm Sạch & Xác Thực)

📄 **Tài liệu chi tiết**: `SILVER_LAYER.md`

**4 Bảng Transformed**:
- `clean_facility_master` - Metadata làm sạch (SCD Type 1)
- `clean_hourly_energy` - Năng lượng với 7 quality checks
- `clean_hourly_weather` - Thời tiết với validation bounds
- `clean_hourly_air_quality` - Không khí với range checking

**Quality Flags**: GOOD | CAUTION | REJECT

**Cách chạy**:
```bash
python -m pv_lakehouse.etl.silver load_all
python -m pv_lakehouse.etl.silver load_hourly_energy
python -m pv_lakehouse.etl.silver load_hourly_weather
```

**Chất Lượng Hiện Tại**:
- Energy: 85.46% GOOD (⚠️ CAUTION)
- Weather: 96.11% GOOD (✅ GOOD)
- Air Quality: 100% GOOD (✅ EXCELLENT)
- **Overall: 97.1/100 → READY FOR PRODUCTION**

---

## Thay Đổi Gần Đây

- **Round 1 Fix** (Applied): Night radiation → REJECT (từ CAUTION)
- **Round 2 Analysis** (Reverted): Threshold relaxation không cải thiện
- **Final Verdict**: Baseline data (85.46% energy) đủ chất lượng cho gold/ML

---

## Next Steps

1. **Gold Layer** - Fact tables từ silver GOOD records
2. **ML Training** - Stratified sampling, normalization
3. **Power BI** - Dimension tables, fact tables
4. **Monitoring** - Quality alerts, SLAs

---

## Tệp Tham Khảo

- Bronze: `/src/pv_lakehouse/etl/bronze/`
- Silver: `/src/pv_lakehouse/etl/silver/`
- Config: `pyproject.toml`
- Test: `notebooks/silver_readiness_for_gold.ipynb`
- Data: `src/pv_lakehouse/exported_data/*.csv`

---

**Liên Quan**:
- [🟫 BRONZE_LAYER.md](./BRONZE_LAYER.md) - Thu thập dữ liệu thô từ API
- [🟩 SILVER_LAYER.md](./SILVER_LAYER.md) - Làm sạch, xác thực, quality check
- [📊 ETL Development Guide](./etl-development.md)
- [🌍 Timezone Implementation](./TIMEZONE_IMPLEMENTATION.md)
