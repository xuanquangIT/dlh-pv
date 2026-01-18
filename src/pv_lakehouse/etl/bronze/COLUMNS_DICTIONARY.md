# 📊 TỪ ĐIỂN CỘT DỮ LIỆU BRONZE LAYER

Tên tiếng Việt và ý nghĩa của các cột trong Bronze tables.

---

## 🌦️ raw_facility_weather (Thời tiết)

**Table:** `lh.bronze.raw_facility_weather`

| Tên cột | Tên tiếng Việt | Đơn vị | Mô tả |
|---------|----------------|--------|-------|
| `facility_code` | Mã nhà máy | - | Mã định danh (VD: NYNGAN) |
| `facility_name` | Tên nhà máy | - | Tên đầy đủ của nhà máy |
| `weather_timestamp` | Thời gian thời tiết | - | Timestamp giờ địa phương |
| `shortwave_radiation` | Bức xạ sóng ngắn tổng | W/m² | Tổng bức xạ mặt trời tới mặt đất |
| `direct_radiation` | Bức xạ trực tiếp | W/m² | Bức xạ chiếu thẳng từ mặt trời |
| `diffuse_radiation` | Bức xạ khuếch tán | W/m² | Bức xạ phân tán qua mây/khí quyển |
| `direct_normal_irradiance` | Bức xạ pháp tuyến trực tiếp (DNI) | W/m² | Bức xạ trực tiếp đo vuông góc với tia sáng |
| `terrestrial_radiation` | Bức xạ mặt đất | W/m² | Bức xạ hồng ngoại từ mặt đất phát ra |
| `temperature_2m` | Nhiệt độ không khí | °C | Nhiệt độ tại độ cao 2 mét |
| `dew_point_2m` | Điểm sương | °C | Nhiệt độ để hơi nước ngưng tụ |
| `wet_bulb_temperature_2m` | Nhiệt độ bóng ướt | °C | Nhiệt độ đo bằng nhiệt kế bóng ướt |
| `cloud_cover` | Độ che phủ mây tổng | % | Phần trăm bầu trời bị mây che |
| `cloud_cover_low` | Mây tầng thấp | % | Mây dưới 2km (cumulus, stratus) |
| `cloud_cover_mid` | Mây tầng trung | % | Mây 2-6km (altostratus, altocumulus) |
| `cloud_cover_high` | Mây tầng cao | % | Mây trên 6km (cirrus, cirrostratus) |
| `precipitation` | Lượng mưa | mm | Lượng mưa trong giờ |
| `is_day` | Ban ngày | 0/1 | 1 = ban ngày, 0 = ban đêm |
| `sunshine_duration` | Thời gian nắng | giây | Số giây có nắng trong giờ (max 3600) |
| `total_column_integrated_water_vapour` | Hơi nước cột tổng | kg/m² | Lượng hơi nước trong cột khí quyển |
| `boundary_layer_height` | Chiều cao lớp biên | m | Độ cao lớp khí quyển tiếp xúc mặt đất |
| `wind_speed_10m` | Tốc độ gió | m/s | Tốc độ gió tại 10 mét |
| `wind_direction_10m` | Hướng gió | độ | 0°=Bắc, 90°=Đông, 180°=Nam, 270°=Tây |
| `wind_gusts_10m` | Gió giật | m/s | Tốc độ gió giật tối đa |
| `pressure_msl` | Áp suất khí quyển | hPa | Áp suất quy về mực nước biển |
| `ingest_mode` | Chế độ nạp | - | "incremental" hoặc "backfill" |
| `ingest_timestamp` | Thời gian nạp | - | Timestamp khi dữ liệu được nạp |
| `weather_date` | Ngày thời tiết | - | Ngày của weather_timestamp |

---

## ⚡ raw_facility_timeseries (Năng lượng)

**Table:** `lh.bronze.raw_facility_timeseries`

| Tên cột | Tên tiếng Việt | Đơn vị | Mô tả |
|---------|----------------|--------|-------|
| `facility_code` | Mã nhà máy | - | Mã định danh nhà máy |
| `facility_name` | Tên nhà máy | - | Tên đầy đủ của nhà máy |
| `network_code` | Mã mạng lưới | - | Mã thị trường điện (NEM, WEM) |
| `network_id` | ID mạng lưới | - | ID nội bộ thị trường |
| `network_region` | Vùng mạng lưới | - | VD: NSW1, QLD1, VIC1, SA1 |
| `unit_code` | Mã tổ máy | - | Mã unit trong nhà máy |
| `metric` | Loại chỉ số | - | "energy" = sản lượng, "power" = công suất |
| `interval` | Khoảng thời gian | - | Độ phân giải: "1h", "5m", v.v. |
| `value_unit` | Đơn vị giá trị | - | "MWh" cho energy, "MW" cho power |
| `interval_start` | Bắt đầu khoảng | - | Timestamp bắt đầu (UTC) |
| `value` | Giá trị | MWh/MW | Sản lượng điện hoặc công suất |
| `interval_ts` | Timestamp khoảng | - | Timestamp chính (UTC) |
| `interval_date` | Ngày khoảng | - | Ngày của interval_ts |
| `ingest_mode` | Chế độ nạp | - | "incremental" hoặc "backfill" |
| `ingest_timestamp` | Thời gian nạp | - | Timestamp khi dữ liệu được nạp |

### Giải thích Energy vs Power

| Metric | Tiếng Việt | Đơn vị | Ý nghĩa |
|--------|------------|--------|---------|
| `energy` | Sản lượng điện | MWh | Năng lượng tích lũy trong khoảng thời gian |
| `power` | Công suất | MW | Công suất tức thời tại thời điểm |

**Công thức:** Energy (MWh) = Power (MW) × Time (h)

---

## 💨 raw_facility_air_quality (Chất lượng không khí)

**Table:** `lh.bronze.raw_facility_air_quality`

| Tên cột | Tên tiếng Việt | Đơn vị | Mô tả |
|---------|----------------|--------|-------|
| `facility_code` | Mã nhà máy | - | Mã định danh nhà máy |
| `facility_name` | Tên nhà máy | - | Tên đầy đủ của nhà máy |
| `air_timestamp` | Thời gian đo | - | Timestamp giờ địa phương |
| `pm2_5` | Bụi mịn PM2.5 | µg/m³ | Hạt bụi đường kính ≤ 2.5 micromet |
| `pm10` | Bụi PM10 | µg/m³ | Hạt bụi đường kính ≤ 10 micromet |
| `dust` | Bụi tổng | µg/m³ | Tổng lượng bụi trong không khí |
| `nitrogen_dioxide` | Nitơ dioxide (NO₂) | µg/m³ | Khí thải từ đốt nhiên liệu |
| `ozone` | Ozon (O₃) | µg/m³ | Ozon tầng mặt đất |
| `sulphur_dioxide` | Lưu huỳnh dioxide (SO₂) | µg/m³ | Khí thải từ đốt than/dầu |
| `carbon_monoxide` | Carbon monoxide (CO) | mg/m³ | Khí độc từ đốt cháy không hoàn toàn |
| `uv_index` | Chỉ số UV | 0-11+ | Mức độ bức xạ tia cực tím |
| `uv_index_clear_sky` | Chỉ số UV trời quang | 0-11+ | UV khi không có mây |
| `air_date` | Ngày đo | - | Ngày của air_timestamp |
| `ingest_mode` | Chế độ nạp | - | "incremental" hoặc "backfill" |
| `ingest_timestamp` | Thời gian nạp | - | Timestamp khi dữ liệu được nạp |

### Bảng chỉ số UV

| UV Index | Mức độ | Khuyến cáo |
|----------|--------|------------|
| 0-2 | Thấp | An toàn ngoài trời |
| 3-5 | Trung bình | Đội mũ, che chắn |
| 6-7 | Cao | Tránh nắng 10h-16h |
| 8-10 | Rất cao | Hạn chế ra ngoài |
| 11+ | Cực cao | Nguy hiểm |

### Bảng tiêu chuẩn PM2.5 (WHO)

| PM2.5 (µg/m³) | Mức độ | Ảnh hưởng sức khỏe |
|---------------|--------|-------------------|
| 0-10 | Tốt | Không ảnh hưởng |
| 10-25 | Trung bình | Nhóm nhạy cảm có thể bị ảnh hưởng |
| 25-50 | Kém | Mọi người có thể bị ảnh hưởng |
| 50-100 | Xấu | Ảnh hưởng sức khỏe rõ rệt |
| >100 | Nguy hại | Cảnh báo sức khỏe |

---

## 📝 GHI CHÚ

1. **Timezone:**
   - Weather & Air Quality: **Local time** (Australia)
   - Energy: **UTC** (cần convert khi xử lý Silver)

2. **Ingest columns:**
   - `ingest_mode`: Chế độ nạp dữ liệu
   - `ingest_timestamp`: Thời gian hệ thống ghi nhận

3. **Đơn vị bức xạ - W/m² (Watt trên mét vuông):**

   **Định nghĩa:** Cường độ năng lượng mặt trời chiếu xuống 1 mét vuông bề mặt.

   **Ví dụ thực tế:**
   
   | Điều kiện | Giá trị | Ý nghĩa |
   |-----------|---------|---------|
   | Đêm | 0 W/m² | Không có bức xạ mặt trời |
   | Sáng sớm/chiều tối | 100-300 W/m² | Nắng yếu |
   | Buổi trưa trời mây | 400-600 W/m² | Nắng trung bình |
   | Buổi trưa trời quang | 800-1000 W/m² | Nắng gắt |
   | Cực đại (Australia) | 1100-1150 W/m² | Nắng cực mạnh |
   | Solar Constant | 1361 W/m² | Ngoài khí quyển Trái Đất |

   **Ứng dụng trong Solar:**
   ```
   Sản lượng điện = Bức xạ × Diện tích panel × Hiệu suất
   
   VD: 1000 W/m² × 10 m² × 20% = 2000 W = 2 kW
   
   Trong 1 giờ: 2 kW × 1h = 2 kWh điện
   ```

   **Các loại bức xạ:**
   - **Shortwave (tổng)** = Direct + Diffuse
   - **Direct (trực tiếp)**: Ánh sáng chiếu thẳng từ mặt trời
   - **Diffuse (khuếch tán)**: Ánh sáng phân tán qua mây/khí quyển
   - **DNI (pháp tuyến)**: Direct đo vuông góc với tia sáng

