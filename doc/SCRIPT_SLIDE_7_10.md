# 📜 SCRIPT THUYẾT TRÌNH - Trang 7 đến 10

**Dự án:** PV Lakehouse - Hệ thống Data Lakehouse cho Năng Lượng Mặt Trời  
**Phần:** Kiến trúc Data Lakehouse & Mô hình dữ liệu

---

## 📌 TRANG 7: KIẾN TRÚC TỔNG THỂ

> Tiếp theo, em xin trình bày về kiến trúc Data Lakehouse của hệ thống.
>
> **Về dữ liệu đầu vào**, chúng em thu thập từ hai nguồn chính:
> - **Open-Meteo** cung cấp dữ liệu thời tiết và chất lượng không khí
> - **OpenElectricity** cung cấp dữ liệu sản lượng điện từ các nhà máy điện mặt trời tại Úc
>
> **Về kiến trúc Medallion**, dữ liệu được tổ chức thành 3 lớp:
> - **Bronze** lưu dữ liệu thô nguyên bản
> - **Silver** lưu dữ liệu đã làm sạch
> - **Gold** chứa các bảng fact và dimension cho phân tích
>
> **Về lưu trữ**, chúng em sử dụng MinIO làm Data Lake với định dạng Apache Iceberg, metadata được quản lý bởi PostgreSQL.
>
> **Về xử lý**, Spark và PySpark để thực hiện ETL pipeline. MLflow quản lý quá trình huấn luyện model và lưu kết quả dự báo vào lớp Gold.
>
>
> **Về truy vấn và BI**, Trino đóng vai trò Query Engine, kết nối với Power BI để xây dựng các dashboard về Performance Ratio, Capacity Factor, Solar Energy Yield và Air Quality Index.

> toàn bộ hệ thống được đóng gói trong **Docker containers** 

---

## 📌 TRANG 8: BRONZE LAYER (RAW)

> Bây giờ em sẽ đi chi tiết vào từng layer, bắt đầu với **Bronze Layer**.
>
> Bronze Layer có nhiệm vụ **ingest dữ liệu thô từ các API** bao gồm: năng lượng, thời tiết và chất lượng không khí.
>
> Dữ liệu được lưu vào **3 bảng chính**:
> - `raw_facility_timeseries` - chứa dữ liệu năng lượng theo giờ
> - `raw_facility_weather` - chứa dữ liệu thời tiết theo giờ
> - `raw_facility_air_quality` - chứa dữ liệu chất lượng không khí theo giờ
>
> **Nguyên tắc thiết kế Bronze Layer:**
>
> - **UPSERT Pattern:** - Khi có dữ liệu mới cho cùng mã nhà máy, timestamp: hệ thống sẽ **UPDATE** bản ghi hiện có với dữ liệu mới nhất. Khi là dữ liệu hoàn toàn mới thì **INSERT**
>
> - **Tracking với ingest_timestamp:**
>   - Đánh dấu thời điểm dữ liệu được nạp vào hệ thống
>   - Dùng để loại bỏ bản ghi trùng lặp
>
> - **Mục tiêu chính:**
>   - Dữ liệu không trùng lặp
>   - Luôn là phiên bản mới nhất
>   - Có thể truy vết nguồn gốc và lịch sử nạp dữ liệu


---

## 📌 TRANG 9: SILVER LAYER (CLEANSED)

> Tiếp theo là **Silver Layer** - nơi dữ liệu được làm sạch và chuẩn hóa.
>
> Silver Layer nhận dữ liệu từ Bronze và thực hiện các bước xử lý để tạo ra dữ liệu chất lượng cao, sạch và chuẩn hóa.
>
> **Các bảng chính bao gồm:**
> - `clean_hourly_energy` - dữ liệu năng lượng theo giờ
> - `clean_hourly_weather` - dữ liệu thời tiết theo giờ  
> - `clean_hourly_air_quality` - dữ liệu chất lượng không khí theo giờ  
>
> **Điểm quan trọng** là chúng em thêm cột:
> - `quality_flag` để đánh để đánh giá chất lượng dữ liệu GOOD, WARNING, hoặc BAD
>
> Điều này giúp lọc ra dữ liệu chất lượng cao cho việc huấn luyện model Machine Learning.

---

## 📌 TRANG 10: GOLD LAYER & STAR SCHEMA

> Cuối cùng là **Gold Layer** - được thiết kế theo mô hình **Star Schema** để tối ưu cho phân tích.
>
> **Fact table chính** là `fact_solar_environmental`, kết hợp dữ liệu năng lượng và môi trường từ Silver layer.
>
> **Các dimension tables bao gồm:**
> - `dim_facility` - thông tin về nhà máy điện mặt trời: vị trí, công suất, timezone
> - `dim_date` và `dim_time` - chiều thời gian: ngày, tháng, năm và khung giờ
> - `dim_aqi_category` - phân loại chất lượng không khí theo chuẩn EPA của Hoa Kỳ
>
> Star Schema này được **tối ưu cho Power BI và dashboard**, cho phép truy vấn linh hoạt theo:
> - Nhà máy cụ thể
> - Khoảng thời gian: ngày, giờ, tuần, tháng
> - Điều kiện môi trường: thời tiết, bức xạ, chất lượng không khí
>
> Đây là nền tảng để tính toán các KPI như Performance Ratio, Capacity Factor và Solar Energy Yield.

---

## 💡 MẸO THUYẾT TRÌNH

1. **Thời lượng gợi ý:** ~2-3 phút cho 4 slides này
2. **Dùng con trỏ:** Chỉ vào sơ đồ khi nói về flow dữ liệu
3. **Nhấn mạnh:** Các từ in đậm là key points
4. **Chuyển slide:** Dùng từ "Tiếp theo" để transition mượt mà
