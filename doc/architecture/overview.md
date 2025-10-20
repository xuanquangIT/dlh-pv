# PV Lakehouse - Tổng Quan Hệ Thống

## Giới Thiệu

**PV Lakehouse** là một nền tảng data lakehouse production-ready, xây dựng bằng các công nghệ open-source, cung cấp các quy ước rõ ràng để phát triển hệ thống ETL từ dữ liệu thô (Raw) → dữ liệu chuẩn hóa (Normalized) → dữ liệu trích xuất phân tích (Curated) với độ tin cậy cao.

Dự án được thiết kế để chạy trên laptop cá nhân hoặc một máy VM đơn lẻ, nhưng vẫn tuân theo các tiêu chuẩn production.

## Mục Đích Sử Dụng

- 📊 **Xây dựng pipeline ETL** hoàn chỉnh từ A-Z
- 🔄 **Định nghiệu quy ước dữ liệu** trong toàn hệ thống
- 🚀 **Tham gia và học tập** về data lakehouse architecture
- 🎯 **Prototyping nhanh** cho các use case phân tích dữ liệu
- 💼 **Production-ready** - có thể scale lên cho deployment thực tế

## Công nghệ Stack

| Thành Phần | Công Nghệ | Vai Trò |
|-----------|-----------|--------|
| **Lưu trữ** | MinIO | S3-compatible object storage |
| **Định dạng dữ liệu** | Apache Iceberg | Open table format v2 |
| **Xử lý batch** | Apache Spark | In-memory distributed computing |
| **Query SQL** | Trino | Distributed SQL query engine |
| **Catalog metadata** | PostgreSQL JDBC | Metadata repository cho Iceberg |
| **Orchestration** | Prefect | Workflow automation & scheduling |
| **ML Tracking** | MLflow | Model registry & experiment tracking |
| **Database** | PostgreSQL | Metadata storage |

## Kiến Trúc Medallion

Dữ liệu được tổ chức theo mô hình Medallion Architecture gồm 3 layers:

```
┌──────────────────────────────────────────────────────────┐
│ Bronze Layer (Raw Data)                                  │
│ - Dữ liệu thô từ các nguồn (OpenNEM, Open-Meteo, etc)  │
│ - Partition: days(ts_utc)                               │
│ - Metadata: _ingest_time, _source, _hash               │
└──────────────────────────────────────────────────────────┘
                          ↓
┌──────────────────────────────────────────────────────────┐
│ Silver Layer (Normalized Data)                           │
│ - Dữ liệu đã chuẩn hóa, loại bỏ duplicate              │
│ - Áp dụng business rules                                │
│ - Sẵn sàng cho phân tích                                │
└──────────────────────────────────────────────────────────┘
                          ↓
┌──────────────────────────────────────────────────────────┐
│ Gold Layer (Analytics/Business Layer)                    │
│ - Dữ liệu được trích xuất cho business intelligence    │
│ - Dashboard-ready, report-ready                         │
│ - Aggregations & derived metrics                        │
└──────────────────────────────────────────────────────────┘
```

## Kiến Trúc Hệ Thống Toàn Cục

```
┌──────────────────────────────────────────────────────────────────┐
│                      Data Lakehouse Platform                     │
├────────────┬──────────────────┬────────────┬──────────────────┤
│  Storage   │   Metadata/      │  Compute   │   Orchestration  │
│            │   Catalog        │            │   & ML           │
├────────────┼──────────────────┼────────────┼──────────────────┤
│            │                  │            │                  │
│  MinIO     │  PostgreSQL      │  Trino     │  Prefect         │
│  (S3 API)  │  + JDBC Catalog  │  (SQL)     │  (Workflows)     │
│            │  (Iceberg meta)  │            │                  │
│  Buckets:  │                  │  Spark     │  MLflow          │
│  - lakehouse  │             │  (Python)  │  (ML Tracking)   │
│  - mlflow  │                  │            │                  │
│            │                  │            │                  │
└────────────┴──────────────────┴────────────┴──────────────────┘
```

### Mô Tả Chi Tiết:

**1. Storage Layer (MinIO)**
- S3-compatible object storage
- Lưu trữ dữ liệu parquet trong Iceberg tables
- 2 buckets: `lakehouse` (data) & `mlflow` (artifacts)
- Access control: least-privilege policies per service user

**2. Metadata/Catalog Layer**
- PostgreSQL lưu trữ metadata của Iceberg
- JDBC Catalog: kết nối trực tiếp, không qua REST API
- Lợi ích: đơn giản, ổn định, tránh conflict version Hadoop
- Trino sử dụng catalog này để resolve tables & schemas

**3. Compute Layer**
- **Trino**: SQL query engine cho ad-hoc queries
- **Spark**: Batch processing, ETL transformations, data loading
- Cả hai đều read/write qua Iceberg catalog

**4. Orchestration & ML Layer**
- **Prefect**: Workflow scheduling & execution
- **MLflow**: Experiment tracking, model registry
- Tích hợp với Python ETL code

## Luồng Dữ Liệu (Data Flow)

```
[External Data Sources]
  │ (OpenNEM API, Open-Meteo API)
  ↓
[Spark ETL Job] → Load to MinIO (Parquet) + Iceberg Metadata
  │
  ├─→ PostgreSQL: Store table metadata
  │
  ↓
[Bronze Tables] (lh.bronze.*)
  │ Defined in: sql/bronze/*.sql
  │ Partition: days(ts_utc)
  │
[Trino] ← Query via JDBC Catalog
  │
  ↓
[Silver Tables] (lh.silver.*) ← Transformations via Spark/Trino
  │
  ↓
[Gold Tables] (lh.gold.*) ← Aggregations & Analytics
  │
  ↓
[BI Tools / Reports / Dashboards]
```

## Thành Phần Chính

### 1. **Data Sources**
- OpenNEM API: Facilities & Generation data
- Open-Meteo API: Weather & Air Quality data

### 2. **Bronze Tables** (Raw data)
- `oe_facilities_raw` - Solar facilities registry
- `oe_generation_hourly_raw` - Generation time series
- `om_weather_hourly_raw` - Weather observations
- `om_air_quality_hourly_raw` - Air quality observations

### 3. **ETL Pipelines**
- Location: `src/pv_lakehouse/etl/`
- Bronze ingestion: `bronze_ingest.py`
- Orchestration: Prefect flows in `flows/`

### 4. **Query Engines**
- Trino: Interactive SQL queries
- Spark SQL: Batch transformations

### 5. **Infrastructure**
- Docker Compose profiles (core, spark, ml, orchestrate)
- Service users with least-privilege policies
- Health checks & smoke tests

## Lợi Ích Kiến Trúc

✅ **Open Source**: Không vendor lock-in, toàn bộ công nghệ open-source
✅ **Scalable**: Từ laptop lên production cloud (AWS, GCP, Azure)
✅ **Clear Conventions**: Quy ước rõ ràng cho dữ liệu & metadata
✅ **ACID Transactions**: Apache Iceberg đảm bảo consistency
✅ **Time Travel**: Query historical versions của data
✅ **Schema Evolution**: Thêm/sửa cột mà không bị lỗi
✅ **Partition Pruning**: Query performance optimization

## Tệp Cấu Trúc Dự Án

```
doc/
├── architecture/          # Kiến trúc hệ thống
│   ├── overview.md       # Tài liệu này
│   ├── system-architecture.md
│   ├── medallion-design.md
│   └── technology-stack.md
├── setup/                # Hướng dẫn cài đặt
├── operations/           # Vận hành & giám sát
├── data-model/           # Mô hình dữ liệu
├── development/          # Hướng dẫn phát triển
└── infrastructure/       # Cấu hình infra
```

## Tài Liệu Liên Quan

- 📖 [System Architecture](system-architecture.md) - Chi tiết kiến trúc từng thành phần
- 📊 [Medallion Design](medallion-design.md) - Thiết kế chi tiết 3 layers
- 🛠️ [Technology Stack](technology-stack.md) - Đặc tả công nghệ
- 🚀 [Setup Guide](../setup/quick-start.md) - Hướng dẫn cài đặt nhanh
- 📋 [Data Model](../data-model/bronze-layer.md) - Định nghiệu schema dữ liệu
- 👨‍💻 [Development Guide](../development/development.md) - Guide phát triển ETL

## Điểm Khởi Đầu

1. **Dành cho người dùng mới**: Đọc [Quick Start](../setup/quick-start.md)
2. **Dành cho DevOps/Infra**: Xem [Infrastructure Setup](../infrastructure/overview.md)
3. **Dành cho Data Engineer**: Đọc [Data Model & ETL](../data-model/bronze-layer.md)
4. **Dành cho Developer**: Xem [Development Guide](../development/development.md)

---

**Version**: 0.0.1  
**Last Updated**: October 2025  
**Repository**: https://github.com/xuanquangIT/dlh-pv
