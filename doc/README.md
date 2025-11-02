# Documentation Index

Hệ thống PV Lakehouse documentation được tổ chức thành 6 thư mục chính:

## 📚 Documentation Structure

### 1. **Architecture** (`doc/architecture/`)
- [overview.md](overview.md) - Tổng quan kiến trúc hệ thống
- [system-architecture.md](system-architecture.md) - Chi tiết kỹ thuật từng thành phần
- [medallion-design.md](medallion-design.md) - Thiết kế 3 layers (Bronze/Silver/Gold)
- [technology-stack.md](technology-stack.md) - Công nghệ & phiên bản sử dụng

**Dành cho:** DevOps, architects, technical leads

### 2. **Setup & Deployment** (`doc/setup/`)
- [quick-start.md](../setup/quick-start.md) - Cài đặt nhanh (5 phút)
- [detailed-setup.md](../setup/detailed-setup.md) - Hướng dẫn cài đặt chi tiết - TBD
- [environment-configuration.md](../setup/environment-configuration.md) - Cấu hình biến môi trường - TBD
- [troubleshooting.md](../setup/troubleshooting.md) - Khắc phục sự cố - TBD

**Dành cho:** DevOps engineers, System administrators

### 3. **Data Model** (`doc/data-model/`)
- [bronze-layer.md](../data-model/bronze-layer.md) - Định nghiệu Bronze tables
- [silver-layer.md](../data-model/silver-layer.md) - Silver layer design - TBD
- [gold-layer.md](../data-model/gold-layer.md) - Gold layer analytics - TBD
- [schemas.md](../data-model/schemas.md) - Schema reference - TBD
- [data-lineage.md](../data-model/data-lineage.md) - Data flow & lineage - TBD

**Dành cho:** Data engineers, analysts

### 4. **Development** (`doc/development/`)
- [development.md](../development/development.md) - Development workflow - TBD
- [etl-development.md](../development/etl-development.md) - Writing ETL code
- [testing.md](../development/testing.md) - Testing strategies - TBD
- [contributing.md](../development/contributing.md) - Contribution guide - TBD

**Dành cho:** Software engineers, data engineers

### 5. **Operations** (`doc/operations/`)
- [operations.md](../operations/operations.md) - Daily operations & monitoring
- [monitoring.md](../operations/monitoring.md) - Setup monitoring - TBD
- [backup-recovery.md](../operations/backup-recovery.md) - Backup strategies - TBD
- [security.md](../operations/security.md) - Security best practices - TBD

**Dành cho:** DevOps, operations teams

### 6. **Infrastructure** (`doc/infrastructure/`)
- [overview.md](../infrastructure/overview.md) - Infrastructure overview - TBD
- [minio-setup.md](../infrastructure/minio-setup.md) - MinIO configuration
- [postgresql-catalog.md](../infrastructure/postgresql-catalog.md) - PostgreSQL & Iceberg Catalog - TBD
- [trino-configuration.md](../infrastructure/trino-configuration.md) - Trino setup - TBD
- [spark-setup.md](../infrastructure/spark-setup.md) - Spark configuration - TBD
- [power-bi-integration.md](../infrastructure/power-bi-integration.md) - Kết nối Trino đến Power BI

**Dành cho:** Infrastructure engineers, DevOps

## 🎯 Quick Navigation

### Tôi muốn...

| Mục Đích | Đọc Document |
|----------|-------------|
| **Hiểu kiến trúc hệ thống** | [Architecture Overview](overview.md) |
| **Cài đặt nhanh trên laptop** | [Quick Start](../setup/quick-start.md) |
| **Deploy to production** | [Detailed Setup](../setup/detailed-setup.md) |
| **Tìm hiểu về data layers** | [Medallion Design](medallion-design.md) |
| **Viết ETL code** | [ETL Development](../development/etl-development.md) |
| **Thiết lập Bronze tables** | [Bronze Layer](../data-model/bronze-layer.md) |
| **Vận hành hệ thống** | [Operations](../operations/operations.md) |
| **Cấu hình MinIO** | [MinIO Setup](../infrastructure/minio-setup.md) |
| **Kết nối Power BI** | [Power BI Integration](../infrastructure/power-bi-integration.md) |
| **Debug issues** | [Troubleshooting](../setup/troubleshooting.md) |

## 📖 Reading Paths

### **For New Team Members**
1. [Overview](overview.md) - 5 min
2. [Quick Start](../setup/quick-start.md) - 5 min
3. [Architecture](system-architecture.md) - 10 min
4. [Bronze Layer](../data-model/bronze-layer.md) - 10 min
5. [Operations](../operations/operations.md) - 10 min

**Total: ~40 minutes**

### **For Data Engineers**
1. [Medallion Design](medallion-design.md) - 15 min
2. [Bronze Layer](../data-model/bronze-layer.md) - 15 min
3. [Silver Layer](../data-model/silver-layer.md) - 15 min
4. [ETL Development](../development/etl-development.md) - 20 min
5. [Testing](../development/testing.md) - 10 min

**Total: ~75 minutes**

### **For DevOps/Infra**
1. [Technology Stack](technology-stack.md) - 10 min
2. [System Architecture](system-architecture.md) - 10 min
3. [Detailed Setup](../setup/detailed-setup.md) - 20 min
4. [MinIO Setup](../infrastructure/minio-setup.md) - 15 min
5. [PostgreSQL Catalog](../infrastructure/postgresql-catalog.md) - 10 min
6. [Operations](../operations/operations.md) - 15 min

**Total: ~80 minutes**

### **For Architects/Technical Leads**
1. [Overview](overview.md) - 5 min
2. [System Architecture](system-architecture.md) - 20 min
3. [Medallion Design](medallion-design.md) - 20 min
4. [Technology Stack](technology-stack.md) - 15 min
5. [Operations](../operations/operations.md) - 10 min

**Total: ~70 minutes**

## 🔍 Document Status

| Document | Status | Last Updated |
|----------|--------|--------------|
| overview.md | ✅ Complete | Oct 2025 |
| system-architecture.md | ✅ Complete | Oct 2025 |
| medallion-design.md | ✅ Complete | Oct 2025 |
| technology-stack.md | ✅ Complete | Oct 2025 |
| quick-start.md | ✅ Complete | Oct 2025 |
| detailed-setup.md | 🔨 In Progress | - |
| bronze-layer.md | ✅ Complete | Oct 2025 |
| silver-layer.md | 📋 Planned | - |
| gold-layer.md | 📋 Planned | - |
| etl-development.md | ✅ Complete | Oct 2025 |
| development.md | 🔨 In Progress | - |
| testing.md | 📋 Planned | - |
| contributing.md | 📋 Planned | - |
| operations.md | ✅ Complete | Oct 2025 |
| monitoring.md | 📋 Planned | - |
| minio-setup.md | ✅ Complete | Oct 2025 |
| postgresql-catalog.md | 📋 Planned | - |
| trino-configuration.md | 📋 Planned | - |
| spark-setup.md | 📋 Planned | - |

**Legend:** ✅ Complete | 🔨 In Progress | 📋 Planned

## 🏗️ Architecture Diagrams

```
Data Lakehouse Architecture
===========================

External APIs
  │ (OpenNEM, Open-Meteo)
  ↓
[Spark ETL] → [MinIO] ← Iceberg Metadata
  │                        ↑
  │                    PostgreSQL
  ↓
Bronze Tables
  ├─ oe_facilities_raw
  ├─ oe_generation_hourly_raw
  ├─ om_weather_hourly_raw
  └─ om_air_quality_hourly_raw
  
Trino Query Engine
  ↓
Silver Tables (Normalized)
  ├─ generation_normalized
  ├─ weather_normalized
  └─ facilities_dimension

Gold Tables (Analytics)
  ├─ fact_daily_generation
  ├─ dim_weather_summary
  └─ metrics_forecast

BI Tools / Dashboards / Reports
```

## 📞 Support & Feedback

- **Issues**: https://github.com/xuanquangIT/dlh-pv/issues
- **Discussions**: https://github.com/xuanquangIT/dlh-pv/discussions
- **Documentation**: This folder (`doc/`)

## 🤝 Contributing to Documentation

To contribute:

1. Edit markdown files in appropriate folder
2. Follow existing format & style
3. Update status in this index
4. Add links to related documents
5. Submit PR for review

## 📚 External References

- [Apache Iceberg](https://iceberg.apache.org/docs/)
- [Apache Spark](https://spark.apache.org/docs/)
- [Trino](https://trino.io/docs/)
- [MinIO](https://min.io/docs/)
- [PostgreSQL](https://www.postgresql.org/docs/)
- [Prefect](https://docs.prefect.io/)
- [MLflow](https://mlflow.org/docs/)

---

**Last Updated**: October 2025  
**Version**: 1.0  
**Repository**: https://github.com/xuanquangIT/dlh-pv
