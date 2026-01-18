# Lý Do Chọn Công Nghệ

## 1. Lưu Trữ & Table Format

### MinIO (Object Storage)
**Tại sao chọn:**
- S3-compatible: Tương thích API Amazon S3, dễ migrate lên cloud
- Open source: Không tốn phí license
- Self-hosted: Kiểm soát hoàn toàn dữ liệu

**Tại sao KHÔNG chọn:**
| Công nghệ | Lý do không chọn |
|-----------|------------------|
| Amazon S3 | Tốn phí, phụ thuộc AWS |
| Azure Blob | Tốn phí, phụ thuộc Azure |
| Google Cloud Storage | Tốn phí, phụ thuộc GCP |
| HDFS | Cần Hadoop cluster phức tạp, khó setup |

---

### Apache Iceberg (Table Format)
**Tại sao chọn:**
- ACID transactions: Đảm bảo tính toàn vẹn dữ liệu
- Time travel: Truy vấn/khôi phục dữ liệu theo thời điểm
- Schema evolution: Thay đổi schema không cần migrate
- Hidden partitioning: Partition tự động, user không cần biết

**Tại sao KHÔNG chọn:**
| Công nghệ | Lý do không chọn |
|-----------|------------------|
| Delta Lake | Gắn chặt với Databricks, ít linh hoạt hơn |
| Apache Hudi | Phức tạp hơn, focus vào CDC/streaming |
| Parquet thuần | Không có ACID, không time travel, không schema evolution |
| CSV/JSON | Hiệu suất kém, không tối ưu cho analytics |

---

### Parquet (File Format)
**Tại sao chọn:**
- Columnar storage: Tối ưu cho analytics (đọc theo cột)
- Compression: Nén dữ liệu hiệu quả (giảm 70-90% dung lượng)
- Predicate pushdown: Filter dữ liệu ngay khi đọc file

**Tại sao KHÔNG chọn:**
| Công nghệ | Lý do không chọn |
|-----------|------------------|
| CSV | Không nén, không schema, chậm với dữ liệu lớn |
| JSON | Không nén, row-based, hiệu suất kém |
| Avro | Row-based, không tối ưu cho analytics |
| ORC | Tương tự Parquet nhưng ít phổ biến hơn |

---

## 2. Processing & Query Engine

### Apache Spark (ETL Engine)
**Tại sao chọn:**
- Distributed processing: Xử lý song song trên cluster
- In-memory computing: Nhanh hơn MapReduce 10-100x
- Python API (PySpark): Dễ phát triển với Python ecosystem

**Tại sao KHÔNG chọn:**
| Công nghệ | Lý do không chọn |
|-----------|------------------|
| Hadoop MapReduce | Chậm (disk-based), API phức tạp |
| Pandas | Không scale với dữ liệu lớn (single machine) |
| Dask | Ecosystem nhỏ hơn Spark, ít tích hợp |
| Flink | Focus streaming, overkill cho batch ETL |
| dbt | Chỉ transform SQL, không xử lý Python/ML |

---

### Trino (Query Engine)
**Tại sao chọn:**
- ANSI SQL: Cú pháp SQL chuẩn, dễ sử dụng
- Interactive queries: Thời gian phản hồi nhanh (giây)
- JDBC/ODBC: Kết nối Power BI, Tableau, Excel

**Tại sao KHÔNG chọn:**
| Công nghệ | Lý do không chọn |
|-----------|------------------|
| Presto | Trino là fork mới hơn, active development |
| SparkSQL | Chậm hơn cho interactive queries, cần cluster lớn |
| Hive | Chậm, cần Hadoop ecosystem |
| Impala | Gắn với Cloudera, ít linh hoạt |
| ClickHouse | Focus OLAP, không federated queries |

---

## 3. ML & Experiment Tracking

### MLflow
**Tại sao chọn:**
- Open source: Miễn phí, không vendor lock-in
- Experiment tracking: Lưu hyperparameters, metrics, artifacts
- Self-hosted: Triển khai local hoặc cloud

**Tại sao KHÔNG chọn:**
| Công nghệ | Lý do không chọn |
|-----------|------------------|
| Weights & Biases | Freemium, cloud-only, tốn phí production |
| Neptune.ai | Freemium, cloud-only |
| Kubeflow | Quá phức tạp, cần Kubernetes |
| SageMaker | Gắn với AWS, tốn phí |
| Vertex AI | Gắn với GCP, tốn phí |

---

## 4. Metadata & Catalog

### PostgreSQL (JDBC Catalog)
**Tại sao chọn:**
- Simple setup: Không cần Hadoop ecosystem phức tạp
- Reliable: RDBMS trưởng thành, ổn định
- Docker-friendly: Dễ triển khai container

**Tại sao KHÔNG chọn:**
| Công nghệ | Lý do không chọn |
|-----------|------------------|
| Hive Metastore | Cần Hadoop/HDFS, setup phức tạp |
| AWS Glue Catalog | Cloud-only, gắn với AWS |
| Nessie | Mới, ít production-proven |
| MySQL | PostgreSQL phổ biến hơn trong data engineering |

---

## 5. Infrastructure

### Docker + Docker Compose
**Tại sao chọn:**
- Reproducibility: Môi trường giống nhau mọi nơi
- Easy deployment: Một lệnh khởi động toàn bộ stack
- Version control: Cấu hình dưới dạng code (IaC)

**Tại sao KHÔNG chọn:**
| Công nghệ | Lý do không chọn |
|-----------|------------------|
| Kubernetes | Quá phức tạp cho development/demo |
| VM/Bare metal | Khó reproduce, không portable |
| Podman | Docker phổ biến hơn, ecosystem lớn hơn |

---

## 6. Data Sources

### Open-Meteo API (Weather & Air Quality)
**Tại sao chọn:**
- Miễn phí: 10,000 requests/day
- Historical data: Dữ liệu từ 1940 đến nay
- Solar radiation: Có đầy đủ GHI, DNI, DHI

**Tại sao KHÔNG chọn:**
| Công nghệ | Lý do không chọn |
|-----------|------------------|
| OpenWeather | Miễn phí chỉ 5 ngày historical, thiếu solar radiation |
| Weather.com (IBM) | Tốn phí, không có historical dài |
| Weatherbit | Tốn phí cho historical data |
| Visual Crossing | Giới hạn free tier nhỏ |

---

### OpenElectricity API (Energy)
**Tại sao chọn:**
- Facility-level data: Dữ liệu từng nhà máy riêng lẻ
- High granularity: 5-minute / 1-hour resolution
- Geographic coordinates: Có lat/lon để match weather

**Tại sao KHÔNG chọn:**
| Công nghệ | Lý do không chọn |
|-----------|------------------|
| EIA (US) | Aggregated by region, không facility-level |
| Entso-E (Europe) | 15-minute only, không facility-level |
| EVN (Vietnam) | Không có API public |

---

## 7. Tổng Kết

| Thành phần | Công nghệ đã chọn | Lý do chính | Alternatives đã loại |
|------------|-------------------|-------------|---------------------|
| Object Storage | MinIO | S3-compatible, free | AWS S3, Azure Blob, HDFS |
| Table Format | Iceberg | ACID, time travel | Delta Lake, Hudi, Parquet thuần |
| File Format | Parquet | Columnar, compression | CSV, JSON, Avro, ORC |
| ETL Engine | Spark | Distributed, in-memory | MapReduce, Pandas, Flink |
| Query Engine | Trino | Interactive SQL, JDBC | Presto, Hive, SparkSQL |
| ML Tracking | MLflow | Open source, self-hosted | W&B, Neptune, Kubeflow |
| Catalog | PostgreSQL | Simple, no Hadoop | Hive Metastore, Glue |
| Container | Docker | Reproducibility | Kubernetes, VM |
