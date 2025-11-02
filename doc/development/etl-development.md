# Hướng Dẫn Phát Triển ETL

## 1. Cấu Trúc Dự Án

```
dlh-pv/
├── src/pv_lakehouse/           # Python package
│   ├── __init__.py
│   ├── etl/
│   │   ├── __init__.py
│   │   ├── bronze_ingest.py    # Bronze ingestion logic
│   │   ├── silver_transform.py # Silver transformations (TBD)
│   │   ├── gold_aggregate.py   # Gold aggregations (TBD)
│   │   └── utils/
│   │       ├── spark_utils.py  # Spark session factory
│   │       └── data_utils.py   # Validation helpers
│   └── config/
│       ├── settings.py         # Configuration
│       └── logger.py           # Logging setup
│
├── flows/                       # Prefect workflows
│   ├── ingest_bronze.py        # Bronze ingestion flow
│   └── transform_silver.py     # Silver transformation flow
│
├── sql/
│   ├── bronze/                 # Bronze DDL
│   ├── silver/                 # Silver DDL (TBD)
│   └── gold/                   # Gold DDL (TBD)
│
├── tests/
│   ├── test_bronze_tables_complete.py
│   ├── create_bronze_tables.sh
│   └── test_smoke.py
│
├── pyproject.toml              # Project metadata
├── requirements.txt            # Dependencies
└── .env.example               # Environment template
```

## 2. Setup Development Environment

### 2.1 Local Development Setup

```bash
# Clone repository
git clone https://github.com/xuanquangIT/dlh-pv.git
cd dlh-pv

# Create Python virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Optional: Install development tools
pip install pytest pytest-cov black ruff
```

### 2.2 Start Docker Services

```bash
# Start core + spark services (for testing)
cd docker
docker compose --profile core --profile spark up -d

# Wait for startup
./scripts/health-check.sh

# Verify Spark is accessible
docker compose exec spark-master spark-submit --version
```

### 2.3 IDE Setup (VS Code)

```json
// .vscode/settings.json
{
  "python.defaultInterpreterPath": "${workspaceFolder}/venv/bin/python",
  "python.linting.enabled": true,
  "python.linting.pylintEnabled": false,
  "python.linting.ruffEnabled": true,
  "[python]": {
    "editor.formatOnSave": true,
    "editor.defaultFormatter": "charliermarsh.ruff",
    "editor.rulers": [100]
  }
}
```

## 3. Writing ETL Code

### 3.1 Spark Session Factory

**File:** `src/pv_lakehouse/etl/utils/spark_utils.py`

```python
from pyspark.sql import SparkSession
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

def create_spark_session(
    app_name: str,
    catalog: str = "lh",
    **kwargs
) -> SparkSession:
    """
    Create Spark session with Iceberg catalog configured.
    
    Args:
        app_name: Spark application name
        catalog: Iceberg catalog name (default: lh)
        **kwargs: Additional configs
    
    Returns:
        Configured SparkSession
    """
    config = {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.lh": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.lh.type": "jdbc",
        "spark.sql.catalog.lh.uri": "jdbc:postgresql://postgres:5432/iceberg_catalog",
        "spark.sql.catalog.lh.jdbc.user": "pvlakehouse",
        "spark.sql.catalog.lh.jdbc.password": "pvlakehouse",
        "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
        "spark.hadoop.fs.s3a.access.key": "spark_svc",
        "spark.hadoop.fs.s3a.secret.key": "spark_secret_change_me",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.sql.iceberg.compression-codec": "snappy",
        "spark.sql.adaptive.enabled": "true",
        **kwargs
    }
    
    builder = SparkSession.builder.appName(app_name)
    for key, value in config.items():
        builder = builder.config(key, value)
    
    return builder.getOrCreate()


def write_iceberg_table(
    df,
    table_name: str,
    mode: str = "append",
    partition_cols: list = None
) -> None:
    """
    Write DataFrame to Iceberg table.
    
    Args:
        df: Spark DataFrame
        table_name: Full table name (e.g., lh.bronze.my_table)
        mode: Write mode (append, overwrite, upsert)
        partition_cols: Partition columns for upsert
    """
    writer = df.writeTo(table_name).using("iceberg")
    
    if mode == "upsert":
        # Upsert logic
        writer = writer.when("MATCHED", "THEN UPDATE *") \
                      .when("NOT MATCHED", "THEN INSERT *")
    else:
        writer = writer.mode(mode)
    
    writer.save()
    logger.info(f"Successfully wrote to {table_name} ({df.count()} rows)")
```

### 3.2 Data Validation Utils

**File:** `src/pv_lakehouse/etl/utils/data_utils.py`

```python
from pyspark.sql import DataFrame
import logging

logger = logging.getLogger(__name__)

class DataValidator:
    """Data quality validation utilities"""
    
    @staticmethod
    def check_nulls(df: DataFrame, columns: list, max_allowed: float = 0.05) -> bool:
        """
        Check null percentage in columns.
        
        Args:
            df: Spark DataFrame
            columns: Columns to check
            max_allowed: Max allowed null percentage (default: 5%)
        
        Returns:
            True if all columns pass validation
        """
        total_rows = df.count()
        
        for col in columns:
            null_count = df.filter(f"{col} IS NULL").count()
            null_pct = null_count / total_rows
            
            if null_pct > max_allowed:
                logger.error(f"Column {col}: {null_pct*100:.2f}% nulls (threshold: {max_allowed*100:.2f}%)")
                return False
            else:
                logger.info(f"Column {col}: {null_pct*100:.2f}% nulls (OK)")
        
        return True
    
    @staticmethod
    def check_ranges(df: DataFrame, checks: dict) -> bool:
        """
        Validate value ranges.
        
        Args:
            df: Spark DataFrame
            checks: Dict of {column: (min, max)}
        
        Returns:
            True if all checks pass
        """
        for column, (min_val, max_val) in checks.items():
            violations = df.filter(
                (f"{column} < {min_val}") | (f"{column} > {max_val}")
            ).count()
            
            if violations > 0:
                logger.error(f"Column {column}: {violations} values outside range [{min_val}, {max_val}]")
                return False
            else:
                logger.info(f"Column {column}: All values in range [{min_val}, {max_val}]")
        
        return True
```

### 3.3 Bronze Ingest Example

**File:** `src/pv_lakehouse/etl/bronze_ingest.py`

```python
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import requests
import logging

from .utils.spark_utils import create_spark_session, write_iceberg_table
from .utils.data_utils import DataValidator

logger = logging.getLogger(__name__)

class BronzeIngestor:
    """Handles ingestion to Bronze layer"""
    
    def __init__(self, spark: SparkSession = None):
        self.spark = spark or create_spark_session("bronze-ingest")
        self.validator = DataValidator()
    
    def ingest_generation(self) -> int:
        """
        Ingest solar generation data from OpenNEM API.
        
        Returns:
            Number of rows written
        """
        logger.info("Fetching generation data from OpenNEM...")
        
        # Fetch data from API
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=2)
        
        url = "https://api.opennem.org.au/v3/data/generation/hourly"
        params = {
            "from": start_date.isoformat(),
            "to": end_date.isoformat(),
            "fueltech": "solar_utility"
        }
        
        response = requests.get(url, params=params)
        data = response.json()
        
        # Transform to DataFrame
        df = self._transform_generation_data(data)
        
        # Validate
        if not self.validator.check_nulls(df, ["ts_utc", "duid", "generation_mw"]):
            raise ValueError("Data validation failed")
        
        if not self.validator.check_ranges(df, {"generation_mw": (0, 1000)}):
            raise ValueError("Range validation failed")
        
        # Add metadata
        df = df.withColumn("_ingest_time", current_timestamp()) \
               .withColumn("_source", lit("opennem_api_v1")) \
               .withColumn("_hash", sha2(concat_ws("|", col("duid"), col("ts_utc")), 256))
        
        # Write to Iceberg (upsert)
        row_count = df.count()
        write_iceberg_table(df, "lh.bronze.oe_generation_hourly_raw", mode="append")
        
        logger.info(f"Successfully ingested {row_count} generation records")
        return row_count
    
    def _transform_generation_data(self, api_data: dict) -> DataFrame:
        """Transform API response to DataFrame"""
        records = []
        
        for item in api_data.get("data", []):
            records.append({
                "ts_utc": item["time"],
                "duid": item["duid"],
                "generation_mw": float(item["generation"]),
                "capacity_factor": float(item.get("capacity_factor", 0)),
                "data_quality_code": "ACTUAL"
            })
        
        return self.spark.createDataFrame(records)


# Entry point
if __name__ == "__main__":
    ingestor = BronzeIngestor()
    ingestor.ingest_generation()
```

## 4. Writing Prefect Flows

### 4.1 Flow Example

**File:** `flows/ingest_bronze.py`

```python
from prefect import flow, task
from prefect.schedules import cron_schedule
from datetime import datetime
import logging

from src.pv_lakehouse.etl.bronze_ingest import BronzeIngestor

logger = logging.getLogger(__name__)

@task(retries=2, retry_delay_seconds=60)
def fetch_generation():
    """Fetch generation data from OpenNEM"""
    ingestor = BronzeIngestor()
    count = ingestor.ingest_generation()
    return count

@task
def validate_data():
    """Validate ingested data"""
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("validate") \
        .getOrCreate()
    
    df = spark.sql("SELECT COUNT(*) as count FROM lh.bronze.oe_generation_hourly_raw WHERE DATE(ts_utc) = CURRENT_DATE")
    count = df.collect()[0]["count"]
    
    if count == 0:
        raise ValueError("No data found in Bronze layer!")
    
    logger.info(f"Validation passed: {count} records in Bronze")
    return count

@flow(name="ingest_bronze_hourly")
def ingest_bronze_flow():
    """Main flow for Bronze ingestion"""
    logger.info("Starting Bronze ingestion flow")
    
    generation_count = fetch_generation()
    validation_count = validate_data()
    
    logger.info(f"Flow complete: {generation_count} fetched, {validation_count} validated")

# Deploy & schedule (6:15 AM daily)
if __name__ == "__main__":
    # Local execution for testing
    ingest_bronze_flow()
    
    # For production, use:
    # ingest_bronze_flow.serve(cron="15 6 * * *")
```

## 5. Testing

### 5.1 Unit Tests

**File:** `tests/test_etl.py`

```python
import pytest
from pyspark.sql import SparkSession
from src.pv_lakehouse.etl.bronze_ingest import BronzeIngestor

@pytest.fixture
def spark():
    """Create test Spark session"""
    return SparkSession.builder \
        .appName("test") \
        .master("local") \
        .getOrCreate()

def test_transform_generation_data(spark):
    """Test generation data transformation"""
    ingestor = BronzeIngestor(spark)
    
    api_data = {
        "data": [
            {
                "time": "2025-01-15T10:00:00Z",
                "duid": "AVLSF1",
                "generation": "35.5",
                "capacity_factor": "0.71"
            }
        ]
    }
    
    df = ingestor._transform_generation_data(api_data)
    
    assert df.count() == 1
    assert df.select("duid").collect()[0][0] == "AVLSF1"

def test_validate_data_nulls(spark):
    """Test null validation"""
    from src.pv_lakehouse.etl.utils.data_utils import DataValidator
    
    data = [{"col1": 1, "col2": 2}, {"col1": None, "col2": 4}]
    df = spark.createDataFrame(data)
    
    validator = DataValidator()
    result = validator.check_nulls(df, ["col1"], max_allowed=0.5)
    assert result == True
```

### 5.2 Running Tests

```bash
# Run all tests
pytest tests/

# Run specific test
pytest tests/test_etl.py::test_transform_generation_data

# Run with coverage
pytest --cov=src tests/
```

## 6. Code Style & Quality

### 6.1 Ruff Configuration

**File:** `pyproject.toml`

```toml
[tool.ruff]
line-length = 100
target-version = "py311"
exclude = ["docker/", "infra/"]

[tool.ruff.lint]
extend-select = ["I"]  # Import sorting
```

### 6.2 Format & Lint

```bash
# Format code
ruff format src/

# Check style
ruff check src/

# Fix issues automatically
ruff check --fix src/
```

## 7. Common Patterns

### 7.1 ETL Error Handling

```python
from prefect import task
from prefect.exceptions import CrashedRun

@task(retries=3, retry_delay_seconds=30)
def robust_ingest():
    """Ingest with automatic retries"""
    try:
        # Do work
        pass
    except Exception as e:
        logger.error(f"Ingest failed: {e}")
        raise  # Trigger retry

@flow
def ingest_with_fallback():
    """Flow with fallback logic"""
    try:
        robust_ingest()
    except CrashedRun:
        logger.error("Max retries exhausted, alerting team...")
        # Send alert, skip this run, etc.
```

### 7.2 Data Lineage Tracking

```python
def add_lineage(df: DataFrame, source: str, transformation: str) -> DataFrame:
    """Add lineage metadata to DataFrame"""
    import json
    
    lineage = {
        "source": source,
        "transformation": transformation,
        "timestamp": datetime.utcnow().isoformat()
    }
    
    return df.withColumn("_lineage", lit(json.dumps(lineage)))
```

## 8. Best Practices

✅ **Always validate data** before writing to Bronze/Silver
✅ **Use Iceberg format** for ACID & schema evolution
✅ **Partition by time** (days(ts_utc)) for time-series
✅ **Add metadata columns** (_ingest_time, _source, _hash)
✅ **Implement retries** in Prefect tasks
✅ **Log comprehensively** for debugging
✅ **Test locally first** before scheduling
✅ **Version dependencies** in requirements.txt
✅ **Document transformations** in code
✅ **Monitor freshness** and alert on delays

## 9. Related Documentation

- [Bronze Layer](../data-model/bronze-layer.md)
- [Silver Layer](../data-model/silver-layer.md)
- [Testing Guide](testing.md)

---

**Next Steps:**
1. Set up local development environment
2. Review bronze ingestion code
3. Write first transformation
4. Deploy to Prefect
5. Set up monitoring & alerts
