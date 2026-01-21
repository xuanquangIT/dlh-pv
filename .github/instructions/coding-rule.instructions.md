---
applyTo: '**'
---

# PV Lakehouse - Coding Standards & Guidelines

> **Project:** Data Lakehouse Platform for Solar Energy Forecasting  
> **Stack:** Python 3.11+, PySpark 3.5, Apache Iceberg, MinIO, PostgreSQL, MLflow  
> **Architecture:** Medallion (Bronze → Silver → Gold)

---

## 1. Project Structure & Module Organization

### 1.1 Directory Structure
```
src/pv_lakehouse/
├── config/              # Configuration management (Pydantic settings)
├── etl/
│   ├── bronze/          # Raw data ingestion (API → Iceberg)
│   ├── silver/          # Data cleansing & validation
│   ├── gold/            # Dimensional modeling (Star schema)
│   ├── clients/         # External API clients (OpenElectricity, OpenMeteo)
│   └── utils/           # Shared utilities (Spark, parsing, dates)
├── ml_pipeline/         # Machine learning workflows
├── api/                 # REST API (FastAPI) - if applicable
└── prefect/             # Orchestration flows
```

### 1.2 Naming Conventions

| Element | Convention | Example |
|---------|------------|---------|
| **Modules** | `snake_case.py` | `load_facility_weather.py` |
| **Classes** | `PascalCase` | `BaseSilverLoader`, `GoldTableConfig` |
| **Functions** | `snake_case` | `create_spark_session()`, `load_api_key()` |
| **Constants** | `UPPER_SNAKE_CASE` | `DEFAULT_SPARK_CONFIG`, `ICEBERG_TABLE` |
| **Private** | `_leading_underscore` | `_validate_options()`, `_spark` |
| **Type aliases** | `PascalCase` | `FacilityCode = str` |

### 1.3 Table Naming (Iceberg)
```
lh.<layer>.<table_name>

Examples:
- lh.bronze.raw_facility_weather
- lh.silver.clean_hourly_energy
- lh.gold.fact_solar_environmental
- lh.gold.dim_facility
```

---

## 2. Python Code Style

### 2.1 General Rules
- **Line length:** 100 characters max (configured in pyproject.toml)
- **Python version:** 3.11+ (use modern syntax)
- **Formatter:** Ruff (auto-format on save)
- **Linter:** Ruff with import sorting (`extend-select = ["I"]`)

### 2.2 Imports Order
```python
# 1. Future imports
from __future__ import annotations

# 2. Standard library
import datetime as dt
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

# 3. Third-party packages
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# 4. Local imports
from pv_lakehouse.etl.utils.spark_utils import create_spark_session
from pv_lakehouse.config import settings
```

### 2.3 Type Hints (Required)
```python
# ✅ GOOD - Full type hints
def fetch_weather_dataframe(
    facility: FacilityLocation,
    *,
    start: dt.date,
    end: dt.date,
    chunk_days: int = 30,
    timezone: str = "UTC",
) -> pd.DataFrame:
    ...

# ✅ GOOD - Use Optional for nullable
def load_api_key(cli_key: Optional[str] = None) -> str:
    ...

# ❌ BAD - Missing type hints
def fetch_data(facility, start, end):
    ...
```

### 2.4 Docstrings (Google Style)
```python
def create_spark_session(
    app_name: str, 
    *, 
    extra_conf: Optional[Dict[str, Any]] = None
) -> SparkSession:
    """Create a Spark session configured for Iceberg + MinIO usage.
    
    Args:
        app_name: Application name for Spark UI identification.
        extra_conf: Additional Spark configuration overrides.
    
    Returns:
        Configured SparkSession instance.
    
    Raises:
        SparkError: If session creation fails.
    
    Example:
        >>> spark = create_spark_session("bronze-loader")
        >>> spark.sql("SELECT 1").show()
    """
```

---

## 3. PySpark Specific Guidelines

### 3.1 DataFrame Operations
```python
# ✅ GOOD - Use F alias for functions, chain operations
from pyspark.sql import functions as F

result = (
    df
    .filter(F.col("energy_mwh").isNotNull())
    .withColumn("date_key", F.date_format(F.col("date_hour"), "yyyyMMdd").cast("int"))
    .withColumn("hour", F.hour(F.col("date_hour")))
    .select("facility_code", "date_key", "hour", "energy_mwh")
)

# ❌ BAD - Multiple separate statements, no chaining
df = df.filter(df.energy_mwh.isNotNull())
df = df.withColumn("date_key", date_format(df.date_hour, "yyyyMMdd"))
df = df.withColumn("hour", hour(df.date_hour))
```

### 3.2 Performance Best Practices
```python
# ✅ GOOD - Use select() instead of multiple withColumn()
result = df.select(
    "facility_code",
    F.col("energy_mwh").cast("double").alias("energy"),
    F.hour("date_hour").alias("hour"),
    F.when(F.col("quality_flag") == "GOOD", 1).otherwise(0).alias("is_valid"),
)

# ✅ GOOD - Cache intermediate results
df = df.cache()
count = df.count()  # Triggers cache
# ... multiple operations on df ...
df.unpersist()

# ✅ GOOD - Use broadcast for small dimension tables
from pyspark.sql.functions import broadcast
fact_df.join(broadcast(dim_facility), on="facility_code", how="left")

# ❌ BAD - Collecting large DataFrames to driver
all_data = df.collect()  # OOM risk!

# ✅ GOOD - Use limit() or toPandas() carefully
sample = df.limit(1000).toPandas()
```

### 3.3 SQL in PySpark
```python
# ✅ GOOD - Use parameterized queries (avoid SQL injection)
table_name = "lh.bronze.raw_facilities"
spark.sql(f"SELECT * FROM {table_name} WHERE facility_code = 'BOMEN1'")

# ✅ GOOD - For complex queries, use multi-line strings
merge_sql = f"""
MERGE INTO {target_table} AS target
USING (
    SELECT * FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY facility_code, date_hour 
                ORDER BY ingest_timestamp DESC
            ) as rn
        FROM {source_view}
    ) WHERE rn = 1
) AS source
ON target.facility_code = source.facility_code 
    AND target.date_hour = source.date_hour
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""
spark.sql(merge_sql)
```

---

## 4. Configuration & Secrets Management

### 4.1 Environment Variables
```python
# ✅ GOOD - Use os.environ.get() with defaults
DEFAULT_BASE_URL = os.environ.get(
    "OPENELECTRICITY_API_URL", 
    "https://api.openelectricity.org.au/v4"
)

# ✅ GOOD - Validate required env vars
def load_api_key(cli_key: Optional[str] = None) -> str:
    """Load API key with fallback chain: CLI → ENV → .env file."""
    if cli_key:
        return cli_key
    
    for key in ["OPENELECTRICITY_API_KEY", "OPEN_NEM_PRIMARY"]:
        value = os.environ.get(key)
        if value:
            return value
    
    raise RuntimeError(
        "Missing API key. Set OPENELECTRICITY_API_KEY environment variable."
    )
```

### 4.2 Spark Configuration
```python
# ✅ GOOD - Externalize config, use environment variables for secrets
DEFAULT_SPARK_CONFIG: Dict[str, str] = {
    "spark.sql.catalog.lh.type": "jdbc",
    "spark.sql.catalog.lh.uri": os.environ.get(
        "ICEBERG_CATALOG_URI", 
        "jdbc:postgresql://postgres:5432/iceberg_catalog"
    ),
    "spark.sql.catalog.lh.jdbc.user": os.environ.get("ICEBERG_CATALOG_USER", "pvlakehouse"),
    "spark.sql.catalog.lh.jdbc.password": os.environ.get("ICEBERG_CATALOG_PASSWORD"),
    # ... other configs
}

# ❌ BAD - Hardcoded credentials (NEVER do this)
"spark.sql.catalog.lh.jdbc.password": "pvlakehouse",
```

---

## 5. Error Handling

### 5.1 Exception Hierarchy
```python
# ✅ GOOD - Use specific exceptions
class PVLakehouseError(Exception):
    """Base exception for PV Lakehouse."""
    pass

class ConfigurationError(PVLakehouseError):
    """Configuration or environment error."""
    pass

class APIClientError(PVLakehouseError):
    """External API communication error."""
    pass

class ETLError(PVLakehouseError):
    """ETL pipeline error."""
    pass
```

### 5.2 Error Handling Patterns
```python
# ✅ GOOD - Specific exception handling with context
try:
    response = requests.get(url, headers=headers, timeout=120)
    response.raise_for_status()
except requests.HTTPError as e:
    if e.response.status_code == 401:
        raise AuthenticationError("Invalid API credentials") from e
    elif e.response.status_code == 429:
        raise RateLimitError("API rate limit exceeded") from e
    raise APIClientError(f"API request failed: {e}") from e
except requests.Timeout as e:
    raise APIClientError(f"Request timeout after 120s: {url}") from e

# ❌ BAD - Bare except or generic Exception
try:
    do_something()
except Exception as e:
    print(f"Error: {e}")  # Lost context, poor logging
```

### 5.3 Logging
```python
import logging

LOGGER = logging.getLogger(__name__)

# ✅ GOOD - Structured logging with context
LOGGER.info(
    "Loading data from %s to %s for %d facilities",
    start_date, end_date, len(facility_codes)
)
LOGGER.warning("No data returned for facility %s, skipping", facility_code)
LOGGER.error("Failed to write to %s: %s", table_name, error, exc_info=True)

# ❌ BAD - Using print() instead of logging
print(f"Loading data...")
```

---

## 6. ETL Layer Patterns

### 6.1 Bronze Layer (Raw Ingestion)
```python
"""Bronze layer: Raw data ingestion with minimal transformation."""

# Pattern: Load → Add metadata → Write
def main() -> None:
    # 1. Fetch from external API
    raw_df = fetch_from_api(facility_codes, date_range)
    
    # 2. Add ingestion metadata (REQUIRED for all Bronze tables)
    spark_df = (
        spark.createDataFrame(raw_df)
        .withColumn("ingest_mode", F.lit(args.mode))
        .withColumn("ingest_timestamp", F.current_timestamp())
        .withColumn("ingest_date", F.to_date("ingest_timestamp"))
    )
    
    # 3. Write to Iceberg (append or merge based on mode)
    write_iceberg_table(spark_df, ICEBERG_TABLE, mode="append")
```

### 6.2 Silver Layer (Cleansing)
```python
"""Silver layer: Cleansed, validated, deduplicated data."""

class SilverHourlyWeatherLoader(BaseSilverLoader):
    bronze_table = "lh.bronze.raw_facility_weather"
    silver_table = "lh.silver.clean_hourly_weather"
    timestamp_column = "weather_timestamp"
    partition_cols = ("date_hour",)
    
    # Validation bounds (externalize to config in future)
    _numeric_columns = {
        "temperature_2m": (-10.0, 50.0),
        "shortwave_radiation": (0.0, 1150.0),
        "cloud_cover": (0.0, 100.0),
    }
    
    def transform(self, bronze_df: DataFrame) -> Optional[DataFrame]:
        """Transform Bronze → Silver with validation."""
        # 1. Select and cast columns
        # 2. Apply validation rules
        # 3. Add quality flags (GOOD, WARNING, BAD)
        # 4. Add audit columns (created_at, updated_at)
        return result
```

### 6.3 Gold Layer (Dimensional)
```python
"""Gold layer: Star schema for analytics."""

class GoldFactSolarEnvironmentalLoader(BaseGoldLoader):
    source_tables: Dict[str, SourceTableConfig] = {
        "hourly_energy": SourceTableConfig(
            table_name="lh.silver.clean_hourly_energy",
            timestamp_column="date_hour",
            required_columns=["facility_code", "energy_mwh"],
        ),
        "dim_facility": SourceTableConfig(
            table_name="lh.gold.dim_facility",
            required_columns=["facility_key", "facility_code"],
        ),
    }
    
    gold_tables: Dict[str, GoldTableConfig] = {
        "fact_solar_environmental": GoldTableConfig(
            iceberg_table="lh.gold.fact_solar_environmental",
            partition_cols=("date_key",),
        ),
    }
```

---

## 7. Testing Standards

### 7.1 Test Structure
```
tests/
├── unit/
│   ├── etl/
│   │   ├── test_bronze_loaders.py
│   │   ├── test_silver_transforms.py
│   │   └── test_gold_aggregations.py
│   ├── clients/
│   │   └── test_openelectricity.py
│   └── utils/
│       └── test_spark_utils.py
├── integration/
│   └── test_e2e_pipeline.py
└── conftest.py  # Shared fixtures
```

### 7.2 Test Patterns
```python
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Create a local Spark session for testing."""
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("test")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

def test_transform_adds_quality_flag(spark: SparkSession):
    """Silver transform should add quality_flag column."""
    # Arrange
    input_df = spark.createDataFrame([
        {"facility_code": "TEST1", "temperature_2m": 25.0},
    ])
    
    # Act
    loader = SilverHourlyWeatherLoader()
    result = loader.transform(input_df)
    
    # Assert
    assert "quality_flag" in result.columns
    assert result.filter(F.col("quality_flag") == "GOOD").count() == 1
```

---

## 8. Docker & Infrastructure

### 8.1 Docker Compose Services
```yaml
# Use environment variables for all configurable values
services:
  spark-master:
    environment:
      SPARK_MASTER_HOST: ${SPARK_MASTER_HOST:-spark-master}
      SPARK_MASTER_PORT: ${SPARK_MASTER_PORT:-7077}
    # Always use healthchecks
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080"]
      interval: 30s
      timeout: 10s
      retries: 3
```

### 8.2 Environment Files
```bash
# .env.example (commit this, template for developers)
POSTGRES_USER=pvlakehouse
POSTGRES_PASSWORD=changeme
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=changeme

# .env (DO NOT COMMIT - in .gitignore)
POSTGRES_PASSWORD=actual_secure_password
```

---

## 9. Git & Version Control

### 9.1 Commit Messages (Conventional Commits)
```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `refactor`: Code change that neither fixes a bug nor adds a feature
- `docs`: Documentation only
- `test`: Adding or updating tests
- `chore`: Maintenance tasks

**Examples:**
```
feat(bronze): add incremental load for weather data
fix(silver): handle null values in temperature validation
refactor(gold): extract dimension lookup to helper function
docs: update README with Trino connection guide
```

### 9.2 Branch Naming
```
feature/add-weather-ingestion
bugfix/fix-null-handling-silver
refactor/extract-spark-utils
```

---

## 10. Code Review Checklist

Before submitting PR, verify:

- [ ] **Type hints** on all public functions
- [ ] **Docstrings** on all public classes and functions
- [ ] **No hardcoded secrets** (use env vars)
- [ ] **Logging** instead of print statements
- [ ] **Specific exceptions** instead of bare `except`
- [ ] **Tests** for new functionality
- [ ] **Ruff** linting passes (`ruff check .`)
- [ ] **Import sorting** is correct
- [ ] **Line length** ≤ 100 characters
- [ ] **DataFrame operations** use `F.col()` syntax
- [ ] **Audit columns** added for new tables (created_at, updated_at)

---

## 11. Quick Reference

### Common Imports
```python
from __future__ import annotations

import datetime as dt
import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pv_lakehouse.etl.utils.spark_utils import create_spark_session, write_iceberg_table
```

### Standard Audit Columns
```python
# Add to ALL tables
.withColumn("created_at", F.current_timestamp())
.withColumn("updated_at", F.current_timestamp())

# Bronze layer additional columns
.withColumn("ingest_mode", F.lit(mode))
.withColumn("ingest_timestamp", F.current_timestamp())
.withColumn("ingest_date", F.to_date("ingest_timestamp"))
```

### Quality Flag Values
```python
# Consistent across all Silver tables
QUALITY_FLAGS = {
    "GOOD": "Record passes all validation rules",
    "WARNING": "Minor issues, usable with caution",
    "BAD": "Failed critical validation, exclude from analytics",
}
```

---

*Last updated: January 2026*