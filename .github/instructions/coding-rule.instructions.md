---
applyTo: '**'
---

# PV Lakehouse - Coding Standards

> **Stack:** Python 3.11+, PySpark 3.5, Apache Iceberg, MinIO, PostgreSQL, MLflow  
> **Architecture:** Medallion (Bronze → Silver → Gold)

---

## 0. 🔥 CRITICAL VIOLATIONS CHECKLIST

Before writing or reviewing ANY code, verify against these violations:

### 🚨 SECURITY (Auto-FAIL):
- ❌ Hardcoded passwords, API keys, tokens, secrets
- ❌ SQL/Command injection vulnerabilities
- ❌ Missing input validation/sanitization
- ❌ HTTP instead of HTTPS for sensitive data
- ❌ Use of eval(), exec(), compile() with user input

### 🚨 RESOURCE LEAKS & MEMORY:
- ❌ File handles not closed (missing `with` statement)
- ❌ Database/network connections not closed
- ❌ Large files loaded entirely into memory
- ❌ Missing context managers
- ❌ Thread/process pools not shutdown

### 🚨 ERROR HANDLING:
- ❌ AttributeError from None access without check
- ❌ KeyError from dict access without check
- ❌ Bare except clauses (`except:` instead of specific)
- ❌ Network requests without timeout/retry
- ❌ Silent exception catching without logging

### 🚨 THREAD SAFETY:
- ❌ Global mutable state without thread locks
- ❌ Singleton patterns without threading.Lock
- ❌ Race conditions in concurrent code
- ❌ Blocking operations in async functions

### 🚨 CODE QUALITY:
- ❌ Magic numbers without constants
- ❌ Hardcoded config values
- ❌ Missing docstrings for public functions
- ❌ camelCase (use snake_case)
- ❌ Lines > 100 chars
- ❌ Missing type hints
- ❌ print() in production (use logging)

### 🚨 PYSPARK:
- ❌ .collect() on large DataFrames
- ❌ Not caching reused DataFrames
- ❌ String column names (use F.col())
- ❌ Multiple withColumn() (use select())
- ❌ Not unpersisting cached DataFrames

### 🚨 PROJECT-SPECIFIC:
- ❌ Hardcoded credentials (use Settings)
- ❌ Missing audit columns (created_at, updated_at)
- ❌ Missing quality_flag in Silver layer
- ❌ Missing ingest metadata in Bronze layer
- ❌ Not using get_settings() singleton
- ❌ Not using spark_config.yaml
- ❌ Table names not lh.<layer>.<name>

---

## 1. Naming Conventions

| Element | Convention | Example |
|---------|------------|---------|
| Modules | `snake_case.py` | `load_facility_weather.py` |
| Classes | `PascalCase` | `BaseSilverLoader` |
| Functions | `snake_case` | `create_spark_session()` |
| Constants | `UPPER_SNAKE_CASE` | `DEFAULT_CONFIG` |
| Private | `_underscore` | `_validate()` |

**Table Naming:** `lh.<layer>.<table_name>`  
Examples: `lh.bronze.raw_facility_weather`, `lh.gold.fact_solar_environmental`

---

## 2. Python Standards

- **Line length:** 100 chars max
- **Python:** 3.11+ 
- **Formatter:** Ruff
- **Type hints:** Required on all public functions
- **Docstrings:** Required (Google style)
- **Imports order:** stdlib → third-party → local

---

## 3. ETL Layer Patterns

### Bronze (Raw Ingestion)
**Required columns:** `ingest_mode`, `ingest_timestamp`, `ingest_date`

### Silver (Cleansing)
**Required columns:** `quality_flag`, `created_at`, `updated_at`  
**Quality flags:** GOOD, WARNING, BAD

### Gold (Dimensional)
**Required columns:** `created_at`, `updated_at`  
**Pattern:** Star schema with fact/dimension tables

---

## 4. Configuration

- **Secrets:** Use `get_settings()` singleton (never hardcode)
- **Spark config:** Load from `spark_config.yaml`
- **Thread-safe:** All singletons must use `threading.Lock`

---

## 5. Error Handling

- Use specific exceptions (never bare `except:`)
- Add try/except for: file ops, network, database
- Always log exceptions with context
- Validate inputs before processing

---

## 6. PySpark Best Practices

- Use `F.col()` not string names
- Use `select()` not multiple `withColumn()`
- Cache reused DataFrames, unpersist after use
- Broadcast small dimension tables
- Never `.collect()` large DataFrames

---

## 7. Testing

- Unit tests for all critical functions
- Thread safety tests for singletons
- Mock external dependencies
- Test edge cases and error paths
- Security tests for hardcoded credentials

---

## 8. Code Review Scoring System

**Evaluation Criteria (Rate Each 1-5):**

1. **Security Assessment**: 5=No issues, 1=Critical violation
2. **Exception Handling**: 5=Proper handling, 1=No error handling
3. **Resource Management**: 5=Proper cleanup, 1=Resource leak
4. **Code Quality**: 5=Best practices, 1=Poor quality
5. **Performance**: 5=No impact, 1=Major degradation
6. **PEP8 Compliance**: 5=Fully compliant, 1=Major violations

**Change Score** = Lowest score among all six categories

**Approval Rules:**
- Change Score ≥ 4: ✅ **APPROVED**
- Change Score < 4: ⚠️ **NEEDS FIXES**
- Change Score ≤ 2: ❌ **REJECT**

---

*Last updated: January 2026*
