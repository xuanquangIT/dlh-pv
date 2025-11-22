# Hướng Dẫn Sử Dụng Scripts

## 🚀 Quick Start

### 1. Train & Load to Gold (toàn bộ data)
```bash
./quick_regression.sh
```

### 2. Kiểm tra kết quả
```bash
./check_regression_results.sh
```

---

## 📊 Chi Tiết Pipeline

### `quick_regression.sh` thực hiện:
1. **Train Model**: RandomForestRegressor với **ALL data** (--limit 0)
2. **Save to Gold**: Predictions được lưu trực tiếp vào Gold layer
3. **Query Results**: Hiển thị metrics tổng quan

**Input:** `lh.silver.clean_hourly_energy` + `clean_hourly_weather`  
**Output:** `lh.gold.fact_solar_forecast_regression`  
**Write Mode:** OVERWRITE (xóa predictions cũ, chỉ giữ kết quả train mới nhất)  
**Split Method:** Deterministic temporal split (95% train / 5% test)  
**Thời gian:** ~5-10 phút

---

### `check_regression_results.sh` hiển thị:
- Performance metrics (MAPE, R², accuracy rate)
- Top 10 best predictions
- Performance by facility

---

## ❓ FAQ

### Q: Script có dùng toàn bộ data không?
**A:** ✅ CÓ - `--limit 0` = không giới hạn (~161K rows)

**Chứng minh:**
```bash
# quick_regression.sh dòng 14:
python3 train_regression_model.py --limit 0

# Output khi chạy:
[TRAIN] Using ALL available data (no limit)
[TRAIN] Loaded 161020 samples
[SPLIT] Training: 152769 samples (94.9%)
[SPLIT] Test: 8251 samples (5.1%)
```

**Lưu ý:** Split là **DETERMINISTIC** - mỗi lần train sẽ có CÙNG test set.

---

### Q: Tại sao mỗi lần train có số predictions khác nhau?
**A:** ❌ **KHÔNG NÊN XẢY RA** - Đã fix bằng deterministic split!

**Trước khi fix:**
- Dùng `approxQuantile()` → split point dao động ±1%
- Lần 1: 8,700 test samples
- Lần 2: 8,880 test samples  
- Lần 3: 8,730 test samples

**Sau khi fix:**
- Dùng exact row number split
- **MỌI LẦN TRAIN GIỐNG HỆT:** ~8,251 test samples
- Đảm bảo reproducibility cho research

---

### Q: Model parameters hiện tại?
```python
RandomForestRegressor(
    numTrees=25,              # Giảm từ 30
    maxDepth=10,              # Giảm từ 12
    minInstancesPerNode=80,   # Tăng từ 50
    subsamplingRate=0.7,      # Giảm từ 0.8
    featureSubsetStrategy="sqrt"
)
```
**Mục đích:** Anti-overfitting

---

### Q: Expected performance?
- **R² Score:** 84-88%
- **MAPE:** <50%
- **Accuracy (≤10% MAPE):** >45%

---

## 🔧 Queries Thủ Công

### Xem total predictions:
```bash
docker compose -f docker/docker-compose.yml exec trino trino \
  --server http://trino:8080 --catalog iceberg --schema gold \
  --execute "SELECT COUNT(*) FROM fact_solar_forecast_regression"
```

### Xem 20 predictions mới nhất:
```bash
docker compose -f docker/docker-compose.yml exec trino trino \
  --server http://trino:8080 --catalog iceberg --schema gold \
  --execute "
    SELECT * FROM fact_solar_forecast_regression 
    ORDER BY forecast_timestamp DESC LIMIT 20
  "
```

---

## 📍 Resources

- **MLflow UI:** http://localhost:5002
- **Trino Console:** http://localhost:8080
- **Training Script:** `src/pv_lakehouse/mlflow/train_regression_model.py`
- **Gold Loader:** `src/pv_lakehouse/etl/gold/fact_solar_forecast_regression.py`
