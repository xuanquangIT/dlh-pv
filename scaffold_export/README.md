# Scaffold Export: PV-ML-Research Port Template

This folder is a self-contained copy of the scaffold you can copy into another repository.

Files included:
- `ingest_adapter.py` — local + S3 adapter (examples)
- `feature_engineering.py` — lag & fold-safe helpers
- `train_task.py` — wrapper to call core training logic
- `test/sample_dataset.csv` — sample CSV used by smoke test
- `.github/workflows/ci_smoke.yml` — GitHub Actions job that runs the smoke test
- `porting_spec_from_pv-ml-research.md` — porting spec and checklist

Usage:
1. Copy the entire `scaffold_export/` directory into your target repo root.
2. (Optional) Move `.github/workflows/ci_smoke.yml` to repo top-level `.github/workflows/` if you want the CI job to run.
3. Adjust `ingest_adapter.py` to use your preferred storage client (S3/ADLS/GCS);
   fill secrets or secret references via your CI provider.
4. Run the smoke test locally:

```powershell
python -m venv .venv
. .venv/Scripts/Activate.ps1
pip install -r requirements.txt
python train_task.py --dataset data/model_ready/dataset.parquet --n-splits 1 --test-size 1 --initial-train-size 3 --estimator ridge
```

Notes:
- The export is intentionally minimal — adapt logging and artifact publishing to your infra (MLflow, S3, etc.).
