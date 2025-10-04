import sys
import pathlib

# Ensure `src/` is on sys.path so tests can import the package in-place.
root = pathlib.Path(__file__).resolve().parents[1]
src_path = str(root / "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from pv_lakehouse.etl import bronze_ingest


def test_bronze_ingest_main():
    assert bronze_ingest.main() is True
