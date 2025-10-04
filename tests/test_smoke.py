import sys
import pathlib

# Ensure `src/` is on sys.path so tests can import the package in-place.
root = pathlib.Path(__file__).resolve().parents[1]
src_path = str(root / "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

def test_true():
    assert True
