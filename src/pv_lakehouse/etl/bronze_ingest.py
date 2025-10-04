"""Simple ETL runner entrypoint for local development and tests."""


def main() -> bool:
    """Run a tiny ingest routine. Return True on success."""
    print("Starting bronze ingest...")
    # pretend we read and wrote data
    print("Ingest complete")
    return True


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
