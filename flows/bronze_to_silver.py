from prefect import flow, task

@task
def transform(record_count: int) -> int:
    """Dummy transform task that 'processes' records."""
    return record_count // 2

@flow(name="bronze_to_silver")
def bronze_to_silver():
    # Example flow: read bronze, transform to silver
    bronze_count = 100
    silver_count = transform(bronze_count)
    print(f"Moved {silver_count} records to silver")

if __name__ == "__main__":
    bronze_to_silver()
