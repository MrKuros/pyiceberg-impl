import os
import tempfile
from iceberg.schema import Column, Schema
from iceberg.parquet import write_parquet, read_parquet

def get_test_schema() -> Schema:
    """Returns a test schema for verification."""
    return Schema(
        schema_id=1,
        columns=[
            Column(1, "id", "int", required=True),
            Column(2, "value", "double", required=True),
        ]
    )

def generate_test_data(num_rows: int = 100) -> list[dict]:
    """Generates test rows matching the test schema."""
    return [{"id": i, "value": float(i * 1.5)} for i in range(1, num_rows + 1)]

def run_verification(temp_dir: str):
    """Runs the end-to-end verification of parquet writes and reads."""
    schema = get_test_schema()
    rows = generate_test_data(100)
    
    file_path = os.path.join(temp_dir, "test_data.parquet")
    
    # 1. Write Data
    print(f"[*] Writing 100 rows to {file_path}")
    stats = write_parquet(schema, rows, file_path)
    
    print("    -> Write stats retrieved:")
    print(f"       Row count: {stats['row_count']}")
    for col_name, min_max in stats['columns'].items():
        print(f"       Column '{col_name}' | min: {min_max['min']} | max: {min_max['max']}")

    # 2. Read Data
    print(f"\n[*] Reading back from {file_path}")
    read_rows = read_parquet(file_path)
    print(f"    -> Read {len(read_rows)} rows successfully")
    
    # 3. Assertions
    print("\n[*] Running assertions...")
    assert stats['row_count'] == 100, "Stats should reflect 100 rows"
    assert len(read_rows) == 100, "Should read exactly 100 rows"
    assert stats['columns']['id']['min'] == 1
    assert stats['columns']['id']['max'] == 100
    assert stats['columns']['value']['min'] == 1.5
    assert stats['columns']['value']['max'] == 150.0
    
    print("    -> ✅ All assertions passed!")

def main():
    print("=== Parquet Verification Script ===\n")
    # Use a temporary directory to avoid cluttering the workspace
    with tempfile.TemporaryDirectory() as temp_dir:
        run_verification(temp_dir)
    print("\n=== Cleanup Complete ===")

if __name__ == "__main__":
    main()
