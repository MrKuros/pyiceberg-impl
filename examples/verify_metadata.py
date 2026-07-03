from iceberg.metadata import new_table, read_metadata, write_metadata
from iceberg.schema import Schema, Column
from iceberg.store import MinIOStore

def run_verification():
    print("=== TableMetadata Verification Script ===")
    
    print("\n[*] Connecting to MinIO...")
    store = MinIOStore()
    
    # 1. Define schema
    schema = Schema(
        schema_id=1,
        columns=[
            Column(field_id=1, name="id", type="int", required=True),
            Column(field_id=2, name="name", type="string", required=False)
        ]
    )
    
    table_name = "test_metadata_table"
    
    # Clean up any existing files for a fresh test
    print(f"[*] Cleaning up previous test tables '{table_name}'...")
    existing = store.list(f"metadata/tables/{table_name}/")
    for key in existing:
        store.client.remove_object(store.bucket_name, key)

    # 2. Create new table (this also writes v1.metadata.json)
    print(f"\n[*] Creating new table '{table_name}'...")
    orig_metadata = new_table(table_name, schema, store)
    
    expected_v1_key = f"metadata/tables/{table_name}/v1.metadata.json"
    
    # 3. Read metadata back
    print(f"\n[*] Reading metadata back from '{expected_v1_key}'...")
    read_back = read_metadata(expected_v1_key, store)
    
    # 4. Verify fields
    print("[*] Verifying all fields round-trip correctly...")
    assert orig_metadata.table_uuid == read_back.table_uuid, "UUID mismatch"
    assert orig_metadata.location == read_back.location, "location mismatch"
    assert orig_metadata.last_column_id == 2 == read_back.last_column_id, "last_column_id mismatch"
    assert orig_metadata.format_version == 2 == read_back.format_version, "format_version mismatch"
    
    assert len(read_back.schemas) == 1
    assert read_back.schemas[0].schema_id == 1
    assert len(read_back.schemas[0].columns) == 2
    assert read_back.schemas[0].columns[0].name == "id"
    assert read_back.schemas[0].columns[1].name == "name"
    
    print("    -> ✅ v1 metadata fields match perfectly!")

    # 5. Write again to verify versioning
    print("\n[*] Writing metadata again to test auto-versioning (should create v2)...")
    v2_key = write_metadata(read_back, store)
    print(f"    -> Successfully wrote to key: {v2_key}")
    assert v2_key == f"metadata/tables/{table_name}/v2.metadata.json", f"Expected v2 key, got {v2_key}"
    
    print("    -> ✅ v2 auto-versioning works perfectly!")

    # Clean up
    print("\n[*] Cleaning up MinIO...")
    store.client.remove_object(store.bucket_name, expected_v1_key)
    store.client.remove_object(store.bucket_name, v2_key)
    print("    -> Cleanup complete.")

if __name__ == "__main__":
    run_verification()
