import json
from iceberg.schema import Schema, Column
from iceberg.store import MinIOStore
from iceberg.metadata import new_table
from iceberg.table import Table
from iceberg.manifest import read_manifest_list, read_manifest
from iceberg.parquet import read_parquet
import uuid

def run_verification():
    print("=== Table Append Verification Script ===\n")
    store = MinIOStore()
    
    table_name = f"test_append_table_{uuid.uuid4().hex[:6]}"
    
    # 1. Define schema and create a new table
    schema = Schema(
        schema_id=1,
        columns=[
            Column(field_id=1, name="id", type="int", required=True),
            Column(field_id=2, name="val", type="double", required=True)
        ]
    )
    print(f"[*] Creating new table '{table_name}'...")
    new_table(table_name, schema, store)
    
    # 2. Instantiate Table
    print("[*] Instantiating Table API...")
    table = Table(table_name, store)
    
    # 3. Append Batch 1
    batch_1 = [{"id": 1, "val": 10.5}, {"id": 2, "val": 20.0}]
    print("[*] Appending Batch 1...")
    table.append(batch_1)
    
    # 4. Append Batch 2
    batch_2 = [{"id": 3, "val": 30.0}, {"id": 4, "val": 40.5}]
    print("[*] Appending Batch 2...")
    table.append(batch_2)
    
    # 5. Reload table to get latest metadata (v3)
    table = Table(table_name, store)
    print(f"\n[*] Latest metadata version loaded. Snapshots count: {len(table.metadata.snapshots)}")
    assert len(table.metadata.snapshots) == 2, "Should have 2 snapshots from 2 appends"
    
    # 6. Traverse the tree from the latest snapshot
    latest_snapshot = table.metadata.snapshots[-1]
    print(f"\n[*] Navigating Metadata Tree from Latest Snapshot ({latest_snapshot.snapshot_id})")
    print(f"    -> Manifest List Key: {latest_snapshot.manifest_list}")
    
    manifest_list = read_manifest_list(latest_snapshot.manifest_list, store)
    print(f"    -> Manifest List has {len(manifest_list.entries)} entries.")
    assert len(manifest_list.entries) == 1, "Latest append should produce 1 manifest"
    
    manifest_key = manifest_list.entries[0].manifest_path
    print(f"    -> Manifest Key: {manifest_key}")
    
    manifest = read_manifest(manifest_key, store)
    print(f"    -> Manifest has {len(manifest.entries)} data file entries.")
    assert len(manifest.entries) == 1, "Latest manifest should point to 1 parquet file"
    
    data_file_key = manifest.entries[0].file_path
    print(f"    -> Data File Key: {data_file_key}")
    
    # Since we can't natively read parquet from a remote MinIO URL with `pyarrow.parquet` in a simple way
    # (requires pyarrow fs config), we download it to a temp file and read it.
    print(f"    -> Downloading data file to verify contents...")
    data_bytes = store.get(data_file_key)
    
    import tempfile
    import os
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = os.path.join(tmpdir, "test_read.parquet")
        with open(tmp_path, "wb") as f:
            f.write(data_bytes)
        rows = read_parquet(tmp_path)
    
    print(f"    -> Data File Contents: {rows}")
    assert rows == batch_2, "Data file contents should match batch 2"
    
    print("\n✅ End-to-end append and metadata tree traversal verified perfectly!")

if __name__ == "__main__":
    run_verification()
