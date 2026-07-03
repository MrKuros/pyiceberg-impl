import os
import uuid
import tempfile
import duckdb
from iceberg.schema import Schema, Column
from iceberg.store import MinIOStore
from iceberg.metadata import new_table
from iceberg.table import Table
from iceberg.manifest import read_manifest_list, read_manifest

def run_verification():
    print("=== DuckDB Verification Script ===\n")
    store = MinIOStore()
    
    table_name = f"test_duckdb_table_{uuid.uuid4().hex[:6]}"
    
    # 1. Create table and append data
    schema = Schema(
        schema_id=1,
        columns=[
            Column(field_id=1, name="id", type="int", required=True),
            Column(field_id=2, name="val", type="double", required=True)
        ]
    )
    new_table(table_name, schema, store)
    
    table = Table(table_name, store)
    rows = [{"id": 1, "val": 10.5}, {"id": 2, "val": 20.0}, {"id": 3, "val": 99.9}]
    
    print(f"[*] Appending rows to '{table_name}': {rows}")
    table.append(rows)
    
    # 2. Extract the Data File Key from the metadata tree
    table = Table(table_name, store)
    latest_snapshot = table.metadata.snapshots[-1]
    manifest_list = read_manifest_list(latest_snapshot.manifest_list, store)
    manifest = read_manifest(manifest_list.entries[0].manifest_path, store)
    data_file_key = manifest.entries[0].file_path
    
    print(f"\n[*] Found Data File in MinIO: {data_file_key}")
    
    # 3. Download the data file locally for DuckDB to scan
    print("[*] Downloading data file for DuckDB...")
    data_bytes = store.get(data_file_key)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = os.path.join(tmpdir, "duckdb_scan.parquet")
        with open(local_path, "wb") as f:
            f.write(data_bytes)
            
        # 4. Query with DuckDB
        print("\n[*] Querying Parquet file with DuckDB:")
        query = f"SELECT * FROM parquet_scan('{local_path}')"
        print(f"    -> {query}")
        
        result = duckdb.sql(query).fetchall()
        print("\n    -> Results:")
        for r in result:
            print(f"       {r}")
            
        assert len(result) == 3
        assert result[0] == (1, 10.5)
        assert result[1] == (2, 20.0)
        assert result[2] == (3, 99.9)

        print("\n✅ DuckDB verification passed! The raw Parquet file is perfectly readable.")

if __name__ == "__main__":
    run_verification()
