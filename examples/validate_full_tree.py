import os
import uuid
import tempfile
from datetime import datetime
import duckdb

from iceberg.schema import Schema, Column
from iceberg.store import MinIOStore
from iceberg.metadata import new_table
from iceberg.table import Table
from iceberg.manifest import read_manifest_list, read_manifest

def run_validation():
    print("=== Iceberg Full Tree Validation ===")
    store = MinIOStore()
    table_name = f"val_table_{uuid.uuid4().hex[:6]}"

    # 1. Create table
    print(f"\n[1] Creating Table '{table_name}'...")
    schema = Schema(
        schema_id=1,
        columns=[
            Column(field_id=1, name="id", type="long", required=True),
            Column(field_id=2, name="name", type="string", required=True),
            Column(field_id=3, name="price", type="double", required=False),
            Column(field_id=4, name="ts", type="timestamp", required=True)
        ]
    )
    new_table(table_name, schema, store)
    
    # 2. Append 3 batches of 1000 rows
    print("\n[2] Appending 3 batches of 1000 rows each...")
    for batch_id in range(3):
        # We need a new Table instance per append to load the freshly updated metadata
        # (Our current Table class loads metadata in __init__, and append() writes it, 
        # but in a loop it's safer to re-init so we don't accidentally stomp versions if we had concurrent writes.
        # Actually our append mutates self.metadata so reusing `table` works too!)
        table = Table(table_name, store)
        
        rows = []
        for i in range(1000):
            val_id = batch_id * 1000 + i
            rows.append({
                "id": val_id,
                "name": f"Item_{val_id}",
                "price": float(val_id) * 1.5,
                "ts": datetime.utcnow()
            })
        table.append(rows)
        print(f"    -> Batch {batch_id+1} appended successfully.")

    # 3. Walk the tree
    table = Table(table_name, store)
    print("\n[3] Walking the Full Metadata Tree...")
    metadata = table.metadata
    print(f"    [Metadata] Version: {metadata.format_version}, Location: {metadata.location}")
    print(f"               Snapshots: {len(metadata.snapshots)}")
    
    current_snapshot = metadata.snapshots[-1]
    assert current_snapshot.snapshot_id == metadata.current_snapshot_id
    print(f"\n    [Snapshot] Current ID: {current_snapshot.snapshot_id}")
    print(f"               Manifest List: {current_snapshot.manifest_list}")
    
    manifest_list = read_manifest_list(current_snapshot.manifest_list, store)
    print(f"\n    [ManifestList] Entries: {len(manifest_list.entries)}")
    
    assert len(manifest_list.entries) == 3, f"Expected 3 manifests, got {len(manifest_list.entries)}. The previous manifest bug is fixed!"

    total_duckdb_rows = 0
    
    with tempfile.TemporaryDirectory() as tmpdir:
        for idx, ml_entry in enumerate(manifest_list.entries):
            print(f"\n        [Manifest {idx}] Path: {ml_entry.manifest_path}")
            manifest = read_manifest(ml_entry.manifest_path, store)
            
            for df_idx, entry in enumerate(manifest.entries):
                print(f"            [DataFile {df_idx}] Path: {entry.file_path}")
                print(f"                           Rows: {entry.record_count}")
                print(f"                           Stats: {entry.column_stats}")
                
                # Download and query
                data_bytes = store.get(entry.file_path)
                local_path = os.path.join(tmpdir, f"data_{idx}_{df_idx}.parquet")
                with open(local_path, "wb") as f:
                    f.write(data_bytes)
                    
                # DuckDB check
                stats_query = f"""
                SELECT 
                    COUNT(*), 
                    MIN(id), MAX(id), 
                    MIN(price), MAX(price)
                FROM parquet_scan('{local_path}')
                """
                res = duckdb.sql(stats_query).fetchone()
                db_count, db_min_id, db_max_id, db_min_price, db_max_price = res
                
                print(f"                           DuckDB : Rows={db_count}, MinId={db_min_id}, MaxId={db_max_id}")
                
                total_duckdb_rows += db_count
                
                # 5. Check manifest stats
                assert db_count == entry.record_count
                assert db_min_id == entry.column_stats[1]['min']
                assert db_max_id == entry.column_stats[1]['max']
                
                if db_min_price is not None:
                    assert abs(db_min_price - entry.column_stats[3]['min']) < 1e-5
                    assert abs(db_max_price - entry.column_stats[3]['max']) < 1e-5

    print("\n[4] Row Count Verification...")
    print(f"    Total rows found by DuckDB across all files: {total_duckdb_rows}")
    assert total_duckdb_rows == 3000, f"Expected 3000 rows, found {total_duckdb_rows}"
    print("    -> ✅ Row counts and stats match exactly! The Iceberg table is fully sound.")

if __name__ == "__main__":
    run_validation()
