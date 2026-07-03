import uuid
from iceberg.manifest import Manifest, ManifestEntry, write_manifest, read_manifest
from iceberg.store import MinIOStore

def run_verification():
    print("=== Manifest Verification Script ===")
    
    # 1. Connect to MinIO
    print("\n[*] Connecting to MinIO (ensure docker-compose is running)...")
    store = MinIOStore()
    
    # 2. Create a Manifest with 3 entries
    entries = []
    for i in range(1, 4):
        entry = ManifestEntry(
            file_path=f"data/file_{i}.parquet",
            file_size_bytes=1024 * i,
            record_count=100 * i,
            column_stats={
                1: {"min": i, "max": i * 10},
                2: {"min": f"a{i}", "max": f"z{i}"}
            }
        )
        entries.append(entry)
        
    original_manifest = Manifest(
        manifest_id=str(uuid.uuid4()),
        entries=entries,
        added_files_count=3,
        added_rows_count=600
    )
    
    # 3. Write Manifest
    print(f"\n[*] Writing manifest '{original_manifest.manifest_id}' to MinIO...")
    print("\n--- Raw JSON Payload ---")
    print(original_manifest.to_json())
    print("------------------------\n")
    key = write_manifest(original_manifest, store)
    print(f"    -> Successfully written to key: {key}")
    
    # 4. Read Manifest
    print("\n[*] Reading manifest back from MinIO...")
    read_back_manifest = read_manifest(key, store)
    print("    -> Successfully read manifest")
    
    # 5. Verify fields round-trip
    print("\n[*] Verifying all fields round-trip correctly...")
    assert read_back_manifest.manifest_id == original_manifest.manifest_id, "manifest_id mismatch"
    assert read_back_manifest.added_files_count == original_manifest.added_files_count, "added_files_count mismatch"
    assert read_back_manifest.added_rows_count == original_manifest.added_rows_count, "added_rows_count mismatch"
    assert len(read_back_manifest.entries) == len(original_manifest.entries), "entries count mismatch"
    
    for i, (orig, read_back) in enumerate(zip(original_manifest.entries, read_back_manifest.entries)):
        assert orig.file_path == read_back.file_path, f"Entry {i}: file_path mismatch"
        assert orig.file_size_bytes == read_back.file_size_bytes, f"Entry {i}: file_size_bytes mismatch"
        assert orig.record_count == read_back.record_count, f"Entry {i}: record_count mismatch"
        
        # Verify column stats (keys should be ints)
        orig_stats = orig.column_stats
        read_stats = read_back.column_stats
        
        assert set(orig_stats.keys()) == set(read_stats.keys()), f"Entry {i}: column_stats keys mismatch"
        for k in orig_stats.keys():
            assert isinstance(k, int), f"Entry {i}: Key {k} is not an integer!"
            assert orig_stats[k] == read_stats[k], f"Entry {i}: Stats for key {k} mismatch"

    print("    -> ✅ All assertions passed! Manifest fields round-tripped perfectly.")
    
    # Clean up
    store.client.remove_object(store.bucket_name, key)
    print(f"\n[*] Cleaned up '{key}' from MinIO.")

if __name__ == "__main__":
    run_verification()
