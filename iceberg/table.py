import os
import time
import uuid
import tempfile
from typing import List, Dict, Any

from iceberg.store import MinIOStore
from iceberg.schema import Schema
from iceberg.parquet import write_parquet
from iceberg.manifest import (
    ManifestEntry, Manifest, 
    ManifestListEntry, ManifestList, 
    write_manifest, write_manifest_list
)
from iceberg.snapshot import Snapshot
from iceberg.metadata import TableMetadata, read_metadata, write_metadata

class Table:
    """The main Iceberg Table public API."""
    
    def __init__(self, name: str, store: MinIOStore):
        self.name = name
        self.store = store
        
        # Load the latest metadata file
        prefix = f"metadata/tables/{self.name}/v"
        existing_keys = self.store.list(prefix)
        
        if not existing_keys:
            raise ValueError(f"Table '{self.name}' does not exist.")
            
        max_version = 0
        latest_key = None
        for key in existing_keys:
            filename = key.split('/')[-1]
            if filename.startswith('v') and filename.endswith('.metadata.json'):
                version_str = filename[1:-14]
                if version_str.isdigit():
                    v = int(version_str)
                    if v > max_version:
                        max_version = v
                        latest_key = key
                        
        if not latest_key:
            raise ValueError(f"No valid metadata found for table '{self.name}'.")
            
        self.metadata = read_metadata(latest_key, self.store)
        
        # Find current schema
        self.schema = next(s for s in self.metadata.schemas if s.schema_id == self.metadata.current_schema_id)

    def append(self, rows: List[Dict[str, Any]]) -> None:
        """Append a list of dictionaries (rows) to the table."""
        if not rows:
            return

        with tempfile.TemporaryDirectory() as tmpdir:
            # 1. Convert rows to a Parquet file locally
            local_parquet_path = os.path.join(tmpdir, "data.parquet")
            stats = write_parquet(self.schema, rows, local_parquet_path)
            
            # 2. Upload to MinIO
            file_uuid = str(uuid.uuid4())
            
            # Strip s3://bucket/ prefix from location to get the raw MinIO key prefix
            prefix_to_strip = f"s3://{self.store.bucket_name}/"
            if self.metadata.location.startswith(prefix_to_strip):
                base_key = self.metadata.location[len(prefix_to_strip):]
            else:
                base_key = self.metadata.location
            
            data_key = f"{base_key}/data/{file_uuid}.parquet"
            
            with open(local_parquet_path, "rb") as f:
                parquet_bytes = f.read()
            self.store.put(data_key, parquet_bytes)

        # Convert column name stats to field_id stats
        field_stats = {}
        for col in self.schema.columns:
            if col.name in stats["columns"]:
                field_stats[col.field_id] = stats["columns"][col.name]

        file_size = len(parquet_bytes)

        # 3. Create ManifestEntry
        manifest_entry = ManifestEntry(
            file_path=data_key,
            file_size_bytes=file_size,
            record_count=stats["row_count"],
            column_stats=field_stats
        )

        # 4. Create Manifest and upload
        manifest = Manifest(
            manifest_id=str(uuid.uuid4()),
            entries=[manifest_entry],
            added_files_count=1,
            added_rows_count=stats["row_count"]
        )
        manifest_key = write_manifest(manifest, self.store)

        # 5. Create ManifestList and upload
        snapshot_id = int(time.time() * 1000)
        
        # Carry over previous manifest list entries
        previous_entries = []
        if self.metadata.current_snapshot_id:
            for snap in self.metadata.snapshots:
                if snap.snapshot_id == self.metadata.current_snapshot_id:
                    from iceberg.manifest import read_manifest_list
                    old_ml = read_manifest_list(snap.manifest_list, self.store)
                    previous_entries = old_ml.entries
                    break
        
        ml_entry = ManifestListEntry(
            manifest_path=manifest_key,
            added_snapshot_id=snapshot_id,
            added_files_count=1,
            added_rows_count=stats["row_count"]
        )
        manifest_list = ManifestList(entries=previous_entries + [ml_entry])
        ml_key = write_manifest_list(manifest_list, snapshot_id, self.store)

        # 6. Create Snapshot
        snapshot = Snapshot(
            snapshot_id=snapshot_id,
            parent_snapshot_id=self.metadata.current_snapshot_id,
            sequence_number=len(self.metadata.snapshots) + 1,
            timestamp_ms=snapshot_id,
            manifest_list=ml_key,
            summary={"operation": "append"}
        )

        # 7. Update TableMetadata
        self.metadata.snapshots.append(snapshot)
        self.metadata.current_snapshot_id = snapshot_id
        self.metadata.last_updated_ms = snapshot_id
        self.metadata.snapshot_log.append({
            "timestamp_ms": snapshot_id,
            "snapshot_id": snapshot_id
        })

        # 8. Write new versioned metadata file to MinIO
        write_metadata(self.metadata, self.store)
