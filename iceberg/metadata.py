import json
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional

import time
import uuid

from iceberg.schema import Schema
from iceberg.snapshot import Snapshot
from iceberg.store import MinIOStore

@dataclass
class TableMetadata:
    """Represents the top-level Iceberg table metadata file."""
    table_uuid: str
    location: str
    last_updated_ms: int
    last_column_id: int
    schemas: List[Schema]
    current_schema_id: int
    partition_specs: List[Any]
    snapshots: List[Snapshot]
    snapshot_log: List[Dict[str, Any]]
    format_version: int = 2
    current_snapshot_id: Optional[int] = None

    def to_json(self) -> str:
        """Serialize the table metadata to a JSON string."""
        data = asdict(self)
        return json.dumps(data, indent=2)

    def to_bytes(self) -> bytes:
        """Serialize the table metadata to JSON bytes (for storage)."""
        return self.to_json().encode("utf-8")

    @classmethod
    def from_json(cls, data: str) -> "TableMetadata":
        """Deserialize table metadata from a JSON string."""
        obj = json.loads(data)
        
        schemas = [Schema.from_dict(s) for s in obj.get("schemas", [])]
        snapshots = [Snapshot.from_dict(s) for s in obj.get("snapshots", [])]
        
        return cls(
            table_uuid=obj["table_uuid"],
            location=obj["location"],
            last_updated_ms=obj["last_updated_ms"],
            last_column_id=obj["last_column_id"],
            schemas=schemas,
            current_schema_id=obj["current_schema_id"],
            partition_specs=obj.get("partition_specs", []),
            snapshots=snapshots,
            snapshot_log=obj.get("snapshot_log", []),
            format_version=obj.get("format_version", 2),
            current_snapshot_id=obj.get("current_snapshot_id")
        )

    @classmethod
    def from_bytes(cls, data: bytes) -> "TableMetadata":
        """Deserialize table metadata from JSON bytes."""
        return cls.from_json(data.decode("utf-8"))


def write_metadata(table_metadata: TableMetadata, store: MinIOStore) -> str:
    """Write table metadata to MinIO with a new version number. Returns the MinIO key."""
    # Infer table name from the last part of the location URI
    table_name = table_metadata.location.rstrip('/').split('/')[-1]
    
    prefix = f"metadata/tables/{table_name}/v"
    existing_keys = store.list(prefix)
    
    max_version = 0
    for key in existing_keys:
        filename = key.split('/')[-1]
        if filename.startswith('v') and filename.endswith('.metadata.json'):
            # slice off 'v' and '.metadata.json'
            version_str = filename[1:-14]
            if version_str.isdigit():
                max_version = max(max_version, int(version_str))
                
    new_version = max_version + 1
    new_key = f"{prefix}{new_version}.metadata.json"
    
    store.put(new_key, table_metadata.to_bytes())
    return new_key


def read_metadata(key: str, store: MinIOStore) -> TableMetadata:
    """Read table metadata from MinIO."""
    data = store.get(key)
    return TableMetadata.from_bytes(data)


def new_table(name: str, schema: Schema, store: MinIOStore) -> TableMetadata:
    """Create a new table and write its initial v1 metadata to MinIO."""
    max_field_id = max((col.field_id for col in schema.columns), default=0)
    
    table_metadata = TableMetadata(
        table_uuid=str(uuid.uuid4()),
        location=f"s3://{store.bucket_name}/tables/{name}",
        last_updated_ms=int(time.time() * 1000),
        last_column_id=max_field_id,
        schemas=[schema],
        current_schema_id=schema.schema_id,
        partition_specs=[],
        snapshots=[],
        snapshot_log=[],
        format_version=2,
        current_snapshot_id=None
    )
    
    write_metadata(table_metadata, store)
    return table_metadata

