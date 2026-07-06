import json
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional

import time
import uuid

from iceberg.schema import Schema, PartitionSpec
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
    partition_specs: List[PartitionSpec]   # history of all specs; enables partition evolution
    current_spec_id: int                   # which spec new data is written with
    snapshots: List[Snapshot]
    snapshot_log: List[Dict[str, Any]]
    format_version: int = 2
    current_snapshot_id: Optional[int] = None

    def to_dict(self) -> dict:
        """Build a JSON-safe dict from this metadata object."""
        return {
            "format_version": self.format_version,
            "table_uuid": self.table_uuid,
            "location": self.location,
            "last_updated_ms": self.last_updated_ms,
            "last_column_id": self.last_column_id,
            "schemas": [s.to_dict() for s in self.schemas],
            "current_schema_id": self.current_schema_id,
            "partition_specs": [p.to_dict() for p in self.partition_specs],
            "current_spec_id": self.current_spec_id,
            "current_snapshot_id": self.current_snapshot_id,
            "snapshots": [asdict(s) for s in self.snapshots],
            "snapshot_log": self.snapshot_log,
        }

    def to_json(self) -> str:
        """Serialize the table metadata to a JSON string."""
        return json.dumps(self.to_dict(), indent=2)

    def to_bytes(self) -> bytes:
        """Serialize the table metadata to JSON bytes (for storage)."""
        return self.to_json().encode("utf-8")

    @classmethod
    def from_json(cls, data: str) -> "TableMetadata":
        """Deserialize table metadata from a JSON string."""
        obj = json.loads(data)
        
        schemas = [Schema.from_dict(s) for s in obj.get("schemas", [])]
        snapshots = [Snapshot.from_dict(s) for s in obj.get("snapshots", [])]
        partition_specs = [PartitionSpec.from_dict(p) for p in obj.get("partition_specs", [])]
        
        return cls(
            table_uuid=obj["table_uuid"],
            location=obj["location"],
            last_updated_ms=obj["last_updated_ms"],
            last_column_id=obj["last_column_id"],
            schemas=schemas,
            current_schema_id=obj["current_schema_id"],
            partition_specs=partition_specs,
            current_spec_id=obj.get("current_spec_id", 0),
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


def new_table(
    name: str,
    schema: Schema,
    store: MinIOStore,
    partition_spec: Optional[PartitionSpec] = None,
) -> "TableMetadata":
    """Create a new table and write its initial v1 metadata to MinIO.

    partition_spec is optional. Pass a PartitionSpec to enable partitioned writes.
    An unpartitioned table gets an empty PartitionSpec with spec_id=0.
    """
    max_field_id = max((col.field_id for col in schema.columns), default=0)
    # Unpartitioned tables get a sentinel empty spec
    specs = [partition_spec] if partition_spec else [PartitionSpec(spec_id=0, fields=[])]
    current_spec_id = specs[0].spec_id

    table_metadata = TableMetadata(
        table_uuid=str(uuid.uuid4()),
        location=f"s3://{store.bucket_name}/tables/{name}",
        last_updated_ms=int(time.time() * 1000),
        last_column_id=max_field_id,
        schemas=[schema],
        current_schema_id=schema.schema_id,
        partition_specs=specs,
        current_spec_id=current_spec_id,
        snapshots=[],
        snapshot_log=[],
        format_version=2,
        current_snapshot_id=None
    )
    
    write_metadata(table_metadata, store)
    return table_metadata

