import json
import uuid
from dataclasses import dataclass, asdict
from typing import List, Dict, Any
from iceberg.store import MinIOStore

@dataclass
class ManifestEntry:
    """An entry in a manifest file tracking a data file."""
    file_path: str
    file_size_bytes: int
    record_count: int
    column_stats: Dict[int, Dict[str, Any]]

@dataclass
class Manifest:
    """A manifest containing a list of manifest entries."""
    manifest_id: str
    entries: List[ManifestEntry]
    added_files_count: int
    added_rows_count: int

    def to_json(self) -> str:
        """Serialize the manifest to a JSON string."""
        return json.dumps(asdict(self), indent=2)

    def to_bytes(self) -> bytes:
        """Serialize the manifest to JSON bytes (for storage)."""
        return self.to_json().encode("utf-8")

    @classmethod
    def from_json(cls, data: str) -> "Manifest":
        """Deserialize a manifest from a JSON string."""
        obj = json.loads(data)
        entries = []
        for e in obj.get("entries", []):
            # JSON dict keys are strings. Convert column_stats keys back to int (field_id).
            raw_stats = e.get("column_stats", {})
            column_stats = {}
            for k, v in raw_stats.items():
                column_stats[int(k)] = v
                
            entries.append(
                ManifestEntry(
                    file_path=e["file_path"],
                    file_size_bytes=e["file_size_bytes"],
                    record_count=e["record_count"],
                    column_stats=column_stats
                )
            )
        return cls(
            manifest_id=obj["manifest_id"],
            entries=entries,
            added_files_count=obj["added_files_count"],
            added_rows_count=obj["added_rows_count"]
        )

    @classmethod
    def from_bytes(cls, data: bytes) -> "Manifest":
        """Deserialize a manifest from JSON bytes."""
        return cls.from_json(data.decode("utf-8"))

def write_manifest(manifest: Manifest, store: MinIOStore) -> str:
    """Write a manifest to MinIO and return its object key."""
    key = f"metadata/manifests/{manifest.manifest_id}.json"
    store.put(key, manifest.to_bytes())
    return key

def read_manifest(key: str, store: MinIOStore) -> Manifest:
    """Read a manifest from MinIO using its object key."""
    data = store.get(key)
    return Manifest.from_bytes(data)
