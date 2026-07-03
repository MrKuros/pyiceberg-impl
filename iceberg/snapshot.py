from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any

@dataclass
class Snapshot:
    """A snapshot representing the state of a table at some time."""
    snapshot_id: int
    parent_snapshot_id: Optional[int]
    sequence_number: int
    timestamp_ms: int
    manifest_list: str
    summary: Dict[str, str]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Snapshot":
        return cls(
            snapshot_id=data["snapshot_id"],
            parent_snapshot_id=data.get("parent_snapshot_id"),
            sequence_number=data["sequence_number"],
            timestamp_ms=data["timestamp_ms"],
            manifest_list=data["manifest_list"],
            summary=data.get("summary", {})
        )
