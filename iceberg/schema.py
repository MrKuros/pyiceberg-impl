from dataclasses import dataclass, asdict
from typing import List, Optional, Any

SUPPORTED_TYPES = {"int", "long", "float", "double", "string", "boolean", "timestamp"}
SUPPORTED_TRANSFORMS = {"identity", "day", "month", "year", "hour"}

@dataclass
class Column:
    field_id: int
    name: str
    type: str
    required: bool = True

    def __post_init__(self):
        if self.type not in SUPPORTED_TYPES:
            raise ValueError(
                f"Unsupported type '{self.type}'. Must be one of: {', '.join(sorted(SUPPORTED_TYPES))}"
            )

@dataclass
class Schema:
    schema_id: int
    columns: List[Column]

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Schema":
        columns = [Column(**col) for col in data.get("columns", [])]
        return cls(schema_id=data["schema_id"], columns=columns)


@dataclass
class PartitionField:
    """Maps one source column (by field_id) to a partition key via a transform.

    Example: PartitionField(field_id=4, name="day", transform="day")
      reads the column with field_id=4 (a timestamp), truncates it to the
      calendar day, and names the resulting partition key "day".
    """
    field_id: int   # field_id of the source column in the schema
    name: str       # label used in the partition path (e.g. "day")
    transform: str  # one of SUPPORTED_TRANSFORMS

    def __post_init__(self):
        if self.transform not in SUPPORTED_TRANSFORMS:
            raise ValueError(
                f"Unsupported transform '{self.transform}'. "
                f"Must be one of: {', '.join(sorted(SUPPORTED_TRANSFORMS))}"
            )

    def apply(self, value: Any) -> str:
        """Apply this transform to a raw column value and return a partition key string.

        The key is stored in ManifestEntry.partition_value so scan() can skip
        entire files without reading their stats.
        """
        if self.transform == "identity":
            return str(value)
        if self.transform in ("day", "month", "year", "hour"):
            from datetime import datetime, date, timezone
            if isinstance(value, datetime):
                dt = value
            elif isinstance(value, date):
                dt = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
            else:
                # Try parsing ISO string
                dt = datetime.fromisoformat(str(value))
            if self.transform == "day":
                return dt.strftime("%Y-%m-%d")
            if self.transform == "month":
                return dt.strftime("%Y-%m")
            if self.transform == "year":
                return dt.strftime("%Y")
            if self.transform == "hour":
                return dt.strftime("%Y-%m-%dT%H")
        raise ValueError(f"Cannot apply transform '{self.transform}' to value {value!r}")

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "PartitionField":
        return cls(
            field_id=data["field_id"],
            name=data["name"],
            transform=data["transform"],
        )


@dataclass
class PartitionSpec:
    """A versioned partition specification for a table.

    spec_id lets Iceberg track partition evolution: when you change how a table
    is partitioned, old files keep their old spec_id and new files get the new one.
    Both coexist in the same table without rewriting historical data.
    """
    spec_id: int
    fields: List[PartitionField]

    def to_dict(self) -> dict:
        return {"spec_id": self.spec_id, "fields": [f.to_dict() for f in self.fields]}

    @classmethod
    def from_dict(cls, data: dict) -> "PartitionSpec":
        fields = [PartitionField.from_dict(f) for f in data.get("fields", [])]
        return cls(spec_id=data["spec_id"], fields=fields)
