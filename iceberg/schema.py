from dataclasses import dataclass
from typing import List

SUPPORTED_TYPES = {"int", "long", "float", "double", "string", "boolean", "timestamp"}

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
        from dataclasses import asdict
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Schema":
        columns = [Column(**col) for col in data.get("columns", [])]
        return cls(schema_id=data["schema_id"], columns=columns)
