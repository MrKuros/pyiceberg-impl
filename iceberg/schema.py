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
