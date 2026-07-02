import os
import pytest
from iceberg.schema import Column, Schema
from iceberg.parquet import write_parquet, read_parquet

def test_schema_validation():
    # Valid schema
    col1 = Column(1, "id", "int", required=True)
    col2 = Column(2, "name", "string", required=False)
    schema = Schema(1, [col1, col2])
    
    assert schema.schema_id == 1
    assert len(schema.columns) == 2
    assert schema.columns[0].name == "id"

    # Invalid type should raise ValueError
    with pytest.raises(ValueError, match="Unsupported type"):
        Column(3, "invalid_col", "invalid_type")

def test_parquet_write_read_stats(tmp_path):
    schema = Schema(
        schema_id=1,
        columns=[
            Column(1, "id", "int", required=True),
            Column(2, "name", "string", required=True),
            Column(3, "price", "double", required=False),
        ]
    )
    
    rows = [
        {"id": 1, "name": "apple", "price": 1.2},
        {"id": 2, "name": "banana", "price": 0.8},
        {"id": 3, "name": "cherry", "price": None},
    ]
    
    file_path = os.path.join(tmp_path, "test.parquet")
    
    # Write and get stats
    stats = write_parquet(schema, rows, file_path)
    
    assert stats["row_count"] == 3
    assert stats["columns"]["id"] == {"min": 1, "max": 3}
    assert stats["columns"]["name"] == {"min": "apple", "max": "cherry"}
    # Note: price has a None, but min/max of non-nulls should still be 0.8 and 1.2
    assert stats["columns"]["price"] == {"min": 0.8, "max": 1.2}
    
    # Read back and verify rows
    read_rows = read_parquet(file_path)
    assert read_rows == rows
