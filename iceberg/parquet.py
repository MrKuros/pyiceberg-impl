import pyarrow as pa
import pyarrow.parquet as pq
from typing import List, Dict, Any
from iceberg.schema import Schema

# Map Iceberg types to PyArrow types
TYPE_MAP = {
    "int": pa.int32(),
    "long": pa.int64(),
    "float": pa.float32(),
    "double": pa.float64(),
    "string": pa.string(),
    "boolean": pa.bool_(),
    "timestamp": pa.timestamp("us"),
}

def to_pyarrow_schema(schema: Schema) -> pa.Schema:
    """Convert an Iceberg Schema to a PyArrow Schema."""
    fields = []
    for col in schema.columns:
        pa_type = TYPE_MAP.get(col.type)
        if pa_type is None:
            raise ValueError(f"Unsupported column type: {col.type}")
        fields.append(
            pa.field(
                name=col.name,
                type=pa_type,
                nullable=not col.required,
                metadata={"field_id": str(col.field_id)}
            )
        )
    return pa.schema(fields)

def get_parquet_stats(path: str) -> Dict[str, Any]:
    """Read file-level stats: row_count and per-column min/max values."""
    metadata = pq.read_metadata(path)
    row_count = metadata.num_rows
    column_stats = {}

    if metadata.num_row_groups > 0:
        num_columns = metadata.num_columns
        for i in range(num_columns):
            col_name = metadata.schema.names[i]
            col_mins = []
            col_maxs = []

            for rg_idx in range(metadata.num_row_groups):
                rg = metadata.row_group(rg_idx)
                col_meta = rg.column(i)
                stats = col_meta.statistics
                if stats is not None and stats.has_min_max:
                    col_mins.append(stats.min)
                    col_maxs.append(stats.max)

            if col_mins and col_maxs:
                overall_min = min(col_mins)
                overall_max = max(col_maxs)

                # Decode bytes to string if needed
                if isinstance(overall_min, bytes):
                    overall_min = overall_min.decode("utf-8")
                if isinstance(overall_max, bytes):
                    overall_max = overall_max.decode("utf-8")

                column_stats[col_name] = {
                    "min": overall_min,
                    "max": overall_max
                }
            else:
                column_stats[col_name] = {
                    "min": None,
                    "max": None
                }

    return {
        "row_count": row_count,
        "columns": column_stats
    }

def write_parquet(schema: Schema, rows: List[Dict[str, Any]], path: str) -> Dict[str, Any]:
    """Write rows as a Parquet file to a local path and return stats."""
    pa_schema = to_pyarrow_schema(schema)
    table = pa.Table.from_pylist(rows, schema=pa_schema)
    pq.write_table(table, path)
    return get_parquet_stats(path)

def read_parquet(path: str) -> List[Dict[str, Any]]:
    """Read a Parquet file back into a list of dicts."""
    table = pq.read_table(path)
    return table.to_pylist()
