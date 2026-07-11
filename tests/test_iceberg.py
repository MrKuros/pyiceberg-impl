"""
Unit tests for the Iceberg implementation.

Tests are split into two groups:
  - Pure unit tests (no MinIO required): test_parquet.py, test_schema.py, test_scan_filter.py
  - Integration tests (require MinIO via docker compose up -d): test_table_*.py

Run unit tests only:
    PYTHONPATH=. pytest tests/ -k "not integration"

Run all tests (MinIO must be running):
    PYTHONPATH=. pytest tests/
"""
import os
import pytest
import tempfile
import uuid

from iceberg.schema import Column, Schema, PartitionField, PartitionSpec
from iceberg.parquet import write_parquet, read_parquet
from iceberg.manifest import ManifestEntry
from iceberg.table import _should_skip


# ─────────────────────────────────────────────────────────────
# Schema validation
# ─────────────────────────────────────────────────────────────

class TestSchemaValidation:
    def test_valid_schema_creation(self):
        schema = Schema(1, [
            Column(1, "id", "long", required=True),
            Column(2, "name", "string", required=False),
        ])
        assert schema.schema_id == 1
        assert len(schema.columns) == 2

    def test_invalid_column_type_raises(self):
        with pytest.raises(ValueError, match="Unsupported type"):
            Column(1, "bad", "unsupported_type")

    def test_schema_round_trip(self):
        """Schema serializes to dict and back without loss."""
        schema = Schema(1, [Column(1, "id", "long", True), Column(2, "val", "double", False)])
        restored = Schema.from_dict(schema.to_dict())
        assert restored.schema_id == schema.schema_id
        assert len(restored.columns) == len(schema.columns)
        assert restored.columns[0].field_id == 1
        assert restored.columns[1].name == "val"

    def test_unsupported_partition_transform_raises(self):
        with pytest.raises(ValueError, match="Unsupported transform"):
            PartitionField(field_id=1, name="p", transform="bucket")


# ─────────────────────────────────────────────────────────────
# Parquet write / read / stats
# ─────────────────────────────────────────────────────────────

class TestParquet:
    def test_write_and_stats(self, tmp_path):
        schema = Schema(1, [
            Column(1, "id", "int", required=True),
            Column(2, "price", "double", required=False),
        ])
        rows = [{"id": 1, "price": 10.0}, {"id": 2, "price": 50.0}, {"id": 3, "price": None}]
        path = str(tmp_path / "test.parquet")
        stats = write_parquet(schema, rows, path)

        assert stats["row_count"] == 3
        assert stats["columns"]["id"] == {"min": 1, "max": 3}
        assert stats["columns"]["price"] == {"min": 10.0, "max": 50.0}

    def test_field_id_in_parquet_metadata(self, tmp_path):
        """Field IDs must be embedded so schema evolution can match columns after renames."""
        import pyarrow.parquet as pq
        schema = Schema(1, [Column(42, "amount", "double", True)])
        path = str(tmp_path / "fid.parquet")
        write_parquet(schema, [{"amount": 1.0}], path)

        pf = pq.read_table(path).schema.field("amount")
        assert pf.metadata[b"field_id"] == b"42"

    def test_string_column_min_max(self, tmp_path):
        schema = Schema(1, [Column(1, "tag", "string", True)])
        rows = [{"tag": "alpha"}, {"tag": "gamma"}, {"tag": "beta"}]
        path = str(tmp_path / "str.parquet")
        stats = write_parquet(schema, rows, path)
        assert stats["columns"]["tag"]["min"] == "alpha"
        assert stats["columns"]["tag"]["max"] == "gamma"


# ─────────────────────────────────────────────────────────────
# Column-stat file skipping logic
# ─────────────────────────────────────────────────────────────

def _entry(min_val, max_val, field_id=3):
    return ManifestEntry(
        file_path="dummy.parquet",
        file_size_bytes=0,
        record_count=100,
        column_stats={field_id: {"min": min_val, "max": max_val}},
    )


class TestShouldSkip:
    def test_gt_skip_when_max_below_value(self):
        assert _should_skip(_entry(10.0, 400.0), {"field_id": 3, "op": "gt", "value": 500.0})

    def test_gt_keep_when_max_above_value(self):
        assert not _should_skip(_entry(10.0, 600.0), {"field_id": 3, "op": "gt", "value": 500.0})

    def test_lt_skip_when_min_above_value(self):
        assert _should_skip(_entry(600.0, 800.0), {"field_id": 3, "op": "lt", "value": 500.0})

    def test_lt_keep_when_min_below_value(self):
        assert not _should_skip(_entry(400.0, 800.0), {"field_id": 3, "op": "lt", "value": 500.0})

    def test_eq_skip_when_value_outside_range(self):
        assert _should_skip(_entry(10.0, 100.0), {"field_id": 3, "op": "eq", "value": 500.0})

    def test_eq_keep_when_value_inside_range(self):
        assert not _should_skip(_entry(400.0, 600.0), {"field_id": 3, "op": "eq", "value": 500.0})

    def test_no_stats_always_keeps(self):
        entry = ManifestEntry("x.parquet", 0, 10, {})
        assert not _should_skip(entry, {"field_id": 3, "op": "gt", "value": 500.0})

    def test_string_eq_skip(self):
        # field_id=3 matches the default column_stats key in _entry()
        assert _should_skip(_entry("Jan 1", "Jan 1"), {"field_id": 3, "op": "eq", "value": "Jan 3"})

    def test_string_eq_keep(self):
        assert not _should_skip(_entry("Jan 1", "Jan 5"), {"field_id": 3, "op": "eq", "value": "Jan 3"})


# ─────────────────────────────────────────────────────────────
# Integration tests (require MinIO — mark with pytest.ini or -m)
# ─────────────────────────────────────────────────────────────

# Integration tests are marked with @pytest.mark.integration
# Register in pytest.ini to avoid warnings


@pytest.fixture(scope="module")
def store():
    """Return a MinIOStore, skipping if MinIO is not reachable."""
    try:
        from iceberg.store import MinIOStore
        s = MinIOStore()
        return s
    except Exception:
        pytest.skip("MinIO not available — run: docker compose up -d")


@pytest.fixture
def table_name():
    return f"test_{uuid.uuid4().hex[:8]}"


@pytest.mark.integration
class TestTableIntegration:
    def test_append_and_query(self, store, table_name):
        from iceberg.metadata import new_table
        from iceberg.table import Table

        schema = Schema(1, [Column(1, "id", "long", True), Column(2, "val", "double", True)])
        new_table(table_name, schema, store)
        t = Table(table_name, store)
        t.append([{"id": i, "val": float(i)} for i in range(10)])

        rows = t.query(f"SELECT COUNT(*) AS cnt FROM {table_name}")
        assert rows[0]["cnt"] == 10

    def test_schema_evolution_add_column(self, store, table_name):
        from iceberg.metadata import new_table
        from iceberg.table import Table

        schema = Schema(1, [Column(1, "id", "long", True)])
        new_table(table_name, schema, store)
        t = Table(table_name, store)
        t.append([{"id": 1}, {"id": 2}])
        t = Table(table_name, store)
        t.add_column("extra", "double")
        t = Table(table_name, store)
        t.append([{"id": 3, "extra": 9.9}])

        rows = t.query(f"SELECT * FROM {table_name} ORDER BY id")
        assert rows[0]["extra"] is None   # old file: column missing → None
        assert rows[2]["extra"] == 9.9    # new file: column present

    def test_time_travel(self, store, table_name):
        import time
        from iceberg.metadata import new_table
        from iceberg.table import Table

        schema = Schema(1, [Column(1, "v", "long", True)])
        new_table(table_name, schema, store)

        t = Table(table_name, store)
        t.append([{"v": 1}])
        ts1 = t.metadata.snapshots[-1].timestamp_ms
        time.sleep(0.05)

        t = Table(table_name, store)
        t.append([{"v": 2}])

        t = Table(table_name, store)
        r_current = t.query(f"SELECT COUNT(*) AS cnt FROM {table_name}")
        r_past = t.query(f"SELECT COUNT(*) AS cnt FROM {table_name}", as_of=ts1)
        assert r_current[0]["cnt"] == 2
        assert r_past[0]["cnt"] == 1

    def test_rename_column_field_id_stable(self, store, table_name):
        from iceberg.metadata import new_table
        from iceberg.table import Table

        schema = Schema(1, [Column(1, "id", "long", True), Column(2, "price", "double", True)])
        new_table(table_name, schema, store)
        t = Table(table_name, store)
        t.append([{"id": 1, "price": 42.0}])
        t = Table(table_name, store)
        t.rename_column(field_id=2, new_name="unit_price")
        t = Table(table_name, store)
        t.append([{"id": 2, "unit_price": 99.0}])

        rows = t.query(f"SELECT unit_price FROM {table_name} ORDER BY id")
        assert rows[0]["unit_price"] == 42.0
        assert rows[1]["unit_price"] == 99.0

    def test_file_skipping_gt(self, store, table_name):
        """Files whose max < filter value must be skipped."""
        from iceberg.metadata import new_table
        from iceberg.table import Table

        schema = Schema(1, [Column(1, "id", "long", True), Column(2, "price", "double", True)])
        new_table(table_name, schema, store)

        # Batch A: price 0–100
        t = Table(table_name, store)
        t.append([{"id": i, "price": float(i)} for i in range(100)])
        # Batch B: price 1000–1100
        t = Table(table_name, store)
        t.append([{"id": 100+i, "price": 1000.0+i} for i in range(100)])

        t = Table(table_name, store)
        files = t.scan(filter={"field_id": 2, "op": "gt", "value": 500.0})
        # Only batch B should survive
        assert len(files) == 1

    def test_expire_snapshots(self, store, table_name):
        import time
        from iceberg.metadata import new_table
        from iceberg.table import Table

        schema = Schema(1, [Column(1, "v", "long", True)])
        new_table(table_name, schema, store)

        for i in range(3):
            t = Table(table_name, store)
            t.append([{"v": i}])
            time.sleep(0.05)

        t = Table(table_name, store)
        # snapshot_log: [snap0, snap1, snap2]. expire older_than snap1's ts → snap0 is removed.
        snap0_id = t.metadata.snapshot_log[0]["snapshot_id"]
        ts_snap1 = t.metadata.snapshot_log[1]["timestamp_ms"]
        t.expire_snapshots(older_than_ms=ts_snap1)

        remaining_ids = {s.snapshot_id for s in t.metadata.snapshots}
        assert snap0_id not in remaining_ids, "Oldest snapshot should have been expired"
        # Current snapshot (snap2) must always be preserved
        assert t.metadata.current_snapshot_id in remaining_ids
