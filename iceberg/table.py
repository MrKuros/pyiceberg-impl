import os
import time
import uuid
import tempfile
from typing import List, Dict, Any
import duckdb

from iceberg.store import MinIOStore
from iceberg.schema import Schema, PartitionSpec
from iceberg.parquet import write_parquet
from iceberg.manifest import (
    ManifestEntry, Manifest, 
    ManifestListEntry, ManifestList, 
    write_manifest, write_manifest_list,
    read_manifest_list, read_manifest
)
from iceberg.snapshot import Snapshot
from iceberg.metadata import TableMetadata, read_metadata, write_metadata

# Operators supported in predicate pushdown
_OPS = {"gt", "lt", "eq"}

def _should_skip(entry: "ManifestEntry", filter: Dict[str, Any]) -> bool:
    """Return True if the manifest entry can be safely skipped for this filter.

    Uses column_stats min/max to decide:
      gt (>): skip when the file's max < value  (all rows are too small)
      lt (<): skip when the file's min > value  (all rows are too large)
      eq (=): skip when value is outside [min, max]  (value can't be present)

    Stats can only EXCLUDE files, never guarantee a match exists.
    """
    field_id = filter["field_id"]
    op = filter["op"]
    value = filter["value"]

    stats = entry.column_stats.get(field_id)
    if stats is None or stats.get("min") is None or stats.get("max") is None:
        # No stats available — must include the file to be safe
        return False

    col_min = stats["min"]
    col_max = stats["max"]

    # Timestamp stats round-trip through JSON as ISO strings; normalize datetime values
    from datetime import datetime as _dt
    if isinstance(value, _dt):
        value = value.isoformat()

    if op == "gt":
        return col_max <= value   # every value in file is <= value, no row can satisfy >
    elif op == "lt":
        return col_min >= value   # every value in file is >= value, no row can satisfy <
    elif op == "eq":
        return col_max < value or col_min > value  # value is outside [min, max]
    return False

class Table:
    """The main Iceberg Table public API."""
    
    def __init__(self, name: str, store: MinIOStore):
        self.name = name
        self.store = store
        
        # Load the latest metadata file
        prefix = f"metadata/tables/{self.name}/v"
        existing_keys = self.store.list(prefix)
        
        if not existing_keys:
            raise ValueError(f"Table '{self.name}' does not exist.")
            
        max_version = 0
        latest_key = None
        for key in existing_keys:
            filename = key.split('/')[-1]
            if filename.startswith('v') and filename.endswith('.metadata.json'):
                version_str = filename[1:-14]
                if version_str.isdigit():
                    v = int(version_str)
                    if v > max_version:
                        max_version = v
                        latest_key = key
                        
        if not latest_key:
            raise ValueError(f"No valid metadata found for table '{self.name}'.")
            
        self.metadata = read_metadata(latest_key, self.store)
        
        # Find current schema
        self.schema = next(s for s in self.metadata.schemas if s.schema_id == self.metadata.current_schema_id)

    def append(self, rows: List[Dict[str, Any]]) -> None:
        """Append a list of dictionaries (rows) to the table.

        If the table has a PartitionSpec, rows are grouped by their partition
        value (e.g. the day of a timestamp) and one Parquet file is written
        per distinct partition value.  Unpartitioned tables write a single file.
        """
        if not rows:
            return

        # Strip s3://bucket/ prefix to get the raw MinIO key prefix
        prefix_to_strip = f"s3://{self.store.bucket_name}/"
        base_key = (
            self.metadata.location[len(prefix_to_strip):]
            if self.metadata.location.startswith(prefix_to_strip)
            else self.metadata.location
        )

        # Resolve the current PartitionSpec
        current_spec = next(
            (s for s in self.metadata.partition_specs
             if s.spec_id == self.metadata.current_spec_id),
            None,
        )

        # Build partition groups: {partition_value_str -> [rows]}
        # For unpartitioned tables there is exactly one group keyed by None.
        if current_spec and current_spec.fields:
            groups: Dict[str, List[Dict]] = {}
            # Build a map from field_id -> column name for fast lookup
            id_to_col = {col.field_id: col for col in self.schema.columns}
            for row in rows:
                # Compute a composite partition key (one transform per field)
                key_parts = []
                for pf in current_spec.fields:
                    col = id_to_col[pf.field_id]
                    key_parts.append(pf.apply(row[col.name]))
                partition_key = "|".join(key_parts)   # e.g. "2024-01-15"
                groups.setdefault(partition_key, []).append(row)
        else:
            groups = {None: rows}

        manifest_entries = []
        total_rows = 0

        with tempfile.TemporaryDirectory() as tmpdir:
            for partition_value, partition_rows in groups.items():
                # 1. Write one Parquet file per partition
                file_uuid = str(uuid.uuid4())
                local_path = os.path.join(tmpdir, f"{file_uuid}.parquet")
                stats = write_parquet(self.schema, partition_rows, local_path)

                # 2. Upload to MinIO — path includes the partition value when set
                if partition_value:
                    data_key = f"{base_key}/data/{partition_value}/{file_uuid}.parquet"
                else:
                    data_key = f"{base_key}/data/{file_uuid}.parquet"

                with open(local_path, "rb") as f:
                    parquet_bytes = f.read()
                self.store.put(data_key, parquet_bytes)

                # 3. Convert column name stats → field_id stats
                field_stats = {}
                for col in self.schema.columns:
                    if col.name in stats["columns"]:
                        field_stats[col.field_id] = stats["columns"][col.name]

                manifest_entries.append(
                    ManifestEntry(
                        file_path=data_key,
                        file_size_bytes=len(parquet_bytes),
                        record_count=stats["row_count"],
                        column_stats=field_stats,
                        partition_value=partition_value,
                    )
                )
                total_rows += stats["row_count"]
                if partition_value:
                    print(f"[append] Wrote {stats['row_count']} rows → partition={partition_value}")

        # 4. Create Manifest and upload
        manifest = Manifest(
            manifest_id=str(uuid.uuid4()),
            entries=manifest_entries,
            added_files_count=len(manifest_entries),
            added_rows_count=total_rows,
        )
        manifest_key = write_manifest(manifest, self.store)

        # 5. Create ManifestList and upload
        snapshot_id = int(time.time() * 1000)
        
        # Carry over previous manifest list entries
        previous_entries = []
        if self.metadata.current_snapshot_id:
            for snap in self.metadata.snapshots:
                if snap.snapshot_id == self.metadata.current_snapshot_id:
                    old_ml = read_manifest_list(snap.manifest_list, self.store)
                    previous_entries = old_ml.entries
                    break
        
        ml_entry = ManifestListEntry(
            manifest_path=manifest_key,
            added_snapshot_id=snapshot_id,
            added_files_count=len(manifest_entries),
            added_rows_count=total_rows,
        )
        manifest_list = ManifestList(entries=previous_entries + [ml_entry])
        ml_key = write_manifest_list(manifest_list, snapshot_id, self.store)

        # 6. Create Snapshot
        snapshot = Snapshot(
            snapshot_id=snapshot_id,
            parent_snapshot_id=self.metadata.current_snapshot_id,
            sequence_number=len(self.metadata.snapshots) + 1,
            timestamp_ms=snapshot_id,
            manifest_list=ml_key,
            summary={"operation": "append"},
        )

        # 7. Update TableMetadata
        self.metadata.snapshots.append(snapshot)
        self.metadata.current_snapshot_id = snapshot_id
        self.metadata.last_updated_ms = snapshot_id
        self.metadata.snapshot_log.append({
            "timestamp_ms": snapshot_id,
            "snapshot_id": snapshot_id,
        })

        # 8. Write new versioned metadata file to MinIO
        write_metadata(self.metadata, self.store)

    def scan(self, filter: Dict[str, Any] = None) -> List[str]:
        """Walk the current metadata tree and return Parquet data file paths.

        Two-level pruning when a filter is provided:
          1. Partition pruning  — skip files whose partition_value proves they
             cannot match the filter (no manifest-stat read needed).
          2. Column-stat skipping — skip files where min/max stats rule out a match.

        Path:
          metadata → current_snapshot_id → snapshot.manifest_list
          → ManifestList.entries → each manifest → each ManifestEntry.file_path
        """
        if self.metadata.current_snapshot_id is None:
            return []

        # Resolve the partition column field_id → transform for the current spec
        partition_field_ids: Dict[int, str] = {}  # {field_id: transform}
        current_spec = next(
            (s for s in self.metadata.partition_specs
             if s.spec_id == self.metadata.current_spec_id),
            None,
        )
        if current_spec:
            for pf in current_spec.fields:
                partition_field_ids[pf.field_id] = pf

        # 1. Find the current snapshot
        current_snapshot = next(
            s for s in self.metadata.snapshots
            if s.snapshot_id == self.metadata.current_snapshot_id
        )

        # 2. Read the manifest list for this snapshot
        manifest_list = read_manifest_list(current_snapshot.manifest_list, self.store)

        # 3. Read every manifest; apply partition pruning then column-stat skipping
        file_paths = []
        skipped = []
        for ml_entry in manifest_list.entries:
            manifest = read_manifest(ml_entry.manifest_path, self.store)
            for entry in manifest.entries:
                fname = entry.file_path.split("/")[-1]

                # --- Partition pruning (cheapest: no network I/O needed) ---
                partition_confirmed = False   # True = this file is in the right partition
                if (
                    filter
                    and entry.partition_value is not None
                    and filter["field_id"] in partition_field_ids
                ):
                    pf = partition_field_ids[filter["field_id"]]
                    # Compute what partition key the filter value maps to
                    try:
                        target_partition = pf.apply(filter["value"])
                    except Exception:
                        target_partition = None

                    if target_partition:
                        if entry.partition_value != target_partition:
                            print(
                                f"[scan] SKIP  {fname}  "
                                f"(partition pruning: file={entry.partition_value}, "
                                f"filter={filter['op']} {filter['value']} → target={target_partition})"
                            )
                            skipped.append(fname)
                            continue
                        else:
                            # The file is in the correct partition — don't let column-stat
                            # skipping override this, because the transform coarsens the value
                            # (e.g. day=2024-01-15 covers all timestamps on that day, but the
                            # filter value 00:00:00 would look like it falls outside the file's
                            # min/max of 12:00:00 for the same day).
                            partition_confirmed = True

                # --- Column-stat skipping (reads manifest JSON, already in memory) ---
                # Only apply if partition pruning did NOT already confirm this file.
                if filter and not partition_confirmed and _should_skip(entry, filter):
                    col_stats = entry.column_stats.get(filter["field_id"], {})
                    print(
                        f"[scan] SKIP  {fname}  "
                        f"(stats: min={col_stats.get('min')}, max={col_stats.get('max')}  "
                        f"filter: {filter['op']} {filter['value']})"
                    )
                    skipped.append(fname)
                    continue

                print(f"[scan] OPEN  {fname}")
                file_paths.append(entry.file_path)

        if filter:
            print(f"[scan] → {len(file_paths)} file(s) opened, {len(skipped)} skipped.")

        return file_paths

    def query(self, sql: str) -> List[Dict[str, Any]]:
        """Run a SQL query against the current snapshot's data.

        1. Parses a single WHERE clause (if present) to build a filter.
        2. scan(filter) skips files via predicate pushdown on manifest stats.
        3. Remaining files are downloaded from MinIO into a temp directory.
        4. DuckDB runs the full SQL and returns results as a list of dicts.

        Use the actual table name in your SQL:
            SELECT * FROM {self.name} WHERE price > 500

        Supported WHERE ops for file skipping: >, <, =
        DuckDB always applies the full predicate on the rows it does read.
        """
        filter = self._parse_where(sql)
        file_paths = self.scan(filter=filter)
        if not file_paths:
            return []

        with tempfile.TemporaryDirectory() as tmpdir:
            # Download only the files that survived predicate pushdown
            for i, remote_path in enumerate(file_paths):
                local_path = os.path.join(tmpdir, f"part_{i}.parquet")
                data = self.store.get(remote_path)
                with open(local_path, "wb") as f:
                    f.write(data)

            # Use a single connection for both reading and querying
            glob = os.path.join(tmpdir, "*.parquet")
            conn = duckdb.connect()
            conn.execute(f"CREATE VIEW \"{self.name}\" AS SELECT * FROM read_parquet('{glob}')")
            cursor = conn.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

        return [dict(zip(columns, row)) for row in rows]

    def _parse_where(self, sql: str) -> Dict[str, Any] | None:
        """Extract a single WHERE condition from SQL and return a scan filter.

        Supported patterns (case-insensitive):
            WHERE <column> > <value>
            WHERE <column> < <value>
            WHERE <column> = <value>

        Returns None if no parseable WHERE clause is found.
        Multi-condition clauses (AND/OR) are not parsed; scan() will read all files.
        """
        import re
        # Match a single simple condition; stop before AND/OR/GROUP/ORDER/LIMIT
        pattern = re.compile(
            r"WHERE\s+(\w+)\s*(>|<|=)\s*([\d.]+)",
            re.IGNORECASE
        )
        m = pattern.search(sql)
        if not m:
            return None

        col_name, op_sym, raw_value = m.group(1), m.group(2), m.group(3)

        # Map symbol → our op string
        op_map = {">": "gt", "<": "lt", "=": "eq"}
        op = op_map[op_sym]

        # Resolve column name → field_id via the current schema
        col = next((c for c in self.schema.columns if c.name == col_name), None)
        if col is None:
            return None  # unknown column — skip pushdown, DuckDB will handle it

        # Cast value to the column's Python type
        try:
            value: float | int | str
            if col.type in ("int", "long"):
                value = int(raw_value)
            else:
                value = float(raw_value)
        except ValueError:
            return None

        return {"field_id": col.field_id, "op": op, "value": value}
