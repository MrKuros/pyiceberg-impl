import os
import time
import uuid
import tempfile
from typing import List, Dict, Any
import duckdb

from iceberg.store import MinIOStore
from iceberg.schema import Schema, Column, PartitionSpec
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

    # ------------------------------------------------------------------ #
    #  Schema Evolution                                                    #
    # ------------------------------------------------------------------ #

    def _next_schema_id(self) -> int:
        return max(s.schema_id for s in self.metadata.schemas) + 1

    def _commit_schema(self, new_schema: Schema) -> None:
        """Append new_schema to metadata, update current_schema_id, and persist."""
        self.metadata.schemas.append(new_schema)
        self.metadata.current_schema_id = new_schema.schema_id
        self.metadata.last_column_id = max(
            (col.field_id for col in new_schema.columns), default=self.metadata.last_column_id
        )
        self.metadata.last_updated_ms = int(time.time() * 1000)
        write_metadata(self.metadata, self.store)
        # Keep self.schema in sync
        self.schema = new_schema

    def add_column(self, name: str, type: str, required: bool = False) -> None:
        """Add a new nullable column to the table schema.

        - Assigns a new field_id (last_column_id + 1).
        - Appends a new Schema to TableMetadata.schemas.
        - Updates current_schema_id.
        - Writes a new versioned metadata file.
        - Does NOT touch any existing Parquet files.

        New columns must be nullable (required=False) because existing Parquet
        files don't have this column; they will return NULL when read.
        """
        if required:
            raise ValueError(
                "Cannot add a required column to a table that already has data. "
                "Add it as nullable (required=False) instead."
            )
        new_field_id = self.metadata.last_column_id + 1
        new_columns = list(self.schema.columns) + [
            Column(field_id=new_field_id, name=name, type=type, required=False)
        ]
        new_schema = Schema(schema_id=self._next_schema_id(), columns=new_columns)
        self._commit_schema(new_schema)
        print(f"[schema] add_column '{name}' (field_id={new_field_id}, type={type}) → schema_id={new_schema.schema_id}")

    def rename_column(self, field_id: int, new_name: str) -> None:
        """Rename a column. The field_id stays the same.

        Because Parquet files embed field_ids in their metadata, old files
        can be read correctly under the new name without any rewrite.
        """
        new_columns = []
        found = False
        for col in self.schema.columns:
            if col.field_id == field_id:
                new_columns.append(Column(field_id=col.field_id, name=new_name, type=col.type, required=col.required))
                found = True
            else:
                new_columns.append(col)
        if not found:
            raise ValueError(f"No column with field_id={field_id} in current schema.")
        new_schema = Schema(schema_id=self._next_schema_id(), columns=new_columns)
        self._commit_schema(new_schema)
        print(f"[schema] rename_column field_id={field_id} → '{new_name}' (schema_id={new_schema.schema_id})")

    def drop_column(self, field_id: int) -> None:
        """Drop a column from the schema. Existing Parquet files are not touched.

        Old files still contain the column's data; it is simply never selected
        when reading under the new schema.
        """
        new_columns = [col for col in self.schema.columns if col.field_id != field_id]
        if len(new_columns) == len(self.schema.columns):
            raise ValueError(f"No column with field_id={field_id} in current schema.")
        new_schema = Schema(schema_id=self._next_schema_id(), columns=new_columns)
        self._commit_schema(new_schema)
        print(f"[schema] drop_column field_id={field_id} → schema_id={new_schema.schema_id}")

    # ------------------------------------------------------------------ #
    #  Time Travel                                                         #
    # ------------------------------------------------------------------ #

    def snapshot_at(self, timestamp_ms: int) -> "Snapshot":
        """Return the latest snapshot whose timestamp_ms <= the requested time.

        Walks snapshot_log (which records every snapshot in chronological order)
        and returns the newest entry that does not exceed the target timestamp.

        Raises ValueError if the table had no snapshots before that point in time.

        Usage:
            snap = table.snapshot_at(1_720_000_000_000)
            files = table.scan_snapshot(snap.snapshot_id)
        """
        # snapshot_log entries: [{"timestamp_ms": ..., "snapshot_id": ...}, ...]
        candidates = [
            entry for entry in self.metadata.snapshot_log
            if entry["timestamp_ms"] <= timestamp_ms
        ]
        if not candidates:
            raise ValueError(
                f"No snapshot exists at or before timestamp_ms={timestamp_ms}. "
                f"Earliest snapshot is at {self.metadata.snapshot_log[0]['timestamp_ms'] if self.metadata.snapshot_log else 'N/A'}."
            )
        # The latest candidate is the one we want
        log_entry = max(candidates, key=lambda e: e["timestamp_ms"])
        target_id = log_entry["snapshot_id"]

        snap = next(
            (s for s in self.metadata.snapshots if s.snapshot_id == target_id), None
        )
        if snap is None:
            raise ValueError(f"snapshot_log references snapshot_id={target_id} but it is not in snapshots list.")
        return snap

    def scan_snapshot(self, snapshot_id: int, filter: Dict[str, Any] = None) -> List[str]:
        """Walk a specific snapshot's manifest tree and return Parquet file paths.

        Identical to scan() but targets an arbitrary past snapshot rather than
        current_snapshot_id. Applies the same two-level pruning:
          1. Partition pruning  — skip files whose partition_value doesn't match.
          2. Column-stat skipping — skip files where min/max rules out a match.

        This is the engine behind time travel queries.
        """
        target_snapshot = next(
            (s for s in self.metadata.snapshots if s.snapshot_id == snapshot_id), None
        )
        if target_snapshot is None:
            raise ValueError(f"Snapshot {snapshot_id} not found in table metadata.")

        # Resolve partition spec (same logic as scan())
        partition_field_ids: Dict[int, Any] = {}
        current_spec = next(
            (s for s in self.metadata.partition_specs
             if s.spec_id == self.metadata.current_spec_id),
            None,
        )
        if current_spec:
            for pf in current_spec.fields:
                partition_field_ids[pf.field_id] = pf

        manifest_list = read_manifest_list(target_snapshot.manifest_list, self.store)

        file_paths = []
        skipped = []
        for ml_entry in manifest_list.entries:
            manifest = read_manifest(ml_entry.manifest_path, self.store)
            for entry in manifest.entries:
                fname = entry.file_path.split("/")[-1]

                # --- Partition pruning ---
                partition_confirmed = False
                if (
                    filter
                    and entry.partition_value is not None
                    and filter["field_id"] in partition_field_ids
                ):
                    pf = partition_field_ids[filter["field_id"]]
                    try:
                        target_partition = pf.apply(filter["value"])
                    except Exception:
                        target_partition = None
                    if target_partition:
                        if entry.partition_value != target_partition:
                            print(f"[scan_snapshot] SKIP {fname} (partition: {entry.partition_value} != {target_partition})")
                            skipped.append(fname)
                            continue
                        else:
                            partition_confirmed = True

                # --- Column-stat skipping ---
                if filter and not partition_confirmed and _should_skip(entry, filter):
                    col_stats = entry.column_stats.get(filter["field_id"], {})
                    print(
                        f"[scan_snapshot] SKIP {fname} "
                        f"(stats: min={col_stats.get('min')}, max={col_stats.get('max')} "
                        f"filter: {filter['op']} {filter['value']})"
                    )
                    skipped.append(fname)
                    continue

                print(f"[scan_snapshot] OPEN {fname}")
                file_paths.append(entry.file_path)

        if filter:
            print(f"[scan_snapshot] → {len(file_paths)} file(s) opened, {len(skipped)} skipped.")
        return file_paths

    def expire_snapshots(self, older_than_ms: int) -> None:
        """Expire snapshots older than the given timestamp.

        Removes old snapshots from the history. We ALWAYS protect the
        current_snapshot_id so the table remains readable, even if it is
        older than older_than_ms.
        """
        new_snapshots = []
        for s in self.metadata.snapshots:
            if s.snapshot_id == self.metadata.current_snapshot_id or s.timestamp_ms >= older_than_ms:
                new_snapshots.append(s)
            else:
                print(f"[expire] Expired snapshot {s.snapshot_id} (ts={s.timestamp_ms})")

        self.metadata.snapshots = new_snapshots

        # 3. Update snapshot_log to match remaining snapshots
        kept_ids = {s.snapshot_id for s in new_snapshots}
        self.metadata.snapshot_log = [
            e for e in self.metadata.snapshot_log if e["snapshot_id"] in kept_ids
        ]

        self.metadata.last_updated_ms = int(time.time() * 1000)
        write_metadata(self.metadata, self.store)

    def delete_orphan_files(self) -> None:
        """Find and delete orphan files in MinIO.

        1. Walks current metadata tree to find all valid referenced files.
        2. Lists all objects in MinIO under the table's location.
        3. Deletes objects that are not referenced.
        """
        # Because this simplified implementation writes manifests and manifest lists
        # to a shared global metadata/ folder (metadata/manifests/, metadata/snap-*),
        # we must collect referenced files from ALL tables to avoid deleting
        # another table's valid manifests.
        referenced_files = set()
        
        # Discover all tables in the warehouse
        table_prefixes = set()
        for key in self.store.list("metadata/tables/"):
            parts = key.split("/")
            if len(parts) >= 3:
                table_prefixes.add(f"metadata/tables/{parts[2]}/")
        
        # Load every table and collect references
        import json
        for t_prefix in table_prefixes:
            # Find the latest metadata file for this table
            meta_files = [k for k in self.store.list(t_prefix) if k.endswith(".metadata.json")]
            if not meta_files:
                continue
            import os
            latest_meta_key = max(meta_files, key=lambda k: int(os.path.basename(k).split("v")[1].split(".")[0]))
            
            # Read its snapshots
            meta_json = json.loads(self.store.get(latest_meta_key).decode("utf-8"))
            for s_dict in meta_json.get("snapshots", []):
                ml_key = s_dict["manifest_list"]
                referenced_files.add(ml_key)
                try:
                    ml = read_manifest_list(ml_key, self.store)
                    for ml_entry in ml.entries:
                        referenced_files.add(ml_entry.manifest_path)
                        m = read_manifest(ml_entry.manifest_path, self.store)
                        for m_entry in m.entries:
                            referenced_files.add(m_entry.file_path)
                except Exception:
                    pass  # Skip if already missing

        # 2. List all objects that could potentially be orphans
        prefix_to_strip = f"s3://{self.store.bucket_name}/"
        base_key = (
            self.metadata.location[len(prefix_to_strip):]
            if self.metadata.location.startswith(prefix_to_strip)
            else self.metadata.location
        )
        
        # We check this table's data directory, plus the global manifest directories
        all_objects = (
            self.store.list(f"{base_key}/data/") + 
            self.store.list("metadata/manifests/") + 
            [k for k in self.store.list("metadata/") if k.startswith("metadata/snap-")]
        )

        # 3. Delete orphans
        deleted_count = 0
        for obj in all_objects:
            if obj not in referenced_files:
                print(f"[orphan] Deleting orphan file: {obj}")
                self.store.client.remove_object(self.store.bucket_name, obj)
                deleted_count += 1
                
        print(f"[orphan] Deleted {deleted_count} orphan file(s).")

    def scan(self, filter: Dict[str, Any] = None) -> List[str]:
        """Walk the current snapshot's manifest tree and return Parquet file paths.

        Delegates to scan_snapshot(current_snapshot_id) so all pruning logic
        lives in one place.
        """
        if self.metadata.current_snapshot_id is None:
            return []
        return self.scan_snapshot(self.metadata.current_snapshot_id, filter=filter)

    def query(self, sql: str, as_of: int = None) -> List[Dict[str, Any]]:
        """Run a SQL query against the table.

        as_of: optional Unix milliseconds timestamp. When provided, the query
        reads the latest snapshot that existed at that point in time instead of
        the current snapshot — this is Iceberg time travel.

        Schema-aware: reconciles Parquet files written under older schemas.
        - Added columns: filled with None for rows from old files.
        - Dropped columns: ignored when reading old files.
        - Renamed columns: matched by field_id, returned under the current name.

        Note: we always reconcile against the CURRENT schema even for time-travel
        queries. This is the simplest correct approach; a full implementation would
        use the schema that was active at the target snapshot.
        """
        filter = self._parse_where(sql)

        if as_of is not None:
            snap = self.snapshot_at(as_of)
            print(f"[time-travel] as_of={as_of} → snapshot_id={snap.snapshot_id} (ts={snap.timestamp_ms})")
            file_paths = self.scan_snapshot(snap.snapshot_id, filter=filter)
        else:
            file_paths = self.scan(filter=filter)

        if not file_paths:
            return []

        # Current schema: the source of truth for column names and field_ids
        current_cols = self.schema.columns          # columns in the current schema
        current_field_ids = {col.field_id for col in current_cols}
        current_name_by_fid = {col.field_id: col.name for col in current_cols}

        with tempfile.TemporaryDirectory() as tmpdir:
            normalized_paths = []

            for i, remote_path in enumerate(file_paths):
                raw_bytes = self.store.get(remote_path)
                raw_path = os.path.join(tmpdir, f"raw_{i}.parquet")
                with open(raw_path, "wb") as f:
                    f.write(raw_bytes)

                # Normalize this file to the current schema
                norm_path = os.path.join(tmpdir, f"part_{i}.parquet")
                self._normalize_parquet(raw_path, norm_path, current_cols, current_name_by_fid, current_field_ids)
                normalized_paths.append(norm_path)

            # Build a UNION VIEW over all normalized files
            glob = os.path.join(tmpdir, "part_*.parquet")
            conn = duckdb.connect()
            conn.execute(f"CREATE VIEW \"{self.name}\" AS SELECT * FROM read_parquet('{glob}')")
            cursor = conn.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

        return [dict(zip(columns, row)) for row in rows]

    def _normalize_parquet(
        self,
        src_path: str,
        dst_path: str,
        current_cols: List[Any],
        current_name_by_fid: Dict[int, str],
        current_field_ids: set,
    ) -> None:
        """Read a single Parquet file and rewrite it aligned to the current schema.

        Strategy:
          1. Read the file's own schema from Parquet metadata to discover field_ids.
          2. For each column in the CURRENT schema:
               - If the file has that field_id: include it, using the CURRENT name.
               - If the file is missing that field_id: fill with None (added column).
          3. Columns in the file but NOT in the current schema are dropped.
        """
        import pyarrow as pa
        import pyarrow.parquet as pq

        file_table = pq.read_table(src_path)
        file_schema = file_table.schema

        # Build a map of field_id -> column index in the file's own schema
        fid_to_file_col_idx: Dict[int, int] = {}
        for idx, field in enumerate(file_schema):
            fid_meta = field.metadata.get(b"field_id") if field.metadata else None
            if fid_meta is not None:
                fid_to_file_col_idx[int(fid_meta)] = idx

        n_rows = len(file_table)
        new_arrays = []
        new_fields = []

        for col in current_cols:
            fid = col.field_id
            if fid in fid_to_file_col_idx:
                # Column exists in the file: take the data, rename to current name
                arr = file_table.column(fid_to_file_col_idx[fid])
                new_arrays.append(arr.cast(arr.type))  # identity cast keeps type
                new_fields.append(pa.field(
                    col.name, arr.type, nullable=not col.required,
                    metadata={"field_id": str(fid)}
                ))
            else:
                # Column was added after this file was written: fill with nulls
                from iceberg.parquet import TYPE_MAP
                pa_type = TYPE_MAP.get(col.type, pa.string())
                new_arrays.append(pa.array([None] * n_rows, type=pa_type))
                new_fields.append(pa.field(
                    col.name, pa_type, nullable=True,
                    metadata={"field_id": str(fid)}
                ))

        new_pa_schema = pa.schema(new_fields)
        normalized = pa.table(new_arrays, schema=new_pa_schema)
        pq.write_table(normalized, dst_path)


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
        # Match a simple condition. Values can be numbers (12.3) or single-quoted strings ('Jan 3')
        pattern = re.compile(
            r"WHERE\s+(\w+)\s*(>|<|=)\s*('([^']+)'|[\d.]+)",
            re.IGNORECASE
        )
        m = pattern.search(sql)
        if not m:
            return None

        col_name, op_sym = m.group(1), m.group(2)
        
        # If it was a quoted string, group 4 has the inner text. Otherwise group 3 has the number.
        raw_value = m.group(4) if m.group(4) is not None else m.group(3)

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
            elif col.type == "double":
                value = float(raw_value)
            else:
                value = str(raw_value)
        except ValueError:
            return None

        return {"field_id": col.field_id, "op": op, "value": value}
