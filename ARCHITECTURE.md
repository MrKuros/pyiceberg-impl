# Architecture: pyiceberg-impl

## 1. What This Is

Object storage like S3 and MinIO are cheap and infinitely scalable, but they have no concept of transactions, schema, or history — you can overwrite or delete data and there is no going back. This implementation proves the core ideas of the [Apache Iceberg table format](https://iceberg.apache.org/spec/): that you can build ACID-like semantics, schema evolution, and point-in-time time travel on top of plain object storage by putting all intelligence into an immutable metadata tree instead of a mutable data store.

---

## 2. The Metadata Tree

Every table is defined entirely by a chain of immutable JSON files. No single file is ever edited in-place; every change creates a new file.

```
metadata/tables/<name>/v3.metadata.json      ← TableMetadata (the entry point)
  │
  │  current_snapshot_id → 1720000003
  │  schemas: [schema_id=1, schema_id=2, schema_id=3]  ← full schema history
  │  snapshot_log: [{ts, snap_id}, ...]                 ← chronological index
  │
  └── snapshots[snapshot_id=1720000003]
        │
        │  parent_snapshot_id → 1720000002     ← links to previous state
        │  timestamp_ms → 1720000003000
        │
        └── manifest_list → metadata/snap-1720000003-<uuid>.json
              │
              │  [each entry = one manifest, one per append batch]
              │
              ├── manifest_path → metadata/manifests/<uuid-A>.json   ← this batch
              │     │
              │     └── entries:
              │           file_path: tables/<name>/data/2024-01-15/<uuid>.parquet
              │           record_count: 10000
              │           column_stats: {field_id: {min, max}}        ← for skipping
              │           partition_value: "2024-01-15"               ← for pruning
              │
              └── manifest_path → metadata/manifests/<uuid-B>.json   ← previous batch
                    └── entries: ...
```

**Why each layer exists:**

| Layer | Purpose |
|---|---|
| `TableMetadata` | Single versioned entry point. Holds schema history, partition spec history, and the pointer to current state. Loading the latest `vN.metadata.json` gives you the complete table definition. |
| `Snapshot` | Represents one atomic commit (one `append()` call). Its `parent_snapshot_id` forms the chain that makes time travel possible. |
| `ManifestList` | An index of all manifests that belong to a snapshot. This is where structural sharing happens: a new snapshot's manifest list re-links all previous manifests plus the new one, without copying any data. |
| `Manifest` | Tracks one batch of data files. Stores per-file statistics (`min`, `max` per column, `record_count`, `partition_value`) so the query engine can skip files without opening them. |
| Parquet file | The actual data. Immutable once written. Column metadata includes `field_id` tags that survive schema renames. |

---

## 3. The Write Path

When `table.append(rows)` is called, these steps happen in this exact order:

```
append(rows)
    │
    ├─ 1. GROUP rows by partition value
    │      (identity transform on 'day' column → "2024-01-15")
    │      Rows that share a partition key go into one Parquet file.
    │      This must happen before writing so each file covers exactly one partition.
    │
    ├─ 2. WRITE Parquet file(s) to a local temp directory
    │      write_parquet() serializes rows, attaches field_id metadata to each
    │      column, and computes per-column {min, max} statistics.
    │      This step is purely local — no network I/O yet.
    │
    ├─ 3. UPLOAD Parquet file(s) to MinIO
    │      Path: tables/<name>/data/<partition_value>/<uuid>.parquet
    │      The UUID makes every file unique; files are never overwritten.
    │
    ├─ 4. WRITE Manifest to MinIO
    │      One Manifest covers all files from this append.
    │      Stores: file_path, record_count, column_stats, partition_value.
    │      Path: metadata/manifests/<uuid>.json
    │
    ├─ 5. READ previous ManifestList (if table has existing snapshots)
    │      Structural sharing: we will prepend the new manifest to
    │      the existing list, not replace it.
    │
    ├─ 6. WRITE new ManifestList to MinIO
    │      Contains: [new_manifest] + all_previous_manifests
    │      Path: metadata/snap-<snapshot_id>-<uuid>.json
    │
    ├─ 7. CREATE Snapshot object in memory
    │      snapshot_id = current Unix milliseconds (also acts as timestamp)
    │      parent_snapshot_id = previous current_snapshot_id
    │
    └─ 8. WRITE new versioned TableMetadata to MinIO
           Appends snapshot to metadata.snapshots list.
           Updates current_snapshot_id.
           Increments version: v2.metadata.json → v3.metadata.json
           This is the atomic commit point. Before this write lands,
           the old snapshot is still current. After it lands, the new one is.
```

Steps 2–3 (Parquet write + upload) account for ~44% of wall-clock time. Step 8 (metadata commit) accounts for ~55% because it requires three sequential MinIO `PUT` calls: manifest, manifest list, metadata file.

---

## 4. The Read Path

When `table.query("SELECT AVG(price) FROM tbl WHERE day = '2024-01-15'")` is called:

```
query(sql)
    │
    ├─ 1. PARSE WHERE clause
    │      _parse_where() extracts {field_id: 4, op: "eq", value: "2024-01-15"}
    │      from the SQL string. This is the predicate pushdown filter.
    │
    ├─ 2. LOAD latest vN.metadata.json from MinIO
    │      Resolves current_snapshot_id → the active snapshot.
    │
    ├─ 3. READ ManifestList for the current snapshot
    │      Gets a list of all manifest paths (one per historical append batch).
    │
    ├─ 4. For each Manifest entry — TWO-LEVEL PRUNING:
    │
    │      Level 1: PARTITION PRUNING (cheapest — string comparison only)
    │      ┌──────────────────────────────────────────────────────────┐
    │      │  filter field_id (4) is a partition field (identity)    │
    │      │  entry.partition_value = "2024-01-14"                   │
    │      │  target = apply(identity, "2024-01-15") = "2024-01-15"  │
    │      │  "2024-01-14" ≠ "2024-01-15"  →  SKIP this file        │
    │      └──────────────────────────────────────────────────────────┘
    │
    │      Level 2: COLUMN-STAT SKIPPING (applied if partition doesn't match)
    │      ┌──────────────────────────────────────────────────────────┐
    │      │  filter: price > 1500                                    │
    │      │  entry.column_stats[field_id=3] = {min: 10.5, max: 200} │
    │      │  max (200) <= value (1500)  →  SKIP this file           │
    │      │                                                          │
    │      │  entry.column_stats[field_id=3] = {min: 1400, max: 2000}│
    │      │  max (2000) > value (1500)  →  OPEN this file           │
    │      └──────────────────────────────────────────────────────────┘
    │
    ├─ 5. DOWNLOAD only the surviving Parquet files from MinIO
    │
    ├─ 6. NORMALIZE each file to the current schema (_normalize_parquet)
    │      Matches columns by field_id. Fills missing columns with None.
    │      Renames columns to current names. Writes a temp normalized file.
    │
    └─ 7. QUERY with DuckDB
           CREATE VIEW AS SELECT * FROM read_parquet('part_*.parquet')
           Execute original SQL over the normalized temp files.
           Return results as list of dicts.
```

**Why partition pruning is faster than column-stat skipping:**
Partition pruning fires at the outer loop (the ManifestList level) and eliminates entire manifests with a single string comparison that is already in memory. Column-stat skipping fires at the inner loop (the Manifest entry level) and requires reading the manifest JSON from MinIO first. In our benchmark with 30 daily partitions, a query for one day skipped 97% of files via partition pruning; no file was even read from MinIO for the eliminated 29 partitions.

---

## 5. Schema Evolution

Schema evolution works without rewriting any Parquet file because every column is identified by a **`field_id`** integer, not its name. Names are just aliases stored in the schema.

When Parquet files are written, `write_parquet()` tags each column's Arrow metadata with its `field_id`:

```python
pa.field("price", pa.float64(), metadata={"field_id": "3"})
```

**Concrete rename example:**

```
Before rename:
  Schema 2: [Column(field_id=1, name='id'), Column(field_id=3, name='price')]
  Parquet file A written under Schema 2: columns tagged field_id=1, field_id=3

After rename_column(field_id=3, new_name='unit_price'):
  Schema 3: [Column(field_id=1, name='id'), Column(field_id=3, name='unit_price')]
  Parquet file B written under Schema 3: columns tagged field_id=1, field_id=3

When reading file A under Schema 3:
  _normalize_parquet() reads file A's Arrow schema.
  Finds field_id=3 → current_name_by_fid[3] = 'unit_price'
  Rewrites the column header to 'unit_price'.
  Returns a table where the column is correctly named 'unit_price'.
  → SELECT unit_price FROM tbl works on both old and new files.
```

**Add column:** assigns a new `field_id` (always monotonically increasing via `last_column_id`). Old files don't have this `field_id`; `_normalize_parquet()` fills those rows with `None`.

**Drop column:** removes the `field_id` from the schema. Old files still contain the column's bytes on disk, but `_normalize_parquet()` simply never selects it.

---

## 6. Time Travel

Every `append()` creates a Snapshot with a `parent_snapshot_id` pointing to the previous snapshot. This forms a linked list of table states stretching back to the first write.

```
Snapshot 1 (ts=1000)  ←parent─  Snapshot 2 (ts=2000)  ←parent─  Snapshot 3 (ts=3000)
  manifest list A                  manifest list A+B                manifest list A+B+C
  [file_A.parquet]                 [file_A, file_B]                 [file_A, file_B, file_C]
```

The `snapshot_log` in `TableMetadata` is a flat, chronologically-ordered list of `{timestamp_ms, snapshot_id}` pairs. This is the index used by `snapshot_at()`:

```python
# query("SELECT AVG(price) FROM tbl", as_of=1500)
#   → snapshot_at(1500)
#   → snapshot_log = [{ts=1000, id=1}, {ts=2000, id=2}, {ts=3000, id=3}]
#   → candidates with ts <= 1500: [{ts=1000, id=1}]
#   → returns Snapshot 1
#   → scan_snapshot(snapshot_id=1) → only file_A.parquet is visible
#   → DuckDB runs query over file_A only
```

**What time travel does not cost:** it does not replay transaction logs, diff file sets, or scan extra data. The query engine opens exactly the same Parquet files it would have opened had the query been run at `ts=1000`. The only overhead is one `snapshot_log` array scan in Python — negligible (measured at < 10ms overhead in our benchmark).

---

## 7. What We Simplified vs Real Iceberg

This is an educational implementation. The following simplifications were intentional; each has a concrete production cost.

| Simplification | What We Did | What Real Iceberg Does | Cost of Our Simplification |
|---|---|---|---|
| **Manifest serialization** | JSON | Apache Avro binary | JSON is ~3–5× larger on disk and slower to parse. Not a correctness issue, but significant at millions of files. |
| **Concurrent writers** | No locking. Last write wins. | Optimistic concurrency control via atomic catalog commits (e.g., AWS Glue, Hive Metastore). | Two simultaneous `append()` calls will silently corrupt metadata by overwriting each other's `vN.metadata.json`. |
| **Delete/Update operations** | Append-only. No row-level deletes. | Position delete files and equality delete files. | Cannot implement `DELETE FROM` or `UPDATE`. Read-time merge of delete files is a significant engine feature. |
| **Compaction** | Never merges small files. | `RewriteDataFiles` job rewrites small Parquet files into larger ones. | Each `append()` adds one file. After 1000 appends there are 1000 files to scan. Query planning (reading all manifest JSONs) slows linearly. |
| **Catalog** | Table names resolved by `LIST metadata/tables/<name>/`. | External catalog (Glue, REST, JDBC) provides atomic table registration and namespace management. | No namespace isolation, no atomic table rename, `LIST` is slow on real S3 at scale. |
| **Schema at time travel** | Always reconciles to current schema, even for old snapshots. | Reads the schema that was current at the target snapshot. | `SELECT * ... as_of=old_ts` returns columns that didn't exist yet as `None` rather than omitting them. Semantically different but rarely matters in practice. |

---

## 8. Benchmark Results

Measured on a local machine against MinIO running in Docker. Numbers reflect the cost model of the implementation, not production Iceberg on cloud object storage.

```
┌─────────────────────────────────────────────────────────────┐
│  1. Write Throughput  (100,000 rows, 10 batches of 10,000)
│─────────────────────────────────────────────────────────────│
│  Total time     : ~0.26s
│  Throughput     : ~390,000 rows/s
│  Phase breakdown:
│    Parquet write : 15% of wall-clock
│    MinIO upload  : 29% of wall-clock
│    Metadata      : 55% of wall-clock   ← bottleneck
├─────────────────────────────────────────────────────────────┤
│  2. Full Scan (no filter)
│─────────────────────────────────────────────────────────────│
│  Files: 10   Rows: 100,000   Time: ~0.21s
├─────────────────────────────────────────────────────────────┤
│  3. Column-stat File Skipping  (price > 1500)
│─────────────────────────────────────────────────────────────│
│  Files opened: 3   Skipped: 7   (70%)   3.6× faster
├─────────────────────────────────────────────────────────────┤
│  4. Partition Pruning  (day = '2024-01-15', 30 partitions)
│─────────────────────────────────────────────────────────────│
│  Files opened: 1   Skipped: 29   (97%)   3.0× faster
├─────────────────────────────────────────────────────────────┤
│  5. Time Travel Overhead
│─────────────────────────────────────────────────────────────│
│  Direct query (10 files)      : ~70ms
│  Same query via as_of=        : ~61ms
│  Overhead                     : < 10ms  (measurement noise)
└─────────────────────────────────────────────────────────────┘
```

**What these numbers mean:**

The **metadata bottleneck at 55%** is the most important finding. Each `append()` makes three sequential MinIO PUT calls: manifest, manifest list, metadata file. On real S3 with 20–50ms per request, this alone would cost 60–150ms per append regardless of data volume. Production Iceberg eliminates this by using an external catalog (e.g., AWS Glue) that provides a single atomic commit operation instead of three sequential file writes.

The **partition pruning vs file skipping gap (97% vs 70%)** confirms the design intent: partition pruning fires before any manifest JSON is fetched from storage, while column-stat skipping requires reading the manifest first. Both are metadata-only operations, but partition pruning eliminates entire manifests whereas column-stat skipping filters individual entries within manifests that have already been downloaded.

The **time travel overhead of < 10ms** confirms the core claim: time travel is a routing decision, not a data scan. `snapshot_at()` is a linear scan of a Python list of log entries. The query engine then reads exactly the files that existed at that timestamp — no extra I/O.

---

## 9. References

- [Apache Iceberg Table Spec v2](https://iceberg.apache.org/spec/) — the canonical reference for the metadata tree, snapshot model, and manifest format this implementation models.
- [Iceberg: A High-Performance Format for Huge Analytic Tables (Netflix, SIGMOD 2020)](https://dl.acm.org/doi/10.1145/3318464.3386128) — the original paper describing the motivation for the hidden partitioning and file-level statistics approach.
- [Apache Arrow Python Documentation](https://arrow.apache.org/docs/python/) — used for Parquet serialization and field-id-tagged schema metadata.
- [DuckDB Python API](https://duckdb.org/docs/api/python/overview) — used as the SQL execution engine over normalized Parquet files.
