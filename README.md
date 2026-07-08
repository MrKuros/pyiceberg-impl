# pyiceberg-impl

A from-scratch implementation of the Apache Iceberg table format spec, demonstrating how ACID guarantees, schema evolution, and time travel work at the file level in object storage systems. This is not a wrapper around PyIceberg — it reimplements the metadata tree, manifest protocol, and snapshot chain directly, so you can read the code and understand exactly why Iceberg works the way it does.

## The Core Insight

Object storage is cheap and infinitely scalable, but it has no concept of transactions, schema, or history. If you overwrite an object, the old version is gone. The trick Iceberg discovered is to put **all the intelligence into an immutable metadata tree** — not into the storage layer. No file is ever edited in place. Every write creates a new file. ACID semantics, time travel, and schema evolution are all emergent properties of that one design decision.

---

## Features

- **Atomic snapshot commits** — every `append()` produces an immutable snapshot; partial writes never become visible
- **Schema evolution without rewriting data** — add, rename, and drop columns; old Parquet files are reconciled at query time by matching `field_id` integer tags, not column names
- **File skipping via column min/max statistics** — manifests store per-file `{min, max}` so queries skip files that cannot match the filter without opening them. Statistics can *exclude* files, never *guarantee* a match — files that pass the stats check are still opened.
- **Partition pruning** — identity, day, month, and year transforms; partition-filtered queries skip files at the manifest level, before any statistics are read. Faster than file skipping because no manifest JSON needs to be fetched first.
- **Time travel** — `query(sql, as_of=timestamp_ms)` scans the `snapshot_log` (a flat in-memory list of `{timestamp_ms, snapshot_id}` pairs) to find the snapshot that was current at that moment, then reads exactly the files that existed then. Overhead: < 10ms, measured.
- **Snapshot expiry and orphan file cleanup** — `expire_snapshots(older_than_ms)` trims old history; `delete_orphan_files()` removes unreferenced Parquet files and manifests from MinIO

---

## Quick Demo

This is time travel. Three appends, three different answers, zero extra I/O overhead for going back:

```python
from iceberg.schema import Schema, Column
from iceberg.store import MinIOStore
from iceberg.metadata import new_table
from iceberg.table import Table

store = MinIOStore()  # local MinIO via docker compose up -d
schema = Schema(1, [Column(1, "id", "long", True), Column(2, "price", "double", True)])
new_table("skins", schema, store)

# Day 1 — AK-47 prices around $30
t = Table("skins", store); t.append([{"id": i, "price": 30 + i*0.01} for i in range(1000)])
ts_day1 = t.metadata.snapshots[-1].timestamp_ms

# Day 2 — prices spike to ~$50
t = Table("skins", store); t.append([{"id": 1000+i, "price": 50 + i*0.01} for i in range(1000)])
ts_day2 = t.metadata.snapshots[-1].timestamp_ms

# Day 3 — prices correct to ~$40
t = Table("skins", store); t.append([{"id": 2000+i, "price": 40 + i*0.01} for i in range(1000)])

t = Table("skins", store)
print(t.query("SELECT AVG(price) FROM skins", as_of=ts_day1)[0])  # → ~30.5
print(t.query("SELECT AVG(price) FROM skins", as_of=ts_day2)[0])  # → ~40.3
print(t.query("SELECT AVG(price) FROM skins")[0])                  # → ~43.5
```

Run the full end-to-end demo (schema evolution + time travel + lifecycle management):

```bash
docker compose up -d
PYTHONPATH=. python demo.py
```

---

## Benchmark Results

Measured on a local machine against MinIO in Docker. Numbers illustrate the cost model — not a production claim.

| Metric | Result |
|---|---|
| Write throughput | ~390,000 rows/s across 10 × 10,000-row batches |
| Metadata overhead (per append) | 55% of write wall-clock — 3 sequential MinIO PUTs |
| Full scan (100k rows, 10 files) | 0.21s |
| File skipping (`price > 1500`, stratified data) | 70% of files skipped, 3.6× faster |
| Partition pruning (1 of 30 daily partitions) | 97% of files skipped, 3.0× faster |
| Time travel overhead | < 10ms — `snapshot_log` list scan only |

**Key insight:** metadata is the bottleneck, not data. Each `append()` requires three sequential object-storage writes (manifest → manifest list → table metadata). In production Iceberg this is replaced by a single atomic catalog commit (e.g., AWS Glue), which is why production write throughput is orders of magnitude higher.

**File skipping vs partition pruning:** partition pruning fires first, at the ManifestList level, eliminating entire manifests with a single string comparison that's already in memory. File skipping fires second, at the individual manifest entry level, after the manifest JSON has been downloaded. In the benchmark above, a query for one specific day skips 97% of files via partition pruning and never even reads the other 29 manifests.

Run the benchmark yourself:

```bash
PYTHONPATH=. python benchmark.py
```

---

## Architecture

The full design is documented in [ARCHITECTURE.md](ARCHITECTURE.md), covering:

- The metadata tree and why each layer exists
- The write path, step by step, with phase timings
- The read path with a worked file-skipping example
- How `field_id` makes schema evolution safe
- How `snapshot_log` enables time travel at zero scan cost
- A table of simplifications vs. real Iceberg and what each costs

---

## What Is NOT Implemented

| Feature | Status | Notes |
|---|---|---|
| Concurrent writers | ❌ | No locking. Simultaneous `append()` calls corrupt metadata. |
| Delete / Update operations | ❌ | Append-only. No position-delete or equality-delete files. |
| Compaction | ❌ | No `RewriteDataFiles`. Small files accumulate indefinitely. |
| Avro manifest serialization | ❌ | Manifests are JSON. Larger and slower than real Avro, but readable. |
| External catalog | ❌ | Tables are registered via MinIO path convention, not Glue/Hive/REST. |
| Schema at time-travel timestamp | ⚠️ | Always reconciles to current schema even for `as_of` queries. |

These are intentional simplifications for an educational implementation. See [ARCHITECTURE.md §7](ARCHITECTURE.md#7-what-we-simplified-vs-real-iceberg) for a full explanation of what each simplification costs in production.

**Why JSON instead of Avro for manifests?** Honest answer: Avro adds schema definition, binary encoding, and a schema registry dependency — three new concepts — without helping you understand anything new about how Iceberg works. JSON manifests are readable in any text editor; you can see exactly what's stored. The cost is real (JSON is ~3–5× larger and slower to parse than Avro), but for learning, the transparency is worth it.

---

## Getting Started

**Prerequisites:** Python 3.11+, Docker

```bash
# 1. Clone and install
git clone https://github.com/MrKuros/pyiceberg-impl.git
cd pyiceberg-impl
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. Start MinIO
docker compose up -d

# 3. Run the demo
PYTHONPATH=. python demo.py

# 4. Run tests
PYTHONPATH=. pytest tests/ -k "not integration"  # unit tests only (no MinIO needed)
PYTHONPATH=. pytest tests/                        # all tests (MinIO must be running)
```

**MinIO console** (inspect files written by the engine): http://localhost:9001  
Login: `admin` / `password`

---

## Project Structure

```
iceberg/
  table.py      — Table API: append(), query(), schema evolution, time travel, lifecycle
  metadata.py   — TableMetadata dataclass + versioned read/write
  manifest.py   — Manifest and ManifestList dataclasses + read/write
  snapshot.py   — Snapshot dataclass
  schema.py     — Schema, Column, PartitionSpec, PartitionField with transforms
  parquet.py    — Parquet write with field_id metadata + column stat extraction
  store.py      — MinIO client wrapper (put, get, list)

tests/
  test_parquet.py       — Parquet write/read/stats (no MinIO)
  test_placeholder.py   — Schema, file-skipping logic, integration tests

demo.py         — End-to-end: schema evolution + time travel + lifecycle
benchmark.py    — Throughput and pruning measurements
ARCHITECTURE.md — Deep-dive design document
```

---

## References

- [Apache Iceberg Table Spec v2](https://iceberg.apache.org/spec/)
- [Iceberg: A High-Performance Format for Huge Analytic Tables (Netflix, SIGMOD 2020)](https://dl.acm.org/doi/10.1145/3318464.3386128)
- [Apache Arrow Python Documentation](https://arrow.apache.org/docs/python/)
- [DuckDB Python API](https://duckdb.org/docs/api/python/overview)
