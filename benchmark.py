"""
Iceberg Implementation Benchmark
=================================
Measures: write throughput, read with/without pruning,
          partition pruning, time travel overhead.
Run:
    PYTHONPATH=. .venv/bin/python benchmark.py
"""

import io
import os
import re
import sys
import time
import uuid
import random
import tempfile
import contextlib
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Tuple

import pyarrow.parquet as pq

from iceberg.schema import Schema, Column, PartitionSpec, PartitionField
from iceberg.store import MinIOStore
from iceberg.metadata import new_table, write_metadata
from iceberg.parquet import write_parquet
from iceberg.manifest import (
    Manifest, ManifestEntry, write_manifest,
    ManifestList, ManifestListEntry, write_manifest_list,
    read_manifest_list,
)
from iceberg.snapshot import Snapshot
from iceberg.table import Table

random.seed(99)
store = MinIOStore()

# ───────────────────────────────────────────────────────────────
# Helpers
# ───────────────────────────────────────────────────────────────

def fmt_num(n: float, unit: str = "") -> str:
    if unit == "ms":
        return f"{n*1000:.1f} ms"
    if unit == "s":
        return f"{n:.3f}s"
    if unit == "rows/s":
        return f"{n:,.0f} rows/s"
    if unit == "%":
        return f"{n:.1f}%"
    return f"{n:,.0f}"


class _Capture(list):
    """Capture print() output into a list of lines."""
    def __enter__(self):
        self._orig = sys.stdout
        sys.stdout = self
        return self
    def write(self, s):
        self.append(s)
    def flush(self): pass
    def __exit__(self, *_):
        sys.stdout = self._orig


@contextlib.contextmanager
def silence():
    """Suppress all print output inside the block."""
    with _Capture():
        yield


@contextlib.contextmanager
def capture_scan():
    """Capture scan log lines and yield (OPEN list, SKIP list)."""
    opened = []
    skipped = []
    with _Capture() as lines:
        yield opened, skipped
    for s in "".join(lines).splitlines():
        if "OPEN" in s:
            opened.append(s)
        elif "SKIP" in s:
            skipped.append(s)


def pct(n, total): return 100 * n / total if total else 0


def banner(title: str):
    print(f"\n{'─'*60}")
    print(f"  {title}")
    print(f"{'─'*60}")


# ───────────────────────────────────────────────────────────────
# Instrumented append that records per-phase timings
# ───────────────────────────────────────────────────────────────

def timed_append(t: Table, rows: List[Dict]) -> Dict[str, float]:
    """Append rows and return {'parquet_s', 'upload_s', 'meta_s', 'total_s'}."""
    prefix_to_strip = f"s3://{t.store.bucket_name}/"
    base_key = (
        t.metadata.location[len(prefix_to_strip):]
        if t.metadata.location.startswith(prefix_to_strip)
        else t.metadata.location
    )
    current_spec = next(
        (s for s in t.metadata.partition_specs
         if s.spec_id == t.metadata.current_spec_id), None
    )

    # Group by partition (unpartitioned → one group)
    if current_spec and current_spec.fields:
        groups: Dict = {}
        id_to_col = {col.field_id: col for col in t.schema.columns}
        for row in rows:
            key_parts = [pf.apply(row[id_to_col[pf.field_id].name])
                         for pf in current_spec.fields]
            groups.setdefault("|".join(key_parts), []).append(row)
    else:
        groups = {None: rows}

    parquet_s = upload_s = meta_s = 0.0
    manifest_entries = []
    total_rows = 0

    t0_total = time.perf_counter()

    with tempfile.TemporaryDirectory() as tmpdir:
        for pv, prows in groups.items():
            fid = str(uuid.uuid4())
            local = os.path.join(tmpdir, f"{fid}.parquet")

            # Phase 1: write Parquet
            t0 = time.perf_counter()
            stats = write_parquet(t.schema, prows, local)
            parquet_s += time.perf_counter() - t0

            data_key = (
                f"{base_key}/data/{pv}/{fid}.parquet" if pv
                else f"{base_key}/data/{fid}.parquet"
            )

            # Phase 2: upload to MinIO
            t0 = time.perf_counter()
            with open(local, "rb") as f:
                raw = f.read()
            t.store.put(data_key, raw)
            upload_s += time.perf_counter() - t0

            field_stats = {
                col.field_id: stats["columns"][col.name]
                for col in t.schema.columns if col.name in stats["columns"]
            }
            manifest_entries.append(ManifestEntry(
                file_path=data_key,
                file_size_bytes=len(raw),
                record_count=stats["row_count"],
                column_stats=field_stats,
                partition_value=pv,
            ))
            total_rows += stats["row_count"]

    # Phase 3: metadata commit
    t0 = time.perf_counter()
    manifest = Manifest(
        manifest_id=str(uuid.uuid4()),
        entries=manifest_entries,
        added_files_count=len(manifest_entries),
        added_rows_count=total_rows,
    )
    manifest_key = write_manifest(manifest, t.store)

    snap_id = int(time.time() * 1000)
    prev_entries = []
    if t.metadata.current_snapshot_id:
        for s in t.metadata.snapshots:
            if s.snapshot_id == t.metadata.current_snapshot_id:
                prev_entries = read_manifest_list(s.manifest_list, t.store).entries
                break

    ml = ManifestList(entries=prev_entries + [ManifestListEntry(
        manifest_path=manifest_key,
        added_snapshot_id=snap_id,
        added_files_count=len(manifest_entries),
        added_rows_count=total_rows,
    )])
    ml_key = write_manifest_list(ml, snap_id, t.store)

    snap = Snapshot(
        snapshot_id=snap_id,
        parent_snapshot_id=t.metadata.current_snapshot_id,
        sequence_number=len(t.metadata.snapshots) + 1,
        timestamp_ms=snap_id,
        manifest_list=ml_key,
        summary={"operation": "append"},
    )
    t.metadata.snapshots.append(snap)
    t.metadata.current_snapshot_id = snap_id
    t.metadata.last_updated_ms = snap_id
    t.metadata.snapshot_log.append({"timestamp_ms": snap_id, "snapshot_id": snap_id})
    write_metadata(t.metadata, t.store)
    meta_s += time.perf_counter() - t0

    return {
        "parquet_s": parquet_s,
        "upload_s":  upload_s,
        "meta_s":    meta_s,
        "total_s":   time.perf_counter() - t0_total,
    }


# ───────────────────────────────────────────────────────────────
# SECTION 1: Write Throughput
# ───────────────────────────────────────────────────────────────

banner("SECTION 1: Write Throughput")

wname = f"bm_write_{uuid.uuid4().hex[:6]}"
schema_w = Schema(1, [
    Column(1, "id",    "long",   True),
    Column(2, "price", "double", True),
])
new_table(wname, schema_w, store)

BATCHES     = 10
BATCH_SIZE  = 10_000
TOTAL_ROWS  = BATCHES * BATCH_SIZE
batch_times: List[Dict] = []

print(f"Writing {BATCHES} batches × {BATCH_SIZE:,} rows = {TOTAL_ROWS:,} rows total…")
print(f"  (prices stratified: batch N has prices in [N*200, N*200+200])")

for b in range(BATCHES):
    t = Table(wname, store)
    # Each batch has a distinct, NON-OVERLAPPING price range so column-stat skipping can work.
    # Batch 0: 0–200, Batch 1: 200–400 ... Batch 7: 1400–1600, Batch 8: 1600–1800, Batch 9: 1800–2000
    lo, hi = b * 200, b * 200 + 200
    rows = [{"id": b * BATCH_SIZE + i, "price": random.uniform(lo, hi)} for i in range(BATCH_SIZE)]
    with silence():
        timing = timed_append(t, rows)
    batch_times.append(timing)
    print(f"  Batch {b+1:2d} (price {lo}–{hi}): total={timing['total_s']*1000:.0f}ms  "
          f"parquet={timing['parquet_s']*1000:.0f}ms  "
          f"upload={timing['upload_s']*1000:.0f}ms  "
          f"meta={timing['meta_s']*1000:.0f}ms")

total_write_s  = sum(d["total_s"]  for d in batch_times)
total_pq_s     = sum(d["parquet_s"]for d in batch_times)
total_up_s     = sum(d["upload_s"] for d in batch_times)
total_meta_s   = sum(d["meta_s"]   for d in batch_times)

print(f"\nTotal: {total_write_s:.3f}s | {TOTAL_ROWS/total_write_s:,.0f} rows/s")
print(f"  Parquet write : {total_pq_s:.3f}s ({pct(total_pq_s,total_write_s):.0f}%)")
print(f"  MinIO upload  : {total_up_s:.3f}s ({pct(total_up_s,total_write_s):.0f}%)")
print(f"  Metadata      : {total_meta_s:.3f}s ({pct(total_meta_s,total_write_s):.0f}%)")

write_results = {
    "total_s": total_write_s,
    "rows_per_s": TOTAL_ROWS / total_write_s,
    "pct_parquet": pct(total_pq_s, total_write_s),
    "pct_upload":  pct(total_up_s, total_write_s),
    "pct_meta":    pct(total_meta_s, total_write_s),
}


# ───────────────────────────────────────────────────────────────
# SECTION 2 & 3: Read Without vs With File Skipping
# ───────────────────────────────────────────────────────────────

banner("SECTION 2: Read Without Pruning")

# Reload the same write table
t = Table(wname, store)
total_files = BATCHES

t0 = time.perf_counter()
with capture_scan() as (opened2, skipped2):
    rows2 = t.query(f"SELECT * FROM {wname}")
read_full_s = time.perf_counter() - t0

print(f"Files opened : {len(opened2)}  skipped: {len(skipped2)}")
print(f"Rows returned: {len(rows2):,}")
print(f"Query time   : {read_full_s:.3f}s")


banner("SECTION 3: Read With File Skipping (price > 1500)")

t = Table(wname, store)
t0 = time.perf_counter()
with capture_scan() as (opened3, skipped3):
    rows3 = t.query(f"SELECT * FROM {wname} WHERE price > 1500")
read_skip_s = time.perf_counter() - t0

skipped_pct3 = pct(len(skipped3), total_files)
speedup3 = read_full_s / read_skip_s if read_skip_s > 0 else float("inf")
print(f"Files opened : {len(opened3)}  skipped: {len(skipped3)}")
print(f"Rows returned: {len(rows3):,}")
print(f"Query time   : {read_skip_s:.3f}s")
print(f"→ Skipped {skipped_pct3:.0f}% of files, query ran {speedup3:.1f}× faster")

skip_results = {
    "full_s": read_full_s,
    "skip_s": read_skip_s,
    "speedup": speedup3,
    "pct_skipped": skipped_pct3,
    "opened": len(opened3),
    "skipped": len(skipped3),
}


# ───────────────────────────────────────────────────────────────
# SECTION 4: Partition Pruning
# ───────────────────────────────────────────────────────────────

banner("SECTION 4: Partition Pruning (30 days)")

DAYS = 30
pname = f"bm_part_{uuid.uuid4().hex[:6]}"
schema_p = Schema(1, [
    Column(1, "id",    "long",   True),
    Column(2, "price", "double", True),
    Column(3, "day",   "string", True),
])
new_table(pname, schema_p, store)

# Add identity partition on 'day'
tp = Table(pname, store)
tp.metadata.partition_specs = [
    PartitionSpec(1, [PartitionField(field_id=3, name="day", transform="identity")])
]
tp.metadata.current_spec_id = 1
tp._commit_schema(schema_p)

base_day = datetime(2024, 1, 1, tzinfo=timezone.utc)
ROWS_PER_DAY = 1000
print(f"Appending {DAYS} days × {ROWS_PER_DAY:,} rows…")

for d in range(DAYS):
    day_str = (base_day + timedelta(days=d)).strftime("%Y-%m-%d")
    tp = Table(pname, store)
    batch = [{"id": d * ROWS_PER_DAY + i, "price": random.uniform(10, 100), "day": day_str}
             for i in range(ROWS_PER_DAY)]
    with silence():
        tp.append(batch)

TARGET_DAY = "2024-01-15"
tp = Table(pname, store)

# --- Without partition pruning (baseline: stat-only on 'price') ---
t0 = time.perf_counter()
with capture_scan() as (opened4b, skipped4b):
    _ = tp.query(f"SELECT * FROM {pname} WHERE price > 0")  # all pass stats
read_nopart_s = time.perf_counter() - t0

# --- With partition pruning ---
t0 = time.perf_counter()
with capture_scan() as (opened4, skipped4):
    rows4 = tp.query(f"SELECT * FROM {pname} WHERE day = '{TARGET_DAY}'")
read_part_s = time.perf_counter() - t0

total_part_files = DAYS
skipped_pct4 = pct(len(skipped4), total_part_files)
speedup4 = read_nopart_s / read_part_s if read_part_s > 0 else float("inf")
print(f"Target day: {TARGET_DAY}")
print(f"Files opened : {len(opened4)}  skipped: {len(skipped4)}")
print(f"Rows returned: {len(rows4):,}")
print(f"Query time   : {read_part_s:.3f}s")
print(f"→ Skipped {skipped_pct4:.0f}% of files, {speedup4:.1f}× faster than full scan")

partition_results = {
    "nopart_s": read_nopart_s,
    "part_s":   read_part_s,
    "speedup":  speedup4,
    "pct_skipped": skipped_pct4,
    "opened":   len(opened4),
    "skipped":  len(skipped4),
}


# ───────────────────────────────────────────────────────────────
# SECTION 5: Time Travel Overhead
# ───────────────────────────────────────────────────────────────

banner("SECTION 5: Time Travel Overhead")

# Use the write table with 10 snapshots.
# We compare: query on CURRENT snapshot (all 10 files) with as_of= the current
# snapshot's own timestamp — both should open the same 10 files; the only
# difference is the metadata lookup path.
t = Table(wname, store)
current_snap_ts = t.metadata.snapshot_log[-1]["timestamp_ms"]
oldest_snap_ts  = t.metadata.snapshot_log[0]["timestamp_ms"]

def median_time(fn, reps=3) -> Tuple[float, Any]:
    times = []
    result = None
    for _ in range(reps):
        t0 = time.perf_counter()
        result = fn()
        times.append(time.perf_counter() - t0)
    times.sort()
    return times[len(times)//2], result

t = Table(wname, store)
with silence():
    current_s, rows_cur = median_time(
        lambda: t.query(f"SELECT COUNT(*) AS cnt FROM {wname}")
    )

t = Table(wname, store)
with silence():
    # as_of current timestamp → same 10 files, but goes through snapshot_at() lookup
    tt_s, rows_tt = median_time(
        lambda: t.query(f"SELECT COUNT(*) AS cnt FROM {wname}", as_of=current_snap_ts)
    )

# Also measure the oldest snapshot (1 file) for row-count contrast display
t = Table(wname, store)
with silence():
    oldest_s, rows_oldest = median_time(
        lambda: t.query(f"SELECT COUNT(*) AS cnt FROM {wname}", as_of=oldest_snap_ts)
    )

overhead_ms = (tt_s - current_s) * 1000
print(f"Current snapshot  (no as_of)              : {current_s*1000:.1f}ms  → {rows_cur[0]['cnt']:,} rows (10 files)")
print(f"Same snapshot  (as_of=current timestamp)  : {tt_s*1000:.1f}ms  → {rows_tt[0]['cnt']:,} rows (10 files)")
print(f"Oldest snapshot   (as_of=oldest, 1 file)  : {oldest_s*1000:.1f}ms  → {rows_oldest[0]['cnt']:,} rows (1 file)")
print(f"Time travel overhead (same files, diff path): {overhead_ms:+.1f}ms")

tt_results = {
    "current_ms": current_s * 1000,
    "travel_ms":  tt_s * 1000,
    "overhead_ms": overhead_ms,
    "oldest_ms":   oldest_s * 1000,
    "oldest_rows":  rows_oldest[0]['cnt'],
}


# ───────────────────────────────────────────────────────────────
# Final Summary Table (README-ready)
# ───────────────────────────────────────────────────────────────

print("\n")
print("=" * 62)
print("  BENCHMARK SUMMARY")
print("=" * 62)

print(f"""
┌─────────────────────────────────────────────────────────────┐
│  1. Write Throughput  ({TOTAL_ROWS:,} rows, {BATCHES} batches)
│─────────────────────────────────────────────────────────────│
│  Total time      : {write_results['total_s']:.3f}s
│  Throughput      : {write_results['rows_per_s']:,.0f} rows/s
│  Phase breakdown :
│    Parquet write : {write_results['pct_parquet']:.0f}% of wall-clock
│    MinIO upload  : {write_results['pct_upload']:.0f}% of wall-clock
│    Metadata      : {write_results['pct_meta']:.0f}% of wall-clock
├─────────────────────────────────────────────────────────────┤
│  2. Full Scan (no filter)
│─────────────────────────────────────────────────────────────│
│  Files opened : {total_files}   rows: {len(rows2):,}   time: {read_full_s:.3f}s
├─────────────────────────────────────────────────────────────┤
│  3. Column-stat File Skipping  (price > 1500)
│─────────────────────────────────────────────────────────────│
│  Files opened : {skip_results['opened']}   skipped: {skip_results['skipped']}   ({skip_results['pct_skipped']:.0f}% of files)
│  Rows returned: {len(rows3):,}
│  Query time   : {skip_results['skip_s']:.3f}s   ({skip_results['speedup']:.1f}× faster than full scan)
├─────────────────────────────────────────────────────────────┤
│  4. Partition Pruning  (day = '{TARGET_DAY}',  {DAYS} days)
│─────────────────────────────────────────────────────────────│
│  Files opened : {partition_results['opened']}    skipped: {partition_results['skipped']}   ({partition_results['pct_skipped']:.0f}% of files)
│  Rows returned: {len(rows4):,}
│  Query time   : {partition_results['part_s']:.3f}s   ({partition_results['speedup']:.1f}× faster than full scan)
├─────────────────────────────────────────────────────────────┤
│  5. Time Travel Overhead
│─────────────────────────────────────────────────────────────│
│  Current (direct)            : {tt_results['current_ms']:.1f}ms  (10 files, 100k rows)
│  Same snapshot (via as_of=)  : {tt_results['travel_ms']:.1f}ms  (10 files, 100k rows)
│  Time travel overhead        : {tt_results['overhead_ms']:+.1f}ms  (snapshot_log lookup only)
│  Oldest snapshot (1 file)    : {tt_results['oldest_ms']:.1f}ms  ({tt_results['oldest_rows']:,} rows, proportional scan)
└─────────────────────────────────────────────────────────────┘
""")
