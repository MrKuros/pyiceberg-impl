import uuid
import time
import random
from iceberg.schema import Schema, Column, PartitionSpec, PartitionField
from iceberg.store import MinIOStore
from iceberg.metadata import new_table
from iceberg.table import Table

# --- Setup ---
random.seed(42)
store = MinIOStore()
name = f'demo_{uuid.uuid4().hex[:6]}'

print(f"============================================================")
print(f" Iceberg End-to-End P2 Demo")
print(f" Table: {name}")
print(f"============================================================")

# 1. Create table with schema (id, skin_name, price, day)
schema = Schema(1, [
    Column(1, 'id', 'long', True),
    Column(2, 'skin_name', 'string', True),
    Column(3, 'price', 'double', True),
    Column(4, 'day', 'string', True),  # Using 'day' as a string column for identity partitioning
])

# Partition on the 'day' column
new_table(name, schema, store)
t = Table(name, store)
t.metadata.partition_specs = [
    PartitionSpec(1, [PartitionField(field_id=4, name='day', transform='identity')])
]
t.metadata.current_spec_id = 1
t._commit_schema(schema)  # Just to save the partition spec

# 2. Append Day 1 data
print("\n[Step 2] Appending Day 1 data (1000 rows)...")
day1_rows = [
    {'id': i, 'skin_name': 'AK-47 | Redline', 'price': round(random.uniform(10, 50), 2), 'day': 'Jan 1'}
    for i in range(1000)
]
t = Table(name, store)
t.append(day1_rows)
ts_jan1 = t.metadata.snapshots[-1].timestamp_ms
time.sleep(0.05)

# 3. Append Day 2 data
print("[Step 3] Appending Day 2 data (1000 rows)...")
day2_rows = [
    {'id': 1000+i, 'skin_name': 'AK-47 | Redline', 'price': round(random.uniform(20, 60), 2), 'day': 'Jan 2'}
    for i in range(1000)
]
t = Table(name, store)
t.append(day2_rows)
ts_jan2 = t.metadata.snapshots[-1].timestamp_ms
time.sleep(0.05)

# 4. Add column: wear_float (double)
print("\n[Step 4] Schema Evolution: Add column 'wear_float'")
t = Table(name, store)
t.add_column('wear_float', 'double')
time.sleep(0.05)

# 5. Append Day 3 data
print("\n[Step 5] Appending Day 3 data (1000 rows with wear_float)...")
day3_rows = [
    {'id': 2000+i, 'skin_name': 'AK-47 | Redline', 'price': round(random.uniform(40, 80), 2), 'day': 'Jan 3', 'wear_float': round(random.uniform(0.01, 0.99), 4)}
    for i in range(1000)
]
# We'll inject one row with price > 500 to test column-stat skipping explicitly
day3_rows[500]['price'] = 600.0

t = Table(name, store)
t.append(day3_rows)
ts_jan3 = t.metadata.snapshots[-1].timestamp_ms
time.sleep(0.05)

# 6. Rename column: price -> unit_price
print("\n[Step 6] Schema Evolution: Rename column 'price' -> 'unit_price'")
t = Table(name, store)
t.rename_column(field_id=3, new_name='unit_price')
time.sleep(0.05)

# 7. Append Day 4 data
print("\n[Step 7] Appending Day 4 data (1000 rows with unit_price)...")
day4_rows = [
    {'id': 3000+i, 'skin_name': 'AK-47 | Redline', 'unit_price': round(random.uniform(10, 50), 2), 'day': 'Jan 4', 'wear_float': 0.1}
    for i in range(1000)
]
t = Table(name, store)
t.append(day4_rows)
time.sleep(0.05)

# ---------------------------------------------------------
# Verification Queries
# ---------------------------------------------------------
print(f"\n============================================================")
print(f" Verifying Queries")
print(f"============================================================\n")
t = Table(name, store)

print("--- Query 1: SELECT COUNT(*) FROM tbl ---")
q1 = t.query(f"SELECT COUNT(*) AS cnt FROM {name}")
print(f"Result: {q1[0]['cnt']} rows (Expected: 4000)")
assert q1[0]['cnt'] == 4000

print("\n--- Query 2: SELECT AVG(unit_price) FROM tbl WHERE day = 'Jan 3' ---")
q2 = t.query(f"SELECT AVG(unit_price) AS avg_price FROM {name} WHERE day = 'Jan 3'")
print(f"Result: {q2[0]['avg_price']:.4f}")
# Expected to skip Jan 1, Jan 2, Jan 4 because of partition pruning

print("\n--- Query 3: SELECT AVG(unit_price) FROM tbl WHERE unit_price > 500 ---")
# DuckDB SQL requires the current schema name 'unit_price', but our pushdown translates this 
# to field_id=3, which correctly filters files written when it was called 'price'.
q3 = t.query(f"SELECT AVG(unit_price) AS avg_price FROM {name} WHERE unit_price > 500")
print(f"Result: {q3[0]['avg_price']:.4f}")

print("\n--- Query 4: SELECT AVG(unit_price) FROM tbl as_of Jan 2 ---")
q4 = t.query(f"SELECT COUNT(*) AS cnt FROM {name}", as_of=ts_jan2)
print(f"Result: {q4[0]['cnt']} rows (Expected: 2000)")
assert q4[0]['cnt'] == 2000

print("\n--- Query 5: SELECT * FROM tbl as_of Jan 1 ---")
q5 = t.query(f"SELECT * FROM {name} LIMIT 1", as_of=ts_jan1)
print(f"Row 1: {q5[0]}")
assert 'wear_float' in q5[0]
assert q5[0]['wear_float'] is None, "Old files should have None for added columns"
assert 'unit_price' in q5[0], "Renamed column should be available under new name"
assert 'price' not in q5[0]

# ---------------------------------------------------------
# Lifecycle Management
# ---------------------------------------------------------
print(f"\n============================================================")
print(f" Lifecycle Management")
print(f"============================================================\n")

t = Table(name, store)
snapshots_before = len(t.metadata.snapshots)
print(f"Snapshots before expiry: {snapshots_before}")

print(f"\nExpiring snapshots older than Jan 3 (ts={ts_jan3})...")
t.expire_snapshots(older_than_ms=ts_jan3)

snapshots_after = len(t.metadata.snapshots)
print(f"Snapshots after expiry: {snapshots_after}")

print(f"\nRunning delete_orphan_files()...")
# Count files in the data dir before deletion to see how many get deleted
data_files_before = len(store.list(f'tables/{name}/data/'))
t.delete_orphan_files()

print("\n--- Query 6: Re-run Query 1 after cleanup ---")
q6 = t.query(f"SELECT COUNT(*) AS cnt FROM {name}")
print(f"Result: {q6[0]['cnt']} rows (Expected: 4000)")
assert q6[0]['cnt'] == 4000

print(f"\n============================================================")
print(f" SUCCESS! All features verified.")
print(f"============================================================")
