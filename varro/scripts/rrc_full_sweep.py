#!/usr/bin/env python3
"""
RRC Texas full sweep - link ALL wells to operators
Steps: 1) Build lookup from CSV, 2) Load existing ops, 3) Create missing ops, 4) Update wells
"""
import csv, json, requests, time
from collections import defaultdict

SUPABASE_URL = "https://temtptsfiksixxhbigkg.supabase.co"
SUPABASE_KEY = "os.environ.get('SUPABASE_KEY')"
H = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal"
}
CSV_PATH = '/home/openclaw/.openclaw/workspace/varro/data/raw/rrc_texas/wellbore_data/OG_WELLBORE_EWA_Report.csv'
LOOKUP_PATH = '/home/openclaw/.openclaw/workspace/varro/data/rrc_full_op_lookup.json'

# ── Step 1: Build operator lookup from CSV ─────────────────────────────────
print("=== Step 1: Streaming CSV to build operator lookup ===")
src_id_by_op = defaultdict(list)

with open(CSV_PATH, encoding='utf-8', errors='replace') as f:
    reader = csv.reader(f)
    next(reader)  # skip header
    row_count = 0
    for row in reader:
        row_count += 1
        if row_count % 200000 == 0:
            print(f"  Scanned {row_count:,} rows, {len(src_id_by_op):,} unique operators so far...")
        if len(row) <= 27:
            continue
        src_id = row[27].strip().strip('"')
        op_name = row[11].strip().strip('"')
        if op_name and src_id:
            src_id_by_op[op_name].append(src_id)

print(f"Unique operators: {len(src_id_by_op):,}")
total_wells = sum(len(v) for v in src_id_by_op.values())
print(f"Total wells mapped: {total_wells:,}")

with open(LOOKUP_PATH, 'w') as f:
    json.dump(src_id_by_op, f)
print("Saved lookup to disk.")

# ── Step 2: Load existing operators from Supabase ─────────────────────────
print("\n=== Step 2: Loading existing operators from Supabase ===")
op_name_to_id = {}
offset = 0
while True:
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/operators",
        headers=H,
        params={"select": "id,name", "limit": "1000", "offset": str(offset)}
    )
    rows = r.json()
    if not rows:
        break
    for row in rows:
        op_name_to_id[row['name'].strip()] = row['id']
    offset += len(rows)
    if len(rows) < 1000:
        break
print(f"Loaded {len(op_name_to_id):,} existing operators")

# ── Step 3: Create missing operators ──────────────────────────────────────
print("\n=== Step 3: Creating missing operators ===")
missing = [n for n in src_id_by_op.keys() if n not in op_name_to_id]
print(f"New operators to create: {len(missing):,}")

for i in range(0, len(missing), 200):
    batch = [{"name": n} for n in missing[i:i+200]]
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/operators",
        headers={**H, "Prefer": "resolution=merge-duplicates,return=representation"},
        json=batch
    )
    if r.status_code in (200, 201):
        for row in r.json():
            op_name_to_id[row['name'].strip()] = row['id']
    else:
        print(f"Error creating batch {i//200}: {r.status_code} {r.text[:200]}")
    time.sleep(0.1)
    if (i // 200) % 10 == 0 and i > 0:
        print(f"  Created batches up to {i+200}, total ops now: {len(op_name_to_id):,}")

print(f"Total operators now: {len(op_name_to_id):,}")

# ── Step 4: Update wells — operator by operator ────────────────────────────
print("\n=== Step 4: Updating wells with operator_id ===")
with open(LOOKUP_PATH) as f:
    src_id_by_op = json.load(f)

updated = 0
errors = 0
last_milestone = 0

for op_idx, (op_name, src_ids) in enumerate(src_id_by_op.items()):
    op_uuid = op_name_to_id.get(op_name)
    if not op_uuid:
        continue

    for i in range(0, len(src_ids), 100):
        chunk = src_ids[i:i+100]
        ids_str = ','.join(chunk)

        r = requests.patch(
            f"{SUPABASE_URL}/rest/v1/wells",
            headers=H,
            params={
                "source_record_id": f"in.({ids_str})",
                "data_source": "eq.RRC_TEXAS"
            },
            json={"operator_id": op_uuid}
        )
        if r.status_code in (200, 204):
            updated += len(chunk)
        else:
            errors += 1
            if errors <= 5:
                print(f"PATCH error: {r.status_code} {r.text[:150]}")
        time.sleep(0.03)

    milestone = (updated // 50000) * 50000
    if milestone > last_milestone and updated > 0:
        last_milestone = milestone
        print(f"Progress: {updated:,} well-slots updated (op {op_idx:,}/{len(src_id_by_op):,})")

print(f"\n=== DONE ===")
print(f"Final: {updated:,} updated, {errors} errors")
