#!/usr/bin/env python3
"""
Link RRC Texas wells to EDGAR-parent operators in Supabase.
source_record_id in Supabase = col 27 (0-indexed) in the CSV (big numeric ID).
col 11: operator name, col 12: RRC operator code/number
"""

import csv
import json
import sys
import urllib.request
import urllib.parse
from collections import defaultdict

SUPABASE_URL = "https://temtptsfiksixxhbigkg.supabase.co"
SUPABASE_KEY = "os.environ.get('SUPABASE_KEY')"
CSV_PATH = "/home/openclaw/.openclaw/workspace/varro/data/raw/rrc_texas/wellbore_data/OG_WELLBORE_EWA_Report.csv"

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation",
}

EDGAR_PATTERNS = {
    "ExxonMobil": ["exxon", "mobil"],
    "Shell": ["shell oil", "shell usa", "shell exploration"],
    "Chevron": ["chevron", "texaco", "gulf oil"],
    "BP": ["bp america", "bp exploration", "amoco", "atlantic richfield", "arco"],
    "ConocoPhillips": ["conoco", "phillips petroleum", "burlington resources"],
    "Occidental": ["occidental", "anadarko", "oxy permian", "oxy usa"],
    "Devon": ["devon energy", "devon gas"],
    "Pioneer": ["pioneer natural", "pioneer petroleum", "mesa petroleum"],
    "TotalEnergies": ["total e&p", "totalenergies"],
    "Equinor": ["equinor", "statoil"],
}

# Pre-build flat lookup: pattern_lower -> edgar_parent
PATTERN_MAP = {}
for parent, patterns in EDGAR_PATTERNS.items():
    for p in patterns:
        PATTERN_MAP[p.lower()] = parent


def match_operator(op_name_lower):
    for pattern, parent in PATTERN_MAP.items():
        if pattern in op_name_lower:
            return parent
    return None


def supabase_get(path):
    url = f"{SUPABASE_URL}/rest/v1/{path}"
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def supabase_post(path, data):
    url = f"{SUPABASE_URL}/rest/v1/{path}"
    body = json.dumps(data).encode()
    req = urllib.request.Request(url, data=body, headers=HEADERS, method="POST")
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        err_body = e.read().decode()
        raise Exception(f"HTTP {e.code}: {err_body}")


def supabase_patch(path, data):
    url = f"{SUPABASE_URL}/rest/v1/{path}"
    body = json.dumps(data).encode()
    h = dict(HEADERS)
    h["Prefer"] = "return=representation"
    req = urllib.request.Request(url, data=body, headers=h, method="PATCH")
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        err_body = e.read().decode()
        raise Exception(f"HTTP {e.code}: {err_body}")


# ── STEP 1+2: Stream CSV, collect EDGAR matches ──────────────────────────────
print("=== Step 1+2: Streaming CSV for EDGAR operator matches ===")
sys.stdout.flush()

# edgar_matches[parent][op_name] = [list of source_record_ids]
edgar_matches = defaultdict(lambda: defaultdict(list))
# op_name -> first-seen RRC operator code (col 12)
op_rrc_codes = {}
# unique operator counts (all)
all_op_counts = defaultdict(int)

row_count = 0
match_count = 0

with open(CSV_PATH, encoding="utf-8", errors="replace") as f:
    reader = csv.reader(f)
    # No header row — first line is data
    for row in reader:
        row_count += 1
        if row_count % 100000 == 0:
            print(f"  ... processed {row_count:,} rows, {match_count:,} EDGAR matches so far")
            sys.stdout.flush()

        if len(row) < 28:
            continue

        source_id = row[27].strip().strip('"')  # col 27: source_record_id
        op_name = row[11].strip().strip('"')    # col 11: operator name
        op_code = row[12].strip().strip('"')    # col 12: RRC operator code

        if not op_name or not source_id:
            continue

        all_op_counts[op_name] += 1

        parent = match_operator(op_name.lower())
        if parent:
            edgar_matches[parent][op_name].append(source_id)
            match_count += 1
            if op_name not in op_rrc_codes and op_code:
                op_rrc_codes[op_name] = op_code

print(f"\nTotal rows processed: {row_count:,}")
print(f"Unique operators total: {len(all_op_counts):,}")
print(f"EDGAR-matched well rows: {match_count:,}")
print()

# Summary by parent
print("=== EDGAR match summary ===")
for parent in sorted(edgar_matches.keys()):
    ops = edgar_matches[parent]
    well_count = sum(len(ids) for ids in ops.values())
    print(f"  {parent}: {len(ops)} operator variants, {well_count:,} wells")

print()
sys.stdout.flush()

# ── STEP 3: Get or create operator records ────────────────────────────────────
print("=== Step 3: Get/create operator records in Supabase ===")

# Build flat map: op_name -> edgar_parent
op_to_parent = {}
for parent, ops in edgar_matches.items():
    for op_name in ops:
        op_to_parent[op_name] = parent

op_uuid_map = {}  # op_name -> uuid

for op_name in sorted(op_to_parent.keys()):
    encoded = urllib.parse.quote(op_name, safe="")
    existing = supabase_get(f"operators?name=eq.{encoded}&select=id,name")
    if existing:
        uuid = existing[0]["id"]
        print(f"  EXISTS: '{op_name}' -> {uuid}")
    else:
        rrc_code = op_rrc_codes.get(op_name)
        create_data = {"name": op_name, "country": "US"}
        if rrc_code:
            create_data["operator_code"] = rrc_code
        try:
            created = supabase_post("operators", create_data)
            uuid = created[0]["id"] if created else None
            print(f"  CREATED: '{op_name}' (rrc_code={rrc_code}) -> {uuid}")
        except Exception as e:
            # operator_code may conflict (unique constraint) — try without it
            if "operator_code" in str(e):
                try:
                    created = supabase_post("operators", {"name": op_name, "country": "US"})
                    uuid = created[0]["id"] if created else None
                    print(f"  CREATED (no code): '{op_name}' -> {uuid}")
                except Exception as e2:
                    print(f"  ERROR creating '{op_name}': {e2}")
                    uuid = None
            else:
                print(f"  ERROR creating '{op_name}': {e}")
                uuid = None

    if uuid:
        op_uuid_map[op_name] = uuid

print(f"\nOperator records ready: {len(op_uuid_map)}")
print()
sys.stdout.flush()

# ── STEP 4: Update wells in Supabase ─────────────────────────────────────────
print("=== Step 4: Updating wells in Supabase ===")

total_updated = 0
total_skipped = 0
BATCH_SIZE = 100

for op_name in sorted(op_uuid_map.keys()):
    uuid = op_uuid_map[op_name]
    parent = op_to_parent[op_name]
    source_ids = edgar_matches[parent][op_name]

    updated_for_op = 0
    errors_for_op = 0
    for i in range(0, len(source_ids), BATCH_SIZE):
        batch = source_ids[i:i + BATCH_SIZE]
        ids_str = ",".join(batch)

        try:
            result = supabase_patch(
                f"wells?source_record_id=in.({ids_str})&data_source=eq.RRC_TEXAS",
                {"operator_id": uuid}
            )
            updated_for_op += len(result) if result else 0
        except Exception as e:
            errors_for_op += len(batch)
            total_skipped += len(batch)
            if i == 0:  # Only print first error per operator
                print(f"  ERROR batch for '{op_name}': {e}")
            continue

    total_updated += updated_for_op
    print(f"  {parent} | '{op_name}': {updated_for_op:,}/{len(source_ids):,} wells updated"
          + (f" ({errors_for_op} errors)" if errors_for_op else ""))
    sys.stdout.flush()

print()
print("=== FINAL REPORT ===")
print(f"Unique operators in CSV: {len(all_op_counts):,}")
print(f"EDGAR-matched operator variants: {len(op_uuid_map):,}")
print(f"Total wells updated: {total_updated:,}")
print(f"Total wells skipped (errors): {total_skipped:,}")
print()
print("Top 10 non-EDGAR operators by well count:")
non_edgar = {k: v for k, v in all_op_counts.items() if k not in op_to_parent}
for name, count in sorted(non_edgar.items(), key=lambda x: -x[1])[:10]:
    print(f"  '{name}': {count:,}")
