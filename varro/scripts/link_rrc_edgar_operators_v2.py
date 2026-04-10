#!/usr/bin/env python3
"""
Link RRC Texas wells to EDGAR-parent operators in Supabase.
api_number = "42" + row[2] + "00" (row[2] is 8-digit RRC well code)
Uses api_number index for fast updates (no data_source filter needed — api_numbers globally unique).
Batches of 400 for ~4s per request.
"""

import csv
import json
import sys
import time
import urllib.request
import urllib.parse
from collections import defaultdict

SUPABASE_URL = "https://temtptsfiksixxhbigkg.supabase.co"
SUPABASE_KEY = "os.environ.get('SUPABASE_KEY')"
CSV_PATH = "/home/openclaw/.openclaw/workspace/varro/data/raw/rrc_texas/wellbore_data/OG_WELLBORE_EWA_Report.csv"

HEADERS_READ = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}
HEADERS_WRITE = {
    **HEADERS_READ,
    "Prefer": "return=minimal",
}
HEADERS_REPR = {
    **HEADERS_READ,
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

PATTERN_MAP = {}
for parent, patterns in EDGAR_PATTERNS.items():
    for p in patterns:
        PATTERN_MAP[p.lower()] = parent


def match_operator(op_name_lower):
    for pattern, parent in PATTERN_MAP.items():
        if pattern in op_name_lower:
            return parent
    return None


def http_get(path):
    url = f"{SUPABASE_URL}/rest/v1/{path}"
    req = urllib.request.Request(url, headers=HEADERS_READ)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def http_post(path, data):
    url = f"{SUPABASE_URL}/rest/v1/{path}"
    body = json.dumps(data).encode()
    req = urllib.request.Request(url, data=body, headers=HEADERS_REPR, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        raise Exception(f"HTTP {e.code}: {e.read().decode()}")


def http_patch_minimal(path, data, retries=3):
    url = f"{SUPABASE_URL}/rest/v1/{path}"
    body = json.dumps(data).encode()
    req = urllib.request.Request(url, data=body, headers=HEADERS_WRITE, method="PATCH")
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                resp.read()
                return True
        except (urllib.error.HTTPError, urllib.error.URLError) as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            raise Exception(f"PATCH failed after {retries} attempts: {e}")


BATCH_SIZE = 400

# ── STEP 1+2: Stream CSV ──────────────────────────────────────────────────────
print("=== Step 1+2: Streaming CSV for EDGAR operator matches ===")
sys.stdout.flush()

# edgar_matches[parent][op_name] = [list of api_numbers]
edgar_matches = defaultdict(lambda: defaultdict(list))
op_rrc_codes = {}  # op_name -> first RRC operator code
all_op_counts = defaultdict(int)
bad_api_count = 0

row_count = 0
match_count = 0

with open(CSV_PATH, encoding="utf-8", errors="replace") as f:
    reader = csv.reader(f)
    for row in reader:
        row_count += 1
        if row_count % 200000 == 0:
            print(f"  ... {row_count:,} rows, {match_count:,} EDGAR matches")
            sys.stdout.flush()

        if len(row) < 13:
            continue

        rrc_code_col2 = row[2].strip().strip('"')  # 8-digit RRC code
        op_name = row[11].strip().strip('"')
        op_code = row[12].strip().strip('"')  # RRC operator number

        if not op_name or not rrc_code_col2:
            continue

        # Build api_number
        if len(rrc_code_col2) != 8:
            bad_api_count += 1
            continue
        api_number = "42" + rrc_code_col2 + "00"

        all_op_counts[op_name] += 1

        parent = match_operator(op_name.lower())
        if parent:
            edgar_matches[parent][op_name].append(api_number)
            match_count += 1
            if op_name not in op_rrc_codes and op_code:
                op_rrc_codes[op_name] = op_code

print(f"\nRows processed: {row_count:,} | Bad api col: {bad_api_count:,}")
print(f"Unique operators: {len(all_op_counts):,}")
print(f"EDGAR-matched wells: {match_count:,}")
print()

print("=== EDGAR match summary ===")
for parent in sorted(edgar_matches.keys()):
    ops = edgar_matches[parent]
    well_count = sum(len(ids) for ids in ops.values())
    print(f"  {parent}: {len(ops)} variants, {well_count:,} wells")
print()
sys.stdout.flush()

# ── STEP 3: Get or create operator records ────────────────────────────────────
print("=== Step 3: Get/create operator records in Supabase ===")
sys.stdout.flush()

op_to_parent = {}
for parent, ops in edgar_matches.items():
    for op_name in ops:
        op_to_parent[op_name] = parent

op_uuid_map = {}  # op_name -> uuid

for op_name in sorted(op_to_parent.keys()):
    encoded = urllib.parse.quote(op_name, safe="")
    existing = http_get(f"operators?name=eq.{encoded}&select=id,name")
    if existing:
        uuid = existing[0]["id"]
    else:
        rrc_code = op_rrc_codes.get(op_name)
        create_data = {"name": op_name, "country": "US"}
        if rrc_code:
            create_data["operator_code"] = rrc_code
        try:
            created = http_post("operators", create_data)
            uuid = created[0]["id"] if created else None
            print(f"  CREATED: '{op_name}' -> {uuid}")
        except Exception as e:
            if "operator_code" in str(e) or "duplicate" in str(e).lower():
                try:
                    created = http_post("operators", {"name": op_name, "country": "US"})
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

# ── STEP 4: Update wells ──────────────────────────────────────────────────────
print("=== Step 4: Updating wells in Supabase ===")
sys.stdout.flush()

total_batches = 0
total_errors = 0
stats_per_parent = defaultdict(lambda: {"ops": 0, "wells": 0, "errors": 0})

for op_name in sorted(op_uuid_map.keys()):
    uuid = op_uuid_map[op_name]
    parent = op_to_parent[op_name]
    api_numbers = edgar_matches[parent][op_name]
    well_count = len(api_numbers)

    errors_for_op = 0
    for i in range(0, well_count, BATCH_SIZE):
        batch = api_numbers[i:i + BATCH_SIZE]
        ids_str = ",".join(batch)
        try:
            http_patch_minimal(
                f"wells?api_number=in.({ids_str})",
                {"operator_id": uuid}
            )
            total_batches += 1
        except Exception as e:
            errors_for_op += len(batch)
            total_errors += len(batch)
            print(f"  ERR [{parent}] '{op_name}' batch {i//BATCH_SIZE}: {e}")

    stats_per_parent[parent]["ops"] += 1
    stats_per_parent[parent]["wells"] += well_count
    stats_per_parent[parent]["errors"] += errors_for_op

    status = "✓" if errors_for_op == 0 else f"⚠ ({errors_for_op} errors)"
    print(f"  [{parent:15s}] '{op_name}': {well_count:,} wells {status}")
    sys.stdout.flush()

print()
print("=== FINAL REPORT ===")
print(f"Unique operators in CSV:       {len(all_op_counts):,}")
print(f"EDGAR-matched variants:        {len(op_uuid_map):,}")
print(f"API calls (batches @ {BATCH_SIZE}):   {total_batches:,}")
print(f"Wells with errors:             {total_errors:,}")
print()
print("By EDGAR parent:")
total_wells_updated = 0
for parent in sorted(stats_per_parent.keys()):
    s = stats_per_parent[parent]
    updated = s["wells"] - s["errors"]
    total_wells_updated += updated
    print(f"  {parent:20s}: {s['ops']:3d} operators, {s['wells']:6,} wells, "
          f"{updated:6,} updated, {s['errors']:4d} errors")
print(f"\n  TOTAL WELLS UPDATED: {total_wells_updated:,}")
print()
print("Top 10 non-EDGAR operators (potential future targets):")
non_edgar = {k: v for k, v in all_op_counts.items() if k not in op_to_parent}
for name, count in sorted(non_edgar.items(), key=lambda x: -x[1])[:10]:
    print(f"  '{name}': {count:,} wells")
