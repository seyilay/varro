#!/usr/bin/env python3
"""
Varro Job Queue Runner
Waits for Supabase to come back up, then runs all pending jobs in order.
Rate-limited: 100ms sleep between batches, never more than 1 bulk writer at a time.
"""
import requests, time, json, csv, sys, os
from collections import defaultdict

SUPABASE_URL = "https://temtptsfiksixxhbigkg.supabase.co"
SUPABASE_KEY = "os.environ.get('SUPABASE_KEY')"
H = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal"
}
BATCH_SLEEP = 0.1   # 100ms between batches
BATCH_SIZE = 500

def wait_for_db(max_wait=600):
    print("Waiting for Supabase to come back online...")
    start = time.time()
    while time.time() - start < max_wait:
        try:
            r = requests.get(f"{SUPABASE_URL}/rest/v1/operators?limit=1",
                headers=H, timeout=15)
            if r.status_code in (200, 201):
                print(f"✅ DB is up! ({int(time.time()-start)}s wait)")
                return True
        except Exception:
            pass
        print(f"  Still waiting... ({int(time.time()-start)}s)")
        time.sleep(15)
    return False

def load_operators():
    op_map = {}
    offset = 0
    while True:
        r = requests.get(f"{SUPABASE_URL}/rest/v1/operators",
            headers=H, params={"select": "id,name", "limit": "1000", "offset": str(offset)},
            timeout=60)
        rows = r.json()
        if not rows or isinstance(rows, dict): break
        for row in rows:
            op_map[row['name'].strip()] = row['id']
        offset += len(rows)
        if len(rows) < 1000: break
    print(f"  Loaded {len(op_map)} operators")
    return op_map

def get_or_create(name, op_map):
    if not name or not name.strip(): return None
    name = name.strip()
    if name in op_map: return op_map[name]
    try:
        r = requests.post(f"{SUPABASE_URL}/rest/v1/operators",
            headers={**H, "Prefer": "resolution=merge-duplicates,return=representation"},
            json=[{"name": name}], timeout=20)
        if r.status_code in (200, 201) and r.json():
            uid = r.json()[0]['id']
            op_map[name] = uid
            return uid
    except Exception as e:
        print(f"  Operator create error: {e}")
    return None

def upsert_wells(batch, op_map):
    if not batch: return 0
    try:
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/wells?on_conflict=api_number",
            headers={**H, "Prefer": "resolution=merge-duplicates,return=minimal"},
            json=batch, timeout=60)
        return len(batch) if r.status_code in (200, 201, 204) else 0
    except Exception as e:
        print(f"  Upsert error: {e}")
        return 0

# ─────────────────────────────────────────────
# JOB 1: Create indexes
# ─────────────────────────────────────────────
def job_indexes():
    print("\n" + "="*60)
    print("JOB 1: Creating indexes via Supabase RPC")
    print("="*60)
    # Note: DDL requires SQL editor — print the SQL to run manually
    sql = """
-- Run these in Supabase SQL editor: https://supabase.com/dashboard/project/temtptsfiksixxhbigkg/sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_wells_operator_id ON wells(operator_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_wells_data_source ON wells(data_source);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_wells_api_number ON wells(api_number);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_wells_source_record_id ON wells(source_record_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_wells_regulatory_jurisdiction ON wells(regulatory_jurisdiction);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_wells_operator_state ON wells(operator_id, state);
"""
    index_path = "/home/openclaw/.openclaw/workspace/varro/data/create_indexes.sql"
    with open(index_path, "w") as f:
        f.write(sql)
    print(f"  ⚠️  Index SQL written to {index_path}")
    print("  ⚠️  Must be run manually in Supabase SQL editor (DDL not allowed via REST)")
    print("  Continuing with data ingests...")

# ─────────────────────────────────────────────
# JOB 2: Alberta AER (659,429 wells)
# ─────────────────────────────────────────────
def job_alberta(op_map):
    print("\n" + "="*60)
    print("JOB 2: Alberta AER (659,429 wells)")
    print("="*60)

    raw_dir = "/home/openclaw/.openclaw/workspace/varro/data/raw/aer_alberta"
    ba_map_path = os.path.join(raw_dir, "ba_code_map.json")
    well_file = os.path.join(raw_dir, "AppData/VAR/VAR3500Report/TXT/2026/WellList.txt")

    if not os.path.exists(well_file):
        # Try finding it
        for root, dirs, files in os.walk(raw_dir):
            for f in files:
                if f.endswith('.txt') and 'Well' in f:
                    well_file = os.path.join(root, f)
                    break

    if not os.path.exists(well_file):
        print(f"  ❌ Well file not found in {raw_dir}")
        return 0

    # Load BA code map
    ba_map = {}
    if os.path.exists(ba_map_path):
        with open(ba_map_path) as f:
            ba_map = json.load(f)
    print(f"  BA codes loaded: {len(ba_map)}")

    # Pre-register all Alberta operators
    print("  Creating Alberta operators...")
    alberta_ops = list(set(ba_map.values()))
    for i in range(0, len(alberta_ops), 200):
        batch_ops = [{"name": n} for n in alberta_ops[i:i+200] if n]
        if batch_ops:
            r = requests.post(f"{SUPABASE_URL}/rest/v1/operators",
                headers={**H, "Prefer": "resolution=merge-duplicates,return=representation"},
                json=batch_ops, timeout=60)
            if r.status_code in (200, 201):
                for row in r.json():
                    op_map[row['name'].strip()] = row['id']
        time.sleep(BATCH_SLEEP)
    print(f"  Operators now: {len(op_map)}")

    batch, total, errors = [], 0, 0
    with open(well_file, encoding='utf-8', errors='replace') as f:
        for line in f:
            parts = line.rstrip('\n').split('\t')
            if len(parts) < 19: continue
            uwi = parts[0].strip()
            ba_code = parts[4].strip()
            status_raw = parts[9].strip()
            spud_raw = parts[10].strip()
            depth_raw = parts[15].strip()
            purpose = parts[18].strip() if len(parts) > 18 else ''

            if not uwi: continue

            op_name = ba_map.get(ba_code, '')
            op_id = get_or_create(op_name, op_map) if op_name else None

            # Normalize status
            # Map to valid DB enum values: PRODUCING, IDLE, SHUT_IN, TA, PA
            status_map = {
                'Active': 'PRODUCING',
                'Issued': 'IDLE',
                'Suspended': 'SHUT_IN',
                'Abandoned': 'PA',
                'RecCertified': 'PA',
                'Cancelled': None,
            }
            status = status_map.get(status_raw, None)

            # Parse spud date
            spud = None
            if spud_raw and len(spud_raw) == 8:
                try: spud = f"{spud_raw[:4]}-{spud_raw[4:6]}-{spud_raw[6:8]}"
                except: pass

            depth = None
            try: depth = int(float(depth_raw) * 3.28084) if depth_raw else None  # m→ft
            except: pass

            well = {
                "api_number": f"CA-{uwi.replace('/', '-').replace(' ', '')}",
                "source_record_id": uwi,
                "well_name": uwi,
                "operator_id": op_id,
                "state": "Alberta",
                "data_source": "AER_ALBERTA",
                "regulatory_jurisdiction": "AER",
                "well_type": "OIL" if "OIL" in purpose.upper() else ("GAS" if "GAS" in purpose.upper() else None),
                "spud_date": spud,
                "total_depth_ft": depth,
                "status": status,
            }
            batch.append(well)

            if len(batch) >= BATCH_SIZE:
                n = upsert_wells(batch, op_map)
                total += n
                if n < len(batch): errors += len(batch) - n
                batch = []
                if total % 50000 == 0:
                    print(f"  Progress: {total:,} upserted, {errors} errors")
                time.sleep(BATCH_SLEEP)

    if batch:
        n = upsert_wells(batch, op_map)
        total += n

    print(f"✅ Alberta: {total:,} wells upserted, {errors} errors")
    return total

# ─────────────────────────────────────────────
# JOB 3: Pipeline sources (Australia, Netherlands, Colombia)
# ─────────────────────────────────────────────
def job_pipeline_sources(op_map):
    print("\n" + "="*60)
    print("JOB 3: Pipeline sources (Australia, Netherlands, Colombia)")
    print("="*60)
    import subprocess
    pipeline_dir = "/home/openclaw/.openclaw/workspace/varro/scripts/pipeline"
    for source in ["AUSTRALIA", "NETHERLANDS", "COLOMBIA"]:
        print(f"\n  Running {source}...")
        try:
            result = subprocess.run(
                ["python3", "-u", "run_pipeline.py", "--source", source, "--skip-download"],
                cwd=pipeline_dir, capture_output=True, text=True, timeout=300)
            output = result.stdout[-500:] if result.stdout else result.stderr[-200:]
            print(f"  {source}: {output.strip()}")
        except Exception as e:
            print(f"  {source} error: {e}")

# ─────────────────────────────────────────────
# JOB 4: RRC Texas sweep (resume)
# ─────────────────────────────────────────────
def job_rrc_sweep(op_map):
    print("\n" + "="*60)
    print("JOB 4: RRC Texas full sweep (resuming)")
    print("="*60)

    lookup_file = "/home/openclaw/.openclaw/workspace/varro/data/rrc_full_op_lookup.json"
    if not os.path.exists(lookup_file):
        print("  Building lookup from CSV first...")
        src_id_by_op = defaultdict(list)
        csv_file = "/home/openclaw/.openclaw/workspace/varro/data/raw/rrc_texas/wellbore_data/OG_WELLBORE_EWA_Report.csv"
        with open(csv_file, encoding='utf-8', errors='replace') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                if len(row) > 27:
                    src_id = row[27].strip().strip('"')
                    op_name = row[11].strip().strip('"')
                    if op_name and src_id:
                        src_id_by_op[op_name].append(src_id)
        with open(lookup_file, 'w') as f:
            json.dump(src_id_by_op, f)
        print(f"  Lookup built: {len(src_id_by_op):,} operators")
    else:
        with open(lookup_file) as f:
            src_id_by_op = json.load(f)
        print(f"  Lookup loaded: {len(src_id_by_op):,} operators")

    updated, errors = 0, 0
    op_list = list(src_id_by_op.items())
    for idx, (op_name, src_ids) in enumerate(op_list):
        op_uuid = get_or_create(op_name, op_map)
        if not op_uuid: continue

        for i in range(0, len(src_ids), 100):
            chunk = src_ids[i:i+100]
            try:
                r = requests.patch(f"{SUPABASE_URL}/rest/v1/wells",
                    headers=H,
                    params={"source_record_id": f"in.({','.join(chunk)})",
                            "data_source": "eq.RRC_TEXAS"},
                    json={"operator_id": op_uuid}, timeout=30)
                if r.status_code in (200, 204): updated += len(chunk)
                else: errors += 1
            except Exception: errors += 1
            time.sleep(BATCH_SLEEP)

        if idx % 1000 == 0 and idx > 0:
            print(f"  Progress: {idx}/{len(op_list)} operators, {updated:,} updated")

    print(f"✅ RRC Texas: {updated:,} wells updated, {errors} errors")
    return updated

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
if __name__ == "__main__":
    print("🚀 Varro Queue Runner starting...")
    print(f"   Jobs queued: Indexes SQL, Alberta AER, Australia, Netherlands, Colombia, RRC Texas")

    if not wait_for_db(max_wait=600):
        print("❌ DB did not come back within 10 minutes. Exiting.")
        sys.exit(1)

    print("\nLoading operator map...")
    op_map = load_operators()

    job_indexes()
    job_alberta(op_map)
    job_pipeline_sources(op_map)
    job_rrc_sweep(op_map)

    print("\n" + "="*60)
    print("✅ ALL JOBS COMPLETE")
    print("="*60)
