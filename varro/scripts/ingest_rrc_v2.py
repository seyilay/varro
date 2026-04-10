#!/usr/bin/env python3
"""
Ingest RRC Texas OG_WELLBORE_EWA_Report.csv into Supabase wells table.
v2: Priority-based status merging (PA > TA > DELINQUENT > SHUT_IN > IDLE > PRODUCING)
"""
import csv, json, sys, os, urllib.request, urllib.error, time, collections

SUPABASE_URL = "https://temtptsfiksixxhbigkg.supabase.co"
SUPABASE_KEY = "os.environ.get('SUPABASE_KEY')"
CSV_FILE = "/home/openclaw/.openclaw/workspace/varro/data/raw/rrc_texas/wellbore_data/OG_WELLBORE_EWA_Report.csv"
BATCH_SIZE = 500
MAX_ROWS = int(sys.argv[1]) if len(sys.argv) > 1 else 1500000

# Status mapping from RRC to Supabase
STATUS_MAP = {
    "ABANDONED": "PA",
    "PARTIAL PLUG": "PA",
    "DELINQUENT": "DELINQUENT",
    "TEMP ABANDONED": "TA",
    "SHUT IN": "SHUT_IN",
    "SHUT IN-MULTI-COMPL": "SHUT_IN",
    "NO PRODUCTION": "IDLE",
    "NOT ELIGIBLE FOR ALLOWABLE": "IDLE",
    "OBSERVATION": "IDLE",
    "PRODUCING": "PRODUCING",
    "INJECTION": "PRODUCING",
    "SWR-10-WELL": "PRODUCING",
    "WATER SUPPLY": "PRODUCING",
    "LPG STORAGE": "PRODUCING",
    "DOMESTIC USE WELL": "PRODUCING",
    "OTHER TYPE SERVICE": "PRODUCING",
    "PROD FACTOR WELL": "PRODUCING",
    "GAS STRG-WITHDRAWAL": "PRODUCING",
    "GAS STRG-INJECTION": "PRODUCING",
    "BRINE MINING": "PRODUCING",
    "GAS STRG-SALT FORMATION": "PRODUCING",
    "GEOTHERMAL WELL": "PRODUCING",
    "TRAINING": "IDLE",
    "": "IDLE",
}

# Priority order for status (higher index = higher priority, wins over lower)
STATUS_PRIORITY = {"PRODUCING": 1, "IDLE": 2, "SHUT_IN": 3, "TA": 4, "DELINQUENT": 5, "ORPHAN": 6, "PA": 7}

def parse_date_8(val):
    v = val.strip() if val else ""
    if len(v) == 8 and v.isdigit() and v != "00000000":
        try:
            y, m, d = int(v[:4]), int(v[4:6]), int(v[6:8])
            if 1800 <= y <= 2100 and 1 <= m <= 12 and 1 <= d <= 31:
                return f"{y:04d}-{m:02d}-{d:02d}"
        except:
            pass
    return None

def parse_date_6(val):
    v = val.strip() if val else ""
    if len(v) == 6 and v.isdigit() and v != "000000":
        try:
            y, m = int(v[:4]), int(v[4:6])
            if 1800 <= y <= 2100 and 1 <= m <= 12:
                return f"{y:04d}-{m:02d}-01"
        except:
            pass
    return None

def make_api_number(well_8digit):
    w = well_8digit.strip().zfill(8)
    return "42" + w + "00"

def parse_row_to_record(row):
    """Convert CSV row to partial record dict."""
    if len(row) < 20 or not row[2].strip().isdigit():
        return None
    
    api = make_api_number(row[2])
    rrc_status = row[18].strip()
    status = STATUS_MAP.get(rrc_status, "IDLE")
    
    # Dates: index 30=spud, 29=completion, 28=most recent event
    spud_date = parse_date_8(row[30]) if len(row) > 30 else None
    completion_date = parse_date_8(row[29]) if len(row) > 29 else None
    status_date = parse_date_8(row[28]) if len(row) > 28 else None
    if not status_date and len(row) > 19 and row[19].strip():
        status_date = parse_date_6(row[19])
    
    total_depth = None
    if len(row) > 15 and row[15].strip().isdigit():
        td = int(row[15].strip())
        if 0 < td < 50000:
            total_depth = td
    
    well_type = {"O": "OIL", "G": "GAS"}.get(row[4].strip() if len(row) > 4 else "", None)
    
    return {
        "api_number": api,
        "well_name": row[5].strip() or None,
        "state": "TX",
        "county": row[3].strip() or None,
        "basin": "TEXAS",
        "well_class": "ONSHORE",
        "well_type": well_type,
        "field_name": row[7].strip() if len(row) > 7 else None,
        "lease_number": row[6].strip() if len(row) > 6 else None,
        "total_depth_ft": total_depth,
        "spud_date": spud_date,
        "completion_date": completion_date,
        "status": status,
        "status_date": status_date,
        "regulatory_jurisdiction": "RRC_TX",
        "data_source": "RRC_TEXAS",
        "source_record_id": row[27].strip() if len(row) > 27 and row[27].strip() else None,
        "_rrc_status": rrc_status,  # internal tracking
    }

def merge_records(existing, new_rec):
    """Merge new record into existing, keeping highest-priority status."""
    if existing is None:
        return new_rec
    
    # Status priority: higher priority wins
    existing_prio = STATUS_PRIORITY.get(existing.get("status", "IDLE"), 0)
    new_prio = STATUS_PRIORITY.get(new_rec.get("status", "IDLE"), 0)
    
    if new_prio >= existing_prio:
        # New record has equal or higher priority - use new status and dates
        merged = {**existing, **{k: v for k, v in new_rec.items() if v is not None and k != "_rrc_status"}}
        merged["status"] = new_rec["status"]  # Ensure status is from higher priority
        if new_prio > existing_prio:
            # Status_date should reflect when PA/TA status was assigned
            merged["status_date"] = new_rec.get("status_date") or existing.get("status_date")
        return merged
    else:
        # Keep existing status, but take better data from new record if missing
        merged = {**new_rec, **{k: v for k, v in existing.items() if v is not None and k != "_rrc_status"}}
        merged["status"] = existing["status"]
        merged["status_date"] = existing.get("status_date")
        return merged

def supabase_upsert(records):
    """Upsert records into Supabase."""
    url = f"{SUPABASE_URL}/rest/v1/wells?on_conflict=api_number"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal"
    }
    # Remove internal tracking fields
    clean = [{k: v for k, v in r.items() if not k.startswith("_")} for r in records]
    data = json.dumps(clean).encode('utf-8')
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            return len(records), None
    except urllib.error.HTTPError as e:
        err = e.read().decode('utf-8', errors='ignore')[:300]
        return 0, f"HTTP {e.code}: {err}"
    except Exception as e:
        return 0, str(e)

def main():
    print(f"Starting RRC Texas wellbore ingestion v2 (priority-based)")
    print(f"File: {CSV_FILE}")
    print(f"Max rows: {MAX_ROWS}")
    print()
    
    total_parsed = 0
    total_inserted = 0
    total_pa = 0
    total_errors = 0
    skipped = 0
    start_time = time.time()
    
    # In-memory accumulator: api -> best record
    well_map = {}
    
    def flush_batch():
        nonlocal total_inserted, total_pa, total_errors
        if not well_map:
            return
        records = list(well_map.values())
        inserted, err = supabase_upsert(records)
        if err:
            print(f"  Batch error: {err}")
            total_errors += 1
        else:
            total_inserted += inserted
        pa_count = sum(1 for r in records if r.get('status') == 'PA')
        total_pa += pa_count
        well_map.clear()
    
    FLUSH_THRESHOLD = BATCH_SIZE  # Flush when we have this many unique APIs
    
    with open(CSV_FILE, 'r', encoding='latin-1') as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if i == 0:
                continue
            if total_parsed >= MAX_ROWS:
                break
            
            record = parse_row_to_record(row)
            if record is None:
                skipped += 1
                continue
            
            total_parsed += 1
            api = record["api_number"]
            
            if api in well_map:
                well_map[api] = merge_records(well_map[api], record)
            else:
                well_map[api] = record
            
            # Flush when we have enough unique wells
            if len(well_map) >= FLUSH_THRESHOLD:
                flush_batch()
                if total_inserted % 50000 == 0 and total_inserted > 0:
                    elapsed = time.time() - start_time
                    rate = total_inserted / elapsed
                    print(f"  Progress: {total_inserted} inserted, {total_pa} PA wells ({rate:.0f}/s)")
    
    # Final flush
    if well_map:
        flush_batch()
    
    elapsed = time.time() - start_time
    print(f"\n{'='*50}")
    print(f"INGESTION COMPLETE (v2)")
    print(f"{'='*50}")
    print(f"Total rows scanned:    {total_parsed + skipped}")
    print(f"Records parsed:        {total_parsed}")
    print(f"Records skipped:       {skipped}")
    print(f"Records inserted:      {total_inserted}")
    print(f"PA wells inserted:     {total_pa}")
    print(f"Batch errors:          {total_errors}")
    print(f"Elapsed time:          {elapsed:.1f}s")
    print(f"Rate:                  {total_inserted/elapsed:.0f} records/sec")

if __name__ == "__main__":
    main()
