#!/usr/bin/env python3
"""
Ingest RRC Texas OG_WELLBORE_EWA_Report.csv into Supabase wells table.
"""
import csv, json, sys, os, urllib.request, urllib.error, time

SUPABASE_URL = "https://temtptsfiksixxhbigkg.supabase.co"
SUPABASE_KEY = "os.environ.get('SUPABASE_KEY')"
CSV_FILE = "/home/openclaw/.openclaw/workspace/varro/data/raw/rrc_texas/wellbore_data/OG_WELLBORE_EWA_Report.csv"
BATCH_SIZE = 1000
MAX_ROWS = int(sys.argv[1]) if len(sys.argv) > 1 else 50000  # Default: 50k sample

# Status mapping from RRC EWA status to Supabase status values
STATUS_MAP = {
    "PRODUCING": "PRODUCING",
    "SHUT IN": "SHUT_IN",
    "SHUT IN-MULTI-COMPL": "SHUT_IN",
    "INJECTION": "PRODUCING",
    "TEMP ABANDONED": "TA",
    "NO PRODUCTION": "IDLE",
    "PARTIAL PLUG": "PA",
    "ABANDONED": "PA",
    "SWR-10-WELL": "PRODUCING",
    "WATER SUPPLY": "PRODUCING",
    "LPG STORAGE": "PRODUCING",
    "NOT ELIGIBLE FOR ALLOWABLE": "IDLE",
    "DOMESTIC USE WELL": "PRODUCING",
    "OTHER TYPE SERVICE": "PRODUCING",
    "PROD FACTOR WELL": "PRODUCING",
    "OBSERVATION": "IDLE",
    "GAS STRG-WITHDRAWAL": "PRODUCING",
    "GAS STRG-INJECTION": "PRODUCING",
    "BRINE MINING": "PRODUCING",
    "GAS STRG-SALT FORMATION": "PRODUCING",
    "GEOTHERMAL WELL": "PRODUCING",
    "TRAINING": "IDLE",
    "DELINQUENT": "DELINQUENT",
    "": "IDLE",
}

def parse_date_8(val):
    """Parse YYYYMMDD string to YYYY-MM-DD"""
    v = val.strip()
    if len(v) == 8 and v.isdigit() and v != "00000000":
        try:
            y, m, d = int(v[:4]), int(v[4:6]), int(v[6:8])
            if 1800 <= y <= 2100 and 1 <= m <= 12 and 1 <= d <= 31:
                return f"{y:04d}-{m:02d}-{d:02d}"
        except:
            pass
    return None

def parse_date_6(val):
    """Parse YYYYMM string to YYYY-MM-01"""
    v = val.strip()
    if len(v) == 6 and v.isdigit() and v != "000000":
        try:
            y, m = int(v[:4]), int(v[4:6])
            if 1800 <= y <= 2100 and 1 <= m <= 12:
                return f"{y:04d}-{m:02d}-01"
        except:
            pass
    return None

def make_api_number(county_code, well_8digit):
    """Construct 12-digit API number: 42 + 8digit + 00"""
    c = county_code.strip().zfill(3)
    w = well_8digit.strip().zfill(8)
    # Verify alignment: first 3 digits of well_8digit should match county_code
    return "42" + w + "00"  # 12 digits: state(2) + county(3) + well(5) + sidetrack(2)

def supabase_upsert(table, records):
    """Upsert records into Supabase with conflict resolution on api_number."""
    url = f"{SUPABASE_URL}/rest/v1/{table}?on_conflict=api_number"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal"
    }
    data = json.dumps(records).encode('utf-8')
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            return len(records), None
    except urllib.error.HTTPError as e:
        err = e.read().decode('utf-8', errors='ignore')[:300]
        return 0, f"HTTP {e.code}: {err}"
    except Exception as e:
        return 0, str(e)

def parse_row(row):
    """Parse a CSV row into a Supabase wells record."""
    if len(row) < 32:
        return None
    
    # Build API number
    county_code = row[1].strip()
    well_8digit = row[2].strip()
    if not county_code or not well_8digit or not well_8digit.isdigit():
        return None
    
    api_number = make_api_number(county_code, well_8digit)
    
    # Status
    rrc_status = row[18].strip()
    status = STATUS_MAP.get(rrc_status, "IDLE")
    
    # Dates
    # row[30] = oldest date (likely spud)
    # row[29] = middle date (likely completion)  
    # row[28] = most recent event date (plug date for PA wells)
    spud_date = parse_date_8(row[30]) if len(row) > 30 else None
    completion_date = parse_date_8(row[29]) if len(row) > 29 else None
    
    # For status_date: use row[19] (YYYYMM) or row[28] (YYYYMMDD) for PA wells
    status_date = None
    if len(row) > 28 and row[28].strip():
        status_date = parse_date_8(row[28])
    if not status_date and len(row) > 19 and row[19].strip():
        status_date = parse_date_6(row[19])
    
    # Well name
    well_name = row[5].strip() if row[5].strip() else None
    
    # County
    county = row[3].strip() if row[3].strip() else None
    
    # Field name
    field_name = row[7].strip() if len(row) > 7 and row[7].strip() else None
    
    # Total depth
    total_depth = None
    if len(row) > 15 and row[15].strip().isdigit():
        td = int(row[15].strip())
        if 0 < td < 50000:
            total_depth = td
    
    # Well type (Oil/Gas)
    well_type = None
    if len(row) > 4:
        t = row[4].strip()
        if t == "O":
            well_type = "OIL"
        elif t == "G":
            well_type = "GAS"
    
    # Lease number
    lease_number = row[6].strip() if len(row) > 6 and row[6].strip() else None
    
    # Source record ID
    source_record_id = row[27].strip() if len(row) > 27 and row[27].strip() else None
    
    # District/basin
    district = row[0].strip()
    basin = f"RRC_DISTRICT_{district}" if district else "TEXAS"
    
    # Always include all fields (PostgREST requires consistent keys in batch)
    record = {
        "api_number": api_number,
        "well_name": well_name,
        "state": "TX",
        "county": county,
        "basin": "TEXAS",
        "well_class": "ONSHORE",
        "well_type": well_type,
        "field_name": field_name,
        "lease_number": lease_number,
        "total_depth_ft": total_depth,
        "spud_date": spud_date,
        "completion_date": completion_date,
        "status": status,
        "status_date": status_date,
        "regulatory_jurisdiction": "RRC_TX",
        "data_source": "RRC_TEXAS",
        "source_record_id": source_record_id,
    }
    
    return record

def main():
    print(f"Starting RRC Texas wellbore ingestion")
    print(f"File: {CSV_FILE}")
    print(f"Max rows: {MAX_ROWS}")
    print(f"Batch size: {BATCH_SIZE}")
    print()
    
    total_parsed = 0
    total_inserted = 0
    total_pa = 0
    total_errors = 0
    skipped = 0
    batch = []
    start_time = time.time()
    
    with open(CSV_FILE, 'r', encoding='latin-1') as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if i == 0:
                continue  # No header in this file, first row is data
            
            if total_parsed >= MAX_ROWS:
                break
            
            record = parse_row(row)
            if record is None:
                skipped += 1
                continue
            
            total_parsed += 1
            
            # Track PA wells
            if record.get('status') == 'PA':
                total_pa += 1
            
            batch.append(record)
            
            if len(batch) >= BATCH_SIZE:
                # Deduplicate within batch by api_number (keep last = most recent)
                seen = {}
                for rec in batch:
                    seen[rec['api_number']] = rec
                deduped = list(seen.values())
                inserted, err = supabase_upsert("wells", deduped)
                if err:
                    print(f"  Batch error at row {i}: {err}")
                    total_errors += 1
                else:
                    total_inserted += inserted
                batch = []
                
                if total_inserted % 10000 == 0 and total_inserted > 0:
                    elapsed = time.time() - start_time
                    rate = total_inserted / elapsed
                    print(f"  Progress: {total_inserted} inserted ({rate:.0f}/s), {total_pa} PA wells")
        
        # Flush remaining batch
        if batch:
            seen = {}
            for rec in batch:
                seen[rec['api_number']] = rec
            deduped = list(seen.values())
            inserted, err = supabase_upsert("wells", deduped)
            if err:
                print(f"  Final batch error: {err}")
                total_errors += 1
            else:
                total_inserted += inserted
    
    elapsed = time.time() - start_time
    print(f"\n{'='*50}")
    print(f"INGESTION COMPLETE")
    print(f"{'='*50}")
    print(f"Total rows scanned:   {i}")
    print(f"Total records parsed: {total_parsed}")
    print(f"Total records skipped: {skipped}")
    print(f"Total inserted:       {total_inserted}")
    print(f"PA wells (with plug): {total_pa}")
    print(f"Batch errors:         {total_errors}")
    print(f"Elapsed time:         {elapsed:.1f}s")
    print(f"Rate:                 {total_inserted/elapsed:.0f} records/sec")
    
    return total_inserted, total_pa

if __name__ == "__main__":
    main()
