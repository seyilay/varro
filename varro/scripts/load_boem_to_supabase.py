#!/usr/bin/env python3
"""
Load BOEM data into Supabase
"""

import csv
import json
import urllib.request
import urllib.parse
from datetime import datetime
import os

# Supabase config
SUPABASE_URL = "https://temtptsfiksixxhbigkg.supabase.co"
SUPABASE_KEY = "os.environ.get('SUPABASE_KEY')"

# Data paths
DATA_DIR = "/home/openclaw/.openclaw/workspace/varro/data/raw/boem"

def supabase_request(endpoint, data, method="POST"):
    """Make a request to Supabase REST API"""
    url = f"{SUPABASE_URL}/rest/v1/{endpoint}"
    
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }
    
    req = urllib.request.Request(
        url,
        data=json.dumps(data).encode('utf-8') if data else None,
        headers=headers,
        method=method
    )
    
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            return response.status, response.read().decode('utf-8')
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode('utf-8')

def upsert_batch(table, records, batch_size=100):
    """Insert records in batches"""
    total = len(records)
    inserted = 0
    
    for i in range(0, total, batch_size):
        batch = records[i:i+batch_size]
        
        # Use upsert with on_conflict
        url = f"{SUPABASE_URL}/rest/v1/{table}"
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates,return=minimal"
        }
        
        req = urllib.request.Request(
            url,
            data=json.dumps(batch).encode('utf-8'),
            headers=headers,
            method="POST"
        )
        
        try:
            with urllib.request.urlopen(req, timeout=60) as response:
                inserted += len(batch)
                print(f"  Inserted {inserted}/{total} records...")
        except urllib.error.HTTPError as e:
            print(f"  Error at batch {i}: {e.code} - {e.read().decode('utf-8')[:200]}")
            # Continue with next batch
    
    return inserted

def load_companies():
    """Load company/operator data"""
    print("\n=== Loading Companies/Operators ===")
    
    filepath = f"{DATA_DIR}/Company/CompanyRawData/mv_companies_all.txt"
    if not os.path.exists(filepath):
        print(f"File not found: {filepath}")
        return
    
    records = []
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Map to our schema
            record = {
                "name": row.get("COMPANY_NAME", "").strip(),
                "operator_code": row.get("COMPANY_NUMBER", "").strip(),
                "country": "US"
            }
            if record["name"]:
                records.append(record)
    
    print(f"Parsed {len(records)} companies")
    
    # Dedupe by name
    seen = set()
    unique = []
    for r in records:
        if r["name"] not in seen:
            seen.add(r["name"])
            unique.append(r)
    
    print(f"Unique companies: {len(unique)}")
    inserted = upsert_batch("operators", unique)
    print(f"Loaded {inserted} operators")

def load_wells():
    """Load borehole/well data"""
    print("\n=== Loading Wells ===")
    
    filepath = f"{DATA_DIR}/Borehole/BoreholeRawData/mv_boreholes_all.txt"
    if not os.path.exists(filepath):
        print(f"File not found: {filepath}")
        return
    
    records = []
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        reader = csv.DictReader(f)
        for row in reader:
            api = row.get("API_WELL_NUMBER", "").strip()
            if not api:
                continue
            
            # Parse dates
            spud_date = None
            if row.get("WELL_SPUD_DATE"):
                try:
                    spud_date = datetime.strptime(row["WELL_SPUD_DATE"], "%m/%d/%Y").strftime("%Y-%m-%d")
                except:
                    pass
            
            # Parse water depth
            water_depth = None
            if row.get("WATER_DEPTH"):
                try:
                    water_depth = int(row["WATER_DEPTH"])
                except:
                    pass
            
            # Parse total depth
            total_depth = None
            if row.get("BH_TOTAL_MD"):
                try:
                    total_depth = int(row["BH_TOTAL_MD"])
                except:
                    pass
            
            # Parse lat/long
            lat = None
            lng = None
            try:
                lat = float(row.get("SURF_LATITUDE", ""))
                lng = float(row.get("SURF_LONGITUDE", ""))
            except:
                pass
            
            # Map status
            status_map = {
                "PA": "PA",
                "TA": "TA", 
                "COM": "PRODUCING",
                "DRL": "PRODUCING",
                "ST": "SHUT_IN"
            }
            status = status_map.get(row.get("BOREHOLE_STAT_CD", ""), "IDLE")
            
            record = {
                "api_number": api,
                "well_name": row.get("WELL_NAME", "").strip(),
                "latitude": lat,
                "longitude": lng,
                "basin": row.get("BOTM_AREA_CODE", "").strip(),
                "block_number": row.get("BOTM_BLOCK_NUM", "").strip(),
                "lease_number": row.get("BOTM_LEASE_NUMBER", "").strip(),
                "well_class": "OFFSHORE",
                "water_depth_ft": water_depth,
                "total_depth_ft": total_depth,
                "spud_date": spud_date,
                "status": status,
                "regulatory_jurisdiction": "BOEM",
                "data_source": "BOEM"
            }
            records.append(record)
    
    print(f"Parsed {len(records)} wells")
    inserted = upsert_batch("wells", records, batch_size=500)
    print(f"Loaded {inserted} wells")

def load_decom_costs():
    """Load decommissioning cost estimates"""
    print("\n=== Loading Decommissioning Cost Estimates ===")
    
    filepath = f"{DATA_DIR}/DecomCostEst/DecomCostEstRawData/mv_decom_cost_spud_well.txt"
    if not os.path.exists(filepath):
        print(f"File not found: {filepath}")
        return
    
    records = []
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        reader = csv.DictReader(f)
        for row in reader:
            api = row.get("API_WELL_NUMBER", "").strip()
            if not api:
                continue
            
            # Parse costs
            p50 = None
            p70 = None
            p90 = None
            try:
                p50 = float(row.get("WELL_INST_DCOM_P50", 0))
                p70 = float(row.get("WELL_INST_DCOM_P70", 0))
                p90 = float(row.get("WELL_INST_DCOM_P90", 0))
            except:
                pass
            
            if p50 and p50 > 0:
                # Parse water depth from area code/block
                record = {
                    "api_number": api,
                    "actual_cost": p50,  # Using P50 as the estimate
                    "cost_year": 2026,
                    "basin": row.get("BOTM_AREA_CODE", "").strip(),
                    "data_source": "BOEM",
                    "source_record_id": f"BOEM-{api}",
                    "is_proprietary": False
                }
                records.append(record)
    
    print(f"Parsed {len(records)} cost estimates")
    inserted = upsert_batch("comparable_costs", records, batch_size=500)
    print(f"Loaded {inserted} cost estimates")

def main():
    print("=" * 50)
    print("BOEM Data Loader for Supabase")
    print("=" * 50)
    print(f"Supabase URL: {SUPABASE_URL}")
    print(f"Data Directory: {DATA_DIR}")
    
    # Test connection
    print("\nTesting Supabase connection...")
    status, resp = supabase_request("operators?select=count", None, method="GET")
    if status == 200:
        print("✓ Connection successful")
    else:
        print(f"✗ Connection failed: {status} - {resp[:200]}")
        return
    
    # Load data
    load_companies()
    load_wells()
    load_decom_costs()
    
    print("\n" + "=" * 50)
    print("DONE!")
    print("=" * 50)

if __name__ == "__main__":
    main()
