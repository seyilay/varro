#!/usr/bin/env python3
"""
BOEM Operators Ingestion + Wells Linkage
Varro ARO Intelligence Platform
"""
import csv
import json
import urllib.request
import urllib.error
import time
import sys

SUPABASE_URL = "https://temtptsfiksixxhbigkg.supabase.co"
SUPABASE_KEY = "os.environ.get('SUPABASE_KEY')"

COMPANIES_FILE = "/home/openclaw/.openclaw/workspace/varro/data/raw/boem/Company/CompanyRawData/mv_companies_all.txt"
BOREHOLES_FILE = "/home/openclaw/.openclaw/workspace/varro/data/raw/boem/Borehole/BoreholeRawData/mv_boreholes_all.txt"


def supabase_request(method, path, data=None, params=None, headers_extra=None):
    url = f"{SUPABASE_URL}/rest/v1/{path}"
    if params:
        url += "?" + "&".join(f"{k}={v}" for k, v in params.items())
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }
    if headers_extra:
        headers.update(headers_extra)
    body = json.dumps(data).encode() if data is not None else None
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            resp_body = r.read()
            if resp_body:
                return json.loads(resp_body)
            return None
    except urllib.error.HTTPError as e:
        err_body = e.read().decode()
        print(f"HTTP {e.code} on {method} {path}: {err_body[:500]}", file=sys.stderr)
        raise


def supabase_upsert(table, records, batch_size=500):
    """Upsert records in batches."""
    total = len(records)
    for i in range(0, total, batch_size):
        batch = records[i:i + batch_size]
        supabase_request(
            "POST", table, data=batch,
            headers_extra={"Prefer": "resolution=merge-duplicates,return=minimal"}
        )
        print(f"  Upserted {min(i + batch_size, total)}/{total} records to {table}")
        time.sleep(0.2)


def parse_companies(filepath):
    """Parse mv_companies_all.txt into operator records."""
    operators = []
    with open(filepath, newline='', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f, quoting=csv.QUOTE_ALL)
        for row in reader:
            company_num = row.get("MMS_COMPANY_NUM", "").strip()
            name = row.get("BUS_ASC_NAME", "").strip()
            country_name = row.get("COUNTRY_NAME", "").strip()

            if not name:
                continue

            # Normalize country
            country = "US"
            if country_name and country_name.lower() not in ("united states", "usa", "us", ""):
                country = country_name[:100]

            operators.append({
                "name": name,
                "operator_code": company_num if company_num else None,
                "country": country,
            })

    return operators


def parse_boreholes(filepath):
    """Parse mv_boreholes_all.txt — return {api_number: company_name}."""
    api_to_company = {}
    with open(filepath, newline='', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f, quoting=csv.QUOTE_ALL)
        for row in reader:
            api = row.get("API_WELL_NUMBER", "").strip()
            company = row.get("COMPANY_NAME", "").strip()
            if api and company:
                api_to_company[api] = company
    return api_to_company


def get_all_operators_map():
    """Fetch all operators from Supabase, return {name: id, ...}."""
    name_to_id = {}
    limit = 1000
    offset = 0
    while True:
        results = supabase_request(
            "GET", "operators",
            params={"select": "id,name,operator_code", "limit": str(limit), "offset": str(offset)}
        )
        if not results:
            break
        for r in results:
            if r["name"]:
                name_to_id[r["name"]] = r["id"]
        if len(results) < limit:
            break
        offset += limit
        time.sleep(0.1)
    return name_to_id


def get_wells_needing_operator():
    """Fetch all BOEM wells with null operator_id, return [{id, api_number}, ...]."""
    wells = []
    limit = 1000
    offset = 0
    while True:
        results = supabase_request(
            "GET", "wells",
            params={
                "select": "id,api_number",
                "data_source": "eq.BOEM",
                "operator_id": "is.null",
                "limit": str(limit),
                "offset": str(offset)
            }
        )
        if not results:
            break
        wells.extend(results)
        if len(results) < limit:
            break
        offset += limit
        time.sleep(0.1)
    return wells


def update_wells_operator(well_updates, batch_size=200):
    """
    well_updates: list of {id: uuid, operator_id: uuid}
    Use individual PATCH calls grouped efficiently.
    """
    count = 0
    for update in well_updates:
        supabase_request(
            "PATCH", "wells",
            data={"operator_id": update["operator_id"]},
            params={"id": f"eq.{update['id']}"},
            headers_extra={"Prefer": "return=minimal"}
        )
        count += 1
        if count % 100 == 0:
            print(f"  Updated {count}/{len(well_updates)} wells...")
            time.sleep(0.2)
    return count


# ─── MAIN ─────────────────────────────────────────────────────────────────────

print("=" * 60)
print("BOEM Operators Ingestion + Wells Linkage")
print("=" * 60)

# Step 1: Parse companies file
print("\n[1/5] Parsing BOEM company file...")
operators = parse_companies(COMPANIES_FILE)
print(f"  Parsed {len(operators)} operators from file")

# Step 2: Upsert operators to Supabase
print("\n[2/5] Upserting operators to Supabase...")
supabase_upsert("operators", operators)
print(f"  ✓ {len(operators)} operators upserted")

# Step 3: Get name→id map from Supabase
print("\n[3/5] Fetching operator name→id map from Supabase...")
name_to_id = get_all_operators_map()
print(f"  ✓ {len(name_to_id)} operators in DB")

# Step 4: Parse borehole data for api→company mapping
print("\n[4/5] Parsing borehole data for well→company links...")
api_to_company = parse_boreholes(BOREHOLES_FILE)
print(f"  Parsed {len(api_to_company)} borehole records with company info")

# Fetch wells needing operator_id
wells = get_wells_needing_operator()
print(f"  Found {len(wells)} BOEM wells with null operator_id")

# Build update list
well_updates = []
unmatched_companies = set()
matched = 0
no_borehole_match = 0

for well in wells:
    api = well.get("api_number", "")
    company_name = api_to_company.get(api)
    if not company_name:
        no_borehole_match += 1
        continue
    op_id = name_to_id.get(company_name)
    if not op_id:
        unmatched_companies.add(company_name)
        continue
    well_updates.append({"id": well["id"], "operator_id": op_id})
    matched += 1

print(f"  Matched {matched} wells to operators")
print(f"  No borehole record: {no_borehole_match} wells")
print(f"  Unmatched company names: {len(unmatched_companies)}")
if unmatched_companies:
    sample = sorted(unmatched_companies)[:10]
    print(f"  Sample unmatched: {sample}")

# Step 5: Update wells
print(f"\n[5/5] Updating {len(well_updates)} wells with operator_id...")
if well_updates:
    updated = update_wells_operator(well_updates)
    print(f"  ✓ Updated {updated} wells")
else:
    print("  No wells to update.")

# Final summary
print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)
print(f"  Operators upserted: {len(operators)}")
print(f"  Operators in DB:    {len(name_to_id)}")
print(f"  Wells linked:       {len(well_updates)}")
print(f"  Wells unlinked:     {no_borehole_match + len(unmatched_companies)}")
print("  Done ✓")
