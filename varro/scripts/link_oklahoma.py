#!/usr/bin/env python3
"""
Link OCC_OKLAHOMA wells to operators by re-fetching from the OCC ESRI API.
~441K wells. api_number format: OK-{str(int(api)).zfill(14)}
"""

import json, time, re, sys
import urllib.request, urllib.error
from collections import defaultdict

SUPABASE_URL = "https://temtptsfiksixxhbigkg.supabase.co"
KEY = "os.environ.get('SUPABASE_KEY')"

READ_H = {"apikey": KEY, "Authorization": f"Bearer {KEY}", "Prefer": "statement_timeout=120000"}
WRITE_H = {"apikey": KEY, "Authorization": f"Bearer {KEY}", "Content-Type": "application/json",
           "Prefer": "return=minimal,statement_timeout=120000"}
POST_H  = {"apikey": KEY, "Authorization": f"Bearer {KEY}", "Content-Type": "application/json",
           "Prefer": "return=representation,statement_timeout=120000"}

OCC_URL = "https://gis.occ.ok.gov/server/rest/services/Hosted/RBDMS_WELLS/FeatureServer/220/query"

SUFFIX_RE = re.compile(
    r'\b(inc|llc|corp|ltd|limited|as|asa|plc|co|company|gmbh|bv|nv|sa|ag|ab|oy|se|srl|spa|lp|llp)\b\.?$',
    re.IGNORECASE
)
def normalize(name):
    if not name: return ""
    n = name.strip().lower()
    n = re.sub(r'\s+', ' ', n)
    prev = None
    while prev != n:
        prev = n
        n = SUFFIX_RE.sub("", n).strip().rstrip(",").strip()
    return n

def sb_get(url, retries=5):
    for attempt in range(retries):
        req = urllib.request.Request(url, headers=READ_H)
        try:
            return json.loads(urllib.request.urlopen(req, timeout=130).read())
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            if e.code in (500, 503) and attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                print(f"  FATAL GET {e.code}: {body[:200]}")
                raise
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2)
            else:
                raise

def sb_patch(table, filter_param, data):
    url = f"{SUPABASE_URL}/rest/v1/{table}?{filter_param}"
    body = json.dumps(data).encode()
    req = urllib.request.Request(url, data=body, headers=WRITE_H, method="PATCH")
    try:
        urllib.request.urlopen(req, timeout=130).read()
        return True
    except urllib.error.HTTPError as e:
        print(f"  PATCH error {e.code}: {e.read().decode()[:100]}")
        return False

def sb_post_operator(name, country="US"):
    data = {"name": name, "country": country}
    body = json.dumps(data).encode()
    req = urllib.request.Request(f"{SUPABASE_URL}/rest/v1/operators", data=body, headers=POST_H, method="POST")
    try:
        result = json.loads(urllib.request.urlopen(req, timeout=30).read())
        if isinstance(result, list) and result:
            return result[0]["id"]
    except urllib.error.HTTPError as e:
        if e.code == 409:
            enc = urllib.request.quote(name, safe="")
            r = sb_get(f"{SUPABASE_URL}/rest/v1/operators?name=eq.{enc}&select=id")
            return r[0]["id"] if r else None
        print(f"  POST op error {e.code}: {e.read().decode()[:100]}")
    return None

def fetch_all_operators():
    results, offset, limit = [], 0, 1000
    while True:
        url = f"{SUPABASE_URL}/rest/v1/operators?select=id,name&limit={limit}&offset={offset}"
        batch = sb_get(url)
        results.extend(batch)
        if len(batch) < limit: break
        offset += limit
        time.sleep(0.1)
    return results

def build_lookup(operators):
    lookup = {}
    for op in operators:
        norm = normalize(op["name"])
        if norm:
            lookup[norm] = op["id"]
    return lookup

def find_op(name, lookup):
    if not name: return None
    norm = normalize(name)
    if norm in lookup: return lookup[norm]
    for key, uid in lookup.items():
        if len(norm) > 5 and (norm in key or key in norm):
            return uid
    return None

def batch_fetch_wells(api_numbers, batch_size=200):
    result = {}
    api_list = list(api_numbers)
    for i in range(0, len(api_list), batch_size):
        chunk = api_list[i:i+batch_size]
        enc = ",".join(urllib.request.quote(v, safe="") for v in chunk)
        url = f"{SUPABASE_URL}/rest/v1/wells?api_number=in.({enc})&select=id,api_number,operator_id"
        try:
            rows = sb_get(url)
            for row in rows:
                result[row["api_number"]] = row
        except Exception as e:
            print(f"  [batch_fetch] Error at {i}: {e}")
        time.sleep(0.05)
        if (i // batch_size + 1) % 50 == 0:
            print(f"  Wells fetched: {min(i+batch_size, len(api_list))}/{len(api_list)}")
    return result

def batch_patch_wells(op_to_well_ids, batch_size=200):
    updated = 0
    for op_id, well_ids in op_to_well_ids.items():
        for i in range(0, len(well_ids), batch_size):
            chunk = well_ids[i:i+batch_size]
            if sb_patch("wells", f"id=in.({','.join(chunk)})", {"operator_id": op_id}):
                updated += len(chunk)
        time.sleep(0.03)
        if updated % 50000 == 0 and updated > 0:
            print(f"  Patched so far: {updated:,}")
    return updated


def main():
    print("=" * 60)
    print("OCC_OKLAHOMA - Link Wells to Operators")
    print("=" * 60)

    # Step 1: Fetch all OK wells from OCC API
    print("\n[1/5] Fetching OK wells from OCC API...")
    api_to_op = {}
    offset = 0
    page_size = 2000
    
    while True:
        url = f"{OCC_URL}?where=1=1&outFields=api,operator&f=json&resultOffset={offset}&resultRecordCount={page_size}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        try:
            data = json.loads(urllib.request.urlopen(req, timeout=45).read())
        except Exception as e:
            print(f"  OCC API error at offset {offset}: {e}")
            break
        
        features = data.get("features", [])
        if not features:
            break
        
        for feat in features:
            a = feat["attributes"]
            api_raw = a.get("api")
            op = (a.get("operator") or "").strip()
            if api_raw and op:
                api_str = str(int(api_raw)).zfill(14)
                api_to_op[f"OK-{api_str}"] = op
        
        offset += len(features)
        if offset % 50000 == 0:
            print(f"  OCC: {offset:,} fetched...")
        
        if len(features) < page_size and not data.get("exceededTransferLimit"):
            break
        time.sleep(0.15)
    
    print(f"  OK wells with operator: {len(api_to_op):,}")

    # Step 2: Load operators
    print("\n[2/5] Loading operators from Supabase...")
    operators = fetch_all_operators()
    lookup = build_lookup(operators)
    print(f"  Loaded: {len(operators):,}, lookup entries: {len(lookup):,}")

    # Step 3: Create missing operators
    print("\n[3/5] Creating missing operators...")
    unique_ops = set(api_to_op.values())
    print(f"  Unique operators in OK data: {len(unique_ops):,}")
    
    new_ops = 0
    for op in sorted(unique_ops):
        if not find_op(op, lookup):
            new_id = sb_post_operator(op, country="US")
            if new_id:
                lookup[normalize(op)] = new_id
                new_ops += 1
    print(f"  New operators created: {new_ops}")

    # Step 4: Fetch OK wells from Supabase
    print(f"\n[4/5] Fetching {len(api_to_op):,} OK wells from Supabase...")
    well_map = batch_fetch_wells(list(api_to_op.keys()))
    print(f"  Found in DB: {len(well_map):,}")
    
    already = sum(1 for w in well_map.values() if w["operator_id"] is not None)
    to_link = {api: w for api, w in well_map.items() if w["operator_id"] is None}
    print(f"  Already linked: {already:,}")
    print(f"  Need linking: {len(to_link):,}")

    # Build operator groups
    op_to_wells = defaultdict(list)
    no_match = 0
    for api, well in to_link.items():
        op_name = api_to_op.get(api, "")
        op_id = find_op(op_name, lookup)
        if op_id:
            op_to_wells[op_id].append(well["id"])
        else:
            no_match += 1
    
    print(f"  Operator groups: {len(op_to_wells):,}")
    print(f"  No match: {no_match:,}")

    # Step 5: Patch
    print(f"\n[5/5] Patching {len(to_link) - no_match:,} wells...")
    updated = batch_patch_wells(op_to_wells)
    print(f"  ✓ Updated: {updated:,}")

    print("\n" + "=" * 60)
    print("SUMMARY: OCC_OKLAHOMA")
    print(f"  API records: {len(api_to_op):,}")
    print(f"  In DB: {len(well_map):,}")
    print(f"  Already linked: {already:,}")
    print(f"  Newly linked: {updated:,}")
    print(f"  New operators: {new_ops}")
    print(f"  No match: {no_match:,}")
    print("✓ Done")


if __name__ == "__main__":
    main()
