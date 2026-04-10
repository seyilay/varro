#!/usr/bin/env python3
"""
Part C: Link remaining sources (Colombia, North Dakota, Alaska) to operators.
Strategy: api_number=in.() indexed batch queries.
"""

import json, csv, time, sys, re, ssl
import urllib.request, urllib.error
from collections import defaultdict

SUPABASE_URL = "https://temtptsfiksixxhbigkg.supabase.co"
KEY = "os.environ.get('SUPABASE_KEY')"

READ_H = {"apikey": KEY, "Authorization": f"Bearer {KEY}", "Prefer": "statement_timeout=120000"}
WRITE_H = {"apikey": KEY, "Authorization": f"Bearer {KEY}", "Content-Type": "application/json",
           "Prefer": "return=minimal,statement_timeout=120000"}
POST_H  = {"apikey": KEY, "Authorization": f"Bearer {KEY}", "Content-Type": "application/json",
           "Prefer": "return=representation,statement_timeout=120000"}

COLOMBIA_EXPLORATORIO = "/home/openclaw/.openclaw/workspace/varro/data/raw/colombia/pozos_exploratorios.json"
COLOMBIA_CRUDO        = "/home/openclaw/.openclaw/workspace/varro/data/raw/colombia/campos_crudo.json"

# ─── HTTP helpers ─────────────────────────────────────────────────────────────

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
        print(f"  PATCH error {e.code}: {e.read().decode()[:200]}")
        return False

def sb_post_operator(name, country=None):
    data = {"name": name}
    if country:
        data["country"] = country
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
        if len(batch) < limit:
            break
        offset += limit
        time.sleep(0.1)
    return results

SUFFIX_RE = re.compile(
    r'\b(inc|llc|corp|ltd|limited|as|asa|plc|co|company|gmbh|bv|nv|sa|ag|ab|oy|se|srl|spa|lp|llp)\b\.?$',
    re.IGNORECASE
)
def normalize(name):
    if not name:
        return ""
    n = name.strip().lower()
    n = re.sub(r'\s+', ' ', n)
    prev = None
    while prev != n:
        prev = n
        n = SUFFIX_RE.sub("", n).strip().rstrip(",").strip()
    return n

def build_lookup(operators):
    lookup = {}
    for op in operators:
        norm = normalize(op["name"])
        if norm:
            lookup[norm] = op["id"]
    return lookup

def find_op(name, lookup):
    if not name:
        return None
    norm = normalize(name)
    if norm in lookup:
        return lookup[norm]
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
        time.sleep(0.08)
        if (i // batch_size + 1) % 20 == 0:
            print(f"  Fetched {min(i+batch_size, len(api_list))}/{len(api_list)}...")
    return result

def batch_patch_wells(op_to_well_ids, batch_size=200):
    updated = 0
    for op_id, well_ids in op_to_well_ids.items():
        for i in range(0, len(well_ids), batch_size):
            chunk = well_ids[i:i+batch_size]
            if sb_patch("wells", f"id=in.({','.join(chunk)})", {"operator_id": op_id}):
                updated += len(chunk)
        time.sleep(0.05)
    return updated


# ═══════════════════════════════════════════════════════════════════
# COLOMBIA
# ═══════════════════════════════════════════════════════════════════

def link_colombia(lookup):
    print("\n" + "═"*55)
    print("Colombia (ANH_COLOMBIA)")
    print("═"*55)
    
    # Load exploratory wells
    api_to_op = {}  # {api_number: operator_name}
    
    try:
        wells_exp = json.load(open(COLOMBIA_EXPLORATORIO))
        for w in wells_exp:
            name = (w.get("nombre_de_pozo") or "").strip()
            op = (w.get("operador_actual") or "").strip()
            if name and op:
                # Replicate api_number logic from ingest_colombia.py
                api = f"CO-{name.upper().replace(' ', '_').replace('/', '-')[:50]}"
                api_to_op[api] = op
        print(f"  Exploratory wells: {len(wells_exp)}, with operator: {len(api_to_op)}")
    except Exception as e:
        print(f"  Error loading Colombia data: {e}")
        return {"source": "ANH_COLOMBIA", "error": str(e)}

    unique_ops = set(api_to_op.values())
    print(f"  Unique operators: {len(unique_ops)}")
    
    # Create missing operators
    new_ops = 0
    for op in sorted(unique_ops):
        if not find_op(op, lookup):
            new_id = sb_post_operator(op, country="CO")
            if new_id:
                lookup[normalize(op)] = new_id
                new_ops += 1
    print(f"  New operators created: {new_ops}")
    
    # Fetch wells from Supabase
    print(f"  Fetching {len(api_to_op)} Colombia wells from Supabase...")
    well_map = batch_fetch_wells(list(api_to_op.keys()))
    print(f"  Found: {len(well_map)}")
    
    already = sum(1 for w in well_map.values() if w["operator_id"] is not None)
    to_link = {api: w for api, w in well_map.items() if w["operator_id"] is None}
    print(f"  Already linked: {already}, need linking: {len(to_link)}")
    
    op_to_wells = defaultdict(list)
    no_match = 0
    for api, well in to_link.items():
        op_name = api_to_op.get(api, "")
        op_id = find_op(op_name, lookup)
        if op_id:
            op_to_wells[op_id].append(well["id"])
        else:
            no_match += 1
    
    updated = batch_patch_wells(op_to_wells)
    print(f"  ✓ Updated: {updated}, no_match: {no_match}")
    
    return {"source": "ANH_COLOMBIA", "total_csv": len(api_to_op), "in_db": len(well_map),
            "already": already, "linked": updated, "new_ops": new_ops}


# ═══════════════════════════════════════════════════════════════════
# NORTH DAKOTA (re-fetch from ESRI API)
# ═══════════════════════════════════════════════════════════════════

def fetch_nd_api():
    """Fetch all ND wells with operator from DMR API."""
    base = "https://gis.dmr.nd.gov/dmrpublicservices/rest/services/OilGasPublicMapDataVectorTiles/Wells/FeatureServer/0/query"
    api_to_op = {}
    offset = 0
    page_size = 5000
    
    while True:
        url = f"{base}?where=1=1&outFields=api_no,api,operator&f=json&resultOffset={offset}&resultRecordCount={page_size}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        try:
            data = json.loads(urllib.request.urlopen(req, timeout=45).read())
        except Exception as e:
            print(f"  ND API error at offset {offset}: {e}")
            break
        
        features = data.get("features", [])
        if not features:
            break
        
        for feat in features:
            a = feat["attributes"]
            api_raw = a.get("api_no") or a.get("api")
            op = (a.get("operator") or "").strip()
            if api_raw and op:
                api_num = f"ND-{api_raw}"
                api_to_op[api_num] = op
        
        offset += len(features)
        if offset % 20000 == 0:
            print(f"  ND API: {offset} fetched...")
        
        if len(features) < page_size and not data.get("exceededTransferLimit"):
            break
        time.sleep(0.2)
    
    return api_to_op


def link_north_dakota(lookup):
    print("\n" + "═"*55)
    print("North Dakota (DMR_NORTH_DAKOTA)")
    print("═"*55)
    
    print("  Fetching ND wells from DMR API...")
    api_to_op = fetch_nd_api()
    print(f"  ND wells with operator from API: {len(api_to_op)}")
    
    if not api_to_op:
        print("  No data fetched from ND API.")
        return {"source": "DMR_NORTH_DAKOTA", "error": "No data from API"}
    
    unique_ops = set(api_to_op.values())
    print(f"  Unique operators: {len(unique_ops)}")
    
    # Create missing operators
    new_ops = 0
    for op in sorted(unique_ops):
        if not find_op(op, lookup):
            new_id = sb_post_operator(op, country="US")
            if new_id:
                lookup[normalize(op)] = new_id
                new_ops += 1
    print(f"  New operators created: {new_ops}")
    
    # Batch fetch from Supabase
    print(f"  Fetching {len(api_to_op)} wells from Supabase...")
    well_map = batch_fetch_wells(list(api_to_op.keys()))
    print(f"  Found in DB: {len(well_map)}")
    
    already = sum(1 for w in well_map.values() if w["operator_id"] is not None)
    to_link = {api: w for api, w in well_map.items() if w["operator_id"] is None}
    print(f"  Already linked: {already}, need linking: {len(to_link)}")
    
    op_to_wells = defaultdict(list)
    no_match = 0
    for api, well in to_link.items():
        op_name = api_to_op.get(api, "")
        op_id = find_op(op_name, lookup)
        if op_id:
            op_to_wells[op_id].append(well["id"])
        else:
            no_match += 1
    
    updated = batch_patch_wells(op_to_wells)
    print(f"  ✓ Updated: {updated}, no_match: {no_match}")
    
    return {"source": "DMR_NORTH_DAKOTA", "total_from_api": len(api_to_op), "in_db": len(well_map),
            "already": already, "linked": updated, "new_ops": new_ops, "no_match": no_match}


# ═══════════════════════════════════════════════════════════════════
# ALASKA (re-fetch from ESRI API)
# ═══════════════════════════════════════════════════════════════════

def fetch_alaska_api():
    base = "https://aogweb.state.ak.us/MapService_Public/rest/services/Well_Services/Well_Service_All_Fields/FeatureServer/0/query"
    api_to_op = {}
    offset = 0
    page_size = 2000
    
    while True:
        url = f"{base}?where=1=1&outFields=APINumber,Operator&f=json&resultOffset={offset}&resultRecordCount={page_size}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        try:
            data = json.loads(urllib.request.urlopen(req, timeout=45).read())
        except Exception as e:
            print(f"  AK API error at offset {offset}: {e}")
            break
        
        features = data.get("features", [])
        if not features:
            break
        
        for feat in features:
            a = feat["attributes"]
            api = a.get("APINumber")
            op = (a.get("Operator") or "").strip()
            if api and op:
                api_to_op[f"AK-{api}"] = op
        
        offset += len(features)
        if len(features) < page_size and not data.get("exceededTransferLimit"):
            break
        time.sleep(0.2)
    
    return api_to_op


def link_alaska(lookup):
    print("\n" + "═"*55)
    print("Alaska (AOGCC_ALASKA)")
    print("═"*55)
    
    print("  Fetching AK wells from AOGCC API...")
    try:
        api_to_op = fetch_alaska_api()
        print(f"  AK wells with operator: {len(api_to_op)}")
    except Exception as e:
        print(f"  AK API error: {e}")
        return {"source": "AOGCC_ALASKA", "error": str(e)}
    
    if not api_to_op:
        print("  No data - skipping")
        return {"source": "AOGCC_ALASKA", "in_db": 0, "linked": 0}
    
    unique_ops = set(api_to_op.values())
    print(f"  Unique operators: {len(unique_ops)}")
    
    new_ops = 0
    for op in sorted(unique_ops):
        if not find_op(op, lookup):
            new_id = sb_post_operator(op, country="US")
            if new_id:
                lookup[normalize(op)] = new_id
                new_ops += 1
    print(f"  New operators created: {new_ops}")
    
    print(f"  Fetching {len(api_to_op)} wells from Supabase...")
    well_map = batch_fetch_wells(list(api_to_op.keys()))
    print(f"  Found in DB: {len(well_map)}")
    
    already = sum(1 for w in well_map.values() if w["operator_id"] is not None)
    to_link = {api: w for api, w in well_map.items() if w["operator_id"] is None}
    
    op_to_wells = defaultdict(list)
    no_match = 0
    for api, well in to_link.items():
        op_name = api_to_op.get(api, "")
        op_id = find_op(op_name, lookup)
        if op_id:
            op_to_wells[op_id].append(well["id"])
        else:
            no_match += 1
    
    updated = batch_patch_wells(op_to_wells)
    print(f"  ✓ Updated: {updated}, already linked: {already}, no_match: {no_match}")
    
    return {"source": "AOGCC_ALASKA", "total_from_api": len(api_to_op), "in_db": len(well_map),
            "already": already, "linked": updated, "new_ops": new_ops}


# ═══════════════════════════════════════════════════════════════════
# OKLAHOMA (sample check - too large for full re-fetch here)
# ═══════════════════════════════════════════════════════════════════

def sample_oklahoma():
    """Quick sample to check OK linkage state."""
    print("\n" + "═"*55)
    print("Oklahoma (OCC_OKLAHOMA) — sample check")
    print("═"*55)
    
    # Fetch a small sample from OCC API
    url = "https://gis.occ.ok.gov/server/rest/services/Hosted/RBDMS_WELLS/FeatureServer/220/query?where=1=1&outFields=api,operator&f=json&resultOffset=0&resultRecordCount=50"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        data = json.loads(urllib.request.urlopen(req, timeout=30).read())
        features = data.get("features", [])
        
        sample_apis = []
        for feat in features:
            a = feat["attributes"]
            api_raw = a.get("api")
            if api_raw:
                api_str = str(int(api_raw)).zfill(14)
                sample_apis.append(f"OK-{api_str}")
        
        print(f"  Sample APIs: {sample_apis[:5]}")
        well_map = batch_fetch_wells(sample_apis)
        print(f"  Found in DB: {len(well_map)}/{len(sample_apis)}")
        
        null_ops = sum(1 for w in well_map.values() if w["operator_id"] is None)
        print(f"  Null operator_id: {null_ops}/{len(well_map)}")
        
        return {"source": "OCC_OKLAHOMA", "sample_in_db": len(well_map), "sample_null": null_ops}
    except Exception as e:
        print(f"  Error: {e}")
        return {"source": "OCC_OKLAHOMA", "error": str(e)}


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════

def main():
    print("=" * 55)
    print("Part C: Other Sources")
    print("=" * 55)
    
    print("\nLoading operators from Supabase...")
    operators = fetch_all_operators()
    lookup = build_lookup(operators)
    print(f"  Loaded {len(operators)} operators, {len(lookup)} lookup entries")
    
    results = []
    
    # Colombia
    r = link_colombia(lookup)
    results.append(r)
    
    # North Dakota
    r = link_north_dakota(lookup)
    results.append(r)
    
    # Alaska
    r = link_alaska(lookup)
    results.append(r)
    
    # Oklahoma - sample check only
    r = sample_oklahoma()
    results.append(r)
    
    print("\n\n" + "═"*55)
    print("PART C SUMMARY")
    print("═"*55)
    for r in results:
        src = r.get("source", "?")
        if "error" in r:
            print(f"\n{src}: ERROR - {r['error']}")
        else:
            in_db = r.get("in_db", r.get("total_from_api", 0))
            print(f"\n{src}:")
            for k, v in r.items():
                if k != "source":
                    print(f"  {k}: {v}")
    
    print("\n✓ Done")


if __name__ == "__main__":
    main()
