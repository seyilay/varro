#!/usr/bin/env python3
"""
Link Sodir Norway + BOEM remaining wells to operators.
Strategy: use api_number=in.() batch queries (indexed, fast) instead of data_source filter.

For SODIR: api_number = "NO-{wellboreName}"
For BOEM:  api_number = "{API_WELL_NUMBER}" (raw numeric string)
"""

import csv
import json
import time
import sys
import urllib.request
import urllib.error
from collections import defaultdict
import re

SUPABASE_URL = "https://temtptsfiksixxhbigkg.supabase.co"
KEY = "os.environ.get('SUPABASE_KEY')"

READ_H = {
    "apikey": KEY,
    "Authorization": f"Bearer {KEY}",
    "Prefer": "statement_timeout=120000",
}
WRITE_H = {
    "apikey": KEY,
    "Authorization": f"Bearer {KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal,statement_timeout=120000",
}
POST_H = {
    "apikey": KEY,
    "Authorization": f"Bearer {KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation,statement_timeout=120000",
}

SODIR_CSV = "/home/openclaw/.openclaw/workspace/varro/data/raw/sodir/wellbores.csv"
BOEM_BOREHOLE = "/home/openclaw/.openclaw/workspace/varro/data/raw/boem/Borehole/BoreholeRawData/mv_boreholes_all.txt"
BOEM_COMPANIES = "/home/openclaw/.openclaw/workspace/varro/data/raw/boem/Company/CompanyRawData/mv_companies_all.txt"


# ─── HTTP helpers ────────────────────────────────────────────────────────────

def sb_get(url, retries=5):
    for attempt in range(retries):
        req = urllib.request.Request(url, headers=READ_H)
        try:
            return json.loads(urllib.request.urlopen(req, timeout=130).read())
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            if e.code in (500, 503) and attempt < retries - 1:
                wait = 2 ** attempt
                print(f"  [GET retry {attempt+1}/{retries}] {e.code} ({body[:80]}) — wait {wait}s")
                time.sleep(wait)
            else:
                print(f"  FATAL GET {e.code}: {body[:300]}")
                raise
        except Exception as e:
            if attempt < retries - 1:
                print(f"  [GET error] {e} — retry {attempt+1}")
                time.sleep(2)
            else:
                raise


def sb_patch(table, filter_param, data, retries=5):
    url = f"{SUPABASE_URL}/rest/v1/{table}?{filter_param}"
    body = json.dumps(data).encode()
    for attempt in range(retries):
        req = urllib.request.Request(url, data=body, headers=WRITE_H, method="PATCH")
        try:
            urllib.request.urlopen(req, timeout=130).read()
            return True
        except urllib.error.HTTPError as e:
            err = e.read().decode()
            if e.code in (500, 503) and attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                print(f"  PATCH error {e.code}: {err[:200]}")
                return False


def sb_post_operator(name, country=None, retries=3):
    """Create operator, return id. On 409 conflict, find by name."""
    data = {"name": name}
    if country:
        data["country"] = country
    body = json.dumps(data).encode()
    for attempt in range(retries):
        req = urllib.request.Request(
            f"{SUPABASE_URL}/rest/v1/operators",
            data=body, headers=POST_H, method="POST"
        )
        try:
            result = json.loads(urllib.request.urlopen(req, timeout=30).read())
            if isinstance(result, list) and result:
                return result[0]["id"]
            return None
        except urllib.error.HTTPError as e:
            err = e.read().decode()
            if e.code == 409:
                # Already exists — fetch by exact name
                return find_operator_by_name(name)
            if attempt < retries - 1:
                time.sleep(2)
            else:
                print(f"  POST operator error {e.code}: {err[:200]}")
                return None


def find_operator_by_name(name):
    enc = urllib.request.quote(name, safe="")
    url = f"{SUPABASE_URL}/rest/v1/operators?name=eq.{enc}&select=id"
    try:
        result = sb_get(url)
        if result:
            return result[0]["id"]
    except Exception:
        pass
    return None


def fetch_all_operators():
    """Return list of {id, name}."""
    results = []
    offset = 0
    limit = 1000
    while True:
        url = f"{SUPABASE_URL}/rest/v1/operators?select=id,name&limit={limit}&offset={offset}"
        batch = sb_get(url)
        results.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
        time.sleep(0.1)
    return results


# ─── Operator normalization ───────────────────────────────────────────────────

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
    """Return {normalized_name: id}."""
    lookup = {}
    for op in operators:
        norm = normalize(op["name"])
        if norm:
            lookup[norm] = op["id"]
    return lookup


def find_op(name, lookup):
    """Exact or fuzzy match in lookup."""
    if not name:
        return None
    norm = normalize(name)
    if norm in lookup:
        return lookup[norm]
    # Substring fallback
    for key, uid in lookup.items():
        if len(norm) > 5 and (norm in key or key in norm):
            return uid
    return None


# ─── Batch well queries by api_number ────────────────────────────────────────

def batch_fetch_wells_by_api(api_numbers, batch_size=200):
    """
    Fetch {api_number: {id, operator_id}} for given api_numbers in batches.
    Uses indexed api_number=in.() query.
    """
    result = {}
    total = len(api_numbers)
    api_list = list(api_numbers)

    for i in range(0, total, batch_size):
        chunk = api_list[i:i+batch_size]
        # URL-encode each api_number
        enc_vals = ",".join(urllib.request.quote(v, safe="") for v in chunk)
        url = f"{SUPABASE_URL}/rest/v1/wells?api_number=in.({enc_vals})&select=id,api_number,operator_id"
        try:
            rows = sb_get(url)
            for row in rows:
                result[row["api_number"]] = row
        except Exception as e:
            print(f"  [batch_fetch] Error at batch {i}: {e}")
        time.sleep(0.1)
        if (i // batch_size + 1) % 20 == 0:
            print(f"  Fetched {min(i+batch_size, total)}/{total} well lookups...")

    return result


# ─── Batch patch wells ────────────────────────────────────────────────────────

def batch_patch_wells(op_to_well_ids, batch_size=200):
    """Patch wells grouped by operator_id. Returns count updated."""
    updated = 0
    for op_id, well_ids in op_to_well_ids.items():
        for i in range(0, len(well_ids), batch_size):
            chunk = well_ids[i:i+batch_size]
            ids_csv = ",".join(chunk)
            ok = sb_patch("wells", f"id=in.({ids_csv})", {"operator_id": op_id})
            if ok:
                updated += len(chunk)
            else:
                print(f"  PATCH failed for operator {op_id}, chunk {i}")
            time.sleep(0.05)
    return updated


# ═══════════════════════════════════════════════════════════════════════════════
# PART A: Sodir Norway
# ═══════════════════════════════════════════════════════════════════════════════

def link_sodir(lookup, operators_list):
    print("\n" + "═"*60)
    print("PART A: Sodir Norway")
    print("═"*60)

    # Read CSV
    csv_rows = {}  # {wellbore_name: operator_name}
    with open(SODIR_CSV, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            name = row.get("wlbWellboreName", "").strip()
            op = row.get("wlbDrillingOperator", "").strip()
            if name:
                csv_rows[name] = op

    print(f"  CSV rows: {len(csv_rows)}")
    unique_ops = set(v for v in csv_rows.values() if v)
    print(f"  Unique operators in CSV: {len(unique_ops)}")

    # Create missing operators
    new_ops_created = 0
    for opname in sorted(unique_ops):
        if not find_op(opname, lookup):
            new_id = sb_post_operator(opname, country="NO")
            if new_id:
                lookup[normalize(opname)] = new_id
                new_ops_created += 1
                print(f"  Created operator: {opname}")
            else:
                print(f"  Failed to create: {opname}")

    print(f"  New operators created: {new_ops_created}")

    # Build api_number list for all SODIR wells
    api_to_wbname = {f"NO-{name}": name for name in csv_rows}
    print(f"\n  Fetching {len(api_to_wbname)} SODIR wells from Supabase by api_number...")
    well_map = batch_fetch_wells_by_api(list(api_to_wbname.keys()))
    print(f"  Found {len(well_map)} wells in Supabase")

    # Count already linked
    already_linked = sum(1 for w in well_map.values() if w["operator_id"] is not None)
    to_link = {api: w for api, w in well_map.items() if w["operator_id"] is None}
    print(f"  Already linked: {already_linked}")
    print(f"  Need linking: {len(to_link)}")

    # Group by operator
    op_to_well_ids = defaultdict(list)
    no_op_csv = 0
    no_op_match = 0

    for api_num, well in to_link.items():
        wbname = api_to_wbname.get(api_num)
        opname = csv_rows.get(wbname, "")
        if not opname:
            no_op_csv += 1
            continue
        op_id = find_op(opname, lookup)
        if not op_id:
            no_op_match += 1
            print(f"  No match for operator: {opname}")
            continue
        op_to_well_ids[op_id].append(well["id"])

    print(f"  Wells without operator in CSV: {no_op_csv}")
    print(f"  Wells with unmatched operator: {no_op_match}")
    print(f"  Well groups to patch: {len(op_to_well_ids)}")
    total_to_patch = sum(len(v) for v in op_to_well_ids.values())
    print(f"  Total wells to patch: {total_to_patch}")

    print("\n  Patching wells...")
    updated = batch_patch_wells(op_to_well_ids)
    print(f"  ✓ Updated: {updated}")

    return {
        "source": "SODIR",
        "total_csv": len(csv_rows),
        "total_in_db": len(well_map),
        "already_linked": already_linked,
        "newly_linked": updated,
        "new_operators": new_ops_created,
        "no_op_csv": no_op_csv,
        "no_op_match": no_op_match,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# PART B: BOEM remaining
# ═══════════════════════════════════════════════════════════════════════════════

def link_boem(lookup):
    print("\n" + "═"*60)
    print("PART B: BOEM remaining")
    print("═"*60)

    # Parse borehole file: api → company_name
    print("  Parsing BOEM borehole file...")
    api_to_company = {}
    with open(BOEM_BOREHOLE, newline="", encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f, quoting=csv.QUOTE_ALL):
            api = row.get("API_WELL_NUMBER", "").strip()
            company = row.get("COMPANY_NAME", "").strip()
            if api and company:
                api_to_company[api] = company
    print(f"  Borehole records with company: {len(api_to_company)}")

    unique_companies = set(api_to_company.values())
    print(f"  Unique company names: {len(unique_companies)}")

    # Create missing operators
    new_ops_created = 0
    for company in sorted(unique_companies):
        if not find_op(company, lookup):
            new_id = sb_post_operator(company, country="US")
            if new_id:
                lookup[normalize(company)] = new_id
                new_ops_created += 1
    print(f"  New operators created: {new_ops_created}")

    # Fetch BOEM wells from Supabase by api_number
    print(f"\n  Fetching {len(api_to_company)} BOEM wells from Supabase...")
    well_map = batch_fetch_wells_by_api(list(api_to_company.keys()))
    print(f"  Found {len(well_map)} wells in Supabase")

    already_linked = sum(1 for w in well_map.values() if w["operator_id"] is not None)
    to_link = {api: w for api, w in well_map.items() if w["operator_id"] is None}
    print(f"  Already linked: {already_linked}")
    print(f"  Need linking: {len(to_link)}")

    # Group by operator
    op_to_well_ids = defaultdict(list)
    no_company_match = 0

    for api_num, well in to_link.items():
        company = api_to_company.get(api_num, "")
        op_id = find_op(company, lookup)
        if not op_id:
            no_company_match += 1
            continue
        op_to_well_ids[op_id].append(well["id"])

    print(f"  Unmatched company names: {no_company_match}")
    print(f"  Well groups to patch: {len(op_to_well_ids)}")
    total_to_patch = sum(len(v) for v in op_to_well_ids.values())
    print(f"  Total wells to patch: {total_to_patch}")

    print("\n  Patching BOEM wells...")
    updated = batch_patch_wells(op_to_well_ids)
    print(f"  ✓ Updated: {updated}")

    return {
        "source": "BOEM",
        "total_csv": len(api_to_company),
        "total_in_db": len(well_map),
        "already_linked": already_linked,
        "newly_linked": updated,
        "new_operators": new_ops_created,
        "no_company_match": no_company_match,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# PART C: Other small sources
# ═══════════════════════════════════════════════════════════════════════════════

def check_source_files():
    """Check which source data has operator info."""
    import os
    raw_base = "/home/openclaw/.openclaw/workspace/varro/data/raw"
    sources = {}

    # OCC Oklahoma
    ok_dir = os.path.join(raw_base, "oklahoma")
    if os.path.exists(ok_dir):
        files = os.listdir(ok_dir)
        sources["OCC_OKLAHOMA"] = files
    
    # DMR North Dakota
    nd_dir = os.path.join(raw_base, "north_dakota")
    if os.path.exists(nd_dir):
        files = os.listdir(nd_dir)
        sources["DMR_NORTH_DAKOTA"] = files
    
    # ANH Colombia
    co_dir = os.path.join(raw_base, "colombia")
    if os.path.exists(co_dir):
        files = os.listdir(co_dir)
        sources["ANH_COLOMBIA"] = files
    
    # CalGEM
    ca_dir = os.path.join(raw_base, "calgem")
    if os.path.exists(ca_dir):
        files = os.listdir(ca_dir)
        sources["CALGEM"] = files

    return sources


def sample_wells_for_source(data_source_tag, sample_api_numbers, batch_size=100):
    """
    Check if wells with these api_numbers have null operator_id.
    Returns (total_found, null_count)
    """
    if not sample_api_numbers:
        return 0, 0
    
    well_map = batch_fetch_wells_by_api(sample_api_numbers[:500])  # sample only
    total = len(well_map)
    null_count = sum(1 for w in well_map.values() if w["operator_id"] is None)
    return total, null_count


def link_oklahoma(lookup):
    """Link OCC_OKLAHOMA wells using ingest_us_states data structure."""
    import os
    
    ok_dir = "/home/openclaw/.openclaw/workspace/varro/data/raw/oklahoma"
    if not os.path.exists(ok_dir):
        print("  No Oklahoma raw data dir found.")
        return None
    
    files = os.listdir(ok_dir)
    print(f"  Oklahoma files: {files}")
    
    # Look for CSV files with operator info
    operator_col_guesses = ["operator", "operator_name", "company", "company_name", "OPERATOR"]
    
    for fname in files:
        if fname.endswith(".csv") or fname.endswith(".txt"):
            fpath = os.path.join(ok_dir, fname)
            try:
                with open(fpath, encoding="utf-8", errors="replace") as f:
                    reader = csv.DictReader(f)
                    headers = reader.fieldnames or []
                    print(f"  {fname} headers: {headers[:20]}")
                    # Check first few rows
                    for i, row in enumerate(reader):
                        if i < 2:
                            # Find operator-like column
                            for col in headers:
                                if "oper" in col.lower() or "company" in col.lower():
                                    print(f"    {col}: {row.get(col, 'N/A')}")
                        else:
                            break
            except Exception as e:
                print(f"  Error reading {fname}: {e}")
    
    return None


def link_north_dakota(lookup):
    """Link DMR_NORTH_DAKOTA wells."""
    import os
    
    nd_dir = "/home/openclaw/.openclaw/workspace/varro/data/raw/north_dakota"
    if not os.path.exists(nd_dir):
        print("  No ND raw data dir found.")
        return None
    
    files = os.listdir(nd_dir)
    print(f"  North Dakota files: {files[:10]}")
    
    for fname in files[:5]:  # Check first few
        if fname.endswith(".csv") or fname.endswith(".txt"):
            fpath = os.path.join(nd_dir, fname)
            try:
                with open(fpath, encoding="utf-8", errors="replace") as f:
                    reader = csv.DictReader(f)
                    headers = reader.fieldnames or []
                    print(f"  {fname} headers: {headers[:20]}")
                    for i, row in enumerate(reader):
                        if i < 2:
                            for col in headers:
                                if "oper" in col.lower() or "company" in col.lower():
                                    print(f"    {col}: {row.get(col, 'N/A')}")
                        else:
                            break
            except Exception as e:
                print(f"  Error reading {fname}: {e}")
    
    return None


def check_us_states_operator_fields():
    """Check the ingest_us_states script for operator field handling."""
    try:
        with open("/home/openclaw/.openclaw/workspace/varro/ingest_us_states.py") as f:
            content = f.read()
        # Find operator-related code
        lines = content.split("\n")
        op_lines = [(i+1, l) for i, l in enumerate(lines) if "oper" in l.lower() and "operator" in l.lower()]
        return op_lines[:20]
    except Exception as e:
        return [f"Error: {e}"]


def link_part_c(lookup):
    print("\n" + "═"*60)
    print("PART C: Other Sources")
    print("═"*60)
    
    results = []
    
    # Check ingest_us_states.py for operator handling
    print("\nChecking ingest_us_states.py for operator fields...")
    op_lines = check_us_states_operator_fields()
    for lineno, line in op_lines[:10]:
        print(f"  L{lineno}: {line.strip()}")
    
    # Check Oklahoma source files
    print("\n[OCC_OKLAHOMA]")
    link_oklahoma(lookup)
    
    # Check North Dakota
    print("\n[DMR_NORTH_DAKOTA]")
    link_north_dakota(lookup)
    
    # Check Colombia
    print("\n[ANH_COLOMBIA]")
    import os
    co_dir = "/home/openclaw/.openclaw/workspace/varro/data/raw/colombia"
    if os.path.exists(co_dir):
        files = os.listdir(co_dir)
        print(f"  Colombia files: {files[:10]}")
        for fname in files[:3]:
            fpath = os.path.join(co_dir, fname)
            if fname.endswith(".csv"):
                try:
                    with open(fpath, encoding="utf-8", errors="replace") as f:
                        r = csv.DictReader(f)
                        headers = r.fieldnames or []
                        print(f"  {fname} headers: {headers[:20]}")
                except Exception as e:
                    print(f"  Error: {e}")
    
    # Check CalGEM
    print("\n[CALGEM]")
    cg_dir = "/home/openclaw/.openclaw/workspace/varro/data/raw/calgem"
    if os.path.exists(cg_dir):
        files = os.listdir(cg_dir)
        print(f"  CalGEM files: {files[:10]}")
    
    return results


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 60)
    print("Varro: Link Wells to Operators")
    print("=" * 60)
    
    # Load all operators
    print("\nLoading operators from Supabase...")
    operators = fetch_all_operators()
    print(f"  Loaded {len(operators)} operators")
    lookup = build_lookup(operators)
    print(f"  Lookup entries: {len(lookup)}")
    
    # Part A: SODIR
    sodir_result = link_sodir(lookup, operators)
    
    # Part B: BOEM
    boem_result = link_boem(lookup)
    
    # Part C: Other sources
    link_part_c(lookup)
    
    # Final summary
    print("\n\n" + "═" * 60)
    print("FINAL SUMMARY")
    print("═" * 60)
    
    for r in [sodir_result, boem_result]:
        print(f"\n{r['source']}:")
        print(f"  CSV/source records: {r['total_csv']:,}")
        print(f"  Wells found in DB: {r['total_in_db']:,}")
        print(f"  Already linked: {r['already_linked']:,}")
        print(f"  Newly linked: {r['newly_linked']:,}")
        print(f"  New operators created: {r['new_operators']:,}")
    
    print("\n✓ Done")


if __name__ == "__main__":
    main()
