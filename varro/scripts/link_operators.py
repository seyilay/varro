#!/usr/bin/env python3
"""
Link Sodir Norway + NSTA UK wells to operators in Supabase.
Uses Prefer: statement_timeout=60000 to handle slow unindexed queries.
"""
import csv, json, re, sys, time
import urllib.request, urllib.parse
from urllib.error import HTTPError
from collections import defaultdict

SUPABASE_URL = 'https://temtptsfiksixxhbigkg.supabase.co'
KEY = 'os.environ.get('SUPABASE_KEY')'

# Headers for read (GET) operations
GET_H = {
    'apikey': KEY,
    'Authorization': f'Bearer {KEY}',
    'Prefer': 'statement_timeout=60000',
}

# Headers for write (PATCH/POST) operations
WRITE_H = {
    'apikey': KEY,
    'Authorization': f'Bearer {KEY}',
    'Content-Type': 'application/json',
    'Prefer': 'return=minimal,statement_timeout=60000',
}

SODIR_CSV = '/home/openclaw/.openclaw/workspace/varro/data/raw/sodir/wellbores.csv'

# ─────────────────────────────────────────────
# Supabase helpers
# ─────────────────────────────────────────────

def sb_get_raw(url, retries=5):
    """GET with retry on timeout."""
    for attempt in range(retries):
        req = urllib.request.Request(url, headers=GET_H)
        try:
            return json.loads(urllib.request.urlopen(req, timeout=90).read())
        except HTTPError as e:
            body = e.read().decode()
            if e.code in (500, 503) and attempt < retries - 1:
                wait = 3 ** attempt
                print(f'  [retry {attempt+1}/{retries}] {e.code} - sleeping {wait}s... ({body[:80]})')
                time.sleep(wait)
            else:
                print(f'  FATAL GET {e.code}: {body[:200]}')
                raise

def fetch_all(table, select, filter_='', page_size=500):
    """Offset pagination with retry on timeout."""
    results = []
    offset = 0
    while True:
        if filter_:
            params = f'{filter_}&select={select}&limit={page_size}&offset={offset}'
        else:
            params = f'select={select}&limit={page_size}&offset={offset}'
        url = f'{SUPABASE_URL}/rest/v1/{table}?{params}'
        batch = sb_get_raw(url)
        results.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
        time.sleep(0.2)
    return results

def sb_patch(table, filter_param, data, retries=5):
    url = f'{SUPABASE_URL}/rest/v1/{table}?{filter_param}'
    body = json.dumps(data).encode()
    for attempt in range(retries):
        req = urllib.request.Request(url, data=body, headers=WRITE_H, method='PATCH')
        try:
            urllib.request.urlopen(req, timeout=90).read()
            return True
        except HTTPError as e:
            err = e.read().decode()
            if e.code in (500, 503) and attempt < retries - 1:
                wait = 3 ** attempt
                print(f'  [PATCH retry {attempt+1}] {e.code} - sleeping {wait}s...')
                time.sleep(wait)
            else:
                print(f'  PATCH error {e.code}: {err[:200]}')
                return False

def sb_post(table, data, retries=3):
    """Create a new record, return inserted id."""
    headers = dict(WRITE_H)
    headers['Prefer'] = 'return=representation'
    url = f'{SUPABASE_URL}/rest/v1/{table}'
    body = json.dumps(data).encode()
    for attempt in range(retries):
        req = urllib.request.Request(url, data=body, headers=headers, method='POST')
        try:
            result = json.loads(urllib.request.urlopen(req, timeout=30).read())
            if isinstance(result, list) and result:
                return result[0]['id']
            return None
        except HTTPError as e:
            err = e.read().decode()
            # 409 = conflict (already exists) — try to find by name
            if e.code == 409:
                return None
            if e.code in (500, 503) and attempt < retries - 1:
                time.sleep(2)
            else:
                print(f'  POST error {e.code}: {err[:200]}')
                return None

# ─────────────────────────────────────────────
# Operator normalization
# ─────────────────────────────────────────────

SUFFIX_RE = re.compile(
    r'\b(inc|llc|corp|ltd|limited|as|asa|plc|co|company|gmbh|bv|nv|sa|ag|ab|oy|se|srl|spa|lp|llp|ul)\b\.?$',
    re.IGNORECASE
)

def normalize(name):
    if not name:
        return ''
    n = name.strip().strip('"\'').lower()
    n = re.sub(r'\s+', ' ', n)
    # Remove trailing suffixes repeatedly
    prev = None
    while prev != n:
        prev = n
        n = SUFFIX_RE.sub('', n).strip().rstrip(',').strip()
    return n

def build_operator_lookup(operators):
    """Returns {normalized_name: operator_id}"""
    lookup = {}
    for op in operators:
        norm = normalize(op['name'])
        if norm:
            lookup[norm] = op['id']
    return lookup

def find_operator(name, lookup):
    """Exact normalized match, then substring match."""
    norm = normalize(name)
    if not norm:
        return None
    if norm in lookup:
        return lookup[norm]
    # Substring: find any key that contains norm or is contained in norm
    for key, uid in lookup.items():
        if norm == key:
            return uid
        if len(norm) > 4 and (norm in key or key in norm):
            return uid
    return None

# ─────────────────────────────────────────────
# Batch update wells by operator
# ─────────────────────────────────────────────

def batch_update_wells_by_operator(operator_to_wells, dry_run=False):
    """
    operator_to_wells: {operator_id: [well_id, ...]}
    Updates wells grouped by operator.
    """
    total_updated = 0
    batch_size = 200

    for op_id, well_ids in operator_to_wells.items():
        if isinstance(op_id, str) and op_id.startswith('NEW:'):
            # dry run placeholder
            total_updated += len(well_ids)
            continue
        for i in range(0, len(well_ids), batch_size):
            chunk = well_ids[i:i+batch_size]
            ids_csv = ','.join(chunk)
            filter_param = f'id=in.({ids_csv})'
            if dry_run:
                total_updated += len(chunk)
            else:
                ok = sb_patch('wells', filter_param, {'operator_id': op_id})
                if ok:
                    total_updated += len(chunk)
                else:
                    print(f'  Failed batch for operator {op_id}, chunk {i}')
            time.sleep(0.1)

    return total_updated

# ─────────────────────────────────────────────
# Part A: Sodir Norway
# ─────────────────────────────────────────────

def link_sodir(operators, lookup, dry_run=False):
    print('\n═══ PART A: Sodir Norway ═══')

    # Read CSV: well_name → operator_name
    print('Reading Sodir CSV...')
    csv_ops = {}  # {wellbore_name: operator_name}
    with open(SODIR_CSV, encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            wn = row.get('wlbWellboreName', '').strip()
            op = row.get('wlbDrillingOperator', '').strip()
            if wn:
                csv_ops[wn] = op

    print(f'  CSV rows: {len(csv_ops)}')
    unique_csv_ops = set(v for v in csv_ops.values() if v)
    print(f'  Unique operators in CSV: {len(unique_csv_ops)}')

    # Group CSV by operator → [well_names]
    op_to_wnames = defaultdict(list)
    for wname, opname in csv_ops.items():
        op_to_wnames[opname].append(wname)

    # Fetch all Sodir wells from Supabase
    print('Fetching Sodir wells from Supabase...')
    sodir_wells = fetch_all('wells', 'id,well_name', 'regulatory_jurisdiction=eq.SODIR')
    print(f'  Sodir wells in Supabase: {len(sodir_wells)}')

    # Build well_name → id map
    well_name_to_id = {w['well_name']: w['id'] for w in sodir_wells}

    # Match operators and collect well_id groups
    operator_to_wells = defaultdict(list)
    unmatched_ops = {}  # name → [well_names]
    created_ops = {}    # normalized_name → new_id
    no_op_wells = 0

    for opname, wnames in op_to_wnames.items():
        if not opname:
            no_op_wells += len(wnames)
            continue

        op_id = find_operator(opname, lookup)

        if op_id is None:
            norm = normalize(opname)
            if norm in created_ops:
                op_id = created_ops[norm]
            else:
                if not dry_run:
                    new_id = sb_post('operators', {'name': opname})
                    if new_id:
                        op_id = new_id
                        created_ops[norm] = new_id
                        lookup[norm] = new_id
                        print(f'  Created operator: {opname} → {new_id}')
                    else:
                        print(f'  Failed to create operator: {opname}')
                        unmatched_ops[opname] = wnames
                        continue
                else:
                    op_id = f'NEW:{opname}'
                    created_ops[norm] = op_id
                    unmatched_ops[opname] = wnames

        # Map well names to IDs
        for wname in wnames:
            wid = well_name_to_id.get(wname)
            if wid:
                operator_to_wells[op_id].append(wid)
            # If not found in supabase, skip

    print(f'\n  Wells with no operator in CSV: {no_op_wells}')
    if not dry_run:
        print(f'  New operators created: {len(created_ops)}')
    else:
        print(f'  Would create new operators: {len(unmatched_ops)}')
        if unmatched_ops:
            print('  Unmatched ops:')
            for u in sorted(unmatched_ops.keys())[:15]:
                print(f'    - {u} ({len(unmatched_ops[u])} wells)')
    print(f'  Operator groups to update: {len(operator_to_wells)}')
    total_wells = sum(len(v) for v in operator_to_wells.values())
    print(f'  Total wells to update: {total_wells}')

    print('\nBatch updating Sodir wells...')
    updated = batch_update_wells_by_operator(operator_to_wells, dry_run=dry_run)
    print(f'  ✓ Sodir wells updated: {updated} / {len(sodir_wells)}')

    return {
        'total': len(sodir_wells),
        'linked': updated,
        'unique_operators': len(operator_to_wells),
        'created_operators': len(created_ops),
        'no_op_wells': no_op_wells,
    }

# ─────────────────────────────────────────────
# Part B: NSTA UK
# ─────────────────────────────────────────────

def link_nsta(operators, lookup, dry_run=False):
    print('\n═══ PART B: NSTA UK ═══')
    print('NOTE: NSTA operator data is stored in the `basin` column in Supabase.')

    print('Fetching NSTA wells from Supabase...')
    nsta_wells = fetch_all('wells', 'id,well_name,basin', 'regulatory_jurisdiction=eq.NSTA')
    print(f'  NSTA wells in Supabase: {len(nsta_wells)}')

    with_op = [w for w in nsta_wells if w.get('basin')]
    print(f'  Wells with basin/operator: {len(with_op)}')

    if not with_op:
        print('  No operator data in basin field. NSTA linkage skipped.')
        return {'total': len(nsta_wells), 'linked': 0, 'note': 'No operator data in basin field'}

    # Group by operator name
    op_to_well_ids = defaultdict(list)
    no_op_wells = 0
    for well in nsta_wells:
        opname = (well.get('basin') or '').strip()
        if not opname:
            no_op_wells += 1
            continue
        op_to_well_ids[opname].append(well['id'])

    print(f'  Unique operators: {len(op_to_well_ids)}')
    print(f'  Wells without operator: {no_op_wells}')
    # Sample
    sample = list(op_to_well_ids.keys())[:8]
    print(f'  Sample operators: {sample}')

    # Match operators
    operator_to_wells = defaultdict(list)
    created_ops = {}
    unmatched_ops = {}

    for opname, well_ids in op_to_well_ids.items():
        op_id = find_operator(opname, lookup)

        if op_id is None:
            norm = normalize(opname)
            if norm in created_ops:
                op_id = created_ops[norm]
            else:
                if not dry_run:
                    new_id = sb_post('operators', {'name': opname})
                    if new_id:
                        op_id = new_id
                        created_ops[norm] = new_id
                        lookup[norm] = new_id
                        print(f'  Created operator: {opname} → {new_id}')
                    else:
                        print(f'  Failed to create operator: {opname}')
                        unmatched_ops[opname] = well_ids
                        continue
                else:
                    op_id = f'NEW:{opname}'
                    created_ops[norm] = op_id
                    unmatched_ops[opname] = well_ids

        operator_to_wells[op_id].extend(well_ids)

    if dry_run and unmatched_ops:
        print(f'  Would create {len(unmatched_ops)} new operators:')
        for u in sorted(unmatched_ops.keys())[:10]:
            print(f'    - {u} ({len(unmatched_ops[u])} wells)')

    print(f'\n  Operator groups to update: {len(operator_to_wells)}')
    total_to_update = sum(len(v) for v in operator_to_wells.values())
    print(f'  Total wells to update: {total_to_update}')

    print('\nBatch updating NSTA wells...')
    updated = batch_update_wells_by_operator(operator_to_wells, dry_run=dry_run)
    print(f'  ✓ NSTA wells updated: {updated} / {len(nsta_wells)}')

    return {
        'total': len(nsta_wells),
        'linked': updated,
        'unique_operators': len(operator_to_wells),
        'created_operators': len(created_ops),
        'no_op_wells': no_op_wells,
    }

# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

if __name__ == '__main__':
    dry_run = '--dry-run' in sys.argv

    if dry_run:
        print('🔍 DRY RUN MODE — no changes will be made\n')

    print('Loading operators...')
    operators = fetch_all('operators', 'id,name')
    print(f'  Loaded {len(operators)} operators')

    lookup = build_operator_lookup(operators)
    print(f'  Lookup entries: {len(lookup)}')

    sodir_result = link_sodir(operators, lookup, dry_run=dry_run)
    nsta_result = link_nsta(operators, lookup, dry_run=dry_run)

    print('\n\n══════════════════════════════')
    print('SUMMARY')
    print('══════════════════════════════')
    print(f'Sodir: {sodir_result["linked"]}/{sodir_result["total"]} wells linked')
    print(f'  Unique operators: {sodir_result["unique_operators"]}')
    print(f'  New operators created: {sodir_result["created_operators"]}')
    print(f'  Wells w/o operator in CSV: {sodir_result["no_op_wells"]}')
    print()
    print(f'NSTA: {nsta_result["linked"]}/{nsta_result["total"]} wells linked')
    if 'unique_operators' in nsta_result:
        print(f'  Unique operators: {nsta_result["unique_operators"]}')
        print(f'  New operators created: {nsta_result["created_operators"]}')
        print(f'  Wells w/o operator: {nsta_result["no_op_wells"]}')
    else:
        print(f'  Note: {nsta_result.get("note")}')
