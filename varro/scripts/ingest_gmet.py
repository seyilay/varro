#!/usr/bin/env python3
"""
Ingest filtered GMET data into Supabase methane_emitters table.
Upserts in batches of 500 using the Supabase REST API.

Usage:
    python3 ingest_gmet.py

Prerequisites:
    - Run gmet_schema.sql in Supabase SQL editor first
    - Filtered JSON must exist at the INPUT path below
"""
import json
import urllib.request
import urllib.error
import time

SUPABASE_URL = 'https://temtptsfiksixxhbigkg.supabase.co'
SUPABASE_KEY = 'os.environ.get('SUPABASE_KEY')'
INPUT = '/home/openclaw/.openclaw/workspace/varro/data/raw/gmet/gmet_og_filtered.json'
BATCH_SIZE = 500

HEADERS = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json',
    'Prefer': 'resolution=merge-duplicates,return=minimal',
}

def upsert_batch(records):
    url = f'{SUPABASE_URL}/rest/v1/methane_emitters'
    data = json.dumps(records).encode('utf-8')
    req = urllib.request.Request(url, data=data, headers=HEADERS, method='POST')
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, None
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8', errors='replace')
        return e.code, body

def main():
    print(f"Loading filtered data from {INPUT}...")
    with open(INPUT) as f:
        records = json.load(f)
    print(f"Loaded {len(records)} records")

    # Filter out records where project_id is None (can't upsert without UNIQUE key)
    no_id = [r for r in records if not r.get('project_id')]
    with_id = [r for r in records if r.get('project_id')]
    print(f"  With project_id (upsertable): {len(with_id)}")
    print(f"  Without project_id (skipped for upsert): {len(no_id)}")

    # Upsert in batches
    total_ok = 0
    total_err = 0
    batches = [with_id[i:i+BATCH_SIZE] for i in range(0, len(with_id), BATCH_SIZE)]

    for i, batch in enumerate(batches):
        status, err = upsert_batch(batch)
        if status in (200, 201):
            total_ok += len(batch)
            print(f"  Batch {i+1}/{len(batches)}: {len(batch)} records OK")
        else:
            total_err += len(batch)
            print(f"  Batch {i+1}/{len(batches)}: ERROR {status}: {err[:200] if err else 'unknown'}")
            # Small backoff on error
            time.sleep(2)

    # Insert records without project_id individually with null project_id (no UNIQUE conflict)
    # These go in with INSERT only (no upsert key available)
    if no_id:
        print(f"\nInserting {len(no_id)} records without project_id...")
        # Use insert header without merge-duplicates
        insert_headers = {**HEADERS, 'Prefer': 'return=minimal'}
        no_id_batches = [no_id[i:i+BATCH_SIZE] for i in range(0, len(no_id), BATCH_SIZE)]
        for i, batch in enumerate(no_id_batches):
            url = f'{SUPABASE_URL}/rest/v1/methane_emitters'
            data = json.dumps(batch).encode('utf-8')
            req = urllib.request.Request(url, data=data, headers=insert_headers, method='POST')
            try:
                with urllib.request.urlopen(req) as resp:
                    total_ok += len(batch)
                    print(f"  No-ID Batch {i+1}/{len(no_id_batches)}: {len(batch)} records OK")
            except urllib.error.HTTPError as e:
                body = e.read().decode('utf-8', errors='replace')
                total_err += len(batch)
                print(f"  No-ID Batch {i+1}/{len(no_id_batches)}: ERROR {e.code}: {body[:200]}")
                time.sleep(2)

    print(f"\n=== Ingest complete ===")
    print(f"  Inserted/Updated: {total_ok}")
    print(f"  Errors: {total_err}")

if __name__ == '__main__':
    main()
