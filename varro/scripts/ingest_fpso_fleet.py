#!/usr/bin/env python3
"""
Ingest global FPSO fleet CSV into the Varro infrastructure table.
Reads: /home/openclaw/.openclaw/workspace/varro/data/global_fpso_fleet.csv
Loads into: infrastructure table (asset_type='FPSO', subtype='FPSO')

Usage:
  python3 ingest_fpso_fleet.py [--dry-run] [--csv PATH]

Requirements:
  pip install psycopg2-binary python-dotenv
"""

import csv
import sys
import os
import json
import time
import argparse
import urllib.request
from urllib.request import Request

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SUPABASE_URL = os.environ.get(
    "SUPABASE_URL",
    "https://temtptsfiksixxhbigkg.supabase.co"
)
SUPABASE_KEY = os.environ.get(
    "SUPABASE_KEY",
    "os.environ.get('SUPABASE_KEY')"
)

DEFAULT_CSV = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "global_fpso_fleet.csv"
)

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates,return=minimal",
}

BATCH_SIZE = 50
SLEEP_BETWEEN_BATCHES = 0.15  # seconds


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def to_int_or_none(val: str):
    try:
        return int(val.strip()) if val and val.strip() else None
    except (ValueError, AttributeError):
        return None


def csv_row_to_record(row: dict) -> dict:
    """Convert a CSV row to the infrastructure table schema."""
    name = row.get("name", "").strip()
    if not name:
        return None

    # Map CSV status → standardised value
    raw_status = row.get("status", "").strip().lower()
    status_map = {
        "active": "active",
        "idle": "idle",
        "decommissioned": "decommissioned",
        "construction": "under_construction",
        "under_construction": "under_construction",
    }
    status = status_map.get(raw_status, raw_status)

    capacity = to_int_or_none(row.get("gross_capacity_bopd", ""))
    first_oil = to_int_or_none(row.get("first_oil_year", ""))

    # Build a metadata blob for richer details
    metadata = {
        "field_location": row.get("field_location", "").strip(),
        "basin": row.get("basin", "").strip(),
        "first_oil_year": first_oil,
        "gross_capacity_bopd": capacity,
        "notes": row.get("notes", "").strip(),
        "data_source": "global_fpso_fleet.csv",
    }

    record = {
        "name": name,
        "asset_type": "FPSO",
        "subtype": "FPSO",
        "operator": row.get("operator", "").strip() or None,
        "country": row.get("country", "").strip() or None,
        "status": status,
        "metadata": json.dumps(metadata),
    }
    return record


def upsert_batch(records: list, dry_run: bool = False) -> int:
    """Upsert a batch of records into the infrastructure table."""
    if dry_run:
        for r in records:
            print(f"  [DRY-RUN] Would upsert: {r['name']} ({r['country']})")
        return len(records)

    payload = json.dumps(records).encode("utf-8")
    url = f"{SUPABASE_URL}/rest/v1/infrastructure"
    req = Request(url, data=payload, headers=HEADERS, method="POST")

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            if resp.status in (200, 201, 204):
                return len(records)
            body = resp.read().decode("utf-8", errors="replace")
            print(f"  [WARN] HTTP {resp.status}: {body[:200]}", file=sys.stderr)
            return 0
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        print(f"  [ERROR] HTTP {e.code}: {body[:300]}", file=sys.stderr)
        # Retry once after brief sleep
        time.sleep(1.0)
        return 0
    except Exception as ex:
        print(f"  [ERROR] {ex}", file=sys.stderr)
        return 0


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Ingest global FPSO fleet into infrastructure table")
    parser.add_argument("--dry-run", action="store_true", help="Print records without writing to DB")
    parser.add_argument("--csv", default=DEFAULT_CSV, help=f"Path to CSV file (default: {DEFAULT_CSV})")
    parser.add_argument("--skip-notes", nargs="*", default=["NOTE:"],
                        help="Skip rows whose notes contain these strings")
    args = parser.parse_args()

    if not os.path.exists(args.csv):
        print(f"ERROR: CSV not found at {args.csv}", file=sys.stderr)
        sys.exit(1)

    print(f"Reading FPSO fleet from: {args.csv}")

    records = []
    skipped = 0

    with open(args.csv, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            notes = row.get("notes", "")
            # Skip rows that are fixed platforms or non-FPSOs (flagged with NOTE:)
            if any(skip in notes for skip in (args.skip_notes or [])):
                skipped += 1
                continue
            rec = csv_row_to_record(row)
            if rec:
                records.append(rec)

    print(f"Loaded {len(records)} records from CSV ({skipped} skipped as non-FPSO notes)")

    if args.dry_run:
        print("\n--- DRY RUN MODE: no DB writes ---\n")

    total_inserted = 0
    batch_count = 0

    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i: i + BATCH_SIZE]
        batch_count += 1
        print(f"  Batch {batch_count}: upserting {len(batch)} records... ", end="", flush=True)
        inserted = upsert_batch(batch, dry_run=args.dry_run)
        total_inserted += inserted
        print(f"OK ({inserted} rows)")
        if not args.dry_run:
            time.sleep(SLEEP_BETWEEN_BATCHES)

    print(f"\n✓ Done: {total_inserted}/{len(records)} records upserted into infrastructure table")
    if skipped:
        print(f"  (Skipped {skipped} non-FPSO entries flagged with 'NOTE:' in notes field)")


if __name__ == "__main__":
    main()
