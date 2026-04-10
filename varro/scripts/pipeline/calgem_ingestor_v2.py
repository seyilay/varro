#!/usr/bin/env python3
"""
CalGEM Well Ingestor v2
Fetches ~242k wells from CalGEM WellSTAR ArcGIS REST API and upserts into Supabase.

Two-pass design:
  Pass 1: Collect all unique operators → upsert operators → build code→ID map
  Pass 2: Fetch wells again → upsert wells with operator_id resolved

Schema target (wells table):
  api_number, well_name, operator_id, latitude, longitude, state, county,
  basin, well_type, total_depth_ft, spud_date, status, data_source,
  source_record_id, regulatory_jurisdiction
"""

import requests
import json
import time
import sys
from datetime import datetime

# ── Config ──────────────────────────────────────────────────────────────────
CALGEM_BASE = "https://gis.conservation.ca.gov/server/rest/services/WellSTAR/Wells/MapServer/0/query"
SUPABASE_URL = "https://temtptsfiksixxhbigkg.supabase.co"
SUPABASE_KEY = "os.environ.get('SUPABASE_KEY')"
PAGE_SIZE  = 1000
BATCH_SIZE = 100    # 100-record batches (~2-5s per batch when DB is calm)
BATCH_SLEEP = 0.15  # 150ms between batches

# Set to True to skip Pass 1 and load operators directly from Supabase
# (faster when operators have already been collected in a prior run)
SKIP_OPERATOR_SCAN = True   # operators already in DB from prior run
START_OFFSET = 30000        # resume from this offset (set 0 for full run)

FETCH_FIELDS = (
    "OBJECTID,API,WellDesignation,WellNumber,LeaseName,"
    "WellStatus,WellType,OperatorCode,OperatorName,"
    "FieldName,CountyName,Latitude,Longitude,SpudDate"
)

SB_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

# ── Mappings ─────────────────────────────────────────────────────────────────
STATUS_MAP = {
    "active":                  "PRODUCING",
    "producing":               "PRODUCING",
    "idle":                    "IDLE",
    "new":                     "IDLE",
    "unknown":                 "IDLE",
    "shut-in":                 "SHUT_IN",
    "shut in":                 "SHUT_IN",
    "temp abandoned":          "TA",
    "temporarily abandoned":   "TA",
    "plugged":                 "PA",
    "plugged & abandoned":     "PA",
    "plugged and abandoned":   "PA",
    "abandoned":               "PA",
    "canceled":                "PA",
    "cancelled":               "PA",
}

# Only OG/O → OIL, GAS/DG → GAS; all others (injection, storage, etc.) → None
WELL_TYPE_MAP = {
    "og":  "OIL",
    "o":   "OIL",
    "gas": "GAS",
    "dg":  "GAS",
}

def map_status(raw: str) -> str:
    if not raw:
        return "IDLE"
    return STATUS_MAP.get(raw.lower().strip(), "IDLE")

def map_well_type(raw: str):
    if not raw:
        return None
    return WELL_TYPE_MAP.get(raw.lower().strip(), None)

def parse_date(raw) -> str | None:
    if not raw:
        return None
    if isinstance(raw, (int, float)):
        try:
            return datetime.utcfromtimestamp(raw / 1000).strftime("%Y-%m-%d")
        except Exception:
            return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(str(raw).strip(), fmt).strftime("%Y-%m-%d")
        except Exception:
            continue
    return None


# ── CalGEM API helpers ────────────────────────────────────────────────────────
def fetch_page(offset: int, retries: int = 3):
    """Fetch one page of wells from the CalGEM ArcGIS REST API."""
    params = {
        "where":             "1=1",
        "outFields":         FETCH_FIELDS,
        "orderByFields":     "OBJECTID",
        "f":                 "json",
        "resultOffset":      offset,
        "resultRecordCount": PAGE_SIZE,
    }
    for attempt in range(retries):
        try:
            r = requests.get(
                CALGEM_BASE,
                params=params,
                timeout=90,
                headers={"User-Agent": "VarroARO/1.0"},
            )
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            if attempt < retries - 1:
                wait = 2 ** attempt
                print(f"  [WARN] Fetch error at offset {offset} (attempt {attempt+1}): {exc} — retry in {wait}s")
                time.sleep(wait)
            else:
                print(f"  [ERR]  Fetch FAILED at offset {offset}: {exc}")
                return None


# ── Load operators from Supabase (fast path) ─────────────────────────────────
def load_operators_from_supabase() -> dict:
    """
    Fetch all operators that have an operator_code from Supabase.
    Returns {operator_code: uuid}.
    """
    url = f"{SUPABASE_URL}/rest/v1/operators"
    code_to_id: dict[str, str] = {}
    offset = 0
    page_size = 1000
    while True:
        r = requests.get(
            url,
            params={
                "select": "id,operator_code",
                "operator_code": "not.is.null",
                "limit": page_size,
                "offset": offset,
            },
            headers=SB_HEADERS,
            timeout=30,
        )
        if r.status_code != 200:
            print(f"  [WARN] Operator load error {r.status_code}: {r.text[:200]}")
            break
        batch = r.json()
        for op in batch:
            if op.get("operator_code"):
                code_to_id[op["operator_code"]] = op["id"]
        if len(batch) < page_size:
            break
        offset += page_size
    return code_to_id


# ── Pass 1: Collect operators ─────────────────────────────────────────────────
def collect_operators() -> dict:
    """
    Page through all CalGEM wells once and collect unique OperatorCode→OperatorName.
    Returns {operator_code: operator_name}.
    """
    print("\n── Pass 1: Collecting operators ──")
    operators: dict[str, str] = {}
    offset = 0
    page_num = 0

    while True:
        data = fetch_page(offset)
        if data is None:
            offset += PAGE_SIZE
            continue

        features = data.get("features", [])
        if not features:
            print(f"  No features at offset {offset}; stopping Pass 1.")
            break

        for feat in features:
            a = feat.get("attributes", {})
            code = str(a.get("OperatorCode") or "").strip()
            name = str(a.get("OperatorName") or "").strip()
            if code and name and code not in operators:
                operators[code] = name

        offset += PAGE_SIZE
        page_num += 1

        if page_num % 50 == 0:
            print(f"  Scanned {offset:,} wells — {len(operators)} unique operators so far")
            sys.stdout.flush()

        # Stop when last page returned fewer records than requested
        if len(features) < PAGE_SIZE:
            break

    print(f"  Done. Found {len(operators)} unique operators.")
    return operators


# ── Operator upsert & ID resolution ──────────────────────────────────────────
def upsert_operators(operators: dict) -> dict:
    """
    Upsert operators into Supabase, then query back their UUIDs.
    Returns {operator_code: uuid}.
    """
    print(f"\n── Upserting {len(operators)} operators ──")
    url = f"{SUPABASE_URL}/rest/v1/operators"
    upsert_headers = {
        **SB_HEADERS,
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }

    records = [
        {"name": name, "operator_code": code}
        for code, name in operators.items()
    ]

    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i : i + BATCH_SIZE]
        r = requests.post(url, json=batch, headers=upsert_headers, timeout=60)
        if r.status_code not in (200, 201):
            print(f"  [WARN] Operator batch {i//BATCH_SIZE+1} error {r.status_code}: {r.text[:300]}")
        time.sleep(BATCH_SLEEP)

    print(f"  Operators upserted. Fetching IDs...")

    # Query back IDs in chunks of 200 codes at a time
    code_to_id: dict[str, str] = {}
    code_list = list(operators.keys())
    for i in range(0, len(code_list), 200):
        chunk = code_list[i : i + 200]
        codes_param = "in.(" + ",".join(chunk) + ")"
        r = requests.get(
            url,
            params={"operator_code": codes_param, "select": "id,operator_code"},
            headers=SB_HEADERS,
            timeout=30,
        )
        if r.status_code == 200:
            for op in r.json():
                code_to_id[op["operator_code"]] = op["id"]
        else:
            print(f"  [WARN] ID query error {r.status_code}: {r.text[:200]}")

    print(f"  Resolved {len(code_to_id)}/{len(operators)} operator IDs.")
    return code_to_id


# ── Well transform ────────────────────────────────────────────────────────────
# Fixed key set — ALL records must have EXACTLY these keys (avoids PGRST102)
WELL_KEYS = [
    "api_number", "well_name", "operator_id", "state", "county",
    "basin", "well_type", "total_depth_ft", "spud_date", "status",
    "data_source", "source_record_id", "regulatory_jurisdiction",
    "latitude", "longitude",
]

def transform_feature(feat: dict, code_to_id: dict) -> dict | None:
    """Transform one ArcGIS feature into a Supabase well record."""
    a = feat.get("attributes", {})

    api_raw = str(a.get("API") or "").strip()
    if not api_raw:
        return None  # Skip wells with no API number

    # Construct a display name from designation + lease
    designation = str(a.get("WellDesignation") or a.get("WellNumber") or "").strip()
    lease = str(a.get("LeaseName") or "").strip()
    if designation and lease:
        well_name = f"{lease} #{designation}"
    elif designation:
        well_name = designation
    elif lease:
        well_name = lease
    else:
        well_name = None

    lat = a.get("Latitude")
    lon = a.get("Longitude")
    if lat is not None:
        lat = round(float(lat), 7)
    if lon is not None:
        lon = round(float(lon), 7)

    op_code = str(a.get("OperatorCode") or "").strip()
    operator_id = code_to_id.get(op_code)  # None if not resolved

    rec = {
        "api_number":             api_raw,
        "well_name":              well_name or None,
        "operator_id":            operator_id,
        "state":                  "CA",
        "county":                 str(a.get("CountyName") or "").strip() or None,
        "basin":                  str(a.get("FieldName") or "").strip() or None,
        "well_type":              map_well_type(a.get("WellType")),
        "total_depth_ft":         None,
        "spud_date":              parse_date(a.get("SpudDate")),
        "status":                 map_status(a.get("WellStatus")),
        "data_source":            "CALGEM",
        "source_record_id":       str(a.get("OBJECTID") or ""),
        "regulatory_jurisdiction":"CALGEM",
        "latitude":               lat,
        "longitude":              lon,
    }

    # Guarantee key set matches WELL_KEYS (PGRST102 guard)
    assert set(rec.keys()) == set(WELL_KEYS), f"Key mismatch: {set(rec.keys()) ^ set(WELL_KEYS)}"
    return rec


# ── Supabase well upsert ──────────────────────────────────────────────────────
def upsert_wells_batch(batch: list, retries: int = 3) -> tuple[int, int]:
    """Upsert a batch of well records. Returns (inserted, errors).
    On statement timeout, waits 5s and retries (up to retries attempts)."""
    url = f"{SUPABASE_URL}/rest/v1/wells?on_conflict=api_number"
    headers = {
        **SB_HEADERS,
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    for attempt in range(retries):
        try:
            r = requests.post(url, json=batch, headers=headers, timeout=120)
            if r.status_code in (200, 201):
                return len(batch), 0
            elif r.status_code == 500 and "57014" in r.text:
                if attempt < retries - 1:
                    print(f"  [WARN] Timeout on {len(batch)} records (attempt {attempt+1}), retrying in 5s...")
                    time.sleep(5)
                else:
                    print(f"  [ERR] Timeout after {retries} attempts, skipping {len(batch)} records")
                    return 0, len(batch)
            else:
                print(f"  [ERR] Well upsert {r.status_code}: {r.text[:300]}")
                return 0, len(batch)
        except requests.exceptions.Timeout:
            if attempt < retries - 1:
                print(f"  [WARN] Request timeout (attempt {attempt+1}), retrying in 5s...")
                time.sleep(5)
            else:
                return 0, len(batch)
    return 0, len(batch)


# ── Pass 2: Ingest wells ──────────────────────────────────────────────────────
def ingest_wells(code_to_id: dict, total: int) -> tuple[int, int, int]:
    """
    Page through all CalGEM wells, transform, and upsert in batches of BATCH_SIZE.
    Returns (total_inserted, total_errors, total_skipped).
    """
    print("\n── Pass 2: Ingesting wells ──")
    total_inserted = 0
    total_errors   = 0
    total_skipped  = 0
    offset = START_OFFSET
    if offset > 0:
        print(f"  Resuming from offset {offset:,}")

    while offset < total + PAGE_SIZE:  # small overshoot guard
        data = fetch_page(offset)
        if data is None:
            print(f"  [WARN] Skipping offset {offset} after fetch failure")
            offset += PAGE_SIZE
            continue

        features = data.get("features", [])
        if not features:
            print(f"  No features at offset {offset}; stopping Pass 2.")
            break

        batch = []
        for feat in features:
            rec = transform_feature(feat, code_to_id)
            if rec is None:
                total_skipped += 1
            else:
                batch.append(rec)

        if batch:
            ins, err = upsert_wells_batch(batch)
            total_inserted += ins
            total_errors   += err

        offset += PAGE_SIZE
        time.sleep(BATCH_SLEEP)

        wells_so_far = total_inserted + total_errors + total_skipped
        if wells_so_far > 0 and (wells_so_far // 10_000) > ((wells_so_far - len(features)) // 10_000):
            pct = min(wells_so_far / total * 100, 100)
            print(
                f"  Progress: {wells_so_far:,}/{total:,} ({pct:.1f}%) — "
                f"upserted={total_inserted:,}  errors={total_errors}  skipped={total_skipped}",
                flush=True,
            )

        if len(features) < PAGE_SIZE:
            break

    return total_inserted, total_errors, total_skipped


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    start_ts = datetime.utcnow()
    print(f"=== CalGEM Well Ingestor v2  [{start_ts.strftime('%Y-%m-%d %H:%M:%S')} UTC] ===")
    print(f"Target: {SUPABASE_URL}/rest/v1/wells")

    # Get total well count
    r = requests.get(
        CALGEM_BASE,
        params={"where": "1=1", "returnCountOnly": "true", "f": "json"},
        timeout=30,
        headers={"User-Agent": "VarroARO/1.0"},
    )
    total = r.json().get("count", 0)
    print(f"Total wells in CalGEM: {total:,}")

    # Pass 1 — operators
    if SKIP_OPERATOR_SCAN:
        print("\n── Skipping Pass 1: Loading operators from Supabase ──")
        code_to_id = load_operators_from_supabase()
        print(f"  Loaded {len(code_to_id)} operators from Supabase")
    else:
        operators = collect_operators()
        code_to_id = upsert_operators(operators)

    # Pass 2 — wells
    inserted, errors, skipped = ingest_wells(code_to_id, total)

    end_ts = datetime.utcnow()
    elapsed = (end_ts - start_ts).total_seconds()

    print("\n══════════ DONE ══════════")
    print(f"Duration:          {elapsed/60:.1f} min")
    print(f"Total available:   {total:,}")
    print(f"Upserted:          {inserted:,}")
    print(f"Errors:            {errors:,}")
    print(f"Skipped (no API):  {skipped:,}")

    summary = {
        "run_at":         start_ts.isoformat(),
        "duration_s":     round(elapsed, 1),
        "total_available":total,
        "total_inserted": inserted,
        "total_errors":   errors,
        "total_skipped":  skipped,
        "operators_found":len(operators),
        "operators_resolved": len(code_to_id),
    }
    out_path = "/home/openclaw/.openclaw/workspace/varro/data/raw/calgem/summary_v2.json"
    with open(out_path, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"\nSummary → {out_path}")
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
