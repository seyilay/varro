#!/usr/bin/env python3
"""
Pennsylvania DEP Well Ingestor
Source: PASDA PSU MapServer - Oil Gas Locations Conventional Unconventional (layer 22)
Target: Supabase wells table
Run:    python3 pennsylvania_ingestor.py
"""

import json
import time
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timezone

# ─── Config ───────────────────────────────────────────────────────────────────
SUPABASE_URL = "https://temtptsfiksixxhbigkg.supabase.co"
SUPABASE_KEY = "os.environ.get('SUPABASE_KEY')"

ARCGIS_BASE = (
    "https://mapservices.pasda.psu.edu/server/rest/services/pasda/DEP/MapServer/22"
)
ARCGIS_FIELDS = (
    "PERMIT_NUM,WELL_NAME,OPERATOR,WELL_TYPE,WELL_STATU,"
    "SPUD_DATE,COUNTY,LATITUDE,LONGITUDE,TOTAL_MAXI,SITE_ID"
)

FETCH_PAGE_SIZE = 1000   # ArcGIS max records per page
UPSERT_BATCH   = 500     # Batch size for Supabase inserts
SLEEP_AFTER_UPSERT = 0.1 # 100ms between Supabase writes

DATA_SOURCE  = "PA_DEP"
JURISDICTION = "PA_DEP"
STATE        = "Pennsylvania"

BASE_HEADERS = {
    "apikey":        SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
}
WRITE_HEADERS = {
    **BASE_HEADERS,
    "Content-Type": "application/json",
    "Prefer":       "resolution=merge-duplicates,return=minimal",
}

# ─── Status mapping ───────────────────────────────────────────────────────────
def map_status(raw: str) -> "str | None":
    if not raw:
        return None
    r = raw.strip().lower()
    if "producing" in r:
        return "PRODUCING"
    if "idle" in r:
        return "IDLE"
    if "shut" in r:
        return "SHUT_IN"
    if "temp" in r and "abandon" in r:
        return "TA"
    if "temporarily" in r:
        return "TA"
    if "plugged" in r or "abandoned" in r:
        return "PA"
    return None

# ─── Helpers ──────────────────────────────────────────────────────────────────
def epoch_ms_to_date(ms) -> "str | None":
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).strftime(
            "%Y-%m-%d"
        )
    except Exception:
        return None

def parse_depth(raw) -> "float | None":
    if raw is None:
        return None
    try:
        v = float(str(raw).strip())
        return v if v > 0 else None
    except Exception:
        return None

def http_get_json(url: str) -> dict:
    req = urllib.request.Request(
        url, headers={"User-Agent": "varro-ingestor/1.0"}
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())

def sb_post(path: str, payload: list, extra_prefer: str = "") -> bytes:
    url = f"{SUPABASE_URL}{path}"
    prefer = "resolution=merge-duplicates,return=minimal"
    if extra_prefer:
        prefer = extra_prefer
    headers = {**BASE_HEADERS, "Content-Type": "application/json", "Prefer": prefer}
    body = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.read()
    except urllib.error.HTTPError as e:
        err_body = e.read().decode()
        raise RuntimeError(f"HTTP {e.code} @ {path}: {err_body[:400]}")

def sb_get(path: str) -> list:
    url = f"{SUPABASE_URL}{path}"
    req = urllib.request.Request(url, headers=BASE_HEADERS)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())

# ─── Operator cache & lookup ───────────────────────────────────────────────────
_op_cache: dict = {}   # name → id

def _load_operators_by_names(names: list) -> None:
    """Fetch existing operators from Supabase for a list of names."""
    if not names:
        return
    # chunk to avoid URL length limits
    chunk_size = 80
    for i in range(0, len(names), chunk_size):
        chunk = names[i : i + chunk_size]
        encoded = ",".join(urllib.parse.quote(n, safe="") for n in chunk)
        path = f"/rest/v1/operators?name=in.({encoded})&select=id,name&limit={len(chunk)+5}"
        try:
            rows = sb_get(path)
            for row in rows:
                _op_cache[row["name"]] = row["id"]
        except Exception as exc:
            print(f"  [WARN] operator GET chunk failed: {exc}")

def ensure_operators(names: list) -> None:
    """Ensure all operators in `names` are in the cache (creates missing ones)."""
    unique = list({n.strip() for n in names if n and n.strip()})
    if not unique:
        return

    missing = [n for n in unique if n not in _op_cache]
    if not missing:
        return

    # Fetch existing
    _load_operators_by_names(missing)

    # Create still-missing ones
    still_missing = [n for n in missing if n not in _op_cache]
    if not still_missing:
        return

    # Insert one at a time to get back the IDs (no unique constraint so can't bulk upsert)
    for name in still_missing:
        try:
            raw = sb_post(
                "/rest/v1/operators",
                [{"name": name}],
                extra_prefer="return=representation",
            )
            rows = json.loads(raw)
            if rows:
                _op_cache[name] = rows[0]["id"]
        except RuntimeError as exc:
            print(f"  [WARN] operator insert failed for '{name}': {exc}")
            # Try GET one more time
            try:
                rows = sb_get(
                    f"/rest/v1/operators?name=eq.{urllib.parse.quote(name,safe='')}&select=id,name&limit=1"
                )
                if rows:
                    _op_cache[name] = rows[0]["id"]
            except Exception:
                pass
        time.sleep(0.05)

# ─── ArcGIS fetch ─────────────────────────────────────────────────────────────
def fetch_arcgis_page(offset: int) -> list:
    params = urllib.parse.urlencode(
        {
            "where": "1=1",
            "outFields": ARCGIS_FIELDS,
            "resultOffset": offset,
            "resultRecordCount": FETCH_PAGE_SIZE,
            "f": "json",
        }
    )
    url = f"{ARCGIS_BASE}/query?{params}"
    data = http_get_json(url)
    return data.get("features", [])

# ─── Feature → well dict ──────────────────────────────────────────────────────
# Known-safe well_type values (check constraint allows these)
VALID_WELL_TYPES = {"OIL", "GAS", "INJECTION"}

# Map PA DEP well_type strings → safe DB values
# Unknown/non-standard types are mapped to None to avoid check-constraint violations
WELL_TYPE_MAP = {
    "OIL":             "OIL",
    "GAS":             "GAS",
    "COALBED METHANE": "GAS",   # it's a gas well
    "COMB. OIL&GAS":   "OIL",   # combo, use OIL
    "INJECTION":       "INJECTION",
    "DRY HOLE":        None,    # check constraint doesn't allow this value
    "TEST WELL":       None,
    "UNDETERMINED":    None,
    "STORAGE WELL":    None,
    "WASTE DISPOSAL":  None,
    "OBSERVATION":     None,
}

def map_well_type(raw: str) -> "str | None":
    if not raw:
        return None
    key = raw.strip().upper()
    # Explicit mapping first
    if key in WELL_TYPE_MAP:
        return WELL_TYPE_MAP[key]
    # If it happens to be a known safe value, pass it through
    if key in VALID_WELL_TYPES:
        return key
    # Unknown → null to avoid constraint failures
    return None


def feature_to_well(attrs: dict) -> dict:
    site_id = attrs.get("SITE_ID")
    permit  = (attrs.get("PERMIT_NUM") or "").strip() or None

    # Use permit as api_number; fall back to synthetic ID so it's always unique
    api_num = permit if permit else (f"PA-DEP-{site_id}" if site_id else None)

    op_name = (attrs.get("OPERATOR") or "").strip() or None
    op_id   = _op_cache.get(op_name) if op_name else None

    return {
        "api_number":              api_num,
        "well_name":               (attrs.get("WELL_NAME") or "").strip() or None,
        "operator_id":             op_id,
        "latitude":                attrs.get("LATITUDE") or None,
        "longitude":               attrs.get("LONGITUDE") or None,
        "state":                   STATE,
        "county":                  (attrs.get("COUNTY") or "").strip() or None,
        "well_type":               map_well_type(attrs.get("WELL_TYPE") or ""),
        "total_depth_ft":          parse_depth(attrs.get("TOTAL_MAXI")),
        "spud_date":               epoch_ms_to_date(attrs.get("SPUD_DATE")),
        "status":                  map_status(attrs.get("WELL_STATU") or ""),
        "data_source":             DATA_SOURCE,
        "source_record_id":        str(site_id) if site_id is not None else api_num,
        "regulatory_jurisdiction": JURISDICTION,
    }

# ─── Supabase insert (with retry) ────────────────────────────────────────────
def upsert_wells(batch: list) -> None:
    """Insert batch of wells. Uses plain INSERT (no on_conflict) for speed.
    Retries on 500/timeout with exponential backoff."""
    max_attempts = 4
    for attempt in range(max_attempts):
        try:
            # Plain insert — fastest on large tables
            sb_post("/rest/v1/wells", batch)
            return
        except RuntimeError as exc:
            msg = str(exc)
            if "57014" in msg or "timeout" in msg.lower():
                # Statement timeout — back off and retry
                wait = 2 ** attempt
                print(f"    Timeout on attempt {attempt+1}, backing off {wait}s...")
                time.sleep(wait)
                continue
            elif "23505" in msg or "unique" in msg.lower():
                # Duplicate key — skip (these are re-runs)
                print(f"    Duplicate batch skipped (already inserted)")
                return
            else:
                raise
    # If all retries fail, raise to let caller count it as error
    raise RuntimeError(f"All {max_attempts} attempts failed for batch of {len(batch)}")

# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    print("=== Pennsylvania DEP Well Ingestor ===")
    print(f"Source: PASDA PSU MapServer/22  ({ARCGIS_BASE})")
    print(f"Target: {SUPABASE_URL}/rest/v1/wells")
    print()

    # Total records
    count_url = (
        f"{ARCGIS_BASE}/query?where=1%3D1&returnCountOnly=true&f=json"
    )
    total = http_get_json(count_url).get("count", 0)
    print(f"ArcGIS total: {total:,} records")
    pages = (total + FETCH_PAGE_SIZE - 1) // FETCH_PAGE_SIZE
    print(f"Pages to fetch: {pages} (page size {FETCH_PAGE_SIZE})")
    print()

    inserted   = 0
    err_count  = 0
    # Resume support: skip already-inserted records (12,000 already done)
    START_OFFSET = 12000
    offset     = START_OFFSET
    print(f"Resuming from offset {START_OFFSET:,} (first {START_OFFSET:,} already inserted)")
    well_buf: list = []

    while offset < total:
        # ── Fetch page from ArcGIS ─────────────────────────────────────────
        try:
            features = fetch_arcgis_page(offset)
        except Exception as exc:
            print(f"  [WARN] ArcGIS fetch error at offset {offset}: {exc}")
            time.sleep(3)
            err_count += 1
            offset += FETCH_PAGE_SIZE
            continue

        if not features:
            print(f"  Empty page at offset {offset}, done.")
            break

        # ── Pre-load operators ────────────────────────────────────────────
        op_names = [
            (f["attributes"].get("OPERATOR") or "").strip()
            for f in features
        ]
        try:
            ensure_operators(op_names)
        except Exception as exc:
            print(f"  [WARN] Operator ensure failed: {exc}")

        # ── Map features → well dicts ─────────────────────────────────────
        for feat in features:
            well_buf.append(feature_to_well(feat["attributes"]))

        # ── Flush when buffer big enough ──────────────────────────────────
        while len(well_buf) >= UPSERT_BATCH:
            batch     = well_buf[:UPSERT_BATCH]
            well_buf  = well_buf[UPSERT_BATCH:]
            try:
                upsert_wells(batch)
                inserted += len(batch)
                pct = 100 * (offset + FETCH_PAGE_SIZE) / total
                print(
                    f"  [{pct:5.1f}%] Upserted {inserted:,} wells "
                    f"(fetched up to offset {offset + FETCH_PAGE_SIZE:,})"
                )
            except Exception as exc:
                print(f"  [ERROR] Upsert failed: {exc}")
                err_count += 1
            time.sleep(SLEEP_AFTER_UPSERT)

        offset += FETCH_PAGE_SIZE
        time.sleep(0.05)  # gentle on PASDA

    # ── Final flush ──────────────────────────────────────────────────────────
    if well_buf:
        try:
            upsert_wells(well_buf)
            inserted += len(well_buf)
            print(f"  Final flush: {len(well_buf)} wells")
        except Exception as exc:
            print(f"  [ERROR] Final flush failed: {exc}")
            err_count += 1

    print()
    print("═" * 40)
    print(f"✓  Wells upserted : {inserted:,}")
    print(f"   Operators cached: {len(_op_cache):,}")
    print(f"   Errors           : {err_count}")

    # ── Final count from Supabase ─────────────────────────────────────────────
    try:
        sample = sb_get(
            "/rest/v1/wells?data_source=eq.PA_DEP&select=id&limit=1&offset=0"
        )
        print(f"   Supabase PA_DEP sample check: {len(sample)} row(s) returned")
    except Exception as exc:
        print(f"   Count check skipped: {exc}")

    print("═" * 40)


if __name__ == "__main__":
    main()
