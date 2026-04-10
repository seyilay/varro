#!/usr/bin/env python3
"""
Wyoming WOGCC Well Ingestor
Source: WyGISC GeoHub - WOGCC Active Wells MapServer
URL:    https://services.wygisc.org/HostGIS/rest/services/GeoHub/WOGCCActiveWells/MapServer/0
Target: Supabase wells table
"""

import json
import sys
import time
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timezone

# ─── Config ───────────────────────────────────────────────────────────────────
SUPABASE_URL = "https://temtptsfiksixxhbigkg.supabase.co"
SUPABASE_KEY = "os.environ.get('SUPABASE_KEY')"

ARCGIS_BASE   = "https://services.wygisc.org/HostGIS/rest/services/GeoHub/WOGCCActiveWells/MapServer/0"
ARCGIS_FIELDS = "API_NUMBER,WN,COMPANY,WELL_CLASS,STATUS,TD,SPUD,COUNTY,LATITUDE,LONGITUDE"

FETCH_PAGE_SIZE = 1000   # server max
UPSERT_BATCH    = 500
SLEEP_MS        = 0.1

DATA_SOURCE  = "WOGCC_WY"
JURISDICTION = "WOGCC"
STATE        = "Wyoming"

SB_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates,return=minimal",
}
SB_GET_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
}
SB_POST_REPR_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation",
}

# Wyoming county FIPS (odd) → name
WY_COUNTY = {
    1: "Albany", 3: "Big Horn", 5: "Campbell", 7: "Carbon",
    9: "Converse", 11: "Crook", 13: "Fremont", 15: "Goshen",
    17: "Hot Springs", 19: "Johnson", 21: "Laramie", 23: "Lincoln",
    25: "Natrona", 27: "Niobrara", 29: "Park", 31: "Platte",
    33: "Sheridan", 35: "Sublette", 37: "Sweetwater", 39: "Teton",
    41: "Uinta", 43: "Washakie", 45: "Weston",
}

# ─── Status mapping ───────────────────────────────────────────────────────────
STATUS_MAP = {
    "PR": "PRODUCING", "PA": "PA", "SI": "SHUT_IN",
    "TA": "TA", "AI": "IDLE", "AP": "PRODUCING",
    "PH": "SHUT_IN", "SR": "SHUT_IN", "PS": "PRODUCING",
    "FL": None, "WP": None, "NI": None, "WD": None,
    "DR": None, "GL": None, "SO": None, "EP": None, "DP": None,
    "PL": None, "PG": None, "UK": None, "SP": None, "MW": None,
}


WELL_TYPE_MAP = {
    "O": "OIL", "G": "GAS", "GS": "GAS",
}

def map_well_type(raw: str) -> str | None:
    if not raw:
        return None
    return WELL_TYPE_MAP.get(raw.strip().upper())

def map_status(raw: str) -> str | None:
    if not raw:
        return None
    return STATUS_MAP.get(raw.strip().upper())

# ─── SPUD date: YYYYMM int → ISO date ────────────────────────────────────────
def parse_spud(raw) -> str | None:
    if raw is None:
        return None
    try:
        s = str(int(raw)).zfill(6)
        if len(s) == 6:
            y, m = int(s[:4]), int(s[4:6])
            if 1850 <= y <= 2100 and 1 <= m <= 12:
                return f"{y:04d}-{m:02d}-01"
    except Exception:
        pass
    return None

def parse_depth(raw) -> float | None:
    if raw is None:
        return None
    try:
        v = float(raw)
        return int(v) if v > 0 else None
    except Exception:
        return None

# ─── HTTP helpers ─────────────────────────────────────────────────────────────
def http_get_json(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "varro-ingestor/1.0"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())

def http_post_json(url: str, data: list, headers: dict) -> bytes:
    body = json.dumps(data).encode()
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.read()
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"HTTP {e.code}: {e.read().decode()[:400]}")

# ─── Operator management (GET-then-POST, no on_conflict) ─────────────────────
_op_cache: dict[str, str] = {}  # name → id

def prefetch_operators(names: list[str]):
    """Fetch existing operators by name, create missing ones."""
    unique = list({n.strip() for n in names if n and n.strip()} - set(_op_cache.keys()))
    if not unique:
        return

    # 1. Batch GET to find existing
    for i in range(0, len(unique), 50):
        chunk = unique[i:i+50]
        names_q = ",".join(urllib.parse.quote(n) for n in chunk)
        url = f"{SUPABASE_URL}/rest/v1/operators?name=in.({names_q})&select=id,name&limit=50"
        req = urllib.request.Request(url, headers=SB_GET_HEADERS)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                for row in json.loads(resp.read()):
                    _op_cache[row["name"]] = row["id"]
        except Exception as exc:
            print(f"  [WARN] op GET: {exc}", flush=True)

    # 2. POST only truly new ones
    to_create = [n for n in unique if n not in _op_cache]
    if not to_create:
        return

    # POST in batches of 50
    for i in range(0, len(to_create), 50):
        chunk = to_create[i:i+50]
        payload = [{"name": n} for n in chunk]
        try:
            resp_bytes = http_post_json(
                f"{SUPABASE_URL}/rest/v1/operators",
                payload, SB_POST_REPR_HEADERS
            )
            for row in json.loads(resp_bytes):
                _op_cache[row["name"]] = row["id"]
        except Exception as exc:
            print(f"  [WARN] op POST ({len(chunk)} names): {exc}", flush=True)
            # Fallback: GET them individually
            for name in chunk:
                url2 = f"{SUPABASE_URL}/rest/v1/operators?name=eq.{urllib.parse.quote(name)}&select=id,name&limit=1"
                req2 = urllib.request.Request(url2, headers=SB_GET_HEADERS)
                try:
                    with urllib.request.urlopen(req2, timeout=15) as r:
                        rows = json.loads(r.read())
                        if rows:
                            _op_cache[rows[0]["name"]] = rows[0]["id"]
                except Exception:
                    pass

# ─── ArcGIS pagination ────────────────────────────────────────────────────────
def fetch_page(offset: int) -> list:
    params = urllib.parse.urlencode({
        "where": "1=1",
        "outFields": ARCGIS_FIELDS,
        "resultOffset": offset,
        "resultRecordCount": FETCH_PAGE_SIZE,
        "f": "json",
    })
    return http_get_json(f"{ARCGIS_BASE}/query?{params}").get("features", [])

# ─── Feature → well record ────────────────────────────────────────────────────
def feature_to_well(attrs: dict, operator_id: str | None) -> dict:
    county_num = attrs.get("COUNTY")
    county_name = WY_COUNTY.get(county_num, str(county_num) if county_num else None)
    return {
        "api_number":              attrs.get("API_NUMBER") or None,
        "well_name":               attrs.get("WN") or None,
        "operator_id":             operator_id,
        "latitude":                attrs.get("LATITUDE") or None,
        "longitude":               attrs.get("LONGITUDE") or None,
        "state":                   STATE,
        "county":                  county_name,
        "well_type":               map_well_type(attrs.get("WELL_CLASS") or ""),
        "total_depth_ft":          parse_depth(attrs.get("TD")),
        "spud_date":               parse_spud(attrs.get("SPUD")),
        "status":                  map_status(attrs.get("STATUS") or ""),
        "data_source":             DATA_SOURCE,
        "source_record_id":        attrs.get("API_NUMBER") or None,
        "regulatory_jurisdiction": JURISDICTION,
    }

# ─── Supabase upsert ──────────────────────────────────────────────────────────
def upsert_wells(batch: list[dict]) -> int:
    if not batch:
        return 0
    url = f"{SUPABASE_URL}/rest/v1/wells?on_conflict=api_number"
    for attempt in range(3):
        try:
            http_post_json(url, batch, SB_HEADERS)
            return len(batch)
        except RuntimeError as e:
            if "57014" in str(e) and attempt < 2:
                time.sleep(2 ** attempt)
                continue
            raise
    return 0

# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    print("=== Wyoming WOGCC Well Ingestor ===", flush=True)
    count_url = f"{ARCGIS_BASE}/query?where=1%3D1&returnCountOnly=true&f=json"
    total = http_get_json(count_url).get("count", 0)
    print(f"Total records: {total:,}", flush=True)

    inserted = 0
    errors   = 0
    offset   = 0
    buf: list[dict] = []

    while offset < total:
        try:
            features = fetch_page(offset)
        except Exception as exc:
            print(f"  [WARN] fetch at {offset}: {exc}", flush=True)
            time.sleep(2)
            errors += 1
            offset += FETCH_PAGE_SIZE
            continue

        if not features:
            break

        op_names = [f["attributes"].get("COMPANY", "") or "" for f in features]
        try:
            prefetch_operators(op_names)
        except Exception as exc:
            print(f"  [WARN] op prefetch: {exc}", flush=True)

        for feat in features:
            attrs = feat.get("attributes", {})
            op_name = (attrs.get("COMPANY") or "").strip()
            op_id = _op_cache.get(op_name) if op_name else None
            buf.append(feature_to_well(attrs, op_id))

        while len(buf) >= UPSERT_BATCH:
            batch, buf = buf[:UPSERT_BATCH], buf[UPSERT_BATCH:]
            try:
                inserted += upsert_wells(batch)
                print(f"  Upserted {inserted:,} / {total:,}", flush=True)
            except Exception as exc:
                print(f"  [ERROR] upsert: {exc}", flush=True)
                errors += 1
            time.sleep(SLEEP_MS)

        offset += FETCH_PAGE_SIZE
        time.sleep(0.05)

    if buf:
        try:
            inserted += upsert_wells(buf)
            print(f"  Final flush: {inserted:,} total", flush=True)
        except Exception as exc:
            print(f"  [ERROR] final flush: {exc}", flush=True)
            errors += 1

    print(f"\n=== Done === wells={inserted:,} errors={errors}", flush=True)

    url2 = f"{SUPABASE_URL}/rest/v1/wells?data_source=eq.{DATA_SOURCE}&select=id"
    req = urllib.request.Request(url2, headers={**SB_GET_HEADERS, "Prefer": "count=exact", "Range": "0-0"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            print(f"Supabase {DATA_SOURCE} count: {resp.headers.get('Content-Range', '')}", flush=True)
    except Exception as exc:
        print(f"Count check: {exc}", flush=True)


if __name__ == "__main__":
    main()
