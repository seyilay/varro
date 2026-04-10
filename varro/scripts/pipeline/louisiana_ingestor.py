"""
Louisiana SONRIS Well Ingestor

Source:  FracTracker / ArcGIS Online public feature service
         Primary:   la_wells_021917 (Layer 1) — 85,562 wells
         Secondary: Louisiana_wells_2_24_22 (Layer 0) — 63,111 wells
         (SONRIS direct API at sonris-gis.dnr.la.gov is Cloudflare-blocked)

Fields mapped to schema:
  API_NUM       → api_number
  WELL_NAME     → well_name
  geometry.y/x  → latitude/longitude
  PARISH_NAM    → county
  MEASURED_D    → total_depth_ft
  SPUD_DATE     → spud_date  (epoch ms → YYYY-MM-DD)
  WELL_STATU    → status  (coded → PRODUCING/IDLE/SHUT_IN/TA/PA)
  PRODUCT_TY    → well_type
  ORG_OPER_N    → operator_name

State: Louisiana
Data source: SONRIS_LA
Regulatory jurisdiction: SONRIS

Previous best result: ~85k wells ingested.
To reach 200k: would need direct SONRIS GIS access (currently Cloudflare-blocked).
"""

import time
import urllib.request
import urllib.parse
import json
from datetime import datetime, timezone
from typing import Iterator
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from base_ingestor import BaseIngestor

# ─── ArcGIS Feature Service URLs ───────────────────────────────────────────
# Primary: la_wells_021917 layer 1 (richest SONRIS fields, ~85k wells)
PRIMARY_FS = (
    "https://services.arcgis.com/jDGuO8tYggdCCnUJ/arcgis/rest"
    "/services/la_wells_021917/FeatureServer/1"
)

# Secondary: Louisiana_wells_2_24_22 layer 0 (~63k wells, some non-overlap)
SECONDARY_FS = (
    "https://services.arcgis.com/jDGuO8tYggdCCnUJ/arcgis/rest"
    "/services/Louisiana_wells_2_24_22/FeatureServer/0"
)

PAGE_SIZE = 1000  # ArcGIS maxRecordCount

# ─── SONRIS Well Status Code → pipeline status ─────────────────────────────
SONRIS_STATUS_MAP: dict[str, str] = {
    "01": "PRODUCING",    # Active - Producing
    "10": "PRODUCING",    # Active - Production (different sub-type)
    "02": "IDLE",         # Inactive
    "17": "IDLE",         # Converted (no longer oil/gas producing)
    "18": "IDLE",         # Transferred
    "22": "IDLE",         # Converted to water well
    "23": "IDLE",         # Orphaned well
    "26": "SHUT_IN",      # Shut In
    "03": "TA",           # Temporarily Abandoned
    "73": "TA",           # Temporarily Abandoned (alt code)
    "09": "PA",           # Plugged and Abandoned
    "28": "PA",           # Plugged (another code)
    "31": "PA",           # P&A / Transferred (deemed PA)
    "33": "PA",           # Plugged
    "34": "PA",           # P&A
    "35": "PA",           # P&A
    "37": "PA",           # P&A
    "49": "PA",           # P&A
}

# SONRIS Product Type Code → well_type string
PRODUCT_TYPE_MAP: dict[str, str | None] = {
    "10": "OIL",
    "20": "GAS",
    "00": None,   # unknown / undefined — NULL in DB
}


def _epoch_ms_to_date(ms) -> str | None:
    """Convert epoch milliseconds to YYYY-MM-DD string."""
    if ms is None:
        return None
    try:
        dt = datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


def _arcgis_query(base_url: str, params: dict, timeout: int = 30) -> dict:
    """Send a GET request to an ArcGIS Feature Service query endpoint."""
    params["f"] = "json"
    qs = urllib.parse.urlencode(params)
    url = f"{base_url}/query?{qs}"
    req = urllib.request.Request(url)
    req.add_header("User-Agent", "Varro-Well-Ingestor/1.0")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def _get_total_count(base_url: str) -> int:
    """Get total record count for a feature layer."""
    data = _arcgis_query(base_url, {"where": "1=1", "returnCountOnly": "true"})
    return data.get("count", 0)


def _parse_feature(attrs: dict, geom: dict) -> dict | None:
    """Parse an ArcGIS feature into a well dict. Returns None if invalid."""
    api_num = str(attrs.get("API_NUM") or "").strip()
    if not api_num or api_num == "None":
        return None

    lat = geom.get("y") if geom else None
    lon = geom.get("x") if geom else None

    # WELL_STATU → status
    status_code = str(attrs.get("WELL_STATU") or "").strip()
    status = SONRIS_STATUS_MAP.get(status_code, "IDLE")

    # PRODUCT_TY → well_type
    product_code = str(attrs.get("PRODUCT_TY") or "").strip()
    well_type = PRODUCT_TYPE_MAP.get(product_code, None)

    # MEASURED_D → total_depth_ft (integer in DB)
    depth = attrs.get("MEASURED_D")
    try:
        depth = int(float(depth)) if depth is not None else None
    except (ValueError, TypeError):
        depth = None

    # SPUD_DATE → spud_date
    spud_date = _epoch_ms_to_date(attrs.get("SPUD_DATE"))

    # Operator name
    op_name = str(attrs.get("ORG_OPER_N") or "").strip() or None

    # Parish (county)
    county = str(attrs.get("PARISH_NAM") or "").strip() or None

    # Well name
    well_name = str(attrs.get("WELL_NAME") or "").strip() or None

    return {
        "api_number": api_num,
        "well_name": well_name,
        "operator_name": op_name,
        "latitude": lat,
        "longitude": lon,
        "state": "Louisiana",
        "county": county,
        "basin": None,
        "well_type": well_type,
        "total_depth_ft": depth,
        "spud_date": spud_date,
        "status": status,
        "source_record_id": str(attrs.get("OBJECTID") or api_num),
    }


def _fetch_wells_from_service(
    base_url: str,
    out_fields: str,
    seen_apis: set,
) -> Iterator[dict]:
    """
    Paginate through an ArcGIS Feature Service layer using resultOffset/resultRecordCount.
    Yields well dicts, skipping any API numbers already in seen_apis.
    """
    svc_name = base_url.split("/")[-3]
    print(f"  [fetch] Getting count from {svc_name}...")
    try:
        total = _get_total_count(base_url)
    except Exception as e:
        print(f"  [ERROR] Could not get count: {e}")
        return

    print(f"  [fetch] {total:,} features to retrieve")

    offset = 0
    page_num = 0
    while offset < total:
        page_num += 1
        try:
            data = _arcgis_query(
                base_url,
                {
                    "where": "1=1",
                    "outFields": out_fields,
                    "outSR": "4326",
                    "resultOffset": str(offset),
                    "resultRecordCount": str(PAGE_SIZE),
                    "orderByFields": "OBJECTID ASC",
                },
                timeout=45,
            )
        except Exception as e:
            print(f"  [WARN] Page {page_num} (offset {offset}) failed: {e}")
            time.sleep(3)
            offset += PAGE_SIZE  # skip and continue
            continue

        features = data.get("features", [])
        if not features:
            break

        for feat in features:
            attrs = feat.get("attributes", {}) or {}
            geom = feat.get("geometry") or {}

            well = _parse_feature(attrs, geom)
            if well is None:
                continue
            api_num = well["api_number"]
            if api_num in seen_apis:
                continue
            seen_apis.add(api_num)
            yield well

        offset += len(features)

        # Rate limit between pages
        time.sleep(0.1)

        if page_num % 10 == 0:
            print(f"  [fetch] {offset:,}/{total:,} features retrieved (page {page_num})...")

        # If fewer records returned than requested, we're done
        if len(features) < PAGE_SIZE:
            break


# ─── Secondary source (Louisiana_wells_2_24_22) field mapping ──────────────

def _fetch_secondary_wells(seen_apis: set) -> Iterator[dict]:
    """
    Fetch wells from secondary source (Louisiana_wells_2_24_22).
    Fields: API_NUM, WELL_NAME, geometry x/y, PARISH_NAM, MEASURED_D, SPUD_DATE,
            WELL_STATU, PRODUCT_TY, ORG_OPER_N (same as primary)
    """
    print(f"\n  [secondary] Querying {SECONDARY_FS.split('/')[-3]}...")
    try:
        # Check what fields are available
        req = urllib.request.Request(f"{SECONDARY_FS}?f=json")
        req.add_header("User-Agent", "Varro-Well-Ingestor/1.0")
        with urllib.request.urlopen(req, timeout=20) as resp:
            meta = json.loads(resp.read())
        field_names = {f["name"] for f in meta.get("fields", [])}
        print(f"  [secondary] Fields: {sorted(field_names)}")
    except Exception as e:
        print(f"  [secondary] Could not get metadata: {e}")
        return

    # Check which fields we can use
    out_fields_list = []
    for f in ["API_NUM", "WELL_NAME", "PARISH_NAM", "MEASURED_D",
              "SPUD_DATE", "WELL_STATU", "PRODUCT_TY", "ORG_OPER_N", "OBJECTID"]:
        if f in field_names:
            out_fields_list.append(f)
    out_fields = ",".join(out_fields_list) if out_fields_list else "*"

    yield from _fetch_wells_from_service(SECONDARY_FS, out_fields, seen_apis)


import requests as _requests


class LouisianaSONRISIngestor(BaseIngestor):
    source_name: str = "SONRIS_LA"
    regulatory_jurisdiction: str = "SONRIS"
    UPSERT_BATCH_SIZE: int = 1000

    def download(self):
        """No-op: we stream directly from ArcGIS REST APIs."""
        print("  [download] Streaming live from ArcGIS Feature Services (no local download)")

    SUPABASE_URL: str = "https://temtptsfiksixxhbigkg.supabase.co"

    def get_or_create_operator(self, name: str) -> str | None:
        """Override with retry logic for transient Supabase timeouts."""
        if not name:
            return None
        name = name.strip()
        if not name:
            return None
        if name in self.op_name_to_id:
            return self.op_name_to_id[name]

        for attempt in range(3):
            try:
                r = _requests.post(
                    f"{self.SUPABASE_URL}/rest/v1/operators",
                    headers={
                        **self.H,
                        "Prefer": "resolution=merge-duplicates,return=representation",
                    },
                    json=[{"name": name}],
                    timeout=60,
                )
                if r.status_code in (200, 201) and r.json():
                    uid = r.json()[0]["id"]
                    self.op_name_to_id[name] = uid
                    return uid
                print(f"  [WARN] Operator create failed ({r.status_code}): {name[:50]}")
                return None
            except Exception as e:
                if attempt < 2:
                    print(f"  [RETRY] Operator create attempt {attempt+1} failed: {e}")
                    time.sleep(2 ** attempt)
                else:
                    print(f"  [WARN] Giving up on operator '{name[:40]}': {e}")
                    return None

    def _bulk_ensure_operators(self, operator_names: set) -> None:
        """
        Batch-upsert all unique operator names to Supabase at once.
        Much faster than one-by-one creation during parse.
        """
        new_ops = [
            {"name": n} for n in sorted(operator_names)
            if n and n not in self.op_name_to_id
        ]
        if not new_ops:
            print(f"  [ops] All {len(operator_names)} operators already cached")
            return
        print(f"  [ops] Creating {len(new_ops)} new operators in batches...")
        batch_size = 500
        created = 0
        for i in range(0, len(new_ops), batch_size):
            batch = new_ops[i : i + batch_size]
            for attempt in range(3):
                try:
                    r = _requests.post(
                        f"{self.SUPABASE_URL}/rest/v1/operators",
                        headers={
                            **self.H,
                            "Prefer": "resolution=merge-duplicates,return=representation",
                        },
                        json=batch,
                        timeout=60,
                    )
                    if r.status_code in (200, 201) and r.json():
                        for row in r.json():
                            self.op_name_to_id[row["name"].strip()] = row["id"]
                            created += 1
                    elif r.status_code in (200, 201):
                        pass  # return=minimal, re-fetch
                    else:
                        print(f"  [WARN] Operator batch failed: {r.status_code} {r.text[:80]}")
                    break
                except Exception as e:
                    if attempt < 2:
                        time.sleep(2 ** attempt)
                    else:
                        print(f"  [WARN] Operator batch gave up: {e}")
            time.sleep(0.1)
        print(f"  [ops] {created} operators created/updated")

    def _prefetch_operators(self) -> None:
        """
        Fetch all unique ORG_OPER_N values from the ArcGIS layers,
        then bulk-upsert them to Supabase so parse() hits cache every time.
        """
        print("  [prefetch] Collecting unique operator names from ArcGIS...")
        op_names: set = set()

        for base_url in [PRIMARY_FS, SECONDARY_FS]:
            try:
                data = _arcgis_query(
                    base_url,
                    {
                        "where": "1=1",
                        "outFields": "ORG_OPER_N",
                        "returnDistinctValues": "true",
                        "resultRecordCount": "10000",
                    },
                    timeout=30,
                )
                for feat in data.get("features", []):
                    n = str(feat.get("attributes", {}).get("ORG_OPER_N") or "").strip()
                    if n:
                        op_names.add(n)
            except Exception as e:
                print(f"  [WARN] Could not prefetch from {base_url.split('/')[-3]}: {e}")

        print(f"  [prefetch] {len(op_names)} unique operators found")
        self._bulk_ensure_operators(op_names)
        # Reload cache to capture newly created operators
        self.load_operators()
        print(f"  [prefetch] Operator cache now has {len(self.op_name_to_id)} entries")

    def load_operators(self) -> int:
        """
        Override: only load operators matching the ~510 names we need for LA.
        This avoids paginating through 60k+ operators with slow Supabase connections.
        If op_name_to_id is already populated (from prefetch), return cached count.
        """
        if self.op_name_to_id:
            return len(self.op_name_to_id)

        # Fall back to loading all operators, but with retry
        import requests as _req
        offset = 0
        while True:
            for attempt in range(3):
                try:
                    r = _req.get(
                        f"https://temtptsfiksixxhbigkg.supabase.co/rest/v1/operators",
                        headers=self.H,
                        params={"select": "id,name", "limit": "1000", "offset": str(offset)},
                        timeout=60,
                    )
                    rows = r.json()
                    break
                except Exception as e:
                    if attempt < 2:
                        print(f"  [RETRY] load_operators attempt {attempt+1}: {e}")
                        time.sleep(3)
                    else:
                        print(f"  [WARN] load_operators gave up at offset {offset}: {e}")
                        return len(self.op_name_to_id)

            if not rows or isinstance(rows, dict):
                break
            for row in rows:
                self.op_name_to_id[row["name"].strip()] = row["id"]
            offset += len(rows)
            if len(rows) < 1000:
                break
        return len(self.op_name_to_id)

    def upsert_wells(self, wells: list[dict]) -> int:
        """Override with retry logic and smaller sub-batches."""
        if not wells:
            return 0

        # Use urllib to match the module-level approach (no new deps)
        import urllib.request as _ur
        import urllib.error as _ue

        SUPABASE_URL = "https://temtptsfiksixxhbigkg.supabase.co"
        url = f"{SUPABASE_URL}/rest/v1/wells?on_conflict=api_number"
        headers = {
            "apikey": self.H["apikey"],
            "Authorization": self.H["Authorization"],
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates,return=minimal",
        }

        # Ensure all records have identical keys (required by spec)
        all_keys = set()
        for w in wells:
            all_keys.update(w.keys())
        normalized = []
        for w in wells:
            row = {k: w.get(k) for k in all_keys}
            normalized.append(row)

        # Try in sub-batches (500 records) with retries
        SUB_BATCH = 500
        total_sent = 0
        for i in range(0, len(normalized), SUB_BATCH):
            sub = normalized[i : i + SUB_BATCH]
            payload = json.dumps(sub).encode("utf-8")

            for attempt in range(3):
                try:
                    req = _ur.Request(url, data=payload, headers=headers, method="POST")
                    with _ur.urlopen(req, timeout=90) as resp:
                        status = resp.status
                    if status in (200, 201, 204):
                        total_sent += len(sub)
                        break
                    else:
                        print(f"  [ERROR] Upsert sub-batch failed: HTTP {status}")
                        break
                except _ue.HTTPError as e:
                    body = e.read()[:150].decode(errors="replace")
                    print(f"  [ERROR] Upsert HTTP {e.code}: {body}")
                    break
                except Exception as e:
                    if attempt < 2:
                        wait = 2 ** attempt * 3
                        print(f"  [RETRY] Upsert attempt {attempt+1} failed ({e.__class__.__name__}), waiting {wait}s...")
                        time.sleep(wait)
                    else:
                        print(f"  [WARN] Upsert gave up after 3 attempts: {e}")

            time.sleep(0.1)  # 100ms between batches as required

        return total_sent

    def parse(self) -> Iterator[dict]:
        seen_apis: set = set()

        # ── Primary source ──────────────────────────────────────────────
        print("\n  [parse] === Primary: la_wells_021917 layer 1 ===")
        primary_fields = (
            "OBJECTID,API_NUM,WELL_NAME,WELL_STATU,PARISH_NAM,"
            "SURFACE_LA,SURFACE_LO,MEASURED_D,SPUD_DATE,PRODUCT_TY,ORG_OPER_N"
        )
        yield from _fetch_wells_from_service(PRIMARY_FS, primary_fields, seen_apis)

        # ── Secondary source ────────────────────────────────────────────
        print(f"\n  [parse] === Secondary: Louisiana_wells_2_24_22 layer 0 ===")
        print(f"  [parse] Primary fetched {len(seen_apis):,} unique APIs — deduplicating")
        yield from _fetch_secondary_wells(seen_apis)

        print(f"\n  [parse] Total unique wells: {len(seen_apis):,}")


if __name__ == "__main__":
    ingestor = LouisianaSONRISIngestor()

    print("\n[pre-run] Step A: Pre-fetching operators from ArcGIS + Supabase...")
    # Collect unique operator names from source data
    ingestor._prefetch_operators()
    # After prefetch, op_name_to_id cache is populated.
    # load_operators() in run() will see non-empty cache and return fast.
    print(f"  Operator cache: {len(ingestor.op_name_to_id):,} entries")

    total = ingestor.run(skip_download=True)
    print(f"\n✓ Louisiana SONRIS ingestor complete — {total:,} wells upserted")
