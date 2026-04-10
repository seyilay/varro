#!/usr/bin/env python3
"""
Ingest Netherlands well data from NLOG (Netherlands Oil and Gas) WFS service.

VERIFIED ENDPOINT (tested 2026-04-05):
  WFS: https://www.gdngeoservices.nl/geoserver/nlog/wfs
  Layer: nlog:gdw_ng_wll_all_utm
  Default CRS: EPSG:23031 (ED50/UTM Zone 31N)
  Output format: application/json

FIELD MAPPING:
  BOREHOLE_CODE      → api_number (unique well ID)
  BOREHOLE_NAME      → well_name
  BOREHOLE_TYPE_CODE → well_type (mapped)
  STATUS             → status (mapped)
  CURRENT_OWNER      → operator (looked up in operators table)
  ON_OFFSHORE_CODE   → state (ON=onshore, OF=offshore)
  geometry           → latitude/longitude (converted from EPSG:23031 via pyproj)

DATA SIZE: ~7,000+ boreholes (oil, gas, salt, water, geothermal)
           Filter to hydrocarbon wells (OIL/GAS) ~2,000+

REQUIREMENTS: pyproj 3.7.2, psycopg2, requests

Usage:
  python ingest_netherlands.py          # full ingest
  python ingest_netherlands.py --test   # first 100 rows only
"""

import argparse
import json
import sys
import time
from urllib.request import urlopen, Request
from urllib.parse import urlencode
from urllib.error import URLError

import psycopg2
from pyproj import Transformer

# ── Config ──────────────────────────────────────────────────────────────────
DB_DSN = "postgresql://postgres:$DB_PASSWORD@db.temtptsfiksixxhbigkg.supabase.co:5432/postgres"

WFS_BASE = "https://www.gdngeoservices.nl/geoserver/nlog/wfs"
LAYER    = "nlog:gdw_ng_wll_all_utm"
PAGE_SIZE = 500
DATA_SOURCE = "NLOG_NL"

# EPSG:23031 (ED50/UTM Zone 31N) → EPSG:4326 (WGS84 lat/lon)
# always_xy=True so transform(easting, northing) → (lon, lat)
TRANSFORMER = Transformer.from_crs("EPSG:23031", "EPSG:4326", always_xy=True)

# ── Well type mapping ────────────────────────────────────────────────────────
WELL_TYPE_MAP = {
    # Exploration
    "EXP":    "OIL_GAS",
    "EXPGAS": "GAS",
    "EXPOIL": "OIL",
    # Development
    "DEV":    "OIL_GAS",
    "DEV-GS": "GAS",
    "DEV-PS": None,       # rock salt, not hydrocarbon
    "DEV-OIL":"OIL",
    "DEV-GAS":"GAS",
    # Injection / disposal
    "INJ":    "INJECTION",
    "INJ-WTR":"INJECTION",
    "INJ-GAS":"INJECTION",
    "DISP":   "DISPOSAL",
    # Appraisal
    "APP":    "OIL_GAS",
    # Service / monitor
    "SRV":    None,
    "MON":    None,
    "OBS":    None,
    # Geothermal
    "GEO":    None,
    "GEO-PRD":None,
    "GEO-INJ":"INJECTION",
}

def map_well_type(code: str) -> str | None:
    if not code:
        return None
    code = code.strip().upper()
    # Direct match
    if code in WELL_TYPE_MAP:
        return WELL_TYPE_MAP[code]
    # Prefix match
    for prefix, wtype in WELL_TYPE_MAP.items():
        if code.startswith(prefix):
            return wtype
    # Heuristic from code string
    if "INJ" in code:
        return "INJECTION"
    if "GAS" in code:
        return "GAS"
    if "OIL" in code or "PETR" in code:
        return "OIL"
    if "DISP" in code:
        return "DISPOSAL"
    return "OTHER"

# ── Status mapping ───────────────────────────────────────────────────────────
STATUS_MAP = {
    "in production":        "PRODUCING",
    "producing":            "PRODUCING",
    "shut-in":              "SHUT_IN",
    "shut in":              "SHUT_IN",
    "temporarily abandoned":"TA",
    "temporarily plugged":  "TA",
    "plugged and abandoned":"PA",
    "abandoned":            "PA",
    "idle":                 "IDLE",
    "suspended":            "IDLE",
    "in injection":         "PRODUCING",
    "active":               "PRODUCING",
}

def map_status(status: str) -> str | None:
    if not status:
        return None
    key = status.strip().lower()
    return STATUS_MAP.get(key)

# ── Helpers ──────────────────────────────────────────────────────────────────
def fetch_page(start_index: int, count: int) -> dict:
    """Fetch a page of WFS features in EPSG:23031 as GeoJSON."""
    params = {
        "service":     "WFS",
        "version":     "2.0.0",
        "request":     "GetFeature",
        "typeName":    LAYER,
        "outputFormat":"application/json",
        "srsName":     "EPSG:23031",
        "count":       count,
        "startIndex":  start_index,
    }
    url = WFS_BASE + "?" + urlencode(params)
    req = Request(url, headers={"Accept": "application/json"})
    for attempt in range(3):
        try:
            with urlopen(req, timeout=60) as resp:
                return json.loads(resp.read())
        except URLError as e:
            print(f"  [warn] WFS fetch failed (attempt {attempt+1}): {e}")
            time.sleep(2 ** attempt)
    raise RuntimeError(f"WFS fetch failed after 3 attempts for startIndex={start_index}")


def get_or_create_operator(cur, name: str, cache: dict) -> int | None:
    """Look up or insert operator by name; return operator_id."""
    if not name:
        return None
    if name in cache:
        return cache[name]
    cur.execute("SELECT id FROM operators WHERE name = %s", (name,))
    row = cur.fetchone()
    if row:
        cache[name] = row[0]
        return row[0]
    # Insert new operator
    cur.execute(
        "INSERT INTO operators (name, country) VALUES (%s, %s) "
        "ON CONFLICT (name) DO UPDATE SET name=EXCLUDED.name RETURNING id",
        (name, "NL")
    )
    row = cur.fetchone()
    if row:
        cache[name] = row[0]
        return row[0]
    return None


def convert_coords(easting, northing):
    """Convert EPSG:23031 easting/northing → (lat, lon) in WGS84."""
    try:
        lon, lat = TRANSFORMER.transform(float(easting), float(northing))
        if -90 <= lat <= 90 and -180 <= lon <= 180:
            return round(lat, 8), round(lon, 8)
    except Exception:
        pass
    return None, None


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Ingest Netherlands NLOG wells")
    parser.add_argument("--test", action="store_true", help="Ingest first 100 rows only")
    args = parser.parse_args()

    limit = 100 if args.test else None
    print(f"[NLOG] Starting ingest | test={args.test}")

    conn = psycopg2.connect(DB_DSN)
    cur  = conn.cursor()

    operator_cache: dict[str, int] = {}
    total_inserted = 0
    total_skipped  = 0
    start_index    = 0
    batch_rows     = []

    INSERT_SQL = """
        INSERT INTO wells (
            api_number, well_name, well_type, operator_id,
            latitude, longitude, state, status,
            data_source, source_record_id
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING
    """

    while True:
        fetch_count = PAGE_SIZE
        if limit is not None:
            remaining = limit - (total_inserted + total_skipped + len(batch_rows))
            if remaining <= 0:
                break
            fetch_count = min(PAGE_SIZE, remaining)

        print(f"  Fetching startIndex={start_index} count={fetch_count}...")
        data = fetch_page(start_index, fetch_count)
        features = data.get("features", [])

        if not features:
            print("  No more features. Done.")
            break

        for feat in features:
            props = feat.get("properties", {})
            geom  = feat.get("geometry", {})

            api_number = props.get("BOREHOLE_CODE") or feat.get("id", "")
            if not api_number:
                total_skipped += 1
                continue

            # Coordinates: geometry is in EPSG:23031 (easting, northing)
            lat, lon = None, None
            if geom and geom.get("type") == "Point":
                coords = geom.get("coordinates", [])
                if len(coords) >= 2:
                    lat, lon = convert_coords(coords[0], coords[1])
            # Fallback to SUBMITTED_X/Y (various CRS, less reliable)
            if lat is None and props.get("SUBMITTED_X") and props.get("SUBMITTED_Y"):
                sub_sys = (props.get("SUBMITTED_COORDINATE_SYSTEM") or "").upper()
                if "UTM" in sub_sys or sub_sys == "ED50":
                    lat, lon = convert_coords(props["SUBMITTED_X"], props["SUBMITTED_Y"])

            # Operator
            operator_name = props.get("CURRENT_OWNER") or props.get("ORIGINAL_OPERATOR")
            operator_id   = get_or_create_operator(cur, operator_name, operator_cache)

            # State (on/offshore)
            on_off = props.get("ON_OFFSHORE_CODE", "")
            state  = "NL-ON" if on_off == "ON" else ("NL-OF" if on_off == "OF" else None)

            batch_rows.append((
                str(api_number)[:100],
                (props.get("BOREHOLE_NAME") or "")[:255] or None,
                map_well_type(props.get("BOREHOLE_TYPE_CODE")),
                operator_id,
                lat,
                lon,
                state,
                map_status(props.get("STATUS")),
                DATA_SOURCE,
                str(api_number)[:100],
            ))

            # Commit every 10k rows
            if len(batch_rows) >= 10_000:
                cur.executemany(INSERT_SQL, batch_rows)
                conn.commit()
                total_inserted += len(batch_rows)
                print(f"  Committed {total_inserted} rows so far...")
                batch_rows = []

        start_index += len(features)

        # Respect server (small pause)
        time.sleep(0.2)

        if limit is not None and (total_inserted + len(batch_rows)) >= limit:
            break

    # Flush remainder
    if batch_rows:
        cur.executemany(INSERT_SQL, batch_rows)
        conn.commit()
        total_inserted += len(batch_rows)

    print(f"[NLOG] Done. Inserted={total_inserted}, Skipped={total_skipped}")
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
