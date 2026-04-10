#!/usr/bin/env python3
"""
Ingest Mexico well data from SENER (Secretaría de Energía) open data portal.

VERIFIED ENDPOINT (tested 2026-04-05):
  API:  https://www.datos.gob.mx/api/3/action/package_search?q=coordenadas+pozos
  CSV:  https://repodatos.atdt.gob.mx/api_update/sener/coordenadas_pozos_terminados_{YEAR}/DGACE_pozos_terminados_{months}.csv

⚠️  DATA AVAILABILITY NOTICE:
  As of April 2026, Mexico's datos.gob.mx open data portal only has well-level
  data with coordinates for 2026 (12 wells in Jan-Feb 2026 extract).
  The CNH (Comisión Nacional de Hidrocarburos) comprehensive well registry
  (~10,000+ wells with coordinates) requires registration at:
    - SNIH: http://snih.cnh.gob.mx  (currently unreachable without VPN/registration)
    - CNH GeoPortal: https://geoportal.cnh.gob.mx (unreachable without registration)
  PEMEX datos.gob.mx datasets only provide aggregate counts, not individual wells.

  This script fetches all available yearly datasets from the SENER portal,
  which is the only accessible open data source with individual well coordinates.
  To ingest the full registry, obtain CNH SNIH access credentials and extend
  the fetch logic with authenticated requests.

FIELD MAPPING (from DGACE_pozos_terminados_*.csv):
  nombre_pozo         → api_number + well_name
  operador            → operator (looked up in operators table)
  tipo_pozo           → well_type (mapped)
  estatus             → status (mapped)
  entidad_federativa  → state (Mexican state/region)
  cond_utm_x_real,    → latitude/longitude
  cond_utm_y_real       (converted from UTM WGS84 Zone 14N via pyproj)

COORDINATE SYSTEM:
  Coordinates are in UTM WGS84. Most wells are in Zone 14N (EPSG:32614).
  Zone detection: if cond_x in [150000, 950000] and cond_y in [1800000, 3500000]
  → assume Zone 14N (covers most of Mexico). Offshore wells in Campeche area
  may be Zone 15N. Values outside these ranges are skipped (data anomaly).

Usage:
  python ingest_mexico.py          # full ingest
  python ingest_mexico.py --test   # first 100 rows only
"""

import argparse
import csv
import io
import json
import sys
import time
from urllib.request import urlopen, Request
from urllib.error import URLError

import psycopg2
from pyproj import Transformer

# ── Config ──────────────────────────────────────────────────────────────────
DB_DSN = "postgresql://postgres:$DB_PASSWORD@db.temtptsfiksixxhbigkg.supabase.co:5432/postgres"
DATA_SOURCE = "SENER_MX"

# CKAN API to discover datasets for any year
CKAN_SEARCH = (
    "https://www.datos.gob.mx/api/3/action/package_search"
    "?q=coordenadas+pozos+terminados&fq=organization:sener&rows=20"
)

# Fallback: try known URL patterns if CKAN doesn't find them
REPO_BASE = "https://repodatos.atdt.gob.mx/api_update/sener"

# Pre-computed transformers for common Mexican UTM zones (WGS84)
_TRANSFORMERS: dict[int, Transformer] = {}

def get_transformer(epsg: int) -> Transformer:
    if epsg not in _TRANSFORMERS:
        _TRANSFORMERS[epsg] = Transformer.from_crs(
            f"EPSG:{epsg}", "EPSG:4326", always_xy=True
        )
    return _TRANSFORMERS[epsg]

# UTM zone → EPSG for Mexico
# Zone 13N: 102°W–96°W → 32613
# Zone 14N:  96°W–90°W → 32614  (most land wells)
# Zone 15N:  90°W–84°W → 32615  (Yucatan, E. Campeche offshore)
# Zone 16N:  84°W–78°W → 32616  (rare offshore)
MX_UTM_ZONES = [32614, 32615, 32613, 32616]

def utm_to_latlon(x: float, y: float) -> tuple[float | None, float | None]:
    """Try each Mexican UTM zone, return first valid lat/lon within Mexico bounds."""
    # Mexico rough bounding box: lat 14–33°N, lon -120° to -86°W
    for epsg in MX_UTM_ZONES:
        try:
            t = get_transformer(epsg)
            lon, lat = t.transform(x, y)
            if 14.0 <= lat <= 33.0 and -120.0 <= lon <= -86.0:
                return round(lat, 8), round(lon, 8)
        except Exception:
            continue
    return None, None

# ── Well type mapping ────────────────────────────────────────────────────────
WELL_TYPE_MAP = {
    "pozo de desarrollo":          "OIL_GAS",
    "pozo de delineacion":         "OIL_GAS",
    "pozo de delineación":         "OIL_GAS",
    "pozo exploratorio":           "OIL_GAS",
    "pozo de exploración":         "OIL_GAS",
    "pozo de exploracion":         "OIL_GAS",
    "inyector":                    "INJECTION",
    "pozo inyector":               "INJECTION",
    "reentrada":                   "OIL_GAS",   # workover/reentry, keep as OIL_GAS
    "pozo reparado":               "OIL_GAS",
    "dispersion":                  "DISPOSAL",
    "disposición":                 "DISPOSAL",
}

def map_well_type(raw: str) -> str | None:
    if not raw:
        return None
    key = raw.strip().lower()
    if key in WELL_TYPE_MAP:
        return WELL_TYPE_MAP[key]
    if "explorat" in key or "explorac" in key:
        return "OIL_GAS"
    if "desarrollo" in key or "delinea" in key:
        return "OIL_GAS"
    if "inyect" in key:
        return "INJECTION"
    if "dispos" in key or "dispers" in key:
        return "DISPOSAL"
    return None

# ── Status mapping (estatus field) ───────────────────────────────────────────
STATUS_MAP = {
    "en produccion":               "PRODUCING",
    "en producción":               "PRODUCING",
    "produciendo":                 "PRODUCING",
    "cerrado temporalmente":       "SHUT_IN",
    "shut-in":                     "SHUT_IN",
    "cerrado":                     "IDLE",
    "inactivo":                    "IDLE",
    "abandonado temporalmente":    "TA",
    "abandono temporal":           "TA",
    "abandonado":                  "PA",
    "permanentemente abandonado":  "PA",
    "tapado y abandonado":         "PA",
}

def map_status(raw: str) -> str | None:
    if not raw:
        return None
    key = raw.strip().lower()
    if key in STATUS_MAP:
        return STATUS_MAP[key]
    if "producc" in key or "produc" in key:
        return "PRODUCING"
    if "cerrad" in key or "shut" in key:
        return "SHUT_IN"
    if "temporalm" in key or "temp" in key:
        return "TA"
    if "abandon" in key or "tapado" in key:
        return "PA"
    if "inacti" in key:
        return "IDLE"
    return None

# ── Helpers ──────────────────────────────────────────────────────────────────
def get_or_create_operator(cur, name: str, cache: dict) -> int | None:
    if not name:
        return None
    name = name.strip()
    if name in cache:
        return cache[name]
    cur.execute("SELECT id FROM operators WHERE name = %s", (name,))
    row = cur.fetchone()
    if row:
        cache[name] = row[0]
        return row[0]
    cur.execute(
        "INSERT INTO operators (name, country) VALUES (%s, %s) "
        "ON CONFLICT (name) DO UPDATE SET name=EXCLUDED.name RETURNING id",
        (name, "MX")
    )
    row = cur.fetchone()
    if row:
        cache[name] = row[0]
        return row[0]
    return None


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-MX,es;q=0.8,en-US;q=0.5",
    "Referer": "https://www.datos.gob.mx/",
}


def fetch_url(url: str, timeout: int = 30, retries: int = 3) -> bytes | None:
    req = Request(url, headers=HEADERS)
    for attempt in range(retries):
        try:
            with urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except URLError as e:
            print(f"    [warn] Fetch failed (attempt {attempt+1}): {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return None


def discover_csv_urls() -> list[str]:
    """Discover CSV download URLs from the SENER datos.gob.mx datasets."""
    urls = []
    print(f"  Querying CKAN: {CKAN_SEARCH}")
    data = fetch_url(CKAN_SEARCH, timeout=20, retries=2)
    if data:
        try:
            result = json.loads(data)
            packages = result.get("result", {}).get("results", [])
            for pkg in packages:
                for res in pkg.get("resources", []):
                    url = res.get("url", "")
                    if url.endswith(".csv") or ".csv" in url:
                        print(f"    Found: {url}")
                        urls.append(url)
        except json.JSONDecodeError:
            pass

    # Try guessing historical yearly index pages (quick probe, no retries)
    import datetime
    current_year = datetime.datetime.now().year
    for year in range(current_year, 2019, -1):
        guessed = f"{REPO_BASE}/coordenadas_pozos_terminados_{year}/"
        index_data = fetch_url(guessed, timeout=8, retries=1)
        if index_data and index_data.strip().startswith(b"["):
            try:
                entries = json.loads(index_data)
                for entry in entries:
                    if entry.get("type") == "file" and entry.get("name", "").endswith(".csv"):
                        full_url = guessed + entry["name"]
                        if full_url not in urls:
                            print(f"    Found (guessed): {full_url}")
                            urls.append(full_url)
            except json.JSONDecodeError:
                pass

    return urls


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Ingest Mexico SENER wells")
    parser.add_argument("--test", action="store_true", help="Ingest first 100 rows only")
    args = parser.parse_args()

    limit = 100 if args.test else None
    print(f"[SENER_MX] Starting ingest | test={args.test}")

    csv_urls = discover_csv_urls()
    if not csv_urls:
        print("[SENER_MX] ERROR: No CSV URLs found. Check network and portal availability.")
        sys.exit(1)

    conn = psycopg2.connect(DB_DSN)
    cur  = conn.cursor()

    operator_cache: dict[str, int] = {}
    total_inserted = 0
    total_skipped  = 0
    grand_total    = 0
    batch_rows: list = []

    INSERT_SQL = """
        INSERT INTO wells (
            api_number, well_name, well_type, operator_id,
            latitude, longitude, state, status,
            data_source, source_record_id
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING
    """

    for csv_url in csv_urls:
        if limit is not None and grand_total >= limit:
            break

        print(f"  Downloading: {csv_url}")
        data = fetch_url(csv_url)
        if not data:
            print(f"  [warn] Could not download {csv_url}, skipping.")
            continue

        try:
            csv_text = data.decode("utf-8", errors="replace")
        except Exception as e:
            print(f"  [warn] Decode error: {e}")
            continue

        reader = csv.DictReader(io.StringIO(csv_text))
        file_count = 0

        for row in reader:
            if limit is not None and grand_total >= limit:
                break

            well_name = (row.get("nombre_pozo") or "").strip()
            if not well_name:
                total_skipped += 1
                continue

            # api_number: well name is unique per file but may appear in multiple
            # yearly extracts — prefix with MX- and use name as unique ID
            api_number = f"MX-{well_name}"

            # Coordinates: cond_utm_x_real / cond_utm_y_real (head UTM WGS84)
            lat, lon = None, None
            try:
                x_raw = row.get("cond_utm_x_real") or row.get("obj_utm_x_wgs84_real") or ""
                y_raw = row.get("cond_utm_y_real") or row.get("obj_utm_y_wgs84_real") or ""
                if x_raw and y_raw:
                    x, y = float(x_raw.strip()), float(y_raw.strip())
                    # Sanity: standard UTM easting is 100000–999999, northing for Mexico is 1600000–3500000
                    if 100_000 <= x <= 1_000_000 and 1_600_000 <= y <= 3_500_000:
                        lat, lon = utm_to_latlon(x, y)
                    else:
                        total_skipped += 0  # count but still insert without coords
            except ValueError:
                pass

            # Operator
            operador = (row.get("operador") or "").strip()
            operator_id = get_or_create_operator(cur, operador, operator_cache)

            # State (entidad_federativa = Mexican state or "Golfo de Mexico" for offshore)
            entidad = (row.get("entidad_federativa") or "").strip() or None

            batch_rows.append((
                api_number[:100],
                well_name[:255],
                map_well_type(row.get("tipo_pozo")),
                operator_id,
                lat,
                lon,
                entidad[:50] if entidad else None,
                map_status(row.get("estatus")),
                DATA_SOURCE,
                api_number[:100],
            ))

            file_count += 1
            grand_total += 1

            # Commit every 10k rows
            if len(batch_rows) >= 10_000:
                cur.executemany(INSERT_SQL, batch_rows)
                conn.commit()
                total_inserted += len(batch_rows)
                print(f"  Committed {total_inserted:,} rows so far...")
                batch_rows = []

        print(f"    Processed {file_count} rows from this file.")

    # Flush remainder
    if batch_rows:
        cur.executemany(INSERT_SQL, batch_rows)
        conn.commit()
        total_inserted += len(batch_rows)

    print(f"[SENER_MX] Done. Processed={grand_total}, Inserted={total_inserted}, Skipped={total_skipped}")
    print("NOTE: Only ~12 wells in 2026 dataset are available as open data.")
    print("      For full registry (~10,000+ wells), register with CNH at https://cnh.gob.mx/")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
