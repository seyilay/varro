#!/usr/bin/env python3
"""
Ingest Argentina well data from datos.energia.gob.ar (Secretaría de Energía).

VERIFIED ENDPOINT (tested 2026-04-05):
  CSV: http://datos.energia.gob.ar/dataset/c846e79c-026c-4040-897f-1ad3543b407c/resource/cb5c0f04-7835-45cd-b982-3e25ca7d7751/download/capitulo-iv-pozos.csv

  Dataset: "Producción de petróleo y gas por pozo (Capítulo IV)"
  CKAN API: http://datos.energia.gob.ar/api/3/action/package_show?id=produccion-de-petroleo-y-gas-por-pozo

FIELD MAPPING (from capitulo-iv-pozos.csv):
  idpozo         → api_number (unique numeric well ID)
  sigla          → well_name
  tipopozo       → well_type (mapped from Spanish)
  empresa        → operator (name, looked up in operators table)
  geojson        → latitude/longitude (GeoJSON Point, WGS84)
  provincia      → state (Argentine province)
  tipoestado     → status (mapped)

DATA SIZE: ~60,000+ wells (all Argentine hydrocarbon wells)
COORDINATES: WGS84 lat/lon embedded in geojson column as {"type":"Point","coordinates":[lon,lat]}

NOTE: datos.gob.ar main portal is Cloudflare-protected for browser traffic but
      the datos.energia.gob.ar subdomain allows direct CSV downloads.

Usage:
  python ingest_argentina.py          # full ingest
  python ingest_argentina.py --test   # first 100 rows only
"""

import argparse
import codecs
import csv
import json
import sys
import time
from urllib.request import urlopen, Request
from urllib.error import URLError

import psycopg2

# ── Config ──────────────────────────────────────────────────────────────────
DB_DSN = "postgresql://postgres:$DB_PASSWORD@db.temtptsfiksixxhbigkg.supabase.co:5432/postgres"

CSV_URL = (
    "http://datos.energia.gob.ar/dataset/c846e79c-026c-4040-897f-1ad3543b407c"
    "/resource/cb5c0f04-7835-45cd-b982-3e25ca7d7751/download/capitulo-iv-pozos.csv"
)
DATA_SOURCE = "SE_AR"

# ── Well type mapping (tipopozo field) ────────────────────────────────────────
WELL_TYPE_MAP = {
    "petrolífero":          "OIL",
    "petrolifero":          "OIL",
    "gasífero":             "GAS",
    "gasifero":             "GAS",
    "petrolífero-gasífero": "OIL_GAS",
    "petrolifero-gasifero": "OIL_GAS",
    "inyección de agua":    "INJECTION",
    "inyeccion de agua":    "INJECTION",
    "inyección de gas":     "INJECTION",
    "inyeccion de gas":     "INJECTION",
    "inyección":            "INJECTION",
    "inyeccion":            "INJECTION",
    "disposal":             "DISPOSAL",
    "eliminación":          "DISPOSAL",
    "eliminacion":          "DISPOSAL",
}

def map_well_type(raw: str) -> str | None:
    if not raw:
        return None
    key = raw.strip().lower()
    if key in WELL_TYPE_MAP:
        return WELL_TYPE_MAP[key]
    if "petrol" in key and "gas" in key:
        return "OIL_GAS"
    if "petrol" in key:
        return "OIL"
    if "gas" in key:
        return "GAS"
    if "inyec" in key or "inject" in key:
        return "INJECTION"
    if "dispos" in key or "eliminac" in key:
        return "DISPOSAL"
    return None

# ── Status mapping (tipoestado field) ─────────────────────────────────────────
STATUS_MAP = {
    "extracción efectiva":     "PRODUCING",
    "extraccion efectiva":     "PRODUCING",
    "en producción":           "PRODUCING",
    "en produccion":           "PRODUCING",
    "en inyección efectiva":   "PRODUCING",
    "en inyeccion efectiva":   "PRODUCING",
    "cerrado":                 "IDLE",
    "shut-in":                 "SHUT_IN",
    "temporariamente cerrado": "SHUT_IN",
    "taponado y abandonado":   "PA",
    "taponado y abd.":         "PA",
    "abandonado":              "PA",
    "sin sistema de extracción":"IDLE",
    "sin sistema de extraccion":"IDLE",
    "en perforación":          "IDLE",
    "en perforacion":          "IDLE",
    "en terminación":          "IDLE",
    "en terminacion":          "IDLE",
}

def map_status(raw: str) -> str | None:
    if not raw:
        return None
    key = raw.strip().lower()
    if key in STATUS_MAP:
        return STATUS_MAP[key]
    if "producc" in key or "extrac" in key:
        return "PRODUCING"
    if "cerrado" in key or "shut" in key:
        return "SHUT_IN"
    if "abandon" in key or "taponad" in key:
        return "PA"
    if "temporal" in key:
        return "TA"
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
        (name, "AR")
    )
    row = cur.fetchone()
    if row:
        cache[name] = row[0]
        return row[0]
    return None


def parse_geojson_coords(geojson_str: str) -> tuple[float | None, float | None]:
    """Parse {"type":"Point","coordinates":[lon,lat]} → (lat, lon)."""
    if not geojson_str:
        return None, None
    try:
        # The geojson column may use doubled quotes in some CSV encodings
        gj_str = geojson_str.replace('""', '"').strip()
        if gj_str.startswith('"') and gj_str.endswith('"'):
            gj_str = gj_str[1:-1]
        obj = json.loads(gj_str)
        if obj.get("type") == "Point":
            coords = obj.get("coordinates", [])
            if len(coords) >= 2:
                lon, lat = float(coords[0]), float(coords[1])
                if -90 <= lat <= 90 and -180 <= lon <= 180:
                    return round(lat, 8), round(lon, 8)
    except (json.JSONDecodeError, ValueError, TypeError):
        pass
    return None, None


def stream_rows(limit: int | None):
    """Stream CSV rows from the URL, yielding each row dict."""
    print(f"  Streaming CSV from {CSV_URL} ...")
    req = Request(CSV_URL, headers={"User-Agent": "Mozilla/5.0"})
    for attempt in range(3):
        try:
            resp = urlopen(req, timeout=120)
            # Wrap response in a text-mode reader for csv.DictReader
            reader_stream = codecs.getreader("utf-8")(resp, errors="replace")
            csv_reader = csv.DictReader(reader_stream)
            count = 0
            for row in csv_reader:
                if limit is not None and count >= limit:
                    break
                yield row
                count += 1
            resp.close()
            return
        except URLError as e:
            print(f"  [warn] Stream failed (attempt {attempt+1}): {e}")
            time.sleep(2 ** attempt)
    raise RuntimeError("CSV stream failed after 3 attempts")


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Ingest Argentina wells")
    parser.add_argument("--test", action="store_true", help="Ingest first 100 rows only")
    args = parser.parse_args()

    limit = 100 if args.test else None
    print(f"[SE_AR] Starting ingest | test={args.test}")

    conn = psycopg2.connect(DB_DSN)
    cur  = conn.cursor()

    operator_cache: dict[str, int] = {}
    total_inserted = 0
    total_skipped  = 0
    batch_rows: list = []
    row_count = 0

    INSERT_SQL = """
        INSERT INTO wells (
            api_number, well_name, well_type, operator_id,
            latitude, longitude, state, status,
            data_source, source_record_id
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING
    """

    for row in stream_rows(limit):
        row_count += 1

        # api_number: use idpozo (unique numeric well ID)
        api_number = (row.get("idpozo") or "").strip()
        if not api_number:
            total_skipped += 1
            continue

        # Prefix with country code so it doesn't collide
        api_number = f"AR-{api_number}"

        # Coordinates from geojson column
        lat, lon = parse_geojson_coords(row.get("geojson", ""))

        # Operator
        empresa = (row.get("empresa") or "").strip()
        operator_id = get_or_create_operator(cur, empresa, operator_cache)

        # Province (state)
        provincia = (row.get("provincia") or "").strip() or None

        batch_rows.append((
            api_number[:100],
            (row.get("sigla") or "")[:255] or None,
            map_well_type(row.get("tipopozo")),
            operator_id,
            lat,
            lon,
            provincia[:50] if provincia else None,
            map_status(row.get("tipoestado")),
            DATA_SOURCE,
            api_number[:100],
        ))

        # Commit every 10k rows
        if len(batch_rows) >= 10_000:
            cur.executemany(INSERT_SQL, batch_rows)
            conn.commit()
            total_inserted += len(batch_rows)
            print(f"  Committed {total_inserted:,} rows so far...")
            batch_rows = []

    # Flush remainder
    if batch_rows:
        cur.executemany(INSERT_SQL, batch_rows)
        conn.commit()
        total_inserted += len(batch_rows)

    print(f"[SE_AR] Done. Processed={row_count}, Inserted={total_inserted}, Skipped={total_skipped}")
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
