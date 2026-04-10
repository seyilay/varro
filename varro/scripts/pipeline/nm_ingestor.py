#!/usr/bin/env python3
"""
New Mexico OCD Well Ingestor — psycopg2 + COPY staging (low-latency optimized)
Source: ArcGIS Online - NM Wells District All (OCD)
Target: Supabase wells table via direct Postgres connection

Strategy: minimize DB round-trips (each costs ~30s)
  1. Load all existing operators into cache (1 SELECT)
  2. Fetch all ArcGIS pages into memory (~60 HTTP requests)
  3. Bulk INSERT new operators (1-2 queries)
  4. COPY all wells to temp table + INSERT (2 queries)
  Total: ~5 DB queries = ~3 minutes
"""

import io
import json
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values

# ─── Config ───────────────────────────────────────────────────────────────────
DB_DSN = (
    "postgresql://postgres:$DB_PASSWORD@db.temtptsfiksixxhbigkg.supabase.co:5432/postgres"
    "?options=-c%20statement_timeout%3D0&connect_timeout=60"
)

ARCGIS_BASE = (
    "https://services.arcgis.com/LGtNQDlIZBdntoA9/arcgis/rest/services"
    "/NM_Wells_District_All_UTM_NAD83_Z13_SHP/FeatureServer/0"
)
ARCGIS_FIELDS = (
    "API,wellname,ogrid_name,well_type,status,tot_depth,spud_date,"
    "latitude,longitude,district"
)

FETCH_PAGE_SIZE = 2000
DATA_SOURCE     = "NMOCD_NM"
JURISDICTION    = "NMOCD"
STATE           = "New Mexico"

NM_DISTRICTS = {
    1: "Northwest", 2: "Southeast", 3: "Northeast",
    4: "Southwest", 5: "Central",
}

STATUS_MAP = {
    "P": "PRODUCING", "A": "PRODUCING",
    "S": "SHUT_IN",
    "T": "TA",
    "D": "PA", "X": "PA",
}

WELL_TYPE_MAP = {"O": "OIL", "G": "GAS"}


# ─── HTTP helpers ─────────────────────────────────────────────────────────────
def http_get_json(url: str, retries: int = 3) -> dict:
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "varro-ingestor/2.0"})
            with urllib.request.urlopen(req, timeout=90) as r:
                return json.loads(r.read())
        except Exception as exc:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise


def parse_date(raw) -> str | None:
    if not raw:
        return None
    s = str(raw).strip()
    try:
        parts = s.split("/")
        if len(parts) == 3:
            m, d, y = int(parts[0]), int(parts[1]), int(parts[2])
            if 1800 <= y <= 2100 and 1 <= m <= 12 and 1 <= d <= 31:
                return f"{y:04d}-{m:02d}-{d:02d}"
    except Exception:
        pass
    try:
        ms = int(s)
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    except Exception:
        pass
    return None


def parse_depth(raw) -> int | None:
    try:
        v = float(raw)
        return int(v) if v > 0 else None
    except Exception:
        return None


def esc(v) -> str:
    if v is None:
        return r"\N"
    return str(v).replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")


# ─── Phase 1: Fetch all ArcGIS data ──────────────────────────────────────────
def fetch_all_wells() -> tuple[list[dict], int]:
    """Fetch all wells from ArcGIS, return list of attribute dicts."""
    count_url = f"{ARCGIS_BASE}/query?where=1%3D1&returnCountOnly=true&f=json"
    total = http_get_json(count_url).get("count", 0)
    print(f"  ArcGIS count: {total:,}", flush=True)

    meta = http_get_json(f"{ARCGIS_BASE}?f=json")
    server_max = meta.get("maxRecordCount", FETCH_PAGE_SIZE)
    page_size = min(FETCH_PAGE_SIZE, server_max)

    all_attrs = []
    offset = 0
    while offset < total:
        params = urllib.parse.urlencode({
            "where": "1=1",
            "outFields": ARCGIS_FIELDS,
            "resultOffset": offset,
            "resultRecordCount": page_size,
            "f": "json",
        })
        try:
            features = http_get_json(f"{ARCGIS_BASE}/query?{params}").get("features", [])
        except Exception as exc:
            print(f"  [WARN] fetch offset={offset}: {exc}", flush=True)
            time.sleep(3)
            offset += page_size
            continue

        if not features:
            break

        all_attrs.extend(f["attributes"] for f in features)
        if len(all_attrs) % 10000 < page_size:
            print(f"  Fetched {len(all_attrs):,} / {total:,}", flush=True)
        offset += page_size
        time.sleep(0.05)

    print(f"  Fetch complete: {len(all_attrs):,} records", flush=True)
    return all_attrs, page_size


# ─── DB helpers ───────────────────────────────────────────────────────────────
TEMP_DDL = """
CREATE TEMP TABLE _stage_wells (
    api_number            TEXT,
    well_name             TEXT,
    operator_id           TEXT,
    state                 TEXT,
    county                TEXT,
    well_type             TEXT,
    total_depth_ft        TEXT,
    spud_date             TEXT,
    status                TEXT,
    data_source           TEXT,
    source_record_id      TEXT,
    regulatory_jurisdiction TEXT,
    latitude              TEXT,
    longitude             TEXT
)
"""

INSERT_SQL = """
INSERT INTO wells (
    api_number, well_name, operator_id, state, county,
    well_type, total_depth_ft, spud_date, status,
    data_source, source_record_id, regulatory_jurisdiction,
    latitude, longitude
)
SELECT
    api_number,
    well_name,
    operator_id::uuid,
    state,
    county,
    well_type,
    total_depth_ft::integer,
    spud_date::date,
    status,
    data_source,
    source_record_id,
    regulatory_jurisdiction,
    latitude::numeric,
    longitude::numeric
FROM _stage_wells
WHERE api_number IS NOT NULL
ON CONFLICT (api_number) DO NOTHING
"""


def resolve_operators_bulk(cur, names: set[str]) -> dict[str, str]:
    """Bulk resolve: 1 SELECT + 1 INSERT. Returns name→id map."""
    op_map: dict[str, str] = {}
    name_list = list(names)

    print(f"  SELECT existing operators ({len(name_list):,} names)...", flush=True)
    cur.execute("SELECT id, name FROM operators WHERE name = ANY(%s)", (name_list,))
    for row in cur.fetchall():
        op_map[row[1]] = str(row[0])
    print(f"    {len(op_map):,} existing found", flush=True)

    missing = [n for n in name_list if n not in op_map]
    if missing:
        print(f"  INSERT {len(missing):,} new operators...", flush=True)
        rows = execute_values(
            cur,
            "INSERT INTO operators (name) VALUES %s RETURNING id, name",
            [(n,) for n in missing],
            fetch=True,
            page_size=500,
        )
        for row in rows:
            op_map[row[1]] = str(row[0])
        print(f"    Inserted {len(missing):,}", flush=True)

    return op_map


def build_copy_buffer(attrs_list: list[dict], op_map: dict[str, str]) -> io.StringIO:
    """Build TSV buffer for COPY."""
    buf = io.StringIO()
    for a in attrs_list:
        api_raw = (a.get("API") or "").strip()
        api_number = f"NM-{api_raw}" if api_raw else None
        district = a.get("district")
        county = NM_DISTRICTS.get(district, f"District {district}" if district else None)
        op_name = (a.get("ogrid_name") or "").strip()
        op_id = op_map.get(op_name)
        buf.write("\t".join([
            esc(api_number),
            esc(a.get("wellname") or None),
            esc(op_id),
            esc(STATE),
            esc(county),
            esc(WELL_TYPE_MAP.get((a.get("well_type") or "").strip().upper())),
            esc(parse_depth(a.get("tot_depth"))),
            esc(parse_date(a.get("spud_date"))),
            esc(STATUS_MAP.get((a.get("status") or "").strip().upper())),
            esc(DATA_SOURCE),
            esc(api_raw or None),
            esc(JURISDICTION),
            esc(a.get("latitude") or None),
            esc(a.get("longitude") or None),
        ]) + "\n")
    buf.seek(0)
    return buf


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    print("=== New Mexico OCD Well Ingestor (psycopg2+COPY, low-latency) ===", flush=True)
    t_start = time.time()

    # ── Phase 1: Fetch from ArcGIS ──
    print("\n[Phase 1] Fetching all wells from ArcGIS...", flush=True)
    t0 = time.time()
    all_attrs, page_size = fetch_all_wells()
    print(f"  Fetch phase: {time.time()-t0:.0f}s", flush=True)

    op_names = {(a.get("ogrid_name") or "").strip() for a in all_attrs if (a.get("ogrid_name") or "").strip()}
    print(f"  Unique operators: {len(op_names):,}", flush=True)

    # ── Phase 2: Connect + operators ──
    print("\n[Phase 2] Connecting to DB...", flush=True)
    t0 = time.time()
    conn = psycopg2.connect(DB_DSN)
    conn.autocommit = False
    print(f"  Connected in {time.time()-t0:.1f}s", flush=True)

    print("\n[Phase 3] Resolving operators (2 queries)...", flush=True)
    t0 = time.time()
    with conn.cursor() as cur:
        op_map = resolve_operators_bulk(cur, op_names)
    conn.commit()
    print(f"  Operator phase: {time.time()-t0:.0f}s, {len(op_map):,} operators", flush=True)

    # ── Phase 3: COPY all wells ──
    print("\n[Phase 4] Building COPY buffer...", flush=True)
    t0 = time.time()
    buf = build_copy_buffer(all_attrs, op_map)
    print(f"  Buffer built: {time.time()-t0:.1f}s", flush=True)

    print("[Phase 4] COPY to temp table + INSERT...", flush=True)
    t0 = time.time()
    with conn.cursor() as cur:
        cur.execute(TEMP_DDL)
        cur.copy_expert(r"COPY _stage_wells FROM STDIN WITH (FORMAT text, NULL '\N')", buf)
        print(f"  COPY done in {time.time()-t0:.1f}s, now INSERTing...", flush=True)
        t1 = time.time()
        cur.execute(INSERT_SQL)
        inserted = cur.rowcount
        print(f"  INSERT done in {time.time()-t1:.1f}s", flush=True)
    conn.commit()
    total_db_time = time.time() - t0

    skipped = len(all_attrs) - inserted
    conn.close()

    print(f"\n=== Done ===", flush=True)
    print(f"  State:         {STATE}", flush=True)
    print(f"  Fetched:       {len(all_attrs):,}", flush=True)
    print(f"  Inserted:      {inserted:,}", flush=True)
    print(f"  Skipped/dup:   {skipped:,}", flush=True)
    print(f"  DB phase time: {total_db_time:.0f}s", flush=True)
    print(f"  Total time:    {time.time()-t_start:.0f}s", flush=True)

    conn2 = psycopg2.connect(DB_DSN)
    cur2 = conn2.cursor()
    cur2.execute("SELECT COUNT(*) FROM wells WHERE data_source = %s", (DATA_SOURCE,))
    db_count = cur2.fetchone()[0]
    conn2.close()
    print(f"  DB count ({DATA_SOURCE}): {db_count:,}", flush=True)


if __name__ == "__main__":
    main()
