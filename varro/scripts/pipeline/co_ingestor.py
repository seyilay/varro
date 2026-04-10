#!/usr/bin/env python3
"""
Colorado COGCC Well Ingestor — psycopg2 + COPY staging (low-latency optimized)
Source: ArcGIS Online - Oil and Gas Well Locations COGCC 2022
Target: Supabase wells table via direct Postgres connection

Strategy: minimize DB round-trips (each costs ~30s)
  1. Load all existing operators into cache (1 SELECT)
  2. Fetch all ArcGIS pages into memory (~61 HTTP requests)
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
    "https://services5.arcgis.com/enGOFVOIYC8OyheQ/arcgis/rest/services"
    "/Oil_and_Gas_Well_Locations_COGCC_2022_Mar_23/FeatureServer/0"
)
ARCGIS_FIELDS = (
    "API,API_Label,API_County,Operator,Well_Name,Facil_Stat,"
    "Spud_Date,Max_MD,Max_TVD,Field_Name,Latitude,Longitude"
)

FETCH_PAGE_SIZE = 2000
DATA_SOURCE     = "COGCC_CO"
JURISDICTION    = "COGCC"
STATE           = "Colorado"

STATUS_MAP = {
    "PR": "PRODUCING", "AL": "PRODUCING", "AC": "PRODUCING",
    "PA": "PA", "DA": "PA",
    "SI": "SHUT_IN", "SU": "SHUT_IN",
    "TA": "TA",
}

CO_COUNTIES = {
    "001": "Adams", "003": "Alamosa", "005": "Arapahoe", "007": "Archuleta",
    "009": "Baca", "011": "Bent", "013": "Boulder", "014": "Broomfield",
    "015": "Chaffee", "017": "Cheyenne", "019": "Clear Creek", "021": "Conejos",
    "023": "Costilla", "025": "Crowley", "027": "Custer", "029": "Delta",
    "031": "Denver", "033": "Dolores", "035": "Douglas", "037": "Eagle",
    "039": "Elbert", "041": "El Paso", "043": "Fremont", "045": "Garfield",
    "047": "Gilpin", "049": "Grand", "051": "Gunnison", "053": "Hinsdale",
    "055": "Huerfano", "057": "Jackson", "059": "Jefferson", "061": "Kiowa",
    "063": "Kit Carson", "065": "Lake", "067": "La Plata", "069": "Larimer",
    "071": "Las Animas", "073": "Lincoln", "075": "Logan", "077": "Mesa",
    "079": "Mineral", "081": "Moffat", "083": "Montezuma", "085": "Montrose",
    "087": "Morgan", "089": "Otero", "091": "Ouray", "093": "Park",
    "095": "Phillips", "097": "Pitkin", "099": "Prowers", "101": "Pueblo",
    "103": "Rio Blanco", "105": "Rio Grande", "107": "Routt", "109": "Saguache",
    "111": "San Juan", "113": "San Miguel", "115": "Sedgwick", "117": "Summit",
    "119": "Teller", "121": "Washington", "123": "Weld", "125": "Yuma",
}


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


def epoch_ms_to_date(ms) -> str | None:
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    except Exception:
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
def fetch_all_wells() -> list[dict]:
    """Fetch all wells from ArcGIS, return list of attribute dicts."""
    count_url = f"{ARCGIS_BASE}/query?where=1%3D1&returnCountOnly=true&f=json"
    total = http_get_json(count_url).get("count", 0)
    print(f"  ArcGIS count: {total:,}", flush=True)

    all_features = []
    offset = 0
    while offset < total:
        params = urllib.parse.urlencode({
            "where": "1=1",
            "outFields": ARCGIS_FIELDS,
            "resultOffset": offset,
            "resultRecordCount": FETCH_PAGE_SIZE,
            "f": "json",
        })
        try:
            features = http_get_json(f"{ARCGIS_BASE}/query?{params}").get("features", [])
        except Exception as exc:
            print(f"  [WARN] fetch offset={offset}: {exc}", flush=True)
            time.sleep(3)
            offset += FETCH_PAGE_SIZE
            continue

        if not features:
            break

        all_features.extend(f["attributes"] for f in features)
        if len(all_features) % 10000 < FETCH_PAGE_SIZE:
            print(f"  Fetched {len(all_features):,} / {total:,}", flush=True)
        offset += FETCH_PAGE_SIZE
        time.sleep(0.05)

    print(f"  Fetch complete: {len(all_features):,} records", flush=True)
    return all_features


# ─── Phase 2+3: DB work ───────────────────────────────────────────────────────
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

    # 1 SELECT for all
    print(f"  SELECT existing operators ({len(name_list):,} names)...", flush=True)
    cur.execute("SELECT id, name FROM operators WHERE name = ANY(%s)", (name_list,))
    for row in cur.fetchall():
        op_map[row[1]] = str(row[0])
    print(f"    {len(op_map):,} existing found", flush=True)

    # 1 INSERT for missing
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
        api_label = a.get("API_Label") or a.get("API") or None
        api_number = f"CO-{api_label}" if api_label else None
        county_code = (a.get("API_County") or "").strip().zfill(3)
        county = CO_COUNTIES.get(county_code) or a.get("Field_Name") or None
        op_name = (a.get("Operator") or "").strip()
        op_id = op_map.get(op_name)
        depth = parse_depth(a.get("Max_TVD")) or parse_depth(a.get("Max_MD"))
        buf.write("\t".join([
            esc(api_number),
            esc(a.get("Well_Name") or None),
            esc(op_id),
            esc(STATE),
            esc(county),
            esc(None),  # well_type
            esc(depth),
            esc(epoch_ms_to_date(a.get("Spud_Date"))),
            esc(STATUS_MAP.get((a.get("Facil_Stat") or "").strip().upper())),
            esc(DATA_SOURCE),
            esc(str(a.get("API")) if a.get("API") is not None else None),
            esc(JURISDICTION),
            esc(a.get("Latitude") or None),
            esc(a.get("Longitude") or None),
        ]) + "\n")
    buf.seek(0)
    return buf


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    print("=== Colorado COGCC Well Ingestor (psycopg2+COPY, low-latency) ===", flush=True)
    t_start = time.time()

    # ── Phase 1: Fetch from ArcGIS (no DB needed) ──
    print("\n[Phase 1] Fetching all wells from ArcGIS...", flush=True)
    t0 = time.time()
    all_attrs = fetch_all_wells()
    print(f"  Fetch phase: {time.time()-t0:.0f}s", flush=True)

    # Collect unique operator names
    op_names = {(a.get("Operator") or "").strip() for a in all_attrs if (a.get("Operator") or "").strip()}
    print(f"  Unique operators: {len(op_names):,}", flush=True)

    # ── Phase 2: Connect + resolve operators ──
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

    # Final count
    conn2 = psycopg2.connect(DB_DSN)
    cur2 = conn2.cursor()
    cur2.execute("SELECT COUNT(*) FROM wells WHERE data_source = %s", (DATA_SOURCE,))
    db_count = cur2.fetchone()[0]
    conn2.close()
    print(f"  DB count ({DATA_SOURCE}): {db_count:,}", flush=True)


if __name__ == "__main__":
    main()
