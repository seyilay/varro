"""
Louisiana SONRIS Well Ingestor v2

Uses psycopg2 + COPY for bulk inserts (much faster than REST API).

Sources:
  1. Primary: la_wells_021917 layer 1   — 85,562 wells (richest SONRIS fields)
  2. Secondary: Louisiana_wells_2_24_22 layer 0 — 63,111 wells (Feb 2024 update)
  3. FracTracker national filtered for LA — 53,017 wells (additional coverage)

Strategy:
  - Stream all wells from ArcGIS Feature Services
  - Batch upsert operators via psycopg2
  - COPY wells into _stage temp table, then INSERT ... ON CONFLICT DO NOTHING

Target: 200,000+ unique wells
"""

import io
import json
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

# ─── Connection ────────────────────────────────────────────────────────────
DSN = "postgresql://postgres:$DB_PASSWORD@db.temtptsfiksixxhbigkg.supabase.co:5432/postgres"

# ─── ArcGIS Feature Service URLs ──────────────────────────────────────────
PRIMARY_FS = (
    "https://services.arcgis.com/jDGuO8tYggdCCnUJ/arcgis/rest"
    "/services/la_wells_021917/FeatureServer/1"
)
SECONDARY_FS = (
    "https://services.arcgis.com/jDGuO8tYggdCCnUJ/arcgis/rest"
    "/services/Louisiana_wells_2_24_22/FeatureServer/0"
)
FRACTRACKER_FS = (
    "https://services.arcgis.com/jDGuO8tYggdCCnUJ/arcgis/rest"
    "/services/FracTrackerNationalWells_Part1_012022/FeatureServer/0"
)

PAGE_SIZE = 2000  # ArcGIS max records per page

# ─── Status / type mappings ───────────────────────────────────────────────
SONRIS_STATUS_MAP = {
    "01": "PRODUCING", "10": "PRODUCING",
    "02": "IDLE", "17": "IDLE", "18": "IDLE", "22": "IDLE", "23": "IDLE",
    "26": "SHUT_IN",
    "03": "TA", "73": "TA",
    "09": "PA", "28": "PA", "31": "PA", "33": "PA",
    "34": "PA", "35": "PA", "37": "PA", "49": "PA",
}
PRODUCT_TYPE_MAP = {"10": "OIL", "20": "GAS", "00": None}

FRACTRACKER_STATUS_MAP = {
    "Active": "PRODUCING",
    "Producing": "PRODUCING",
    "Shut In": "SHUT_IN",
    "Shut-In": "SHUT_IN",
    "Idle": "IDLE",
    "Temporarily Abandoned": "TA",
    "TA": "TA",
    "Plugged and Abandoned": "PA",
    "Plugged & Abandoned": "PA",
    "PA": "PA",
    "P&A": "PA",
}

# ─── Helpers ──────────────────────────────────────────────────────────────

def _epoch_ms_to_date(ms):
    if ms is None:
        return None
    try:
        dt = datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


def _arcgis_query(base_url, params, timeout=60):
    params["f"] = "json"
    qs = urllib.parse.urlencode(params)
    url = f"{base_url}/query?{qs}"
    req = urllib.request.Request(url)
    req.add_header("User-Agent", "Varro-Well-Ingestor/2.0")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def _get_total_count(base_url, where="1=1"):
    data = _arcgis_query(base_url, {"where": where, "returnCountOnly": "true"})
    return data.get("count", 0)


def _paginate_features(base_url, out_fields, where="1=1", label=""):
    """Yield all features from an ArcGIS FS layer via pagination."""
    total = _get_total_count(base_url, where)
    print(f"  [{label}] {total:,} total features")

    offset = 0
    page = 0
    while offset < total:
        page += 1
        for attempt in range(4):
            try:
                data = _arcgis_query(
                    base_url,
                    {
                        "where": where,
                        "outFields": out_fields,
                        "outSR": "4326",
                        "resultOffset": str(offset),
                        "resultRecordCount": str(PAGE_SIZE),
                        "orderByFields": "OBJECTID ASC",
                    },
                    timeout=60,
                )
                features = data.get("features", [])
                break
            except Exception as e:
                wait = 2 ** attempt * 2
                print(f"  [{label}] Page {page} attempt {attempt+1} failed: {e}. Waiting {wait}s...")
                time.sleep(wait)
                features = []

        if not features:
            print(f"  [{label}] No features at offset {offset}, stopping")
            break

        yield from features
        offset += len(features)

        if page % 20 == 0:
            print(f"  [{label}] {offset:,}/{total:,} retrieved...")

        if len(features) < PAGE_SIZE:
            break

        time.sleep(0.05)  # be polite to ArcGIS

    print(f"  [{label}] Done: {offset:,} features fetched")


# ─── Well parsers ─────────────────────────────────────────────────────────

def parse_primary_feature(feat):
    """Parse la_wells_021917 layer 1 feature."""
    attrs = feat.get("attributes", {}) or {}
    geom = feat.get("geometry") or {}

    api_num = str(attrs.get("API_NUM") or "").strip()
    if not api_num or api_num == "None":
        return None

    # Status
    status_code = str(attrs.get("WELL_STATU") or "").strip()
    status = SONRIS_STATUS_MAP.get(status_code)

    # Well type
    product_code = str(attrs.get("PRODUCT_TY") or "").strip()
    well_type = PRODUCT_TYPE_MAP.get(product_code)

    # Depth
    depth = attrs.get("MEASURED_D")
    try:
        depth = int(float(depth)) if depth is not None else None
    except (ValueError, TypeError):
        depth = None

    # Spud date
    spud_date = _epoch_ms_to_date(attrs.get("SPUD_DATE"))

    return {
        "api_number": api_num,
        "well_name": str(attrs.get("WELL_NAME") or "").strip() or None,
        "operator_name": str(attrs.get("ORG_OPER_N") or "").strip() or None,
        "latitude": geom.get("y"),
        "longitude": geom.get("x"),
        "county": str(attrs.get("PARISH_NAM") or "").strip() or None,
        "well_type": well_type,
        "total_depth_ft": depth,
        "spud_date": spud_date,
        "status": status,
        "source_record_id": str(attrs.get("OBJECTID") or api_num),
    }


def parse_secondary_feature(feat, field_names):
    """Parse Louisiana_wells_2_24_22 layer 0 feature (same field schema as primary)."""
    return parse_primary_feature(feat)


def parse_fractracker_feature(feat):
    """Parse FracTrackerNationalWells feature (different schema)."""
    attrs = feat.get("attributes", {}) or {}
    geom = feat.get("geometry") or {}

    api_raw = attrs.get("API")
    if api_raw is None:
        return None
    # API is stored as a float like 17001200440000.0
    try:
        api_num = str(int(float(api_raw)))
    except (ValueError, TypeError):
        return None

    if not api_num or len(api_num) < 10:
        return None

    status_raw = str(attrs.get("Status") or "").strip()
    status = FRACTRACKER_STATUS_MAP.get(status_raw)

    spud_date = _epoch_ms_to_date(attrs.get("SpudDt"))

    well_type = str(attrs.get("Type") or "").strip().upper() or None
    if well_type and len(well_type) > 50:
        well_type = well_type[:50]

    return {
        "api_number": api_num,
        "well_name": str(attrs.get("Name") or "").strip() or None,
        "operator_name": str(attrs.get("Operator") or "").strip() or None,
        "latitude": geom.get("y") or attrs.get("Lat"),
        "longitude": geom.get("x") or attrs.get("Long"),
        "county": None,
        "well_type": well_type,
        "total_depth_ft": None,
        "spud_date": spud_date,
        "status": status,
        "source_record_id": str(attrs.get("OBJECTID") or api_num),
    }


# ─── Operator upsert ──────────────────────────────────────────────────────

def upsert_operators(conn, operator_names):
    """Bulk upsert operators, return {name: uuid} map."""
    if not operator_names:
        return {}

    print(f"  [ops] Upserting {len(operator_names):,} unique operators...")
    op_map = {}
    names_list = sorted(n for n in operator_names if n)

    BATCH = 500
    with conn.cursor() as cur:
        for i in range(0, len(names_list), BATCH):
            batch = names_list[i:i + BATCH]
            # Use executemany with ON CONFLICT DO UPDATE RETURNING
            values = [(name,) for name in batch]
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO operators (name)
                VALUES %s
                ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
                RETURNING id, name
                """,
                values,
                template="(%s)",
                page_size=500,
            )
            rows = cur.fetchall()
            for uid, name in rows:
                op_map[name.strip()] = str(uid)
        conn.commit()

    print(f"  [ops] {len(op_map):,} operators in map")
    return op_map


def load_existing_operators(conn):
    """Load all existing operators from DB into {name: uuid} map."""
    print("  [ops] Loading existing operators from DB...")
    op_map = {}
    with conn.cursor() as cur:
        cur.execute("SELECT id, name FROM operators")
        for uid, name in cur.fetchall():
            op_map[name.strip()] = str(uid)
    print(f"  [ops] Loaded {len(op_map):,} existing operators")
    return op_map


# ─── Bulk well insert via COPY ────────────────────────────────────────────

def bulk_insert_wells(conn, wells_batch, op_map):
    """
    Insert a batch of well dicts using COPY to temp table, then INSERT...ON CONFLICT.
    Returns count of newly inserted rows.
    """
    if not wells_batch:
        return 0

    # Build CSV-like buffer for COPY
    buf = io.StringIO()
    cols = [
        "api_number", "well_name", "operator_id", "state", "county",
        "well_type", "total_depth_ft", "spud_date", "status",
        "data_source", "source_record_id", "regulatory_jurisdiction",
        "latitude", "longitude",
    ]

    for w in wells_batch:
        op_name = w.get("operator_name")
        op_id = op_map.get(op_name) if op_name else None

        def escape(v):
            if v is None:
                return "\\N"
            s = str(v).replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")
            return s

        row = [
            escape(w.get("api_number")),
            escape(w.get("well_name")),
            escape(op_id),
            "Louisiana",
            escape(w.get("county")),
            escape(w.get("well_type")),
            escape(w.get("total_depth_ft")),
            escape(w.get("spud_date")),
            escape(w.get("status")),
            "SONRIS_LA",
            escape(w.get("source_record_id")),
            "SONRIS",
            escape(w.get("latitude")),
            escape(w.get("longitude")),
        ]
        buf.write("\t".join(row) + "\n")

    buf.seek(0)

    with conn.cursor() as cur:
        # Create temp table
        cur.execute("""
            CREATE TEMP TABLE IF NOT EXISTS _wells_stage (
                api_number TEXT,
                well_name TEXT,
                operator_id TEXT,
                state TEXT,
                county TEXT,
                well_type TEXT,
                total_depth_ft TEXT,
                spud_date TEXT,
                status TEXT,
                data_source TEXT,
                source_record_id TEXT,
                regulatory_jurisdiction TEXT,
                latitude TEXT,
                longitude TEXT
            ) ON COMMIT PRESERVE ROWS
        """)
        cur.execute("TRUNCATE _wells_stage")

        # COPY into temp table
        cur.copy_expert(
            "COPY _wells_stage FROM STDIN WITH (FORMAT text, NULL '\\N')",
            buf,
        )

        # INSERT from stage into wells
        cur.execute("""
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
                CASE WHEN total_depth_ft ~ '^[0-9]+$' THEN total_depth_ft::integer ELSE NULL END,
                CASE WHEN spud_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN spud_date::date ELSE NULL END,
                CASE WHEN status IN ('PRODUCING','IDLE','SHUT_IN','TA','PA') THEN status ELSE NULL END,
                data_source,
                source_record_id,
                regulatory_jurisdiction,
                CASE WHEN latitude ~ '^-?[0-9]+\.?[0-9]*$' THEN latitude::numeric ELSE NULL END,
                CASE WHEN longitude ~ '^-?[0-9]+\.?[0-9]*$' THEN longitude::numeric ELSE NULL END
            FROM _wells_stage
            WHERE api_number IS NOT NULL AND api_number != ''
            ON CONFLICT (api_number) DO NOTHING
        """)
        inserted = cur.rowcount
        conn.commit()

    return inserted


# ─── Main ──────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("Louisiana SONRIS Well Ingestor v2")
    print("Using psycopg2 + COPY for bulk inserts")
    print("=" * 60)

    # Connect
    print("\n[1] Connecting to Postgres...")
    conn = psycopg2.connect(DSN, connect_timeout=30)
    conn.autocommit = False
    print("  Connected!")

    # ── Collect all wells from all sources ──────────────────────────
    print("\n[2] Fetching wells from ArcGIS sources...")
    all_wells = []
    seen_apis = set()
    all_operator_names = set()

    # Primary source
    print("\n  === Source 1: la_wells_021917 (L1) ===")
    primary_fields = "OBJECTID,API_NUM,WELL_NAME,WELL_STATU,PARISH_NAM,MEASURED_D,SPUD_DATE,PRODUCT_TY,ORG_OPER_N"
    for feat in _paginate_features(PRIMARY_FS, primary_fields, label="primary"):
        w = parse_primary_feature(feat)
        if w and w["api_number"] not in seen_apis:
            seen_apis.add(w["api_number"])
            all_wells.append(w)
            if w.get("operator_name"):
                all_operator_names.add(w["operator_name"])

    print(f"  Primary: {len(all_wells):,} unique wells collected")

    # Secondary source
    print("\n  === Source 2: Louisiana_wells_2_24_22 (L0) ===")
    secondary_fields = "OBJECTID,API_NUM,WELL_NAME,WELL_STATU,PARISH_NAM,MEASURED_D,SPUD_DATE,PRODUCT_TY,ORG_OPER_N"
    before = len(all_wells)
    for feat in _paginate_features(SECONDARY_FS, secondary_fields, label="secondary"):
        w = parse_secondary_feature(feat, set())
        if w and w["api_number"] not in seen_apis:
            seen_apis.add(w["api_number"])
            all_wells.append(w)
            if w.get("operator_name"):
                all_operator_names.add(w["operator_name"])

    print(f"  Secondary added: {len(all_wells) - before:,} new wells")
    print(f"  Running total: {len(all_wells):,}")

    # FracTracker national (Louisiana only)
    print("\n  === Source 3: FracTracker National (Louisiana filter) ===")
    ft_where = "State='LA' OR State='Louisiana'"
    ft_fields = "OBJECTID,API,Name,Operator,SpudDt,Status,Type,Lat,Long"
    before = len(all_wells)
    for feat in _paginate_features(FRACTRACKER_FS, ft_fields, where=ft_where, label="fractracker"):
        w = parse_fractracker_feature(feat)
        if w and w["api_number"] not in seen_apis:
            seen_apis.add(w["api_number"])
            all_wells.append(w)
            if w.get("operator_name"):
                all_operator_names.add(w["operator_name"])

    print(f"  FracTracker added: {len(all_wells) - before:,} new wells")
    print(f"\n  TOTAL unique wells collected: {len(all_wells):,}")
    print(f"  Unique operator names: {len(all_operator_names):,}")

    # ── Upsert operators ───────────────────────────────────────────
    print("\n[3] Upserting operators...")
    op_map = load_existing_operators(conn)
    # Find new operators not yet in DB
    new_ops = all_operator_names - set(op_map.keys())
    if new_ops:
        new_op_map = upsert_operators(conn, new_ops)
        op_map.update(new_op_map)
    print(f"  Operator map size: {len(op_map):,}")

    # ── Bulk insert wells via COPY ─────────────────────────────────
    print("\n[4] Bulk inserting wells via COPY...")
    BATCH_SIZE = 10000
    total_inserted = 0
    total_skipped = 0

    for i in range(0, len(all_wells), BATCH_SIZE):
        batch = all_wells[i:i + BATCH_SIZE]
        inserted = bulk_insert_wells(conn, batch, op_map)
        skipped = len(batch) - inserted
        total_inserted += inserted
        total_skipped += skipped
        pct = (i + len(batch)) / len(all_wells) * 100
        print(f"  Batch {i//BATCH_SIZE + 1}: {inserted:,} inserted, {skipped:,} skipped [{pct:.0f}%]")

    print(f"\n{'='*60}")
    print(f"✓ DONE")
    print(f"  Total wells collected: {len(all_wells):,}")
    print(f"  Newly inserted:        {total_inserted:,}")
    print(f"  Skipped (duplicate):   {total_skipped:,}")
    print(f"{'='*60}")

    conn.close()
    return total_inserted


if __name__ == "__main__":
    inserted = main()
    sys.exit(0 if inserted >= 0 else 1)
