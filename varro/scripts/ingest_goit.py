#!/usr/bin/env python3
"""
Ingest GOIT (Global Oil Infrastructure Tracker) GeoJSON into Supabase.
Source: Global Energy Monitor - https://globalenergymonitor.org/projects/global-oil-infrastructure-tracker/
Data vintage: 2025-03

Usage:
    python3 ingest_goit.py

Requires the table to be created first via:
    /home/openclaw/.openclaw/workspace/varro/data/goit_schema.sql
"""

import json
import urllib.request
import urllib.error
import sys
from urllib.request import Request

# ── Config ────────────────────────────────────────────────────────────────────
SUPABASE_URL = "https://temtptsfiksixxhbigkg.supabase.co"
SUPABASE_KEY = "os.environ.get('SUPABASE_KEY')"
GEOJSON_PATH = "/home/openclaw/.openclaw/workspace/varro/data/raw/goit/goit_map_2025-03.geojson"
TABLE = "oil_infrastructure"
BATCH_SIZE = 200
DATA_VINTAGE = "2025-03"

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates",  # upsert on project_id UNIQUE
}

# ── Geometry helpers ──────────────────────────────────────────────────────────

def centroid_linestring(coords):
    """Mean of all [lon, lat] pairs in a LineString."""
    lons = [c[0] for c in coords]
    lats = [c[1] for c in coords]
    return sum(lons) / len(lons), sum(lats) / len(lats)

def centroid_multilinestring(coord_groups):
    """Mean centroid across all lines in a MultiLineString."""
    all_coords = [c for group in coord_groups for c in group]
    return centroid_linestring(all_coords)

def extract_centroid(geometry):
    """Return (lon, lat) centroid from any geometry. Returns (None, None) on failure."""
    if not geometry:
        return None, None
    gtype = geometry.get("type")
    coords = geometry.get("coordinates")
    if not coords:
        return None, None
    try:
        if gtype == "LineString":
            lon, lat = centroid_linestring(coords)
        elif gtype == "MultiLineString":
            lon, lat = centroid_multilinestring(coords)
        elif gtype == "Point":
            lon, lat = coords[0], coords[1]
        elif gtype == "MultiPoint":
            lon, lat = centroid_linestring(coords)
        elif gtype == "Polygon":
            lon, lat = centroid_linestring(coords[0])  # outer ring
        else:
            return None, None
        return round(lat, 6), round(lon, 6)
    except Exception:
        return None, None

# ── Parsing helpers ───────────────────────────────────────────────────────────

def parse_year(val):
    if not val:
        return None
    try:
        return int(str(val).strip()[:4])
    except Exception:
        return None

def parse_capacity(val):
    if val is None:
        return None
    try:
        return float(val)
    except Exception:
        return None

# ── Feature → row ─────────────────────────────────────────────────────────────

def feature_to_row(feature):
    p = feature.get("properties", {})
    lat, lon = extract_centroid(feature.get("geometry"))

    return {
        "project_id":       p.get("project-id"),
        "name":             p.get("name"),
        "unit_name":        p.get("unit-name"),
        "fuel":             p.get("fuel"),
        "status":           p.get("status") or None,
        "owner":            p.get("owner"),
        "parent":           p.get("parent"),
        "capacity_display": p.get("capacity-display"),
        "capacity_boed":    parse_capacity(p.get("capacity")),
        "start_year":       parse_year(p.get("start-year")),
        "country":          p.get("country-area1"),
        "subnational":      p.get("subnational"),
        "all_countries":    p.get("all-countries"),
        "region":           p.get("region"),
        "region2":          p.get("region2"),
        "tracker":          p.get("tracker-acro"),
        "url":              p.get("url"),
        "latitude":         lat,
        "longitude":        lon,
        "data_vintage":     DATA_VINTAGE,
    }

# ── Supabase upsert ───────────────────────────────────────────────────────────

def upsert_batch(rows, batch_num, total_batches):
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}"
    payload = json.dumps(rows).encode("utf-8")
    req = Request(url, data=payload, headers={**HEADERS, "Content-Length": str(len(payload))}, method="POST")
    try:
        with urllib.request.urlopen(req) as resp:
            status = resp.status
            print(f"  Batch {batch_num}/{total_batches} → HTTP {status} ({len(rows)} rows)")
            return True
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        print(f"  ✗ Batch {batch_num}/{total_batches} failed: HTTP {e.code}")
        print(f"    Response: {body[:500]}")
        if "does not exist" in body or "relation" in body.lower():
            print("\n⚠️  TABLE NOT FOUND — please run goit_schema.sql in the Supabase SQL editor first:")
            print("   /home/openclaw/.openclaw/workspace/varro/data/goit_schema.sql")
        return False
    except Exception as e:
        print(f"  ✗ Batch {batch_num}/{total_batches} failed: {e}")
        return False

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print(f"Loading {GEOJSON_PATH} …")
    with open(GEOJSON_PATH) as f:
        data = json.load(f)

    features = data["features"]
    print(f"Total features: {len(features)}")

    rows = [feature_to_row(f) for f in features]

    # Filter out rows with no project_id (can't upsert without unique key)
    rows_with_id = [r for r in rows if r["project_id"]]
    rows_without_id = len(rows) - len(rows_with_id)
    if rows_without_id:
        print(f"  ⚠️  {rows_without_id} rows skipped (no project_id)")

    batches = [rows_with_id[i:i+BATCH_SIZE] for i in range(0, len(rows_with_id), BATCH_SIZE)]
    total_batches = len(batches)
    print(f"Upserting {len(rows_with_id)} rows in {total_batches} batches of {BATCH_SIZE} …\n")

    success_count = 0
    for i, batch in enumerate(batches, 1):
        ok = upsert_batch(batch, i, total_batches)
        if not ok:
            print(f"\n✗ Stopping after batch {i} failure.")
            sys.exit(1)
        success_count += len(batch)

    print(f"\n✅ Done — {success_count} records upserted to {TABLE}")

if __name__ == "__main__":
    main()
