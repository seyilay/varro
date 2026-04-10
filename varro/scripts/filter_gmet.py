#!/usr/bin/env python3
"""
Filter GMET GeoJSON to O&G relevant features only.
"""
import ijson
import json
from decimal import Decimal

INPUT = '/home/openclaw/.openclaw/workspace/varro/data/raw/gmet/gmet_map_2026-04-02.geojson'
OUTPUT = '/home/openclaw/.openclaw/workspace/varro/data/raw/gmet/gmet_og_filtered.json'

OG_TRACKER_ACROS = {'GOGET', 'GGIT'}
OG_INFRA_TYPES = {
    'offshore platform', 'wellpad', 'Wellpad',
    'oil and gas facility', 'pipeline'
}

def is_og_relevant(props):
    ta = props.get('tracker-acro', '') or ''
    it = props.get('infrastructure-type', '') or ''
    if ta in OG_TRACKER_ACROS:
        return True
    # Check if infrastructure-type contains any OG type
    for og_type in OG_INFRA_TYPES:
        if og_type.lower() in it.lower():
            return True
    return False

def extract_coords(geometry):
    if not geometry:
        return None, None
    gtype = geometry.get('type', '')
    coords = geometry.get('coordinates')
    if not coords:
        return None, None
    if gtype == 'Point':
        return float(coords[1]), float(coords[0])  # lat, lon
    elif gtype == 'MultiPoint':
        return float(coords[0][1]), float(coords[0][0])
    elif gtype in ('LineString', 'MultiLineString'):
        # Use centroid of first point
        if gtype == 'LineString':
            return float(coords[0][1]), float(coords[0][0])
        else:
            return float(coords[0][0][1]), float(coords[0][0][0])
    elif gtype == 'Polygon':
        return float(coords[0][0][1]), float(coords[0][0][0])
    return None, None

def to_float(val):
    if val is None or val == '':
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None

total = 0
filtered = []
infra_breakdown = {}
has_associated_asset = 0

with open(INPUT, 'rb') as f:
    for feature in ijson.items(f, 'features.item'):
        total += 1
        props = feature.get('properties', {})
        if not is_og_relevant(props):
            continue

        lat, lon = extract_coords(feature.get('geometry'))

        # Override with explicit Lat/Lon if available
        explicit_lat = props.get('Latitude', '')
        explicit_lon = props.get('Longitude', '')
        if explicit_lat and explicit_lon:
            try:
                lat = float(explicit_lat)
                lon = float(explicit_lon)
            except (ValueError, TypeError):
                pass

        infra_type = props.get('infrastructure-type', '') or ''
        tracker_acro = props.get('tracker-acro', '') or ''
        associated_asset = props.get('associated-asset', '') or ''

        # Track breakdown
        label = infra_type if infra_type else f'[{tracker_acro}]'
        infra_breakdown[label] = infra_breakdown.get(label, 0) + 1

        if associated_asset:
            has_associated_asset += 1

        record = {
            'project_id': props.get('project-id', '') or None,
            'name': props.get('name', '') or None,
            'tracker_acro': tracker_acro or None,
            'infrastructure_type': infra_type or None,
            'status': props.get('status', '') or None,
            'emissions_mt_co2e': to_float(props.get('emissions')),
            'capacity': to_float(props.get('capacity')),
            'country': props.get('country-area1', '') or props.get('all-countries', '') or None,
            'subnational': props.get('subnational', '') or None,
            'associated_asset': associated_asset or None,
            'latitude': lat,
            'longitude': lon,
            'observation_date': props.get('observation-date', '') or None,
            'data_vintage': '2026-04-02',
        }
        filtered.append(record)

print(f"Total GMET features: {total}")
print(f"Filtered O&G relevant: {len(filtered)}")
print(f"With non-null associated_asset: {has_associated_asset}")
print("\nBreakdown by infrastructure_type / tracker:")
for k, v in sorted(infra_breakdown.items(), key=lambda x: -x[1]):
    print(f"  {k}: {v}")

with open(OUTPUT, 'w') as f:
    json.dump(filtered, f, indent=2)

print(f"\nSaved {len(filtered)} records to {OUTPUT}")
