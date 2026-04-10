"""
Netherlands Well Ingestor — NLOG (Netherlands Oil and Gas Portal).

Source files (already downloaded):
  data/raw/netherlands/wells_batch_*.json  — ~6,500+ wells total
  Format: GeoJSON FeatureCollection per batch

Operator fields:
  properties.CURRENT_OWNER   — current license holder
  properties.ORIGINAL_OPERATOR — operator at drill time
"""

import json
import glob
import os
from typing import Iterator

from base_ingestor import BaseIngestor

RAW_DIR = "/home/openclaw/.openclaw/workspace/varro/data/raw/netherlands"


def _parse_date(s) -> str | None:
    if not s:
        return None
    s = str(s).strip()
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y%m%d"):
        try:
            from datetime import datetime
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except Exception:
            continue
    return None


def _map_status(raw: str | None) -> str:
    if not raw:
        return "IDLE"
    s = raw.strip().upper()
    if any(x in s for x in ("ABANDON", "P&A", "PA", "PLUGGED")):
        return "PA"
    if "PRODUC" in s or "ACTIVE" in s:
        return "PRODUCING"
    if "SHUT" in s:
        return "SHUT_IN"
    if any(x in s for x in ("SUSPEND", "TEMP")):
        return "TA"
    return "IDLE"


class NetherlandsIngestor(BaseIngestor):
    source_name = "NETHERLANDS"
    regulatory_jurisdiction = "NLOG"

    def download(self):
        """Batch files already present. No download needed."""
        files = sorted(glob.glob(os.path.join(RAW_DIR, "wells_batch*.json")))
        if not files:
            raise FileNotFoundError(f"No batch files in {RAW_DIR}")
        print(f"  Netherlands: {len(files)} batch files found — skipping download.")

    def parse(self) -> Iterator[dict]:
        files = sorted(glob.glob(os.path.join(RAW_DIR, "wells_batch*.json")))
        for path in files:
            with open(path) as f:
                data = json.load(f)

            features = data.get("features", [])
            for feat in features:
                props = feat.get("properties", {})
                geom = feat.get("geometry") or {}

                uwi = (props.get("UWI") or "").strip()
                borehole_code = (props.get("BOREHOLE_CODE") or "").strip()
                rec_id = uwi or borehole_code
                if not rec_id:
                    continue

                # Coordinates: geometry is in EPSG:23031 (ED50 / UTM zone 31N)
                # Converting UTM31N → WGS84 requires pyproj (not installed).
                # TODO: pip install pyproj, then use:
                #   from pyproj import Transformer
                #   t = Transformer.from_crs("EPSG:23031", "EPSG:4326", always_xy=True)
                #   lon, lat = t.transform(easting, northing)
                # For now, store None to avoid silently storing wrong coords.
                lat, lon = None, None

                # Use submitted coords as fallback (they're in RD, not useful raw)
                end_depth = props.get("END_DEPTH_MAH")

                # Prefer current owner for operator linkage
                op = props.get("CURRENT_OWNER") or props.get("ORIGINAL_OPERATOR") or None

                yield {
                    "api_number":    f"NL-{rec_id}",
                    "well_name":     props.get("WELL_NAME") or props.get("BOREHOLE_NAME") or None,
                    "operator_name": op,
                    "latitude":      lat,
                    "longitude":     lon,
                    "state":         "NL",
                    "field_name":    props.get("FIELD_NAME") or None,
                    "well_type":     props.get("BOREHOLE_TYPE_DESCRIPTION") or None,
                    "status":        _map_status(props.get("STATUS")),
                    "well_class":    "OFFSHORE" if props.get("ON_OFFSHORE_CODE") == "OF" else "ONSHORE",
                    "total_depth_ft": round(float(end_depth) * 3.28084, 1) if end_depth else None,
                    "spud_date":     _parse_date(props.get("START_DATE_DRILLING")),
                    "source_record_id": rec_id,
                }
