"""
Australia Well Ingestor — Western Australia (WA SLIP) + South Australia (SA SARIG).

Source files (already downloaded):
  data/raw/australia/wa_wells_raw.json   — 4,363 wells (ArcGIS REST)
  data/raw/australia/sa_wells_raw.json   — 3,944 wells (OGC WFS GeoJSON)

Operator field:
  WA: attributes.operator
  SA: properties.OPERATOR
"""

import json
import os
from typing import Iterator

from base_ingestor import BaseIngestor

WA_FILE = "/home/openclaw/.openclaw/workspace/varro/data/raw/australia/wa_wells_raw.json"
SA_FILE = "/home/openclaw/.openclaw/workspace/varro/data/raw/australia/sa_wells_raw.json"

OFFSHORE_BASINS = {
    "carnarvon", "browse", "bonaparte", "timor", "offshore", "exmouth",
    "dampier", "barrow", "joseph", "tantabiddi", "roughrange",
    "north west shelf", "nw shelf", "beagle", "canning offshore",
    "canning coast", "zuytdorp",
}


def _is_offshore(basin: str | None) -> bool:
    if not basin:
        return False
    b = basin.lower()
    return any(ob in b for ob in OFFSHORE_BASINS)


def _map_status(raw: str | None) -> str:
    if not raw:
        return "IDLE"
    s = raw.strip().upper()
    if any(x in s for x in ("ABANDON", "P&A", "PA")):
        return "PA"
    if any(x in s for x in ("PRODUC",)):
        return "PRODUCING"
    if "SHUT" in s:
        return "SHUT_IN"
    if any(x in s for x in ("SUSPEND", "TEMP", "TA")):
        return "TA"
    return "IDLE"


def _ms_to_date(ms) -> str | None:
    if ms is None:
        return None
    try:
        from datetime import datetime, timezone
        dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


def _parse_date_str(s) -> str | None:
    """Parse dates like '2001-03-15' or '15/03/2001'."""
    if not s:
        return None
    s = str(s).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y%m%d"):
        try:
            from datetime import datetime
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except Exception:
            continue
    return None


class AustraliaIngestor(BaseIngestor):
    source_name = "AUSTRALIA"
    regulatory_jurisdiction = "AUSTRALIA_NOPTA"

    def download(self):
        """Raw files already present at data/raw/australia/. No download needed."""
        print("  Australia raw files already downloaded — skipping.")
        for f in [WA_FILE, SA_FILE]:
            if not os.path.exists(f):
                raise FileNotFoundError(f"Expected file not found: {f}")

    def parse(self) -> Iterator[dict]:
        yield from self._parse_wa()
        yield from self._parse_sa()

    def _parse_wa(self) -> Iterator[dict]:
        """Parse Western Australia wells."""
        with open(WA_FILE) as f:
            records = json.load(f)

        for rec in records:
            a = rec.get("attributes", {})
            g = rec.get("geometry") or {}
            basin = a.get("basin") or ""
            spud_raw = a.get("spud_date")
            spud = _ms_to_date(spud_raw) if isinstance(spud_raw, (int, float)) else _parse_date_str(spud_raw)

            uwi = (a.get("uwi") or "").strip()
            if not uwi:
                continue

            lat = a.get("lat")
            lon = a.get("long")

            yield {
                "api_number":    f"AU-WA-{uwi}",
                "well_name":     a.get("well_name") or None,
                "operator_name": a.get("operator") or None,
                "latitude":      float(lat) if lat is not None else None,
                "longitude":     float(lon) if lon is not None else None,
                "state":         "WA",
                "basin":         basin or None,
                "field_name":    a.get("field") or None,
                "well_type":     a.get("class") or None,
                "status":        _map_status(a.get("status")),
                "well_class":    "OFFSHORE" if _is_offshore(basin) else "ONSHORE",
                "spud_date":     spud,
                "source_record_id": str(a.get("objectid") or ""),
            }

    def _parse_sa(self) -> Iterator[dict]:
        """Parse South Australia wells."""
        with open(SA_FILE) as f:
            records = json.load(f)

        for rec in records:
            props = rec.get("properties", {})
            geom = rec.get("geometry") or {}

            well_id = str(props.get("WELL_ID") or props.get("DRILLHOLE_NO") or "").strip()
            if not well_id:
                continue

            # Coords
            lat = props.get("LATITUDE__DEC") or props.get("LATITUDE_DEC")
            lon = props.get("LONGITUDE__DEC") or props.get("LONGITUDE_DEC")
            if lat is None and geom.get("coordinates"):
                coords = geom["coordinates"]
                if coords and len(coords) >= 2:
                    lon, lat = coords[0], coords[1]

            basin = props.get("BASIN") or ""
            td_ft = props.get("TD__FT")

            yield {
                "api_number":    f"AU-SA-{well_id}",
                "well_name":     props.get("WELL_NAME") or props.get("PLOTNAME") or None,
                "operator_name": props.get("OPERATOR") or None,
                "latitude":      float(lat) if lat is not None else None,
                "longitude":     float(lon) if lon is not None else None,
                "state":         "SA",
                "basin":         basin or None,
                "well_type":     props.get("CLASS") or props.get("WELL_TYPE") or None,
                "status":        _map_status(props.get("STATUS_RIG_RELEASE")),
                "well_class":    "OFFSHORE" if _is_offshore(basin) else "ONSHORE",
                "total_depth_ft": float(td_ft) if td_ft else None,
                "spud_date":     _parse_date_str(props.get("SPUDDED")),
                "source_record_id": well_id,
            }
