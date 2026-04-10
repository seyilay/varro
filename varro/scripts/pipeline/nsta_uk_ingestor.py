"""
NSTA UK Well Ingestor — UK North Sea (UKCS offshore wellbores).

Source:  NSTA ArcGIS REST API (live)
API:     https://services-eu1.arcgis.com/OZMfUznmLTnWccBc/ArcGIS/rest/services
         /UKCS_offshore_wellbore_top_holes_(WGS84)/FeatureServer/0/query
~13,346 wells with operator via SUBOPGRP field.

Note: Previous ingestion (2026-04-02) fetched 13,346 wells, 0 errors.
      Running again will re-upsert with updated operator_id linkage.
"""

import time
import urllib.request
import json
from datetime import datetime, timezone
from typing import Iterator

from base_ingestor import BaseIngestor

ARCGIS_BASE = (
    "https://services-eu1.arcgis.com/OZMfUznmLTnWccBc/ArcGIS/rest/services"
    "/UKCS_offshore_wellbore_top_holes_(WGS84)/FeatureServer/0/query"
)
PAGE_SIZE = 1000

STATUS_MAP = {
    "suspended":     "SHUT_IN",
    "decomissioned": "PA",
    "decommissioned": "PA",
    "constructed":   "IDLE",
    "constructing":  "IDLE",
    "producing":     "PRODUCING",
    "production":    "PRODUCING",
    "abandoned":     "PA",
    "plugged":       "PA",
    "temporarily abandoned": "TA",
    "ta":            "TA",
}


def _map_status(val: str | None) -> str:
    if not val:
        return "IDLE"
    v = val.lower()
    for k, s in STATUS_MAP.items():
        if k in v:
            return s
    return "IDLE"


def _ts_to_date(ms) -> str | None:
    if ms is None:
        return None
    try:
        dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


def _fetch_page(offset: int, retries: int = 3) -> dict | None:
    url = (
        f"{ARCGIS_BASE}?where=1%3D1"
        f"&outFields=OBJECTID,NAME,WELLREGNO,WELLBRSTAT,WELLOPSTAT,COMPLESTAT,"
        f"SPUDDATE,TDMDDEPF,WATDEP_F,ORIGINTENT,CURRWELLIN,SUBOPGRP,COUNTRYCOD"
        f"&returnGeometry=true&geometryType=esriGeometryPoint"
        f"&outSR=4326&f=json"
        f"&resultOffset={offset}&resultRecordCount={PAGE_SIZE}"
    )
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "VarroARO/2.0"})
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.loads(r.read().decode())
        except Exception as e:
            wait = 2 ** attempt
            print(f"  [WARN] NSTA fetch offset={offset} attempt {attempt+1}: {e} — retry in {wait}s")
            time.sleep(wait)
    return None


class NSTAUKIngestor(BaseIngestor):
    source_name = "NSTA_UK"
    regulatory_jurisdiction = "NSTA"

    def download(self):
        """NSTA is a live ArcGIS REST API. No bulk download needed."""
        print("  NSTA UK uses live ArcGIS API — no download step needed.")

    def parse(self) -> Iterator[dict]:
        """Stream UKCS wells from NSTA ArcGIS REST API."""
        offset = 0
        page = 0
        total_yielded = 0

        while True:
            page += 1
            data = _fetch_page(offset)
            if data is None:
                print(f"  [ERROR] Could not fetch page at offset {offset}")
                break

            features = data.get("features", [])
            if not features:
                break

            for feat in features:
                a = feat.get("attributes", {})
                geo = feat.get("geometry") or {}

                name = (a.get("NAME") or "").strip()
                wellregno = str(a.get("WELLREGNO") or "").strip()
                rec_id = wellregno or name
                if not rec_id:
                    continue

                lat = geo.get("y")
                lon = geo.get("x")

                water_depth = a.get("WATDEP_F")
                total_depth = a.get("TDMDDEPF")

                # Operator: SUBOPGRP is the operator group / company
                op = (a.get("SUBOPGRP") or "").strip() or None

                yield {
                    "api_number":    f"GB-{rec_id}",
                    "well_name":     name or None,
                    "operator_name": op,
                    "latitude":      float(lat) if lat is not None else None,
                    "longitude":     float(lon) if lon is not None else None,
                    "state":         "GB",
                    "well_type":     a.get("ORIGINTENT") or a.get("CURRWELLIN") or None,
                    "status":        _map_status(a.get("WELLOPSTAT") or a.get("WELLBRSTAT")),
                    "well_class":    "OFFSHORE",
                    "water_depth_ft": float(water_depth) if water_depth else None,
                    "total_depth_ft": float(total_depth) if total_depth else None,
                    "spud_date":     _ts_to_date(a.get("SPUDDATE")),
                    "source_record_id": rec_id,
                }
                total_yielded += 1

            if not data.get("exceededTransferLimit", False) and len(features) < PAGE_SIZE:
                break

            offset += PAGE_SIZE
            time.sleep(0.1)

        print(f"  NSTA UK streamed {total_yielded:,} wells")
