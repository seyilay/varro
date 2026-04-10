"""
CalGEM (California Geologic Energy Management Division) Well Ingestor.

Source:  CalGEM WellSTAR ArcGIS REST API (live — no local file download needed)
API:     https://gis.conservation.ca.gov/server/rest/services/WellSTAR/Wells/MapServer/0/query
Fields:  API, LeaseName, WellDesignation, WellStatus, WellType, OperatorName,
         FieldName, CountyName, Latitude, Longitude, SpudDate, District

~242,000 wells as of 2026-04.  Previous ingestion (2026-04-01) had 0 errors
but did NOT capture operator_id — this ingestor fixes that.
"""

import time
import urllib.request
import json
from datetime import datetime
from typing import Iterator

from base_ingestor import BaseIngestor

CALGEM_BASE = (
    "https://gis.conservation.ca.gov/server/rest/services"
    "/WellSTAR/Wells/MapServer/0/query"
)
PAGE_SIZE = 1000

STATUS_MAP = {
    "active":                "PRODUCING",
    "producing":             "PRODUCING",
    "idle":                  "IDLE",
    "new":                   "IDLE",
    "shut-in":               "SHUT_IN",
    "shut in":               "SHUT_IN",
    "temp abandoned":        "TA",
    "temporarily abandoned": "TA",
    "plugged":               "PA",
    "plugged & abandoned":   "PA",
    "abandoned":             "PA",
    "plugged and abandoned": "PA",
    "delinquent":            "DELINQUENT",
    "orphan":                "ORPHAN",
}


def _map_status(raw: str) -> str:
    if not raw:
        return "IDLE"
    return STATUS_MAP.get(raw.lower().strip(), "IDLE")


def _parse_date(raw) -> str | None:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        try:
            return datetime.utcfromtimestamp(raw / 1000).strftime("%Y-%m-%d")
        except Exception:
            return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(str(raw).strip(), fmt).strftime("%Y-%m-%d")
        except Exception:
            continue
    return None


def _fetch_page(offset: int, retries: int = 3) -> dict | None:
    url = (
        f"{CALGEM_BASE}?where=1%3D1&outFields=*&f=json"
        f"&resultOffset={offset}&resultRecordCount={PAGE_SIZE}"
    )
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "VarroARO/2.0"})
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.loads(r.read().decode())
        except Exception as e:
            wait = 2 ** attempt
            print(f"  [WARN] Fetch offset={offset} attempt {attempt+1}: {e} — retry in {wait}s")
            time.sleep(wait)
    print(f"  [ERROR] Giving up at offset {offset}")
    return None


class CalGEMIngestor(BaseIngestor):
    source_name = "CALGEM"
    regulatory_jurisdiction = "CALGEM"

    def download(self):
        """
        CalGEM is a live REST API — no bulk file to download.
        Data is streamed directly in parse(). This is a no-op.
        """
        print("  CalGEM uses live API; no download step needed.")

    def _get_total(self) -> int:
        url = f"{CALGEM_BASE}?where=1%3D1&returnCountOnly=true&f=json"
        req = urllib.request.Request(url, headers={"User-Agent": "VarroARO/2.0"})
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read().decode()).get("count", 0)

    def parse(self) -> Iterator[dict]:
        """Stream wells from CalGEM WellSTAR API, one page at a time."""
        total = self._get_total()
        print(f"  CalGEM total wells: {total:,}")

        offset = 0
        while offset < total:
            data = _fetch_page(offset)
            if data is None:
                print(f"  Skipping offset {offset}")
                offset += PAGE_SIZE
                continue

            features = data.get("features", [])
            if not features:
                print(f"  No features at offset {offset}, stopping.")
                break

            for feat in features:
                a = feat.get("attributes", {})
                api_raw = (a.get("API") or "").strip()
                if not api_raw:
                    continue

                lat = a.get("Latitude")
                lon = a.get("Longitude")

                yield {
                    "api_number":    f"CA-{api_raw}",
                    "well_name":     a.get("WellDesignation") or a.get("LeaseName") or None,
                    "operator_name": a.get("OperatorName") or None,
                    "latitude":      round(float(lat), 7) if lat is not None else None,
                    "longitude":     round(float(lon), 7) if lon is not None else None,
                    "state":         "CA",
                    "county":        a.get("CountyName") or None,
                    "basin":         a.get("FieldName") or a.get("District") or None,
                    "field_name":    a.get("FieldName") or None,
                    "well_type":     a.get("WellTypeLabel") or a.get("WellType") or None,
                    "status":        _map_status(a.get("WellStatus")),
                    "well_class":    "ONSHORE",
                    "spud_date":     _parse_date(a.get("SpudDate")),
                    "source_record_id": str(a.get("OBJECTID") or ""),
                }

            offset += PAGE_SIZE
            time.sleep(0.1)  # be polite to CalGEM
