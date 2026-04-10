"""
Norway Well Ingestor — Sodir (Norwegian Offshore Directorate) / NPD.

Source file (already downloaded):
  data/raw/norway/wellbore_exploration_all.csv  — ~300 exploration wellbores

Operator field:  wlbDrillingOperator
Download URL:    https://www.sodir.no/en/npdio/exploration-wellbores/
"""

import csv
from typing import Iterator

from base_ingestor import BaseIngestor

WELLBORE_FILE = "/home/openclaw/.openclaw/workspace/varro/data/raw/norway/wellbore_exploration_all.csv"


def _parse_date(s) -> str | None:
    if not s or s.strip() in ("", "n/a", "N/A"):
        return None
    s = s.strip()
    for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
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
    if "P&A" in s or "ABANDON" in s:
        return "PA"
    if "PRODUC" in s:
        return "PRODUCING"
    if "SHUT" in s:
        return "SHUT_IN"
    if "SUSPEND" in s or "TEMP" in s:
        return "TA"
    return "IDLE"


class NorwayIngestor(BaseIngestor):
    source_name = "NORWAY"
    regulatory_jurisdiction = "SODIR"

    def download(self):
        """File already present."""
        import os
        if not os.path.exists(WELLBORE_FILE):
            raise FileNotFoundError(f"Expected: {WELLBORE_FILE}")
        print("  Norway wellbore file present — skipping download.")

    def parse(self) -> Iterator[dict]:
        with open(WELLBORE_FILE, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                name = (row.get("wlbWellboreName") or "").strip()
                if not name:
                    continue

                water_depth = row.get("wlbWaterDepth") or None
                total_depth = row.get("wlbTotalDepth") or row.get("wlbFinalVerticalDepth") or None

                yield {
                    "api_number":    f"NO-{name.replace('/', '-')}",
                    "well_name":     name,
                    "operator_name": row.get("wlbDrillingOperator") or None,
                    "latitude":      None,  # coordinates need EPSG conversion from NPD
                    "longitude":     None,
                    "state":         "NO",
                    "basin":         None,
                    "field_name":    row.get("wlbField") or None,
                    "well_type":     row.get("wlbWellType") or None,
                    "status":        _map_status(row.get("wlbStatus")),
                    "well_class":    "OFFSHORE",  # all are Norwegian offshore
                    "water_depth_ft": round(float(water_depth) * 3.28084, 1) if water_depth else None,
                    "total_depth_ft": round(float(total_depth) * 3.28084, 1) if total_depth else None,
                    "spud_date":     _parse_date(row.get("wlbEntryDate")),
                    "source_record_id": name,
                }
