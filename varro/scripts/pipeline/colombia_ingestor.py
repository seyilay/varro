"""
Colombia ANH Well Ingestor.

Source files (already downloaded):
  data/raw/colombia/campos_crudo.json    — 997 oil fields with operadora
  data/raw/colombia/pozos_exploratorios.json — 255 exploratory wells

Operator fields:
  campos_crudo:        row['operadora']
  pozos_exploratorios: row['operador_actual']
"""

import json
from typing import Iterator

from base_ingestor import BaseIngestor

CAMPOS_FILE = "/home/openclaw/.openclaw/workspace/varro/data/raw/colombia/campos_crudo.json"
POZOS_FILE = "/home/openclaw/.openclaw/workspace/varro/data/raw/colombia/pozos_exploratorios.json"


def _parse_date(s) -> str | None:
    if not s:
        return None
    s = str(s).strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            from datetime import datetime
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except Exception:
            continue
    return None


class ColombiaIngestor(BaseIngestor):
    source_name = "COLOMBIA"
    regulatory_jurisdiction = "ANH"

    def download(self):
        """Raw files already present."""
        import os
        for f in [CAMPOS_FILE, POZOS_FILE]:
            if not os.path.exists(f):
                raise FileNotFoundError(f"Expected: {f}")
        print("  Colombia raw files present — skipping download.")

    def parse(self) -> Iterator[dict]:
        yield from self._parse_campos()
        yield from self._parse_pozos()

    def _parse_campos(self) -> Iterator[dict]:
        """Parse oil fields (campos_crudo). Fields treated as wells with field-level granularity."""
        with open(CAMPOS_FILE) as f:
            records = json.load(f)

        for i, row in enumerate(records):
            campo = (row.get("campo") or "").strip()
            if not campo:
                continue

            lat_raw = row.get("latitud")
            lon_raw = row.get("longitud")

            # Try geolocalizacion if direct coords missing
            geo = row.get("geolocalizacion") or {}
            if lat_raw is None and isinstance(geo, dict):
                lat_raw = geo.get("latitude")
                lon_raw = geo.get("longitude")

            yield {
                "api_number":    f"CO-CAMPO-{i+1:05d}-{campo[:20].replace(' ', '_')}",
                "well_name":     campo,
                "operator_name": row.get("operadora") or None,
                "latitude":      float(lat_raw) if lat_raw is not None else None,
                "longitude":     float(lon_raw) if lon_raw is not None else None,
                "state":         row.get("departamento") or "CO",
                "basin":         row.get("contrato") or None,
                "well_type":     "FIELD",
                "status":        "PRODUCING",
                "well_class":    "ONSHORE",
                "source_record_id": campo,
            }

    def _parse_pozos(self) -> Iterator[dict]:
        """Parse exploratory wells (pozos_exploratorios)."""
        with open(POZOS_FILE) as f:
            records = json.load(f)

        for i, row in enumerate(records):
            nombre = (row.get("nombre_de_pozo") or "").strip()
            contrato = (row.get("contrato") or "").strip()
            rec_id = nombre or contrato or str(i)

            lat_raw = None
            lon_raw = None
            surf = row.get("superficie")
            if isinstance(surf, dict):
                lat_raw = surf.get("latitude")
                lon_raw = surf.get("longitude")

            yield {
                "api_number":    f"CO-POZO-{i+1:05d}-{rec_id[:20].replace(' ', '_')}",
                "well_name":     nombre or None,
                "operator_name": row.get("operador_actual") or None,
                "latitude":      float(lat_raw) if lat_raw is not None else None,
                "longitude":     float(lon_raw) if lon_raw is not None else None,
                "state":         row.get("departamento") or "CO",
                "county":        row.get("municipio") or None,
                "basin":         row.get("cuenca") or None,
                "well_type":     row.get("tipo") or row.get("clasificaci_n_lahee") or None,
                "status":        "IDLE",
                "well_class":    "ONSHORE",
                "spud_date":     _parse_date(row.get("fecha_td")),
                "source_record_id": rec_id,
            }
