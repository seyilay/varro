"""
BaseIngestor — reusable ingestion pipeline for all well data sources.

Each source subclasses this and implements:
  - download() → saves raw file to data/raw/{source}/
  - parse()    → yields standardized well dicts
  - get_operator_name(row) → returns operator name string from raw row

The base class handles:
  - Operator lookup/creation
  - Well upsert (with on_conflict=api_number)
  - operator_id linkage
  - Progress logging

Standard well dict keys yielded by parse():
  api_number          (required, unique key)
  well_name           (optional)
  operator_name       (raw string — base pops this and resolves to operator_id)
  latitude            (float or None)
  longitude           (float or None)
  state               (2-letter code or full name)
  county              (optional)
  basin               (optional)
  field_name          (optional)
  well_type           (optional)
  status              (PRODUCING|IDLE|SHUT_IN|TA|PA|DELINQUENT|ORPHAN)
  well_class          (ONSHORE|OFFSHORE)
  water_depth_ft      (float or None)
  total_depth_ft      (float or None)
  spud_date           (YYYY-MM-DD string or None)
  completion_date     (YYYY-MM-DD string or None)
  source_record_id    (optional — source's own record ID)
  data_source         (set by base from source_name)
  regulatory_jurisdiction (set by base from regulatory_jurisdiction)
"""

import requests
import time
import json
import os
from typing import Iterator

SUPABASE_URL = "https://temtptsfiksixxhbigkg.supabase.co"
SUPABASE_KEY = "os.environ.get('SUPABASE_KEY')"


class BaseIngestor:
    source_name: str = ""          # e.g. "CALGEM", "LOUISIANA", "WYOMING"
    regulatory_jurisdiction: str = ""  # e.g. "CALGEM", "SONRIS", "WOGCC"

    # Batch sizes
    UPSERT_BATCH_SIZE: int = 500
    OPERATOR_RATE_LIMIT_SLEEP: float = 0.0  # sleep between operator creates

    def __init__(self):
        self.H = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        }
        self.op_name_to_id: dict[str, str] = {}
        self.raw_dir = (
            f"/home/openclaw/.openclaw/workspace/varro/data/raw/"
            f"{self.source_name.lower()}"
        )
        os.makedirs(self.raw_dir, exist_ok=True)

    # ------------------------------------------------------------------
    # Override in subclass
    # ------------------------------------------------------------------

    def download(self):
        """Download raw data to self.raw_dir. Override per source."""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement download()"
        )

    def parse(self) -> Iterator[dict]:
        """
        Yield standardized well dicts (see module docstring for keys).
        Override per source. Caller pops 'operator_name' before upserting.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement parse()"
        )

    # ------------------------------------------------------------------
    # Operator helpers
    # ------------------------------------------------------------------

    def load_operators(self) -> int:
        """Load all existing operators from Supabase into name→id cache."""
        offset = 0
        while True:
            r = requests.get(
                f"{SUPABASE_URL}/rest/v1/operators",
                headers=self.H,
                params={
                    "select": "id,name",
                    "limit": "1000",
                    "offset": str(offset),
                },
                timeout=30,
            )
            rows = r.json()
            if not rows or isinstance(rows, dict):
                break
            for row in rows:
                self.op_name_to_id[row["name"].strip()] = row["id"]
            offset += len(rows)
            if len(rows) < 1000:
                break
        return len(self.op_name_to_id)

    def get_or_create_operator(self, name: str) -> str | None:
        """
        Return operator UUID for name, creating a new record if needed.
        Returns None if name is empty or creation fails.
        """
        if not name:
            return None
        name = name.strip()
        if not name:
            return None

        if name in self.op_name_to_id:
            return self.op_name_to_id[name]

        # Create new operator
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/operators",
            headers={
                **self.H,
                "Prefer": "resolution=merge-duplicates,return=representation",
            },
            json=[{"name": name}],
            timeout=30,
        )
        if r.status_code in (200, 201) and r.json():
            uid = r.json()[0]["id"]
            self.op_name_to_id[name] = uid
            if self.OPERATOR_RATE_LIMIT_SLEEP:
                time.sleep(self.OPERATOR_RATE_LIMIT_SLEEP)
            return uid

        print(f"  [WARN] Could not create operator '{name}': {r.status_code} {r.text[:80]}")
        return None

    # ------------------------------------------------------------------
    # Well upsert
    # ------------------------------------------------------------------

    def upsert_wells(self, wells: list[dict]) -> int:
        """
        POST batch of well records with on_conflict=api_number (merge-duplicates).
        Returns count sent (not confirmed — Supabase returns 204 with no body).
        """
        if not wells:
            return 0
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/wells?on_conflict=api_number",
            headers={
                **self.H,
                "Prefer": "resolution=merge-duplicates,return=minimal",
            },
            json=wells,
            timeout=60,
        )
        if r.status_code in (200, 201, 204):
            return len(wells)
        print(f"  [ERROR] Upsert failed: {r.status_code} {r.text[:120]}")
        return 0

    # ------------------------------------------------------------------
    # Main pipeline
    # ------------------------------------------------------------------

    def run(self, skip_download: bool = False) -> int:
        """
        Execute full pipeline:
          1. download()           — fetch raw data
          2. load_operators()     — populate operator name→id cache
          3. parse() + upsert()  — stream wells, resolve operators, upsert

        Returns total number of records sent to Supabase.
        """
        print(f"\n{'=' * 60}")
        print(f"Pipeline: {self.source_name}  [{self.regulatory_jurisdiction}]")
        print(f"{'=' * 60}")

        if not skip_download:
            print("Step 1: Downloading...")
            self.download()
            print("  Download complete.")
        else:
            print("Step 1: Skipping download (skip_download=True)")

        print("Step 2: Loading operators...")
        n_ops = self.load_operators()
        print(f"  {n_ops:,} operators loaded into cache")

        print("Step 3: Parsing and upserting wells...")
        batch: list[dict] = []
        total_upserted = 0
        total_parsed = 0
        op_misses = 0   # operator names that yielded no id
        op_creates = 0  # new operators created

        for well in self.parse():
            # Pop operator_name before upserting (not a DB column)
            op_name = well.pop("operator_name", None)

            # Stamp source metadata
            well["data_source"] = self.source_name
            well["regulatory_jurisdiction"] = self.regulatory_jurisdiction

            # Resolve operator_id
            if op_name:
                prior_count = len(self.op_name_to_id)
                op_id = self.get_or_create_operator(op_name)
                if op_id:
                    well["operator_id"] = op_id
                    if len(self.op_name_to_id) > prior_count:
                        op_creates += 1
                else:
                    op_misses += 1

            batch.append(well)
            total_parsed += 1

            if len(batch) >= self.UPSERT_BATCH_SIZE:
                total_upserted += self.upsert_wells(batch)
                batch = []
                time.sleep(0.02)  # gentle rate limiting

            if total_parsed % 10_000 == 0:
                print(
                    f"  {total_parsed:,} parsed | {total_upserted:,} upserted | "
                    f"{op_creates} new ops | {op_misses} op misses"
                )

        # Flush remaining
        if batch:
            total_upserted += self.upsert_wells(batch)

        print(
            f"\nDone: {total_parsed:,} parsed | {total_upserted:,} upserted | "
            f"{op_creates} operators created | {op_misses} op misses"
        )
        return total_upserted
