#!/usr/bin/env python3
"""
Varro ARO Variance Engine
=========================
Computes the gap between operator balance-sheet ARO provisions (EDGAR)
and Varro's market-calibrated cost estimates (BOEM regression + basin benchmarks).

Outputs:
  - Updates wells.balance_sheet_aro_usd (allocated from EDGAR totals)
  - Updates wells.estimated_cost_p50 / p10 / p90 (from cost model)
  - Updates wells.aro_variance_usd (auto-computed column)
  - Inserts aro_variance_snapshots rows per operator
  - Inserts commodity_prices (steel) and basin_weather_windows if not present

Prerequisites:
  - Run /varro/data/schema_additions_v2.sql in Supabase SQL Editor first
  - Python packages: requests, json (stdlib)

Usage:
  python3 aro_variance_engine.py [--dry-run] [--operator <name>]
"""

import json
import math
import os
import sys
import argparse
import requests
from datetime import date

# ── Config ──────────────────────────────────────────────────────────────────

SUPABASE_URL = "https://temtptsfiksixxhbigkg.supabase.co"
SUPABASE_KEY = "os.environ.get('SUPABASE_KEY')"
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal",
}

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "..", "data")
COST_MODEL_PATH = os.path.join(SCRIPT_DIR, "..", "app", "src", "cost-model", "boem_regression.json")
EDGAR_DATA_PATH = os.path.join(DATA_DIR, "raw", "edgar", "aro_disclosures.json")
WEATHER_PATH = os.path.join(DATA_DIR, "raw", "weather", "basin_weather_windows.json")
STEEL_PATH = os.path.join(DATA_DIR, "raw", "commodities", "steel_scrap_prices.json")
OPERATOR_MAPPING_PATH = os.path.join(DATA_DIR, "operator-parent-mapping-v4.json")

TODAY = date.today().isoformat()
MODEL_VERSION = "1.1.0"

# ── EDGAR ARO data (hardcoded from 2024 10-K filings) ─────────────────────
# Format: operator name pattern → (aro_usd_millions, fiscal_year, edgar_source)
EDGAR_ARO = {
    "ExxonMobil":      (12_500, 2024, "ExxonMobil 10-K 2024"),
    "Shell":           (21_400, 2024, "Shell 20-F 2024"),
    "Chevron":         (15_000, 2024, "Chevron 10-K 2024"),
    "BP":              (12_300, 2024, "BP 20-F 2024"),
    "Equinor":         (13_600, 2024, "Equinor 20-F 2024"),
    "TotalEnergies":   ( 9_300, 2024, "TotalEnergies 20-F 2024"),
    "ConocoPhillips":  ( 8_400, 2024, "ConocoPhillips 10-K 2024"),
    "Occidental":      ( 4_600, 2024, "Occidental 10-K 2024"),
    "Devon":           (   906, 2024, "Devon Energy 10-K 2024"),
    "Pioneer":         (   459, 2023, "Pioneer Natural Resources 10-K 2023"),
}

# ── GOGET operator → EDGAR parent mapping ────────────────────────────────
# Built from GOGET parent fields; maps subsidiary/raw names → EDGAR parent entity.
# Loaded at startup by load_operator_mapping().

_OPERATOR_MAPPING_BY_UUID: dict[str, str] = {}   # operator_uuid → edgar_parent
_OPERATOR_MAPPING_BY_NAME: dict[str, str] = {}   # operator_name_lower → edgar_parent

def load_operator_mapping():
    """
    Load operator-parent-mapping-v2.json (UUID-keyed).
    Populates both UUID lookup and name fallback dicts.
    Falls back to empty dicts if file not found.
    """
    global _OPERATOR_MAPPING_BY_UUID, _OPERATOR_MAPPING_BY_NAME
    if not os.path.exists(OPERATOR_MAPPING_PATH):
        print(f"  WARN: {OPERATOR_MAPPING_PATH} not found — using legacy key-name logic only.")
        _OPERATOR_MAPPING_BY_UUID = {}
        _OPERATOR_MAPPING_BY_NAME = {}
        return

    with open(OPERATOR_MAPPING_PATH) as f:
        data = json.load(f)

    by_uuid = {}
    by_name = {}
    mapping = data.get("mapping", {})
    # v2 format: mapping is a dict keyed by UUID
    if isinstance(mapping, dict):
        for uuid, entry in mapping.items():
            edgar = entry.get("edgar_parent", "").strip()
            op_name = entry.get("operator_name", "").strip()
            if uuid and edgar:
                by_uuid[uuid] = edgar
            if op_name and edgar:
                by_name[op_name.lower()] = edgar
    else:
        # v1 fallback: list of {raw_name, edgar_parent}
        for entry in mapping:
            raw = entry.get("raw_name", "").strip()
            edgar = entry.get("edgar_parent", "").strip()
            if raw and edgar:
                by_name[raw.lower()] = edgar

    _OPERATOR_MAPPING_BY_UUID = by_uuid
    _OPERATOR_MAPPING_BY_NAME = by_name
    version = data.get("version", "1.x")
    matched = data.get("matched", len(by_uuid))
    print(f"  Loaded operator-parent-mapping v{version}: {len(by_uuid)} UUID entries, {len(by_name)} name entries")
    print(f"  Coverage: {matched}/{data.get('total_operators','?')} operators ({data.get('coverage_pct','?')}%)")


def resolve_edgar_parent(op_id: str, op_name: str) -> str | None:
    """
    Resolve an operator to its EDGAR parent entity.

    Strategy (in order):
      1. UUID lookup in v2 mapping
      2. Exact name match (case-insensitive) in v2 mapping
      3. Substring match: does the operator name contain a mapping key?
      4. Legacy fallback: does any EDGAR_ARO key appear in op_name?

    Returns EDGAR parent string (e.g. "ExxonMobil") or None.
    """
    # 1. UUID lookup (fastest, most accurate)
    if op_id and op_id in _OPERATOR_MAPPING_BY_UUID:
        return _OPERATOR_MAPPING_BY_UUID[op_id]

    op_lower = op_name.lower().strip()

    # 2. Exact name match
    if op_lower in _OPERATOR_MAPPING_BY_NAME:
        return _OPERATOR_MAPPING_BY_NAME[op_lower]

    # 3. Substring match
    for name_lower, edgar in _OPERATOR_MAPPING_BY_NAME.items():
        if len(name_lower) >= 5:
            if name_lower in op_lower or (len(op_lower) >= 5 and op_lower in name_lower):
                return edgar

    # 4. Legacy key-in-name fallback
    for key in EDGAR_ARO:
        if key.lower() in op_lower:
            return key

    return None


# ── Cost model ────────────────────────────────────────────────────────────

def load_cost_model():
    with open(COST_MODEL_PATH) as f:
        return json.load(f)

def estimate_cost(model, water_depth_ft: float, area_code: str = None) -> dict:
    """
    Returns P10/P50/P90 cost estimates in USD using the BOEM log-linear regression.
    P10 = P50 * 0.4 (optimistic), P90 = P50 * 2.8 (pessimistic) — GOM empirical spread.
    """
    coeffs = model["coefficients"]
    depth_classes = {
        "shallow":    (0,    200),
        "mid_shallow":(200,  500),
        "mid":        (500,  1000),
        "deep":       (1000, 3000),
        "ultradeep":  (3000, float("inf")),
    }

    depth_ft = water_depth_ft or 100  # default to shallow if unknown

    is_mid_shallow = 1 if 200 <= depth_ft < 500  else 0
    is_mid         = 1 if 500 <= depth_ft < 1000 else 0
    is_deep        = 1 if 1000 <= depth_ft < 3000 else 0
    is_ultradeep   = 1 if depth_ft >= 3000        else 0

    log_cost = (
        coeffs["intercept"]
        + coeffs["water_depth_ft_per_100ft"] * (depth_ft / 100)
        + coeffs["is_mid_shallow_dummy"] * is_mid_shallow
        + coeffs["is_mid_dummy"] * is_mid
        + coeffs["is_deep_dummy"] * is_deep
        + coeffs["is_ultradeep_dummy"] * is_ultradeep
    )

    p50_base = math.exp(log_cost)

    # Apply basin adjustment if area_code known
    basin_adj = model["basin_adjustments"].get(area_code, 1.0) if area_code else 1.0
    bias_corr = model["bias_correction_factor"]

    p50 = p50_base * basin_adj * bias_corr

    return {
        "p10": round(p50 * 0.40, 2),   # optimistic end of GOM spread
        "p50": round(p50, 2),
        "p90": round(p50 * 2.80, 2),   # pessimistic end
        "model_version": model["model_version"],
    }

# ── Supabase helpers ──────────────────────────────────────────────────────

def sb_get(path: str, params: dict = None):
    url = f"{SUPABASE_URL}/rest/v1/{path}"
    r = requests.get(url, headers={**HEADERS, "Prefer": "count=exact"}, params=params)
    r.raise_for_status()
    return r.json()

def sb_patch(path: str, match: dict, data: dict, dry_run=False):
    if dry_run:
        print(f"  [DRY-RUN] PATCH {path} WHERE {match} → {list(data.keys())}")
        return
    params = {k: f"eq.{v}" for k, v in match.items()}
    url = f"{SUPABASE_URL}/rest/v1/{path}"
    r = requests.patch(url, headers=HEADERS, params=params, json=data)
    r.raise_for_status()

def sb_upsert(path: str, rows: list, dry_run=False, on_conflict="merge-duplicates"):
    if dry_run:
        print(f"  [DRY-RUN] UPSERT {path} — {len(rows)} rows")
        return
    url = f"{SUPABASE_URL}/rest/v1/{path}"
    # Use plain INSERT (no on-conflict resolution) — caller should pre-delete if needed
    headers = {**HEADERS, "Prefer": "return=minimal"}
    ok = 0
    # Batch in chunks of 500
    for i in range(0, len(rows), 500):
        chunk = rows[i:i+500]
        r = requests.post(url, headers=headers, json=chunk)
        if r.status_code not in (200, 201, 204):
            print(f"  WARN: {path} batch {i//500+1} → {r.status_code}: {r.text[:200]}")
        else:
            ok += len(chunk)
    print(f"  Inserted {ok}/{len(rows)} rows → {path}")

# ── Seed tables (weather, steel) ─────────────────────────────────────────

def seed_weather_windows(dry_run=False):
    print("\n[1] Seeding basin_weather_windows...")
    with open(WEATHER_PATH) as f:
        rows = json.load(f)
    sb_upsert("basin_weather_windows", rows, dry_run, on_conflict="ignore-duplicates")

def seed_commodity_prices(dry_run=False):
    print("\n[2] Seeding commodity_prices...")
    with open(STEEL_PATH) as f:
        rows = json.load(f)
    sb_upsert("commodity_prices", rows, dry_run, on_conflict="ignore-duplicates")

# ── Main variance computation ─────────────────────────────────────────────

def run_variance_engine(dry_run=False, operator_filter=None, no_well_updates=False):
    model = load_cost_model()
    print(f"\n[3] Loaded cost model v{model['model_version']} (R²={model['r_squared']})")

    # Load GOGET operator → EDGAR parent mapping
    load_operator_mapping()

    # Load ALL operators from Supabase (paginate — hard limit is 1000/request)
    operators = []
    page_size = 1000
    offset = 0
    while True:
        batch = sb_get("operators", {"select": "id,name", "limit": str(page_size), "offset": str(offset)})
        operators.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    print(f"    {len(operators)} operators loaded")

    snapshot_rows = []
    well_update_count = 0

    for op in operators:
        op_name = op.get("name", "")
        op_id = op["id"]

        if operator_filter and operator_filter.lower() not in op_name.lower():
            continue

        # Match to EDGAR data via v2 UUID mapping (then name fallback)
        edgar_parent = resolve_edgar_parent(op_id, op_name)
        if not edgar_parent or edgar_parent not in EDGAR_ARO:
            continue  # No EDGAR data for this operator
        edgar_match = EDGAR_ARO[edgar_parent]

        edgar_aro_m, fiscal_year, edgar_source = edgar_match
        print(f"\n  Operator: {op_name}")
        print(f"    EDGAR ARO: ${edgar_aro_m:,.0f}M (FY{fiscal_year})")

        # Fetch this operator's active wells (not P&A'd)
        wells = sb_get("wells", {
            "select": "id,water_depth_ft,basin,api_number",
            "operator_id": f"eq.{op_id}",
            "status": "not.eq.PLUGGED_AND_ABANDONED",
            "limit": "50000"
        })
        well_count = len(wells)
        if well_count == 0:
            print(f"    No active wells found, skipping")
            continue

        # Allocate balance sheet ARO per well (simple equal allocation)
        per_well_aro = (edgar_aro_m * 1_000_000) / well_count
        print(f"    Active wells: {well_count:,} → ${per_well_aro:,.0f}/well allocated ARO")

        # Compute Varro estimates and variance
        total_p50 = 0
        total_p90 = 0
        updates = []

        for well in wells:
            depth = well.get("water_depth_ft") or 100
            basin = well.get("basin") or ""
            est = estimate_cost(model, depth, basin)
            variance = est["p50"] - per_well_aro
            total_p50 += est["p50"]
            total_p90 += est["p90"]

            updates.append({
                "id": well["id"],
                "balance_sheet_aro_usd": round(per_well_aro, 2),
                "estimated_cost_p10": est["p10"],
                "estimated_cost_p50": est["p50"],
                "estimated_cost_p90": est["p90"],
                "cost_model_version": MODEL_VERSION,
                "cost_estimated_at": f"{TODAY}T18:00:00Z",
            })

        varro_p50_m = total_p50 / 1_000_000
        varro_p90_m = total_p90 / 1_000_000
        variance_m = varro_p50_m - edgar_aro_m
        variance_pct = (variance_m / edgar_aro_m) * 100 if edgar_aro_m else 0

        flag = "⚠️ UNDERPROVISIONED" if variance_m > 0 else "✅ overprovisioned"
        print(f"    Varro P50: ${varro_p50_m:,.0f}M | EDGAR: ${edgar_aro_m:,.0f}M | Δ: ${variance_m:+,.0f}M ({variance_pct:+.1f}%) {flag}")

        # Batch update wells via PATCH (update only specified columns, no NOT NULL issues)
        if not dry_run and not no_well_updates:
            print(f"    Updating {len(updates):,} wells...")
            ok_count = 0
            for upd in updates:
                well_id = upd.pop("id")
                url = f"{SUPABASE_URL}/rest/v1/wells"
                r = requests.patch(url, headers=HEADERS, params={"id": f"eq.{well_id}"}, json=upd)
                if r.status_code not in (200, 201, 204):
                    pass  # skip noisy per-well errors
                else:
                    ok_count += 1
            print(f"    Updated {ok_count}/{len(updates)} wells OK")
            well_update_count += ok_count
        elif no_well_updates:
            print(f"    [SKIP] Well updates skipped (--no-well-updates)")
        else:
            print(f"    [DRY-RUN] Would update {len(updates)} wells")

        # Build snapshot row
        snapshot_rows.append({
            "operator_id": op_id,
            "snapshot_date": TODAY,
            "edgar_aro_usd_millions": edgar_aro_m,
            "edgar_source": edgar_source,
            "edgar_fiscal_year": fiscal_year,
            "varro_estimate_p50_usd_millions": round(varro_p50_m, 2),
            "varro_estimate_p90_usd_millions": round(varro_p90_m, 2),
            "well_count": well_count,
            "variance_pct": round(variance_pct, 2),
        })

    # Insert variance snapshots
    if snapshot_rows:
        print(f"\n[4] Writing {len(snapshot_rows)} aro_variance_snapshots...")
        sb_upsert("aro_variance_snapshots", snapshot_rows, dry_run)

    print(f"\n✅ Done. Wells updated: {well_update_count:,} | Snapshots: {len(snapshot_rows)}")
    return snapshot_rows


# ── Entry point ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Varro ARO Variance Engine")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    parser.add_argument("--operator", type=str, help="Filter to one operator name")
    parser.add_argument("--seed-only", action="store_true", help="Only seed weather/steel tables")
    parser.add_argument("--no-well-updates", action="store_true", help="Skip per-well PATCH updates (snapshots only)")
    args = parser.parse_args()

    seed_weather_windows(args.dry_run)
    seed_commodity_prices(args.dry_run)

    if not args.seed_only:
        results = run_variance_engine(dry_run=args.dry_run, operator_filter=args.operator, no_well_updates=args.no_well_updates)

        if results:
            print("\n── Variance Summary ──────────────────────────────────")
            total_edgar = sum(r["edgar_aro_usd_millions"] for r in results)
            total_varro = sum(r["varro_estimate_p50_usd_millions"] for r in results)
            print(f"  EDGAR total ARO (covered operators): ${total_edgar:,.0f}M")
            print(f"  Varro P50 estimate:                  ${total_varro:,.0f}M")
            print(f"  Aggregate gap:                       ${total_varro - total_edgar:+,.0f}M")
            underprov = [r for r in results if r["varro_estimate_p50_usd_millions"] > r["edgar_aro_usd_millions"]]
            print(f"  Underprovisioned operators:          {len(underprov)}/{len(results)}")
