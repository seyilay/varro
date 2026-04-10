"""
PRO-189: Comparable well selection algorithm.

Selects peer wells from comparable_costs table using progressive fallback:
  Level 1: same basin + well_type + depth band + vintage window
  Level 2: same basin + depth band (relax well_type)
  Level 3: region-wide (e.g. all GOM basins) + depth band
  Level 4: depth band only (minimum floor)

Returns cost list, confidence score, data density, match level.
"""

import math
import psycopg2
import psycopg2.extras
from typing import Optional, List, Dict, Any

DB = 'postgresql://postgres:$DB_PASSWORD@db.temtptsfiksixxhbigkg.supabase.co:5432/postgres'

# BOEM area codes that map to GOM (for regional fallback)
GOM_BASINS = {'EI','SS','WC','HI','VR','ST','MP','SM','GA','DE','GC','EB','MC','AC','KC','AT'}

# Adjacent depth buckets for ±1 band fallback
DEPTH_BANDS = [
    ("<3000",   0,     3000),
    ("3-8k",    3000,  8000),
    ("8-15k",   8000,  15000),
    ("15k+",    15000, 9_999_999),
]

WATER_DEPTH_BANDS = [
    ("0-300",   0,    300),
    ("300-1k",  300,  1000),
    ("1k+",     1000, 9_999_999),
]

# Known understated sources — exclude per PRO-189 spec
EXCLUDED_SOURCES = {'FIELDWOOD_ESTIMATE', 'COX_ESTIMATE', 'OPERATOR_SELF_REPORT_UNVERIFIED'}

# CPI base year for inflation adjustment
CPI_BASE_YEAR = 2024
CPI_ANNUAL_RATE = 0.035  # ~3.5% annual average


def _cpi_adjust(cost: float, cost_year: int) -> float:
    """Inflate historical cost to CPI_BASE_YEAR USD."""
    if not cost_year or cost_year >= CPI_BASE_YEAR:
        return cost
    years = CPI_BASE_YEAR - cost_year
    return cost * ((1 + CPI_ANNUAL_RATE) ** years)


def _depth_bucket(val: Optional[float], bands=DEPTH_BANDS) -> str:
    for label, lo, hi in bands:
        if lo <= (val or 0) < hi:
            return label
    return bands[-1][0]


def _adjacent_buckets(val: Optional[float], bands=DEPTH_BANDS) -> tuple:
    """Return (lo, hi) covering current bucket ± 1 bucket."""
    labels = [b[0] for b in bands]
    ranges = [(b[1], b[2]) for b in bands]
    bucket = _depth_bucket(val, bands)
    idx = labels.index(bucket) if bucket in labels else 1
    lo_idx = max(0, idx - 1)
    hi_idx = min(len(bands) - 1, idx + 1)
    return ranges[lo_idx][0], ranges[hi_idx][1]


def _vintage_window(year: Optional[int], window: int = 10) -> tuple:
    y = year or 2000
    return max(1900, y - window), y + window


def _confidence(count: int, match_level: str) -> str:
    if count >= 30 and match_level == "exact":
        return "HIGH"
    elif count >= 10:
        return "MEDIUM"
    elif count >= 3:
        return "LOW"
    return "INSUFFICIENT"


def _data_density(count: int) -> str:
    if count >= 30:   return "high"
    elif count >= 10: return "medium"
    elif count >= 3:  return "low"
    return "insufficient"


def select_comparables(
    basin: Optional[str] = None,
    state: Optional[str] = None,
    well_type: Optional[str] = None,
    well_class: Optional[str] = None,
    total_depth_ft: Optional[float] = None,
    water_depth_ft: Optional[float] = None,
    vintage_year: Optional[int] = None,
    max_count: int = 50,
) -> Dict[str, Any]:
    """
    Select comparable wells and return CPI-adjusted cost list.

    Returns:
        costs             : list[float] — CPI-adjusted USD
        comparable_count  : int
        confidence        : "HIGH"|"MEDIUM"|"LOW"|"INSUFFICIENT"
        data_density      : "high"|"medium"|"low"|"insufficient"
        match_level       : "exact"|"basin_relaxed"|"region_wide"|"depth_only"
        p50_comparable    : float | None
    """
    depth_lo, depth_hi = _adjacent_buckets(total_depth_ft, DEPTH_BANDS)
    vint_lo, vint_hi = _vintage_window(vintage_year)
    wt = well_type or "OIL"
    b = basin or ""

    result_template = dict(
        costs=[], comparable_count=0,
        confidence="INSUFFICIENT", data_density="insufficient",
        match_level="depth_only", p50_comparable=None
    )

    try:
        with psycopg2.connect(DB, connect_timeout=10) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

                # ── LEVEL 1: exact basin + well_type + depth + vintage ──────
                cur.execute("""
                    SELECT actual_cost, cost_year FROM comparable_costs
                    WHERE basin = %(basin)s
                      AND (well_type = %(well_type)s OR well_type IS NULL)
                      AND (total_depth_ft IS NULL OR total_depth_ft BETWEEN %(depth_lo)s AND %(depth_hi)s)
                      AND (vintage_year IS NULL OR vintage_year BETWEEN %(vint_lo)s AND %(vint_hi)s)
                      AND is_campaign_cost = FALSE
                      AND actual_cost > 0
                    ORDER BY RANDOM() LIMIT %(n)s
                """, dict(basin=b, well_type=wt, depth_lo=depth_lo,
                          depth_hi=depth_hi, vint_lo=vint_lo,
                          vint_hi=vint_hi, n=max_count))
                rows = cur.fetchall()

                if len(rows) >= 3:
                    costs = [_cpi_adjust(float(r['actual_cost']), r['cost_year'] or CPI_BASE_YEAR)
                             for r in rows]
                    n = len(costs)
                    s = sorted(costs)
                    return dict(
                        costs=costs,
                        comparable_count=n,
                        confidence=_confidence(n, "exact"),
                        data_density=_data_density(n),
                        match_level="exact",
                        p50_comparable=s[n // 2],
                    )

                # ── LEVEL 2: same basin, relax well_type ─────────────────
                cur.execute("""
                    SELECT actual_cost, cost_year FROM comparable_costs
                    WHERE basin = %(basin)s
                      AND (total_depth_ft IS NULL OR total_depth_ft BETWEEN %(depth_lo)s AND %(depth_hi)s)
                      AND is_campaign_cost = FALSE
                      AND actual_cost > 0
                    ORDER BY RANDOM() LIMIT %(n)s
                """, dict(basin=b, depth_lo=depth_lo, depth_hi=depth_hi, n=max_count))
                rows = cur.fetchall()

                if len(rows) >= 3:
                    costs = [_cpi_adjust(float(r['actual_cost']), r['cost_year'] or CPI_BASE_YEAR)
                             for r in rows]
                    n = len(costs)
                    s = sorted(costs)
                    return dict(
                        costs=costs,
                        comparable_count=n,
                        confidence=_confidence(n, "basin_relaxed"),
                        data_density=_data_density(n),
                        match_level="basin_relaxed",
                        p50_comparable=s[n // 2],
                    )

                # ── LEVEL 3: region-wide (all GOM basins) ────────────────
                basins_in_region = list(GOM_BASINS) if b in GOM_BASINS else [b]
                cur.execute("""
                    SELECT actual_cost, cost_year FROM comparable_costs
                    WHERE basin = ANY(%(basins)s)
                      AND (total_depth_ft IS NULL OR total_depth_ft BETWEEN %(depth_lo)s AND %(depth_hi)s)
                      AND is_campaign_cost = FALSE
                      AND actual_cost > 0
                    ORDER BY RANDOM() LIMIT %(n)s
                """, dict(basins=basins_in_region, depth_lo=depth_lo,
                          depth_hi=depth_hi, n=max_count))
                rows = cur.fetchall()

                if len(rows) >= 3:
                    costs = [_cpi_adjust(float(r['actual_cost']), r['cost_year'] or CPI_BASE_YEAR)
                             for r in rows]
                    n = len(costs)
                    s = sorted(costs)
                    return dict(
                        costs=costs,
                        comparable_count=n,
                        confidence=_confidence(n, "region_wide"),
                        data_density=_data_density(n),
                        match_level="region_wide",
                        p50_comparable=s[n // 2],
                    )

                # ── LEVEL 4: depth band only (floor) ─────────────────────
                cur.execute("""
                    SELECT actual_cost, cost_year FROM comparable_costs
                    WHERE (total_depth_ft IS NULL OR total_depth_ft BETWEEN %(depth_lo)s AND %(depth_hi)s)
                      AND is_campaign_cost = FALSE
                      AND actual_cost > 0
                    ORDER BY RANDOM() LIMIT %(n)s
                """, dict(depth_lo=depth_lo, depth_hi=depth_hi, n=max_count))
                rows = cur.fetchall()

                if rows:
                    costs = [_cpi_adjust(float(r['actual_cost']), r['cost_year'] or CPI_BASE_YEAR)
                             for r in rows]
                    n = len(costs)
                    s = sorted(costs)
                    return dict(
                        costs=costs,
                        comparable_count=n,
                        confidence="INSUFFICIENT",
                        data_density=_data_density(n),
                        match_level="depth_only",
                        p50_comparable=s[n // 2] if n > 0 else None,
                    )

    except Exception as e:
        return {**result_template, "error": str(e)}

    return result_template


def percentiles_from_comparables(costs: List[float]) -> Dict[str, Optional[float]]:
    """
    PRO-190: Derive P10/P25/P50/P75/P90 from comparable cost list.
    All calculations deterministic (sorted index).
    Requires >= 3 for P90; >= 1 for P50.
    """
    if not costs:
        return dict(p10=None, p25=None, p50=None, p75=None, p90=None)
    s = sorted(costs)
    n = len(s)
    def pct(p):
        idx = min(int(n * p), n - 1)
        return round(s[idx])
    return dict(
        p10 = pct(0.10) if n >= 3 else None,
        p25 = pct(0.25) if n >= 3 else None,
        p50 = pct(0.50),
        p75 = pct(0.75) if n >= 3 else None,
        p90 = pct(0.90) if n >= 3 else None,
    )
