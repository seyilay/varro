"""
ARO Probabilistic Cost Estimator v1.2
PRO-189 / PRO-190 / PRO-191 / PRO-214

v1.2 changes:
  - PRO-189: comparable_selector integration (live DB query)
  - PRO-190: empirical P10/P50/P90 from comparable set when available
  - PRO-191: bias correction factor documented + applied per region
  - PRO-214: weather window multiplier applied to P90 for offshore wells
"""

import math
from typing import Optional
from .regions import (
    BASE_P50, SPREAD_BY_TIER, REGION_CONFIG,
    DEPTH_MULTIPLIERS, VINTAGE_MULTIPLIERS, WELL_TYPE_MULTIPLIERS,
    STATE_TO_REGION, FULL_NAME_TO_REGION, MODEL_VERSION
)

MODEL_VERSION_V2 = "1.2.0"

def _bucket(val, buckets):
    for label, lo, hi in buckets:
        if lo <= (val or 0) < hi: return label
    return buckets[-1][0]

DEPTH_BUCKETS = [
    ("<3000", 0, 3000), ("3000-8000", 3000, 8000),
    ("8000-15000", 8000, 15000), ("15000+", 15000, 9999999)
]
VINTAGE_BUCKETS = [
    ("<1970", 0, 1970), ("1970-1985", 1970, 1985),
    ("1985-2000", 1985, 2000), ("2000-2010", 2000, 2010), ("2010+", 2010, 9999)
]
WATER_BUCKETS = [
    ("0-300", 0, 300), ("300-1000", 300, 1000),
    ("1000-5000", 1000, 5000), ("5000+", 5000, 9999999)
]


def get_cost_region(state, well_class, water_depth_ft):
    s = state or ""
    region = STATE_TO_REGION.get(s) or FULL_NAME_TO_REGION.get(s, "GLOBAL_INFERENCE")
    if well_class in ("OFFSHORE", "PLATFORM", "SUBSEA"):
        if state in ("TX", "LA", "MS", "GOM"):
            region = "GOM_DEEPWATER" if (water_depth_ft or 0) >= 1000 else "GOM_SHELF"
        elif state in ("UK", "GB"):
            region = "UK_NORTH_SEA_DEEP" if (water_depth_ft or 0) >= 500 else "UK_NORTH_SEA_SHALLOW"
    return region if region in REGION_CONFIG else "GLOBAL_INFERENCE"


def estimate_well_cost(
    state=None, well_class=None, well_type=None,
    total_depth_ft=None, water_depth_ft=None,
    vintage_year=None, status=None,
    basin=None,
    comparable_actuals=None,           # pre-fetched actuals (legacy path)
    use_comparable_db=False,           # PRO-189: query comparable_costs live
    hlv_day_rate_usd: float = 400_000, # PRO-214: HLV day rate (user-adjustable)
    apply_weather_multiplier=True,     # PRO-214: toggle weather risk on P90
) -> dict:
    """
    Estimate decommissioning cost for a single well.

    Returns P10/P25/P50/P75/P90 in USD with metadata.
    When use_comparable_db=True, queries comparable_costs table live (PRO-189).
    Weather multiplier applied to P90 for offshore wells (PRO-214).
    Bias correction factor applied from REGION_CONFIG (PRO-191).
    """
    well_class = well_class or "ONSHORE"
    cost_region = get_cost_region(state, well_class, water_depth_ft)
    tier, bias = REGION_CONFIG[cost_region]
    p10_factor, p90_factor = SPREAD_BY_TIER[tier]

    # ── Base P50 (parametric) ────────────────────────────────────────────
    base_key = (cost_region, well_class)
    if base_key not in BASE_P50:
        fallback_class = "ONSHORE" if well_class == "ONSHORE" else "OFFSHORE"
        base_key = (cost_region, fallback_class)
        if base_key not in BASE_P50:
            base_key = ("GLOBAL_INFERENCE", fallback_class)

    base = BASE_P50[base_key]
    depth_bucket   = _bucket(total_depth_ft or 5000, DEPTH_BUCKETS)
    vintage_bucket = _bucket(vintage_year or 2000, VINTAGE_BUCKETS)
    depth_mult     = DEPTH_MULTIPLIERS.get(depth_bucket, 1.0)
    vintage_mult   = VINTAGE_MULTIPLIERS.get(vintage_bucket, 1.0)
    type_mult      = WELL_TYPE_MULTIPLIERS.get(well_type or "OIL", 1.0)

    p50_parametric = round(base * depth_mult * vintage_mult * type_mult * bias)

    # ── PRO-189: Live comparable selection ───────────────────────────────
    comparable_n = 0
    match_level = "parametric"
    comp_confidence = {1: "HIGH", 2: "MEDIUM", 3: "LOW"}.get(tier, "LOW")

    if use_comparable_db and not comparable_actuals:
        try:
            from .comparable_selector import select_comparables
            comp = select_comparables(
                basin=basin, state=state, well_type=well_type,
                well_class=well_class, total_depth_ft=total_depth_ft,
                water_depth_ft=water_depth_ft, vintage_year=vintage_year,
            )
            if comp['costs']:
                comparable_actuals = comp['costs']
                match_level = comp['match_level']
                comp_confidence = comp['confidence']
        except Exception:
            pass  # Fall through to parametric

    # ── PRO-190: Percentile calculation ─────────────────────────────────
    if comparable_actuals and len(comparable_actuals) >= 3:
        s = sorted(comparable_actuals)
        n = len(s)
        comparable_n = n

        def pct(p):
            return s[min(int(n * p), n - 1)]

        p10_emp = round(pct(0.10))
        p25_emp = round(pct(0.25))
        p50_emp = round(pct(0.50))
        p75_emp = round(pct(0.75))
        p90_emp = round(pct(0.90)) if n >= 3 else None

        # Blend: weight empirical more heavily when more comparables available
        blend = min(0.9, 0.5 + (n / 100))  # 0.5 at n=0, up to 0.9 at n=40+
        p50 = round(blend * p50_emp + (1 - blend) * p50_parametric)

        sigma = (math.log(p90_factor) - math.log(p10_factor)) / (2 * 1.282)
        p10 = p10_emp if n >= 10 else round(p50 * math.exp(-1.282 * sigma))
        p25 = p25_emp if n >= 10 else round(p50 * math.exp(-0.674 * sigma))
        p75 = p75_emp if n >= 10 else round(p50 * math.exp(0.674 * sigma))
        p90 = p90_emp if n >= 3 else round(p50 * math.exp(1.282 * sigma))
        confidence = comp_confidence

    else:
        # Pure parametric (PRO-191 bias already applied in p50_parametric)
        p50 = p50_parametric
        sigma = (math.log(p90_factor) - math.log(p10_factor)) / (2 * 1.282)
        p10 = round(p50 * math.exp(-1.282 * sigma))
        p25 = round(p50 * math.exp(-0.674 * sigma))
        p75 = round(p50 * math.exp(0.674 * sigma))
        p90 = round(p50 * math.exp(1.282 * sigma))
        confidence = comp_confidence

    # ── PRO-214: Weather window multiplier on P90 ────────────────────────
    weather_mult = 1.0
    weather_basin = None
    extra_days_p90 = 0
    if apply_weather_multiplier and well_class in ("OFFSHORE", "PLATFORM", "SUBSEA"):
        try:
            from .weather_multiplier import get_weather_multiplier
            wm = get_weather_multiplier(
                basin=basin, state=state, well_class=well_class,
                depth_bucket=depth_bucket, hlv_day_rate_usd=hlv_day_rate_usd,
                base_p50_usd=p50,
            )
            weather_mult = wm['p90_weather_multiplier']
            weather_basin = wm['weather_basin']
            extra_days_p90 = wm['extra_days_p90']
            if weather_mult > 1.0 and p90:
                p90 = round(p90 * weather_mult)
                p75 = round(p75 * ((weather_mult - 1) * 0.5 + 1))  # partial uplift on P75
        except Exception:
            pass

    return dict(
        # Core outputs
        p10_usd=p10, p25_usd=p25, p50_usd=p50, p75_usd=p75, p90_usd=p90,
        # Region + metadata
        cost_region=cost_region,
        depth_bucket=depth_bucket,
        vintage_bucket=vintage_bucket,
        # PRO-189/190: comparable set
        comparable_n=comparable_n,
        match_level=match_level,
        # PRO-191: bias correction
        bias_correction_factor=bias,
        bias_correction_applied=True,
        # PRO-214: weather
        weather_multiplier=weather_mult,
        weather_basin=weather_basin,
        extra_days_p90=extra_days_p90,
        hlv_day_rate_usd=hlv_day_rate_usd,
        # Quality
        confidence=confidence,
        model_version=MODEL_VERSION_V2,
    )
