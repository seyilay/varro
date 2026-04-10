"""
Infrastructure ARO cost estimator — Kaiser-calibrated.
Source: Kaiser 2017 (FERC pipelines), Kaiser 2014/2015 (platforms), BSEE 2024.
"""
import math

def estimate_pipeline_decom(
    length_miles: float = None,
    length_km: float = None,
    water_depth_m: float = 0,
    is_damaged: bool = False,
    service_type: str = 'GAS',   # 'OIL','GAS','BULK_OIL','BULK_GAS','UMBILICAL','SERVICE'
    diameter_in: float = 12,
    is_removal: bool = False,
) -> dict:
    """
    Pipeline decommissioning cost estimate.
    Kaiser (2017): avg $301k/mile shallow water (2014 USD).
    Kaiser/BSEE (2024): $1.35-1.6M/segment deepwater.
    """
    if length_km and not length_miles:
        length_miles = length_km * 0.621371
    if not length_miles:
        length_miles = 5.0  # default segment length

    is_deepwater = water_depth_m > 120  # 400ft threshold

    if is_deepwater:
        # Per-segment pricing (Kaiser BSEE 2024)
        avg_segment_miles = 5.0
        n_segments = max(1, length_miles / avg_segment_miles)
        if service_type in ('BULK_OIL', 'BULK_GAS', 'UMBILICAL', 'SERVICE'):
            cost_per_segment = 1_600_000
        else:
            cost_per_segment = 1_350_000
        base_cost = n_segments * cost_per_segment
    else:
        # Per-mile pricing (Kaiser 2017 FERC empirical)
        base_cost = length_miles * 301_000

    # Removal premium (Kaiser 2017: removal ~2-3x AIP cost)
    if is_removal:
        base_cost *= 2.5

    # Hurricane/damage premium (Kaiser 2017: 3.4x for damaged)
    if is_damaged:
        base_cost *= 3.4

    # Lognormal spread — Kaiser observes σ ≈ μ for offshore
    # Use wide distribution consistent with empirical observations
    if is_deepwater:
        p10_factor, p90_factor = 0.15, 5.0
    else:
        p10_factor, p90_factor = 0.20, 4.0

    return {
        'p10_usd': round(base_cost * p10_factor),
        'p25_usd': round(base_cost * 0.40),
        'p50_usd': round(base_cost),
        'p75_usd': round(base_cost * 2.0),
        'p90_usd': round(base_cost * p90_factor),
        'confidence': 'MEDIUM' if not is_damaged else 'LOW',
        'source': 'kaiser_2017_ferc' if not is_deepwater else 'kaiser_bsee_2024',
        'length_miles': length_miles,
        'water_depth_m': water_depth_m,
        'is_deepwater': is_deepwater,
    }


def estimate_platform_decom(
    water_depth_m: float,
    platform_type: str = 'FIXED',  # 'FIXED','TLP','SPAR','SEMI','FPSO','CAIS'
    mass_t: float = None,           # topside mass in tonnes (from NSTA data!)
    n_wells: int = 0,
    vintage_year: int = None,
) -> dict:
    """
    Platform decommissioning cost estimate.
    Kaiser 2014: fixed platforms by water depth.
    Kaiser 2015: deepwater floating structures.
    NSTA SDC data provides mass_t for UK North Sea structures.
    """
    wd_ft = water_depth_m * 3.28084

    if platform_type in ('TLP', 'SPAR', 'SEMI', 'FPSO'):
        # Kaiser 2015 deepwater floaters — work decomposition estimate
        # Base: well P&A + pipeline decom + umbilical + deck + hull
        base = 50_000_000  # $50M base for floaters
        if platform_type == 'FPSO':
            base = 200_000_000  # FPSOs are 4-5x more complex
        elif platform_type == 'SPAR':
            base = 80_000_000
        elif platform_type == 'TLP':
            base = 70_000_000

        # Add per-well cost
        base += n_wells * 3_500_000  # $3.5M/well deepwater P&A

        p10, p90 = base * 0.3, base * 3.0
        confidence = 'LOW'

    elif platform_type in ('FIXED', 'JACKET'):
        # Kaiser 2014: fixed platforms by water depth
        if wd_ft < 200:
            base = 4_000_000 + (wd_ft / 200) * 6_000_000  # $4-10M
        elif wd_ft < 500:
            base = 10_000_000 + (wd_ft / 500) * 70_000_000  # $10-80M
        else:
            base = 80_000_000 + (wd_ft / 500) * 50_000_000  # deepwater fixed

        # Mass adjustment if available (NSTA provides mass_t)
        if mass_t and mass_t > 0:
            # Rule of thumb: $5,000-15,000 per tonne for removal
            mass_cost = mass_t * 8_000  # mid estimate
            base = (base + mass_cost) / 2  # blend

        # Add per-well cost
        base += n_wells * 500_000  # $500k/well shallow water P&A

        p10, p90 = base * 0.3, base * 3.5
        confidence = 'MEDIUM' if wd_ft < 200 else 'LOW'

    elif platform_type == 'CAIS':
        # Caissons — simple structures, mostly shallow water
        base = 500_000 + n_wells * 200_000
        p10, p90 = base * 0.5, base * 2.5
        confidence = 'HIGH'
    else:
        base = 5_000_000
        p10, p90 = base * 0.2, base * 4.0
        confidence = 'LOW'

    # Vintage penalty: older structures cost more (deferred maintenance, corrosion)
    if vintage_year and vintage_year < 1990:
        age_factor = 1.0 + (1990 - vintage_year) * 0.015  # +1.5%/year
        base *= age_factor
        p10 *= age_factor
        p90 *= age_factor

    return {
        'p10_usd': round(p10),
        'p25_usd': round(base * 0.55),
        'p50_usd': round(base),
        'p75_usd': round(base * 1.8),
        'p90_usd': round(p90),
        'confidence': confidence,
        'source': 'kaiser_2014_fixed' if platform_type in ('FIXED','JACKET','CAIS') else 'kaiser_2015_floaters',
        'platform_type': platform_type,
        'water_depth_m': water_depth_m,
    }


if __name__ == '__main__':
    print("=== INFRASTRUCTURE COST ESTIMATOR (Kaiser-calibrated) ===\n")

    print("PIPELINES:")
    examples = [
        ("5-mile shallow gas pipeline", dict(length_miles=5, water_depth_m=30, service_type='GAS')),
        ("21-mile shallow pipeline", dict(length_miles=21, water_depth_m=50, service_type='GAS')),
        ("5-mile hurricane-damaged", dict(length_miles=5, water_depth_m=50, is_damaged=True)),
        ("10-mile deepwater export", dict(length_miles=10, water_depth_m=500, service_type='OIL')),
        ("50-mile deepwater umbilical", dict(length_miles=50, water_depth_m=1500, service_type='UMBILICAL')),
    ]
    for label, kwargs in examples:
        r = estimate_pipeline_decom(**kwargs)
        print(f"  {label}: P50=${r['p50_usd']/1e6:.1f}M [P10=${r['p10_usd']/1e6:.1f}M P90=${r['p90_usd']/1e6:.1f}M] [{r['source']}]")

    print("\nPLATFORMS:")
    platform_examples = [
        ("Shallow caisson (30m, 2 wells)", dict(water_depth_m=30, platform_type='CAIS', n_wells=2, vintage_year=1990)),
        ("Shallow fixed jacket (60m, 10 wells)", dict(water_depth_m=60, platform_type='FIXED', n_wells=10, vintage_year=1985)),
        ("Mid-water fixed (120m, 20 wells)", dict(water_depth_m=120, platform_type='FIXED', n_wells=20, vintage_year=1975)),
        ("GOM TLP (900m, 30 wells)", dict(water_depth_m=900, platform_type='TLP', n_wells=30)),
        ("GOM SPAR (1500m, 25 wells)", dict(water_depth_m=1500, platform_type='SPAR', n_wells=25)),
        ("FPSO (200m, 40 wells)", dict(water_depth_m=200, platform_type='FPSO', n_wells=40)),
    ]
    for label, kwargs in platform_examples:
        r = estimate_platform_decom(**kwargs)
        print(f"  {label}: P50=${r['p50_usd']/1e6:.0f}M [P10=${r['p10_usd']/1e6:.0f}M P90=${r['p90_usd']/1e6:.0f}M] [{r['confidence']}]")
