"""
PRO-214: Weather window multiplier for P90 schedule risk.

Pulls basin_weather_windows data and computes a schedule-risk multiplier
that widens the P90 estimate to account for weather-driven delays.

Logic:
  - A decommissioning campaign requires N workable days (estimated from well depth/type)
  - If P50 workable days/month = X but P10 = Y (bad weather), campaign takes longer
  - Extra days × HLV_DAY_RATE_USD = schedule risk premium
  - This premium is added to P90 as an uplift multiplier

HLV day rate is user-configurable (default $400K/day for heavy lift vessel).
"""

import psycopg2
import psycopg2.extras
from typing import Optional, Dict

DB = 'postgresql://postgres:$DB_PASSWORD@db.temtptsfiksixxhbigkg.supabase.co:5432/postgres'

# Default HLV day rate USD (range $300K–$600K per PRO-214 spec)
DEFAULT_HLV_DAY_RATE_USD = 400_000

# Basin mapping from well state/area to weather_windows basin key
BASIN_TO_WEATHER = {
    # GOM basins
    'EI': 'GOM', 'SS': 'GOM', 'WC': 'GOM', 'HI': 'GOM',
    'VR': 'GOM', 'ST': 'GOM', 'MP': 'GOM', 'SM': 'GOM',
    'GOM': 'GOM', 'TX': 'GOM', 'LA': 'GOM',
    # North Sea
    'UK': 'NORTH_SEA', 'GB': 'NORTH_SEA', 'NO': 'NORTH_SEA',
    'Norway': 'NORTH_SEA', 'UK_NORTH_SEA_SHALLOW': 'NORTH_SEA',
    'UK_NORTH_SEA_DEEP': 'NORTH_SEA',
    # Australia
    'AU': 'NW_SHELF_AU', 'WA': 'NW_SHELF_AU',
    # SE Asia
    'MY': 'SE_ASIA', 'ID': 'SE_ASIA', 'VN': 'SE_ASIA',
    # Barents / Arctic
    'RU': 'BARENTS_SEA',
}

# Estimated decom campaign duration in HLV-days by well class and depth
# (simplified — real would be from Kaiser/BOEM benchmarks)
CAMPAIGN_DAYS_ESTIMATE = {
    ("OFFSHORE", "<3000"):   12,
    ("OFFSHORE", "3-8k"):    18,
    ("OFFSHORE", "8-15k"):   28,
    ("OFFSHORE", "15k+"):    45,
    ("PLATFORM", "<3000"):   20,
    ("PLATFORM", "3-8k"):    30,
    ("SUBSEA",   "<3000"):   8,
    ("SUBSEA",   "3-8k"):    14,
    ("ONSHORE",  "<3000"):   2,
    ("ONSHORE",  "3-8k"):    4,
    ("ONSHORE",  "8-15k"):   7,
    ("ONSHORE",  "15k+"):    12,
}

_weather_cache: Dict[str, dict] = {}


def _load_weather_windows() -> Dict[str, dict]:
    """Load basin_weather_windows from DB, return by basin."""
    global _weather_cache
    if _weather_cache:
        return _weather_cache

    try:
        with psycopg2.connect(DB, connect_timeout=10) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT basin,
                           SUM(workable_days_p50) AS annual_p50,
                           SUM(workable_days_p10) AS annual_p10,
                           SUM(workable_days_p90) AS annual_p90
                    FROM basin_weather_windows
                    GROUP BY basin
                """)
                for row in cur.fetchall():
                    _weather_cache[row['basin']] = {
                        'p50': float(row['annual_p50'] or 250),
                        'p10': float(row['annual_p10'] or 180),
                        'p90': float(row['annual_p90'] or 300),
                    }
    except Exception:
        # Fallback: use conservative defaults if DB unavailable
        _weather_cache = {
            'GOM':        {'p50': 300, 'p10': 240, 'p90': 330},
            'NORTH_SEA':  {'p50': 200, 'p10': 140, 'p90': 260},
            'NW_SHELF_AU':{'p50': 250, 'p10': 190, 'p90': 300},
            'SE_ASIA':    {'p50': 280, 'p10': 220, 'p90': 320},
            'BARENTS_SEA':{'p50': 150, 'p10': 90,  'p90': 200},
        }
    return _weather_cache


def get_weather_multiplier(
    basin: Optional[str],
    state: Optional[str],
    well_class: Optional[str],
    depth_bucket: Optional[str],
    hlv_day_rate_usd: float = DEFAULT_HLV_DAY_RATE_USD,
    base_p50_usd: float = 1_000_000,
) -> Dict[str, float]:
    """
    Compute weather-window schedule risk uplift for P90.

    Returns:
        p90_weather_multiplier : float (e.g. 1.18 = 18% uplift)
        extra_days_p90         : float (expected extra days at P10 weather)
        weather_basin          : str | None
        hlv_day_rate_usd       : float
    """
    # Map to weather basin key
    key = (basin or state or "").upper()
    weather_basin = BASIN_TO_WEATHER.get(key) or BASIN_TO_WEATHER.get(state or "")

    if not weather_basin or (well_class or "ONSHORE") == "ONSHORE":
        # Onshore wells have minimal weather risk
        return dict(
            p90_weather_multiplier=1.0,
            extra_days_p90=0,
            weather_basin=None,
            hlv_day_rate_usd=hlv_day_rate_usd,
        )

    windows = _load_weather_windows()
    w = windows.get(weather_basin, {'p50': 250, 'p10': 200, 'p90': 300})

    wc = (well_class or "OFFSHORE").upper()
    db = depth_bucket or "<3000"
    campaign_days = CAMPAIGN_DAYS_ESTIMATE.get((wc, db), 14)

    # At P10 weather (bad year), how long does campaign_days of work actually take?
    # workable fraction = p10_workable / 365
    p50_fraction = w['p50'] / 365
    p10_fraction = w['p10'] / 365  # worst 10% of years

    # Calendar days needed at P10 weather
    calendar_days_p50 = campaign_days / p50_fraction
    calendar_days_p10_weather = campaign_days / p10_fraction  # longer = worse
    extra_days = max(0, calendar_days_p10_weather - calendar_days_p50)

    # Cost of extra days
    extra_cost = extra_days * hlv_day_rate_usd
    multiplier = 1.0 + (extra_cost / max(base_p50_usd, 1))

    # Cap multiplier at 1.6× to prevent extreme outliers
    multiplier = min(multiplier, 1.60)

    return dict(
        p90_weather_multiplier=round(multiplier, 4),
        extra_days_p90=round(extra_days, 1),
        weather_basin=weather_basin,
        hlv_day_rate_usd=hlv_day_rate_usd,
    )
