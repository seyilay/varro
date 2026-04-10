"""
Regional cost configuration for the ARO cost model v1.1.
Base P50 costs are per-well in USD, calibrated per region × well_class.
Sources: BOEM actuals, AER LLR actuals, NSTA estimates, industry surveys.
"""

MODEL_VERSION = "1.1.0"

# Base P50 cost per well (USD) by (cost_region, well_class)
# well_class: ONSHORE | OFFSHORE | PLATFORM | SUBSEA
# Depth/vintage multipliers applied on top; these are for a 5000ft/2000s well
BASE_P50 = {
    # ── US ─────────────────────────────────────────────────────────────────
    ("US_ONSHORE_PERMIAN",        "ONSHORE"):  200_000,
    ("US_ONSHORE_APPALACHIAN",    "ONSHORE"):  130_000,
    ("US_ONSHORE_MIDCON",         "ONSHORE"):  160_000,
    ("US_ONSHORE_ROCKIES",        "ONSHORE"):  190_000,
    ("US_ONSHORE_CALIFORNIA",     "ONSHORE"):  250_000,
    ("US_ONSHORE_OTHER",          "ONSHORE"):  160_000,
    ("GOM_SHELF",                 "OFFSHORE"): 2_200_000,
    ("GOM_SHELF",                 "PLATFORM"): 3_500_000,
    ("GOM_DEEPWATER",             "OFFSHORE"): 12_000_000,
    ("GOM_DEEPWATER",             "PLATFORM"): 18_000_000,
    ("GOM_DEEPWATER",             "SUBSEA"):   22_000_000,
    # ── Canada ──────────────────────────────────────────────────────────────
    ("CANADA_AB_CONVENTIONAL",    "ONSHORE"):  160_000,
    ("CANADA_AB_OILSANDS",        "ONSHORE"):  400_000,   # surface reclamation
    ("CANADA_BC_CONVENTIONAL",    "ONSHORE"):  180_000,
    ("CANADA_SK_CONVENTIONAL",    "ONSHORE"):  140_000,
    ("CANADA_OFFSHORE",           "OFFSHORE"): 5_000_000,
    ("CANADA_OFFSHORE",           "PLATFORM"): 8_000_000,
    # ── UK / Norway ──────────────────────────────────────────────────────────
    ("UK_NORTH_SEA_SHALLOW",      "OFFSHORE"): 4_500_000,
    ("UK_NORTH_SEA_SHALLOW",      "PLATFORM"): 8_000_000,
    ("UK_NORTH_SEA_DEEP",         "OFFSHORE"): 9_000_000,
    ("UK_NORTH_SEA_DEEP",         "PLATFORM"): 15_000_000,
    ("UK_NORTH_SEA_DEEP",         "SUBSEA"):   18_000_000,
    ("NORWAY_NCS",                "OFFSHORE"): 7_000_000,
    ("NORWAY_NCS",                "PLATFORM"): 12_000_000,
    ("NORWAY_NCS",                "SUBSEA"):   20_000_000,
    # ── Australia ────────────────────────────────────────────────────────────
    ("AUSTRALIA_OFFSHORE",        "OFFSHORE"): 5_000_000,
    ("AUSTRALIA_OFFSHORE",        "PLATFORM"): 9_000_000,
    ("AUSTRALIA_ONSHORE",         "ONSHORE"):  190_000,
    # ── Africa ───────────────────────────────────────────────────────────────
    ("NIGERIA_ONSHORE",           "ONSHORE"):  120_000,
    ("NIGERIA_OFFSHORE",          "OFFSHORE"): 3_500_000,
    ("NIGERIA_OFFSHORE",          "PLATFORM"): 6_000_000,
    ("ANGOLA_OFFSHORE",           "OFFSHORE"): 4_000_000,
    ("ANGOLA_OFFSHORE",           "PLATFORM"): 7_000_000,
    ("AFRICA_OTHER",              "ONSHORE"):  110_000,
    ("AFRICA_OTHER",              "OFFSHORE"): 2_500_000,
    # ── LatAm ────────────────────────────────────────────────────────────────
    ("BRAZIL_OFFSHORE",           "OFFSHORE"): 6_000_000,
    ("BRAZIL_OFFSHORE",           "SUBSEA"):   25_000_000,
    ("BRAZIL_ONSHORE",            "ONSHORE"):  130_000,
    ("ARGENTINA_ONSHORE",         "ONSHORE"):  120_000,
    ("LATAM_OTHER",               "ONSHORE"):  100_000,
    ("LATAM_OTHER",               "OFFSHORE"): 2_000_000,
    # ── Middle East / Asia ───────────────────────────────────────────────────
    ("MIDDLE_EAST_ONSHORE",       "ONSHORE"):  80_000,
    ("MIDDLE_EAST_OFFSHORE",      "OFFSHORE"): 2_500_000,
    ("ASIA_PAC_OFFSHORE",         "OFFSHORE"): 3_000_000,
    ("ASIA_PAC_OFFSHORE",         "PLATFORM"): 5_000_000,
    # ── Fallback ─────────────────────────────────────────────────────────────
    ("GLOBAL_INFERENCE",          "ONSHORE"):  150_000,
    ("GLOBAL_INFERENCE",          "OFFSHORE"): 3_000_000,
    ("GLOBAL_INFERENCE",          "PLATFORM"): 5_000_000,
    ("GLOBAL_INFERENCE",          "SUBSEA"):   15_000_000,
}

# P10/P90 spread factors (lognormal sigma calibration)
# (p10_factor, p90_factor) relative to P50, by region tier
SPREAD_BY_TIER = {
    1: (0.40, 2.50),   # Tier 1: empirically calibrated (BOEM, AER, NSTA)
    2: (0.30, 3.20),   # Tier 2: multiplier-estimated; wider uncertainty
    3: (0.20, 5.00),   # Tier 3: inference only; very wide
}

# Region tier and bias uplift
REGION_CONFIG = {
    "US_ONSHORE_PERMIAN":       (1, 1.15),
    "US_ONSHORE_APPALACHIAN":   (1, 1.20),
    "US_ONSHORE_MIDCON":        (1, 1.15),
    "US_ONSHORE_ROCKIES":       (1, 1.15),
    "US_ONSHORE_CALIFORNIA":    (1, 1.20),
    "US_ONSHORE_OTHER":         (1, 1.15),
    "GOM_SHELF":                (1, 1.15),
    "GOM_DEEPWATER":            (1, 1.15),
    "CANADA_AB_CONVENTIONAL":   (1, 1.25),
    "CANADA_AB_OILSANDS":       (2, 1.25),
    "CANADA_BC_CONVENTIONAL":   (2, 1.25),
    "CANADA_SK_CONVENTIONAL":   (2, 1.25),
    "CANADA_OFFSHORE":          (2, 1.20),
    "UK_NORTH_SEA_SHALLOW":     (1, 1.10),
    "UK_NORTH_SEA_DEEP":        (1, 1.10),
    "NORWAY_NCS":               (2, 1.15),
    "AUSTRALIA_OFFSHORE":       (2, 1.20),
    "AUSTRALIA_ONSHORE":        (2, 1.20),
    "NIGERIA_ONSHORE":          (2, 1.40),
    "NIGERIA_OFFSHORE":         (2, 1.40),
    "ANGOLA_OFFSHORE":          (3, 1.35),
    "AFRICA_OTHER":             (3, 1.40),
    "BRAZIL_OFFSHORE":          (2, 1.25),
    "BRAZIL_ONSHORE":           (3, 1.25),
    "ARGENTINA_ONSHORE":        (3, 1.30),
    "LATAM_OTHER":              (3, 1.35),
    "MIDDLE_EAST_ONSHORE":      (3, 1.50),
    "MIDDLE_EAST_OFFSHORE":     (3, 1.50),
    "ASIA_PAC_OFFSHORE":        (3, 1.30),
    "GLOBAL_INFERENCE":         (3, 1.40),
}

# Depth multipliers (relative to 5000ft reference well)
DEPTH_MULTIPLIERS = {
    "<3000":     0.60,
    "3000-8000": 1.00,
    "8000-15000":1.65,
    "15000+":    3.00,
}

# Vintage multipliers
VINTAGE_MULTIPLIERS = {
    "<1970":     1.50,
    "1970-1985": 1.25,
    "1985-2000": 1.10,
    "2000-2010": 1.00,
    "2010+":     0.90,
}

# Well type adjustment (modest — type matters less than location/depth)
WELL_TYPE_MULTIPLIERS = {
    "OIL":      1.00,
    "GAS":      0.92,
    "OIL_GAS":  1.05,
    "INJECTION": 0.75,
    "DISPOSAL":  0.70,
    "OTHER":    0.65,
}

# State → cost region mapping
STATE_TO_REGION = {
    "TX": "US_ONSHORE_PERMIAN",
    "PA": "US_ONSHORE_APPALACHIAN",
    "WV": "US_ONSHORE_APPALACHIAN",
    "OH": "US_ONSHORE_APPALACHIAN",
    "OK": "US_ONSHORE_MIDCON",
    "KS": "US_ONSHORE_MIDCON",
    "WY": "US_ONSHORE_ROCKIES",
    "CO": "US_ONSHORE_ROCKIES",
    "UT": "US_ONSHORE_ROCKIES",
    "CA": "US_ONSHORE_CALIFORNIA",
    "LA": "US_ONSHORE_OTHER",
    "ND": "US_ONSHORE_ROCKIES",
    "MI": "US_ONSHORE_OTHER",
    "MS": "US_ONSHORE_OTHER",
    "AR": "US_ONSHORE_OTHER",
    "Alberta":          "CANADA_AB_CONVENTIONAL",
    "British Columbia": "CANADA_BC_CONVENTIONAL",
    "Saskatchewan":     "CANADA_SK_CONVENTIONAL",
    "Manitoba":         "CANADA_SK_CONVENTIONAL",
    "UK":  "UK_NORTH_SEA_SHALLOW",
    "GB":  "UK_NORTH_SEA_SHALLOW",
    "NO":  "NORWAY_NCS",
    "NCS": "NORWAY_NCS",
    "RN":  "BRAZIL_ONSHORE",   # Rio Grande do Norte, Brazil (was incorrectly NORWAY_NCS)
    "AU":  "AUSTRALIA_OFFSHORE",
    "WA":  "AUSTRALIA_OFFSHORE",
    "BR":  "BRAZIL_OFFSHORE",
    "RJ":  "BRAZIL_OFFSHORE",
    "BA":  "BRAZIL_ONSHORE",
    "AM":  "BRAZIL_ONSHORE",
    "SE_AR": "ARGENTINA_ONSHORE",  # Argentina state code
    # Note: "AR" is deliberately NOT mapped here — AR = Arkansas (US) which maps via FULL_NAME_TO_REGION
    # Argentina uses "SE_AR" or the full name "Argentina"
    "NG":  "NIGERIA_OFFSHORE",
}

# Extended mapping: full state/country names → cost region
FULL_NAME_TO_REGION = {
    # US full names
    "Texas":            "US_ONSHORE_PERMIAN",
    "Pennsylvania":     "US_ONSHORE_APPALACHIAN",
    "West Virginia":    "US_ONSHORE_APPALACHIAN",
    "Ohio":             "US_ONSHORE_APPALACHIAN",
    "Oklahoma":         "US_ONSHORE_MIDCON",
    "Kansas":           "US_ONSHORE_MIDCON",
    "Wyoming":          "US_ONSHORE_ROCKIES",
    "Colorado":         "US_ONSHORE_ROCKIES",
    "Utah":             "US_ONSHORE_ROCKIES",
    "California":       "US_ONSHORE_CALIFORNIA",
    "Louisiana":        "US_ONSHORE_OTHER",
    "North Dakota":     "US_ONSHORE_ROCKIES",
    "Michigan":         "US_ONSHORE_OTHER",
    "Mississippi":      "US_ONSHORE_OTHER",
    "Arkansas":         "US_ONSHORE_OTHER",
    "Alaska":           "US_ONSHORE_OTHER",
    "New Mexico":       "US_ONSHORE_ROCKIES",
    "Montana":          "US_ONSHORE_ROCKIES",
    "United States":    "US_ONSHORE_OTHER",
    # Canada full names
    "Alberta":          "CANADA_AB_CONVENTIONAL",
    "British Columbia": "CANADA_BC_CONVENTIONAL",
    "BC":               "CANADA_BC_CONVENTIONAL",
    "Saskatchewan":     "CANADA_SK_CONVENTIONAL",
    "Manitoba":         "CANADA_SK_CONVENTIONAL",
    "Canada":           "CANADA_AB_CONVENTIONAL",
    # Europe
    "United Kingdom":   "UK_NORTH_SEA_SHALLOW",
    "Norway":           "NORWAY_NCS",
    "Netherlands":      "UK_NORTH_SEA_SHALLOW",  # similar regime
    "Denmark":          "UK_NORTH_SEA_SHALLOW",
    "DK":               "UK_NORTH_SEA_SHALLOW",
    # Australia
    "Australia":        "AUSTRALIA_OFFSHORE",
    # Americas
    "Brazil":           "BRAZIL_OFFSHORE",
    "Argentina":        "ARGENTINA_ONSHORE",
    "Colombia":         "LATAM_OTHER",
    "Mexico":           "GLOBAL_INFERENCE",   # CNH data limited; was wrongly MIDDLE_EAST_ONSHORE
    "Venezuela":        "LATAM_OTHER",
    "Ecuador":          "LATAM_OTHER",
    # Africa / Middle East
    "Nigeria":          "NIGERIA_OFFSHORE",
    "Angola":           "ANGOLA_OFFSHORE",
    "Egypt":            "MIDDLE_EAST_OFFSHORE",
    "Libya":            "MIDDLE_EAST_OFFSHORE",
    # Asia
    "China":            "ASIA_PAC_OFFSHORE",
    "Indonesia":        "ASIA_PAC_OFFSHORE",
    "Malaysia":         "ASIA_PAC_OFFSHORE",
    "Russia":           "GLOBAL_INFERENCE",
    "Iran":             "MIDDLE_EAST_ONSHORE",
    "Poland":           "US_ONSHORE_OTHER",  # onshore shale
}
