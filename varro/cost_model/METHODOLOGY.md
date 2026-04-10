# Varro ARO Cost Model — Methodology

**Model version:** 1.2.0  
**Status:** Production  
**Last updated:** 2026-04-09

---

## Overview

The Varro ARO cost model estimates asset retirement obligation (ARO) costs for oil and gas wells and infrastructure across 22+ jurisdictions. It produces probabilistic P10/P50/P90 outputs calibrated against BOEM comparable cost data, ERA5 weather windows, and operator-disclosed ARO provisions.

---

## PRO-189: Comparable Well Selection

### Source data
- **Table:** `comparable_costs` — 37,638 BOEM/BSEE regulatory cost records
- **Basins covered:** EI, SS, WC, HI, VR, ST, MP, SM, GA, DE, GC, EB, MC, AC, KC, AT (all GOM sub-basins)

### Selection algorithm
Four-level progressive fallback:

| Level | Criteria | Confidence penalty |
|-------|----------|--------------------|
| 1 (exact) | Basin + well_type + depth band + vintage window | None |
| 2 (basin_relaxed) | Basin + depth band (relax well_type) | −15% |
| 3 (region_wide) | All GOM basins + depth band | −35% |
| 4 (depth_only) | Depth band only (floor) | −70% |

**Minimum set:** 3 comparables required for P90; warning issued below 5.

**Null handling:** BOEM data frequently omits `total_depth_ft` and `vintage_year`. Queries use `IS NULL OR BETWEEN` pattern so NULL records participate in matching.

**Excluded sources:** Fieldwood/Cox exclusion not applicable — all records are BOEM/BSEE regulatory submissions. High-outlier records (>10× basin median) are excluded automatically via `is_campaign_cost = FALSE` filter.

### Depth bands (from BSEE TAD)
- Shallow: 0–3,000 ft
- Mid: 3,000–10,000 ft  
- Deep: 10,000+ ft

### Water depth bands
- Shelf: 0–300 ft
- Deepwater: 300–1,000 ft
- Ultra-deepwater: 1,000+ ft

---

## PRO-190: Percentile Calculation

### Method
Deterministic sorted-index percentiles (no interpolation) from CPI-adjusted comparable set.

**CPI adjustment:** All historical costs inflated to 2024 USD using a fixed 3.5%/year compound rate from cost_year to 2024.

**Output fields:**
```
p10_cost_usd, p25_cost_usd, p50_cost_usd, p75_cost_usd, p90_cost_usd,
comparable_count, confidence_score (0–1), data_density (high/medium/low/insufficient)
```

**Edge cases:**
- n < 3: P10/P25/P75/P90 = null; P50 = single value
- n < 1: all null

**Confidence score formula:** `min(n / 50, 1.0) × level_penalty`

---

## PRO-191: Bias Correction

### Rationale
BSEE TAP 738AA (2016) explicitly states operator-submitted cost estimates are systematically understated. Reasons include:
- Strategic lowballing during permit approval phase
- Exclusion of indirect costs (camp, logistics, compliance)
- Optimistic weather assumptions
- P&A scope creep not anticipated

### Correction factors (v1.2.0)

| Region | Factor | Source |
|--------|--------|--------|
| GOM_SHELF | 1.35× | BSEE TAP 738AA |
| GOM_DEEPWATER | 1.25× | BSEE TAP 738AA |
| UK_NCS | 1.20× | NSTA Decom Benchmark |
| NORWAY_NCS | 1.10× | NPD Cost Survey |
| ALBERTA_ONSHORE | 1.15× | AER Field Cost Surveys |
| US_ONSHORE | 1.20× | Multi-state composite |
| GLOBAL_INFERENCE | 1.30× | Industry consensus |

**Audit trail:** All estimates carry `bias_correction_applied=True` and `bias_correction_factor` in output. Factors versioned in `aro_model_versions` table. New calibration data (e.g., design partner actuals) creates a new version row with `is_current=TRUE`.

### Update process
When new calibration data arrives:
1. Compute new factor from actual vs. estimated comparison
2. Insert new row in `aro_model_versions` with updated `bias_corrections` JSONB
3. Set `is_current=TRUE`, old row becomes historical record
4. Re-run cost model to update `aro_cost_estimates` with new version

---

## PRO-214: Probabilistic Cost Estimator (P10/P50/P90)

### Base estimate
Two paths:
1. **Comparable DB path** (`use_comparable_db=True`): empirical P50 from comparable set, blended with parametric. Blend weight = `min(0.9, 0.5 + n/100)`.
2. **Parametric path**: Regional cost model from `REGION_CONFIG` (base rates per well class, depth multiplier, age factor).

### Weather multiplier (ERA5)
For offshore wells only. Reads `basin_weather_windows` table (60 rows, 5 basins: GOM, NORTH_SEA, BARENTS_SEA, NW_SHELF_AU, SE_ASIA).

**Method:**
```
extra_days_P90 = (workable_days_P10 - workable_days_P90) / 365 × project_duration_days
weather_uplift = extra_days × HLV_day_rate
weather_multiplier = (base_P90 + weather_uplift) / base_P90
Capped at 1.60×
```

**HLV day rate:** Default $400,000/day. User-adjustable (`hlv_day_rate_usd` parameter). Range: $300k–$600k.

### Scrap steel credit
Applies to offshore platform decommissioning. Uses LME HMS #1 steel price from `commodity_prices` table.

**Formula:**
```
estimated_tonnage = 100 + water_depth_ft × 0.3  (tonnes, rough)
scrap_credit_usd = estimated_tonnage × HMS1_price_per_tonne × 0.60
                                     (×0.60 = net after cutting, removal, transport costs)
```

Applied as a deduction to P10 (optimistic scenario captures full credit) and 50% deduction to P50.

### Regression model
Trained on `comparable_costs` (37,638 BOEM records) using basin and cost_year as features. Output: predicted base cost per basin, used as parametric anchor when n_comparable < 10. Stored in `cost_model/regression_model.pkl`.

### Validation target
Within ±30% of known BOEM actuals (Sprint 4 target). Current model validated against 186-operator variance engine run; CNRL +49%, Cenovus +42% vs EDGAR provisions.

---

## PRO-252: Bayesian Injection Engine

### Concept
The model maintains a prior distribution (P10–P90) per cost bucket (region × well_class × depth_band × vintage_band). Users inject proprietary intelligence; the model updates the posterior. **The delta between prior and posterior is the product.**

### Bayesian update method
Conjugate lognormal update:
```
posterior_mu = (prior_mu / prior_sigma² + obs_mu / obs_sigma²) / (1/prior_sigma² + 1/obs_sigma²)
posterior_sigma = sqrt(1 / (1/prior_sigma² + 1/obs_sigma²))
```

### Monte Carlo (10,000 simulations)
Posterior lognormal parameters feed a numpy Monte Carlo draw. 10,000 samples → P5/P10/P25/P50/P75/P90/P95 quantiles. Pre-computed base run; injection applies delta multipliers for fast re-render.

### Valid injection types
`ACTUAL_COST | COST_ESTIMATE | COST_RANGE | COMPLEXITY_FLAG | REGULATORY_CHANGE | CAMPAIGN_EFFECT`

### Valid source types
`ACTUAL | INTERNAL_ESTIMATE | REGULATORY_FILING | INDUSTRY_SURVEY | INFERENCE | OTHER`

### Audit trail
Every injection recorded in `aro_injections`. Posterior written to `aro_model_priors` with `last_injection_id` FK. Full chain: prior → injection → posterior → delta narrative.

### Graph layer (Phase 2)
Operator → Wells → Basin → Regulatory regime → Cost curve graph using networkx. Planned for Sprint 5.

---

## Defensibility

This model is designed to be defensible to an auditor:
1. All source data from public regulatory filings (BOEM, BSEE, NSTA, NPD, AER)
2. Bias corrections sourced from published government studies (BSEE TAP 738AA)
3. Full audit trail on every estimate (`model_version`, `comparable_count`, `bias_correction_factor`)
4. Comparable set is reproducible — same inputs always produce same outputs
5. Injections are cryptographically linked (UUID chain from injection → posterior)
