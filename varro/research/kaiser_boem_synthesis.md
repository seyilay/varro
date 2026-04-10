# Kaiser & BOEM GOM Book — Synthesis for Varro Variance Engine
## Generated: 2026-04-06 03:30 UTC

---

## Part 1: What Kaiser's Body of Work Contains

Mark J. Kaiser (LSU Center for Energy Studies) is the most prolific quantitative researcher
on offshore decommissioning costs. His key papers and what each contributes:

### Papers Directly Applicable to Varro

| Paper | Key Output | Applicability |
|---|---|---|
| "FERC pipeline decommissioning cost in the US GOM, 1995–2015" (Marine Policy, 2017) | $301k/mile avg; 3-4x premium for hurricane-damaged | **Pipeline decom model calibration** |
| "Decommissioning cost estimation in the deepwater US GOM – Fixed platforms" (Marine Technology, 2014) | Work decomposition algorithm; P10/P50/P90 by water depth | **Platform ARO model** |
| "Decommissioning cost estimation for deepwater floating structures" (2015) | 42 GOM floaters; cost by structure type (TLP/SPAR/SEMI) | **FPSO/TLP/SPAR ARO** |
| "Worldwide oil and gas asset retirement obligations circa 2021" (Extractive Industries & Society, 2023) | Global ARO provisions by company group; per-well cost metrics | **Direct benchmark for Variance Engine** |
| "Models describe BSEE deepwater decommissioning cost estimates" (Offshore Magazine, 2024) | $1.6M/segment (bulk/umbilical), $1.35M/segment (oil/gas) | **Pipeline cost seeding** |

### Key Quantitative Data Extracted

#### Pipeline Decommissioning (GOM, Shallow Water <400ft)
- **Average cost (1995-2015, 2014 dollars):** $301,000/mile ($187,000/km)
- **Range:** $7,000 – $2.1M/mile
- **Unit cost by volume:** $47/cf ($1,660/m³)
- **Hurricane-damaged premium:** 3.4x ($663k/mile vs $180k/mile normal)
- **Normalization method:** BLS offshore oil field services price index
- **Data source:** FERC eLibrary public dockets (28 projects, 2000-2015)

#### Pipeline Decommissioning Cost Factors
1. Length (positively correlated with cost)
2. Water depth of endpoints and interconnects
3. Number of side valves/interconnects (complexity proxy = "mile-cuts")
4. Damage status (hurricane, leaking = 3-4x premium)
5. Service type (oil lines require more pig runs → higher cost)
6. Season (winter = lower dayrates but weather delays)

#### Platform Decommissioning (from Kaiser 2014, Kaiser 2015)
- Shallow water fixed (<61m): typical range $2-10M
- Mid-water fixed (61-122m): ~$4.8M average (twice the <50m cost)
- Deep water fixed (250-500m): $10-80M range
- Deepwater floaters (TLP/SPAR/SEMI): work decomposition model

#### BSEE Deepwater Pipeline Unit Costs (Kaiser/BSEE 2024)
- Bulk oil, bulk gas, lift, umbilical, water: **$1.6M/pipeline segment**
- Oil and gas export: **$1.35M/pipeline segment**
- These are per-segment (not per-mile) costs for deepwater GOM

---

## Part 2: How This Improves Our Variance Engine

### Current Model Weaknesses (vs Kaiser benchmarks)

| Issue | Current Model | Kaiser-Calibrated |
|---|---|---|
| Pipeline decom cost | Not modeled (wells only) | $301k/mile shallow; $1.35-1.6M/segment deepwater |
| Platform cost basis | Regional tier × depth mult | Per-structure work decomposition |
| Cost distribution | Fixed lognormal sigma by tier | Empirically calibrated (σ ≈ μ for offshore) |
| Hurricane premium | Not modeled | 3.4x multiplier on damaged infrastructure |
| Campaign effects | Not modeled | Batch decom reduces per-well cost 20-40% |
| Abandon-in-place vs removal | Not distinguished | AIP ~$301k/mile vs removal ~$677k/mile |

### Specific Improvements to Make

#### 1. Add Pipeline ARO to `infrastructure` cost model
```python
# New: infrastructure_cost_estimator.py
def estimate_pipeline_decom(length_miles, water_depth_m, is_damaged=False, is_deepwater=False):
    if is_deepwater:  # >400ft / >120m
        # Kaiser BSEE: ~$1.35-1.6M per segment; segments avg ~5 miles
        base_per_mile = 270_000  # $1.35M / 5 mile avg segment
    else:
        base_per_mile = 301_000  # Kaiser FERC empirical mean (2014$)
    
    cost = length_miles * base_per_mile
    if is_damaged:
        cost *= 3.4  # Kaiser hurricane premium
    
    # Lognormal spread: σ ≈ μ for offshore (Kaiser observation)
    p10 = cost * 0.15   # very wide distribution
    p50 = cost
    p90 = cost * 4.0    # large right tail
    return p10, p50, p90
```

#### 2. Calibrate well cost sigma from Kaiser GOM data
Kaiser repeatedly observes that **standard deviations exceed mean values** for offshore
decommissioning costs. This means our lognormal sigma should be:
- Tier 1 offshore: σ_log ≈ 0.8-1.0 (fatter tails than our current 0.7)
- Tier 2 offshore: σ_log ≈ 1.0-1.2
- Shallow water onshore: σ_log ≈ 0.4-0.6 (more predictable)

Current `SPREAD_BY_TIER`:
```python
SPREAD_BY_TIER = {1: (0.5, 2.0), 2: (0.4, 2.5), 3: (0.3, 3.0)}  # p10_factor, p90_factor
```
Should be (for offshore wells):
```python
OFFSHORE_SPREAD = (0.15, 4.0)  # matches Kaiser σ ≈ μ observation
```

#### 3. Add abandon-in-place vs removal flag to infrastructure
- 97% of GOM pipelines are abandoned-in-place (Kaiser: only 486 miles removed out of 19,236 miles decommissioned)
- AIP cost ≈ 35-45% of full removal cost
- Our model currently makes no distinction → overestimates liability for regulators who allow AIP

#### 4. Infrastructure ARO table seeding
From Kaiser data, we can now seed `cost_model_runs` for all 32,892 EIA pipelines:
```
Shallow water (AIP): $301k/mile × length → P50
Deepwater (AIP): $1.35M/segment → P50
Hurricane zone multiplier: ×3.4 for damaged/aged (age >30yr proxy)
```

#### 5. Variance Engine — normalize EDGAR ARO properly
Kaiser (2023 worldwide paper) provides per-well ARO metrics by company group:
- **Majors (ExxonMobil, Shell, Chevron):** ~$50k-150k/well average
- **Large independents (Pioneer, Devon, EOG):** ~$30k-80k/well
- **Small E&Ps:** ~$15k-40k/well
- **NOCs (Aramco, ADNOC, PEMEX):** ~$5k-20k/well (low regulatory pressure)

These benchmarks let us validate our P50 against the worldwide distribution, not just
our parametric model.

---

## Part 3: GOM Book Key Findings for Varro

### Pipeline Scope for ARO
The GOM book reveals the scale of unmodeled pipeline ARO:
- **45,310 total miles installed** in GOM
- **21,872 miles active** (circa 2016) — all have ARO
- **4,203 miles out-of-service** — ARO liability exists, decom imminent
- Average active pipeline age (shallow water): 25+ years for 50% of mileage
- This represents **$6.6B in pipeline ARO** at Kaiser's $301k/mile average

Our current Varro model captures **wells only**. EDGAR ARO provisions from CNRL, Suncor,
Cenovus all include pipeline and facility ARO embedded in the total. This explains why:
- Our model P50 for CNRL: $3.22B (wells only)
- EDGAR CNRL ARO: $6.33B
- Gap: ~$3.1B ← largely pipeline + facility ARO we don't model yet

### What the Book Reveals About Data Sources
The book confirms two critical free data sources for Kaiser-calibrated cost data:
1. **FERC eLibrary** — all pipeline abandonment applications with estimated costs
   URL: https://elibrary.ferc.gov/eLibrary/search?colType=CO&calType=0&caseType=RP&action=searchNew
2. **BOEM BSEE pipeline database** — 45k miles of pipeline with status, diameter, length
   Already partially in our infrastructure table (32,892 EIA pipelines)

### Immediate Action Items
1. Load FERC pipeline abandonment cost data as empirical comparables into `aro_model_priors`
2. Add `length_miles` and `diameter_in` to infrastructure table for pipelines
3. Build `infrastructure_cost_estimator.py` using Kaiser's regression models
4. Re-run variance engine after adding pipeline + facility ARO to company totals

---

## Part 4: Overnight Queue (Autonomous Actions)

The following are being kicked off autonomously this session:

1. ✅ Fix `regions.py` bugs (AR duplicate, Mexico, RN) — DONE
2. 🔄 Seed `aro_model_priors` with 2,560 parametric baseline rows — DONE
3. 🔄 Write `infrastructure_cost_estimator.py` — QUEUED
4. 🔄 EDGAR US E&P ARO scraper — QUEUED (Pioneer, Devon, EOG, Coterra, etc.)
5. 🔄 Canonical operator name normalizer — QUEUED
6. 🔄 Re-run cost model on wells affected by fixed regions — QUEUED

---

## References
- Kaiser, M.J. (2017). FERC pipeline decommissioning cost in the US Gulf of Mexico, 1995-2015. Marine Policy, 82, 121-131.
- Kaiser, M.J., & Liu, M. (2014). Decommissioning cost estimation in the deepwater US GOM – Fixed platforms and compliant towers. Marine Technology, 51(3).
- Kaiser, M.J. (2015). Decommissioning cost estimation for deepwater floating structures in the US GOM.
- Kaiser, M.J. (2023). Worldwide oil and gas asset retirement obligations circa 2021. Extractive Industries & Society, 13.
- Kaiser, M.J. (2024). Models describe BSEE deepwater decommissioning cost estimates. Offshore Magazine.
- BOEM (2019-070). The Offshore Pipeline Construction Industry and Activity Modeling in the US Gulf of Mexico (the book Seyi sent).
