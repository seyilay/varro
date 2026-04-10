# ARO Cost Model — Design Spec

## Core Principle: Regional Stratification
P25 for Shell Canada ≠ P25 for Shell Nigeria — even if same operator, same well type.
The comparable set is always filtered by: **region × well_class × depth_bucket × vintage_bucket**

## Inputs per well
```
well_type:      OIL | GAS | OIL_GAS | INJECTION | DISPOSAL
well_class:     ONSHORE | OFFSHORE | PLATFORM | SUBSEA
total_depth_ft: integer (bucketed: <3k, 3-8k, 8-15k, 15k+)
vintage_year:   integer (bucketed: <1970, 1970-1985, 1985-2000, 2000-2010, 2010+)
state/country:  region key → maps to cost_region
status:         PRODUCING | IDLE | TA | PA | SHUT_IN (affects complexity)
water_depth_ft: for offshore (bucketed: 0-300ft, 300-1000ft, 1000-5000ft, 5000ft+)
```

## Cost Regions (Tier 1 = calibrated, Tier 2 = multiplier-estimated)
| Region Key | Examples | Tier | Anchor Source |
|---|---|---|---|
| GOM_DEEPWATER | GOM >1000ft | 1 | BOEM P&A actuals |
| GOM_SHELF | GOM <1000ft | 1 | BOEM P&A actuals |
| US_ONSHORE_PERMIAN | TX Permian Basin | 1 | BOEM/RRC actuals |
| US_ONSHORE_APPALACHIAN | PA, WV, OH | 1 | State reg cost data |
| US_ONSHORE_MIDCON | OK, KS | 1 | OCC cost reports |
| US_ONSHORE_ROCKIES | WY, CO, UT | 1 | State data |
| US_ONSHORE_CALIFORNIA | CA | 1 | CalGEM + DOGGR |
| CANADA_AB_CONVENTIONAL | Alberta onshore | 1 | AER LLR actuals |
| CANADA_AB_OILSANDS | Alberta oil sands | 2 | AER + inference |
| CANADA_BC_CONVENTIONAL | BC | 2 | BC OGC estimates |
| UK_NORTH_SEA_SHALLOW | UKCS <500m | 1 | NSTA actuals |
| UK_NORTH_SEA_DEEP | UKCS >500m | 1 | NSTA actuals |
| NORWAY_NCS | NCS | 2 | Sodir + NSTA multiplier |
| AUSTRALIA_OFFSHORE | NOPTA | 2 | NOPSEMA estimates |
| NIGERIA_ONSHORE | Niger Delta onshore | 2 | Inference |
| NIGERIA_OFFSHORE | Shallow Niger Delta | 2 | Inference |
| BRAZIL_OFFSHORE | Pre-salt / BMS | 2 | ANP + inference |
| GLOBAL_INFERENCE | All others | 3 | Multiplier only |

## Cost Components (additive model)
1. **Well P&A cost** — dominant; varies by depth, type, region
2. **Infrastructure removal** — for platform/subsea wells: per-well share of platform decom
3. **Site remediation** — onshore: soil/water; offshore: seabed survey + monitoring
4. **Regulatory buffer** — jurisdiction-specific (UK adds 20%, Norway 30%, Nigeria 40% risk)

## P10/P25/P50/P75/P90 Generation
- Pull comparable wells (same region × type × depth_bucket × vintage_bucket)
- If N ≥ 30: use empirical percentiles from the distribution
- If N = 10-29: use parametric fit (lognormal) on the comparable set
- If N < 10: widen bucket one step and retry; if still < 10, use regional multiplier from Tier 1 anchor
- Return confidence_score = HIGH (empirical N≥30) | MEDIUM (parametric) | LOW (multiplier)

## Bias Correction (PRO-191)
Public P&A cost data systematically understates:
- Mobilisation costs (not always captured)
- Environmental baseline surveys
- Government/regulator fees
Published correction factors:
- BOEM actuals vs industry survey: +15% bias uplift
- AER LLR actuals vs survey: +25% (accounts for surface reclamation)
- NSTA UK: actuals tend to be close; +10%

## Schema: cost_model_runs table (new DDL needed)
```sql
CREATE TABLE cost_model_runs (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  well_id UUID REFERENCES wells(id),
  model_version TEXT NOT NULL,
  run_at TIMESTAMPTZ DEFAULT NOW(),
  cost_region TEXT,
  depth_bucket TEXT,
  vintage_bucket TEXT,
  comparable_n INTEGER,
  confidence TEXT CHECK (confidence IN ('HIGH','MEDIUM','LOW')),
  p10_usd NUMERIC,
  p25_usd NUMERIC,
  p50_usd NUMERIC,
  p75_usd NUMERIC,
  p90_usd NUMERIC,
  bias_correction_factor NUMERIC DEFAULT 1.0,
  notes TEXT
);
```
