-- PRO-252: ARO Intelligence Layer — Living Bayesian Model
-- Run in Supabase SQL editor: https://supabase.com/dashboard/project/temtptsfiksixxhbigkg/sql
-- Three tables: aro_injections, aro_model_priors, aro_model_runs

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. aro_injections
--    Stores every piece of intelligence a user injects into the model.
--    An injection can target a specific well, a region bucket, or an operator.
--    Every injection is immutable once written (audit trail).
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE aro_injections (
  id                  UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  created_at          TIMESTAMPTZ DEFAULT NOW(),

  -- Who injected this
  injected_by         TEXT NOT NULL,             -- user email / org key
  org_id              TEXT,                      -- future: multi-tenant org

  -- What scope does this injection apply to?
  scope               TEXT NOT NULL CHECK (scope IN (
                        'WELL',        -- single well actual cost
                        'REGION',      -- regional cost intelligence
                        'OPERATOR',    -- operator-level cost intelligence
                        'ASSET_TYPE',  -- e.g. "our FPSOs avg $800M"
                        'GLOBAL'       -- override all model defaults
                      )),

  -- Scope selectors (at least one required depending on scope)
  well_id             UUID REFERENCES wells(id),
  cost_region         TEXT,          -- e.g. "CANADA_AB_CONVENTIONAL"
  operator_id         UUID REFERENCES operators(id),
  asset_type          TEXT,          -- e.g. "FPSO", "PLATFORM"

  -- The intelligence itself
  injection_type      TEXT NOT NULL CHECK (injection_type IN (
                        'ACTUAL_COST',        -- real P&A/decom cost incurred
                        'COST_ESTIMATE',      -- operator's own internal estimate
                        'COST_RANGE',         -- p10/p90 range from operator
                        'COMPLEXITY_FLAG',    -- this well/region is harder/easier
                        'REGULATORY_CHANGE',  -- new regulation changes cost basis
                        'CAMPAIGN_EFFECT'     -- batch decom reduces cost by X%
                      )),

  -- Cost values (USD)
  p10_usd             NUMERIC,
  p25_usd             NUMERIC,
  p50_usd             NUMERIC,
  p75_usd             NUMERIC,
  p90_usd             NUMERIC,

  -- Metadata about the injection
  confidence          TEXT CHECK (confidence IN ('HIGH','MEDIUM','LOW')),
  source_type         TEXT CHECK (source_type IN (
                        'ACTUAL',          -- verified cost from invoice/AFE
                        'INTERNAL_ESTIMATE','REGULATORY_FILING',
                        'INDUSTRY_SURVEY','INFERENCE','OTHER'
                      )),
  reference_year      INTEGER,       -- year this cost applies to
  notes               TEXT,
  source_document     TEXT,          -- URL or doc name
  is_public           BOOLEAN DEFAULT FALSE,  -- can be shared across org

  -- Bayesian weight (how much this injection moves the posterior)
  -- 1.0 = full belief, 0.1 = weak signal, 2.0 = high-confidence override
  weight              NUMERIC DEFAULT 1.0 CHECK (weight > 0 AND weight <= 5.0)
);

CREATE INDEX idx_aro_inj_well ON aro_injections(well_id) WHERE well_id IS NOT NULL;
CREATE INDEX idx_aro_inj_region ON aro_injections(cost_region) WHERE cost_region IS NOT NULL;
CREATE INDEX idx_aro_inj_operator ON aro_injections(operator_id) WHERE operator_id IS NOT NULL;
CREATE INDEX idx_aro_inj_org ON aro_injections(org_id);
CREATE INDEX idx_aro_inj_scope ON aro_injections(scope, injection_type);


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. aro_model_priors
--    The current Bayesian posterior for each region × class × depth × vintage
--    bucket. This is the "living" state of the model — updated whenever new
--    injections arrive. The prior starts as our parametric model (v1.1.0).
--    Each update is a new row (append-only), so we keep full history.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE aro_model_priors (
  id                  UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  created_at          TIMESTAMPTZ DEFAULT NOW(),

  -- Bucket keys (same as estimator.py buckets)
  cost_region         TEXT NOT NULL,
  well_class          TEXT NOT NULL CHECK (well_class IN ('ONSHORE','OFFSHORE','PLATFORM','SUBSEA')),
  depth_bucket        TEXT NOT NULL CHECK (depth_bucket IN ('<3000','3000-8000','8000-15000','15000+')),
  vintage_bucket      TEXT NOT NULL CHECK (vintage_bucket IN ('<1970','1970-1985','1985-2000','2000-2010','2010+')),

  -- The posterior distribution (USD)
  p10_usd             NUMERIC NOT NULL,
  p25_usd             NUMERIC NOT NULL,
  p50_usd             NUMERIC NOT NULL,
  p75_usd             NUMERIC NOT NULL,
  p90_usd             NUMERIC NOT NULL,

  -- Model metadata
  model_version       TEXT NOT NULL,           -- e.g. "1.1.0-bayesian"
  n_injections        INTEGER DEFAULT 0,       -- how many injections fed this
  n_actuals           INTEGER DEFAULT 0,       -- how many real actuals
  confidence          TEXT CHECK (confidence IN ('HIGH','MEDIUM','LOW')),
  bias_factor         NUMERIC DEFAULT 1.0,

  -- Injection provenance
  last_injection_id   UUID REFERENCES aro_injections(id),
  org_id              TEXT,                    -- NULL = global model, else org-specific

  -- Whether this is the current active prior for this bucket
  is_current          BOOLEAN DEFAULT TRUE
);

CREATE INDEX idx_aro_priors_bucket ON aro_model_priors(cost_region, well_class, depth_bucket, vintage_bucket);
CREATE INDEX idx_aro_priors_current ON aro_model_priors(is_current, org_id);
CREATE INDEX idx_aro_priors_org ON aro_model_priors(org_id, cost_region);


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. aro_model_runs
--    Log of every full model run (Monte Carlo simulation pass).
--    Captures the delta between model estimate and EDGAR/balance sheet ARO.
--    This delta IS the product — it's what tells operators where they're
--    over/under-reserved.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE aro_model_runs (
  id                  UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  run_at              TIMESTAMPTZ DEFAULT NOW(),
  model_version       TEXT NOT NULL,
  triggered_by        TEXT,                    -- 'injection', 'scheduled', 'manual'
  injection_id        UUID REFERENCES aro_injections(id),

  -- Scope of this run
  run_scope           TEXT CHECK (run_scope IN ('FULL','OPERATOR','REGION','WELL')),
  operator_id         UUID REFERENCES operators(id),
  cost_region         TEXT,
  well_id             UUID REFERENCES wells(id),

  -- Aggregate outputs
  wells_estimated     INTEGER,
  total_p50_usd       NUMERIC,                 -- sum of all well P50s in scope
  total_p10_usd       NUMERIC,
  total_p90_usd       NUMERIC,

  -- vs EDGAR/balance sheet
  edgar_aro_usd       NUMERIC,                 -- from aro_provisions_ifrs
  variance_usd        NUMERIC,                 -- edgar_aro - model_p50
  variance_pct        NUMERIC,                 -- variance / edgar_aro * 100

  -- Monte Carlo parameters
  mc_iterations       INTEGER DEFAULT 10000,
  mc_seed             INTEGER,

  run_duration_ms     INTEGER,
  notes               TEXT
);

CREATE INDEX idx_aro_runs_operator ON aro_model_runs(operator_id) WHERE operator_id IS NOT NULL;
CREATE INDEX idx_aro_runs_run_at ON aro_model_runs(run_at DESC);
CREATE INDEX idx_aro_runs_scope ON aro_model_runs(run_scope, cost_region);
