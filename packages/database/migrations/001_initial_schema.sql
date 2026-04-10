-- Varro — Initial Database Schema
-- Migration: 001_initial_schema.sql
-- Supabase / PostgreSQL 15

-- ============================================================
-- ENUMS
-- ============================================================

CREATE TYPE well_type AS ENUM (
  'OIL', 'GAS', 'DRY', 'INJECTION', 'DISPOSAL', 'OBSERVATION', 'OTHER'
);

CREATE TYPE well_status AS ENUM (
  'ACTIVE', 'IDLE', 'TEMP_ABANDONED', 'PERMANENTLY_ABANDONED',
  'PLUGGED', 'PLUGGED_ABANDONED', 'UNKNOWN'
);

CREATE TYPE basin AS ENUM ('GOM', 'ONSHORE', 'AK', 'PACIFIC');

CREATE TYPE data_source AS ENUM (
  'BSEE_API', 'BOEM_LEASES', 'BOEM_PA_COSTS', 'IOGCC', 'EPA_ECHO'
);

CREATE TYPE ingestion_status AS ENUM ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED');

-- ============================================================
-- OPERATORS
-- ============================================================

CREATE TABLE operators (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  canonical_name TEXT NOT NULL,
  aliases TEXT[] DEFAULT '{}',
  boem_code TEXT,
  bsee_code TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(canonical_name)
);

CREATE INDEX idx_operators_canonical_name ON operators(canonical_name);
CREATE INDEX idx_operators_boem_code ON operators(boem_code) WHERE boem_code IS NOT NULL;

-- ============================================================
-- WELLS
-- ============================================================

CREATE TABLE wells (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  api_number CHAR(14) NOT NULL,  -- 14-digit API number (canonical key)
  operator_id UUID REFERENCES operators(id),
  operator_raw TEXT,             -- raw operator name as ingested
  well_name TEXT,
  basin basin,
  state CHAR(2),
  county TEXT,
  latitude DOUBLE PRECISION,
  longitude DOUBLE PRECISION,
  well_type well_type,
  well_status well_status DEFAULT 'UNKNOWN',
  spud_date DATE,
  total_depth_ft INTEGER,
  water_depth_ft INTEGER,       -- offshore only
  is_delinquent BOOLEAN NOT NULL DEFAULT FALSE,
  delinquency_date DATE,
  -- source tracking
  bsee_last_seen TIMESTAMPTZ,
  boem_last_seen TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(api_number)
);

CREATE INDEX idx_wells_api_number ON wells(api_number);
CREATE INDEX idx_wells_operator_id ON wells(operator_id);
CREATE INDEX idx_wells_is_delinquent ON wells(is_delinquent) WHERE is_delinquent = TRUE;
CREATE INDEX idx_wells_basin ON wells(basin);
CREATE INDEX idx_wells_state ON wells(state);
CREATE INDEX idx_wells_well_status ON wells(well_status);

-- Spatial index for map queries (requires PostGIS — enable in Supabase dashboard)
-- CREATE INDEX idx_wells_location ON wells USING GIST(ST_MakePoint(longitude, latitude));

-- ============================================================
-- ARO ESTIMATES
-- ============================================================

CREATE TABLE aro_estimates (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  well_id UUID NOT NULL REFERENCES wells(id) ON DELETE CASCADE,
  estimate_p50_usd NUMERIC(14, 2) NOT NULL,
  estimate_p90_usd NUMERIC(14, 2) NOT NULL,
  model_version TEXT NOT NULL,
  comparable_count INTEGER NOT NULL DEFAULT 0,
  methodology_notes TEXT,
  citation_urls TEXT[] DEFAULT '{}',
  -- inputs snapshot (for audit trail)
  input_well_type well_type,
  input_depth_ft INTEGER,
  input_water_depth_ft INTEGER,
  input_basin basin,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_aro_estimates_well_id ON aro_estimates(well_id);
CREATE INDEX idx_aro_estimates_model_version ON aro_estimates(model_version);
-- Latest estimate per well
CREATE INDEX idx_aro_estimates_well_created ON aro_estimates(well_id, created_at DESC);

-- ============================================================
-- BOEM P&A COST ACTUALS (historical plugging cost records)
-- ============================================================

CREATE TABLE boem_pa_cost_actuals (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  well_id UUID REFERENCES wells(id),
  api_number CHAR(14),
  operator TEXT,
  total_cost_usd NUMERIC(14, 2) NOT NULL,
  plugging_cost_usd NUMERIC(14, 2),
  abandonment_cost_usd NUMERIC(14, 2),
  well_type well_type,
  depth_ft INTEGER,
  water_depth_ft INTEGER,
  basin basin,
  completion_year INTEGER,
  data_source_file TEXT,
  raw_data JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_boem_pa_actuals_well_id ON boem_pa_cost_actuals(well_id) WHERE well_id IS NOT NULL;
CREATE INDEX idx_boem_pa_actuals_basin ON boem_pa_cost_actuals(basin);
CREATE INDEX idx_boem_pa_actuals_depth ON boem_pa_cost_actuals(depth_ft);
CREATE INDEX idx_boem_pa_actuals_year ON boem_pa_cost_actuals(completion_year);

-- ============================================================
-- INGESTION RUNS (audit log)
-- ============================================================

CREATE TABLE ingestion_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source data_source NOT NULL,
  status ingestion_status NOT NULL DEFAULT 'PENDING',
  records_processed INTEGER NOT NULL DEFAULT 0,
  records_failed INTEGER NOT NULL DEFAULT 0,
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ,
  error_message TEXT,
  metadata JSONB DEFAULT '{}'
);

CREATE INDEX idx_ingestion_runs_source ON ingestion_runs(source);
CREATE INDEX idx_ingestion_runs_status ON ingestion_runs(status);
CREATE INDEX idx_ingestion_runs_started_at ON ingestion_runs(started_at DESC);

-- ============================================================
-- USERS (Supabase Auth extension)
-- ============================================================

CREATE TABLE user_profiles (
  id UUID PRIMARY KEY REFERENCES auth.users(id) ON DELETE CASCADE,
  email TEXT NOT NULL,
  full_name TEXT,
  company TEXT,
  role TEXT,                    -- 'admin' | 'operator' | 'viewer'
  plan TEXT DEFAULT 'trial',    -- 'trial' | 'design_partner' | 'paid'
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- ROW-LEVEL SECURITY
-- ============================================================

ALTER TABLE user_profiles ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Users can view own profile" ON user_profiles
  FOR SELECT USING (auth.uid() = id);
CREATE POLICY "Users can update own profile" ON user_profiles
  FOR UPDATE USING (auth.uid() = id);

-- Wells and estimates: read-only for authenticated users (all plans)
ALTER TABLE wells ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Authenticated users can read wells" ON wells
  FOR SELECT TO authenticated USING (true);

ALTER TABLE aro_estimates ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Authenticated users can read ARO estimates" ON aro_estimates
  FOR SELECT TO authenticated USING (true);

-- ============================================================
-- UPDATED_AT TRIGGERS
-- ============================================================

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_operators_updated_at
  BEFORE UPDATE ON operators
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_wells_updated_at
  BEFORE UPDATE ON wells
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_user_profiles_updated_at
  BEFORE UPDATE ON user_profiles
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
