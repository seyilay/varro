-- Varro — Development Seed Data
-- Migration: 002_seed_dev.sql
-- Run in dev/staging only

-- Seed a handful of well-known GOM operators
INSERT INTO operators (canonical_name, aliases, boem_code) VALUES
  ('Talos Energy', ARRAY['Talos Energy LLC', 'Talos Energy Inc', 'TALOS'], 'E00059'),
  ('W&T Offshore', ARRAY['W&T Offshore Inc', 'WTI', 'W & T OFFSHORE'], 'E00027'),
  ('Callon Petroleum', ARRAY['Callon Petroleum Co', 'CALLON'], 'E00082'),
  ('Stone Energy', ARRAY['Stone Energy Corporation', 'STONE ENERGY'], 'E00047'),
  ('Energy XXI', ARRAY['Energy XXI Ltd', 'Energy XXI Gulf Coast', 'EXXIP'], 'E00091')
ON CONFLICT (canonical_name) DO NOTHING;

-- Sample delinquent well records (fictional API numbers for dev testing)
INSERT INTO wells (
  api_number, operator_raw, well_name, basin, state,
  latitude, longitude, well_type, well_status,
  total_depth_ft, water_depth_ft, is_delinquent, delinquency_date
) VALUES
  ('60-035-00001-00', 'TALOS ENERGY LLC', 'Green Canyon A-1', 'GOM', 'LA',
   27.5123, -90.2341, 'OIL', 'TEMP_ABANDONED',
   18500, 4200, TRUE, '2023-06-15'),
  ('60-035-00002-00', 'W&T OFFSHORE INC', 'Ship Shoal B-3', 'GOM', 'LA',
   28.1234, -91.4567, 'OIL', 'IDLE',
   12800, 350, TRUE, '2022-11-01'),
  ('60-035-00003-00', 'STONE ENERGY CORPORATION', 'Mississippi Canyon C-2', 'GOM', 'LA',
   27.8901, -89.1234, 'GAS', 'PERMANENTLY_ABANDONED',
   22000, 6800, FALSE, NULL)
ON CONFLICT (api_number) DO NOTHING;
