# Varro Gap Implementation Checklist
_Derived from: Strategic Data Audit (Apr 5, 2026)_
_Loop: run through → check off → repeat_

---

## 🟢 SECTION 5 — Free Data Wins Queue (execute first)

### No-barrier downloads
- [x] BC wells (MapServer Layer 54) — 16,979 new ✅
- [x] Sodir Norway pipelines (MapServer Layer 311) — 82 segments ✅
- [ ] **Canada CER onshore pipelines** — ~67k km | open.canada.ca
- [ ] **Australia Geoscience ecat pipelines** — ~18k km | ecat.ga.gov.au
- [x] **EIA US underground gas storage** — 16 US fields ✅ (EIA ArcGIS layer, public)
- [x] **GIE AGSI European gas storage** — 126 EU facilities ✅ (via /api/about public endpoint)
- [ ] **Global Energy Monitor LNG terminals** — BLOCKED: download page form-gated (Cloudflare)
- [x] **NSTA UK exploration/appraisal wellbores** — 110 wells ✅ (UKCS E&A ED50 FeatureServer)
- [x] **Sodir Norway exploration wellbores** — 186 wells ✅ (MapServer Layer 204, outSR=4326)
- [x] **BOEM US lease blocks** — 201k+ ✅ (AK=156k, ATL=48k; GOM service down)
- [x] **EIA OilGasInfrastructure** — 1,646 new ✅ (refineries 131, gas plants 478, petro terminals 1,471, platforms 2,566)
- [x] **AER Alberta pipelines** — 324,431 segments ✅ (Pipelines_SHP.zip)

### Requires registration (Roop Boop credentials)
- [x] SEDAR+ — bypassed via EDGAR ✅ (Suncor, CNRL, Cenovus ARO ingested)
- [ ] **Australia NOPIMS** — onshore wells ~100k | nopims.gov.au
- [ ] **Brazil BDEP** — ~500k wells | bdep.anp.gov.br (site was down Apr 5)

### No free source — inference only
- [ ] Saudi Aramco wells (~10-12k) — inference from SAR 30B ARO ÷ est. well count
- [ ] Russia wells (~450-500k) — Rosneft IFRS ratio model
- [ ] China onshore wells (~120-150k) — CNOOC known; CNPC/Sinopec via production ratio
- [ ] Global FPSOs non-UK (~185) — Marine Traffic AIS + annual report mining
- [ ] Subsea trees globally (~10,000+) — well count × tieback ratio per basin

---

## 🟡 SECTION 2 — Value Chain Depth (execute after Section 5)

### 2A. Exploration Assets
- [ ] **Enrich wells.well_class** — tag exploration/appraisal vs development from source APIs
  - Sources: NSTA (well_class in existing data), Sodir (formationPurpose), BOEM (well_type), RRC TX (permit type)
  - ~13,346 NSTA wells | ~9,758 Sodir wells | ~7,626 BOEM wells have source metadata
  - Also: NSTA + Sodir exploration wellbore ingests (above) add new E&A records

### 2B. Drilling Assets
- [x] Decision: OUT OF SCOPE ✅ (rig owner liability — Transocean, Valaris, etc.)

### 2C. Production Wells — Coverage Gaps
- [ ] **China onshore wells** — zero; inference only (CNPC/Sinopec/CNOOC)
- [ ] **Saudi Arabia wells** — zero; inference only
- [ ] **Russia wells** — zero; inference only
- [x] US ~2.4M ✅ | Canada ~820k ✅ | UK 13k ✅ | Norway 10k ✅
- [ ] **OCC Oklahoma** — 99% linked ✅ | remaining 3 wells trivial
- [ ] **RRC Texas 19% unlinked** — 191k wells; ceiling reached without new lookup
- [ ] **Louisiana SONRIS current data** — contact DNR directly (2019 snapshot only)
- [ ] **Netherlands** — 6,797 wells in DB ✅
- [ ] **Argentina** — 11,734 wells ✅
- [ ] **Australia NOPTA offshore** — 3,571 ✅ | onshore needs NOPIMS
- [ ] **Colombia ANH** — try browser CSV export (datos.gov.co/resource/4dai-7crq)
- [ ] **Mexico CNH** — 12 public wells only; register at snih.cnh.gob.mx

### 2D. Offshore Production Assets — Critical Gaps
- [x] Fixed steel jackets — ~2,250 in DB (~40% of global ~5,500) ✅ partial
- [x] FPSOs — 191 in DB (was 15; global fleet ~200) ✅ near complete
- [ ] **SPARs** — 0 in DB; ~20-22 globally; load from BOEM GOM
- [ ] **TLPs** — 0 in DB; ~27-30 globally; load from BOEM + Structurae
- [ ] **GBS platforms** — 0 explicitly typed; Sodir/NSTA data exists, needs classification
- [ ] **FLNGs** — 0 in DB; 4-5 operating globally; GIIGNL Annual Report
- [ ] **Subsea trees/manifolds/flowlines** — 0 in DB; no free global database (Westwood paid)

### 2E. Processing Assets
- [ ] **Gas processing plants** — US EIA data exists (free); estimate ~500 plants
- [ ] **Oil terminals** — partial via OSPAR/NSTA; need global expansion

### 2F. Transport / Export
- [x] BOEM GOM pipelines — 21,238 ✅
- [x] NSTA UK pipelines — 9,025 ✅
- [x] EIA US gas pipelines — 32,892 ✅
- [x] Sodir Norway pipelines — 82 ✅
- [ ] **Canada CER pipelines** — ~67k km (in queue above)
- [ ] **Australia GA pipelines** — ~18k km (in queue above)

### 2G. Storage
- [ ] **EIA US underground gas storage** — ~390 fields (in queue above)
- [ ] **GIE AGSI European storage** — ~160 facilities (in queue above)

### 2H. LNG / Gas Export
- [ ] **GEM LNG terminals** — ~70-80 global (in queue above)

---

## 🔵 SECTION 3 — Country Breadth Gaps

### Tier 1 — Zero coverage (inference only)
- [ ] Saudi Arabia | UAE | Kuwait | Iraq | Iran (OFAC-blocked)
- [ ] Russia (Rosneft IFRS 2010-2021 only; no well data)
- [ ] China onshore (CNOOC offshore ARO known; no well data)
- [ ] Venezuela | Libya | Algeria (state-controlled, no public data)

### Tier 2 — Partial coverage
- [x] Canada ✅ | UK ✅ | Norway ✅
- [ ] Australia — offshore done ✅; onshore needs NOPIMS
- [ ] Brazil — 11.7k in DB (production files only); BDEP registration needed for full 500k
- [ ] Argentina — 11.7k ✅ (reasonable coverage for public data)
- [ ] Colombia — blocked (Socrata API); try manual export
- [ ] Mexico — 12 wells (CNH registration needed)
- [ ] Netherlands — 6,797 ✅

### Tier 3 — Accessible with work
- [ ] Nigeria — NNPC data exists (public API to check)
- [ ] Ghana — PIAC reports public
- [ ] Senegal — new producer, limited public data
- [ ] Indonesia — SKK Migas public stats; no well registry

---

## 🔴 SECTION 4 — Operator Coverage Gaps
- [ ] **Wire Canadian ARO into Variance Engine** — Suncor/CNRL/Cenovus now in DB ✅ data ready
- [ ] **Wire CNOOC/ONGC/Rosneft into Variance Engine** — already in DB ✅ data ready
- [ ] **Run Variance Engine v3** — with full operator linkage (99% OCC, 81% RRC)
- [ ] **Build inference models** — Saudi, Russia, China

---

## EXECUTION ORDER
1. ✅ Section 5 free downloads — running now
2. ✅ 2A well_class enrichment — starting after queue
3. 2D offshore asset gaps — SPAR/TLP/GBS from BOEM
4. Variance Engine v3 re-run
5. Inference models (Saudi/Russia/China)
6. PRO-252 Sprint 3b — Bayesian injection layer

_Last updated: 2026-04-05 11:35 UTC_
