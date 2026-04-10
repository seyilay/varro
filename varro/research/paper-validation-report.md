# Decommissioning / ARO DaaS Thesis Validation Report

**Prepared by:** Hunter (Research Lead)  
**Date:** 2026-03-29  
**Method:** Supabase papers database analysis + PDF retrieval + web research  
**Database:** 3,133 papers; 44 decommissioning-specific + 15 abandonment-specific papers retrieved

---

## Executive Summary

The thesis — *ARO/decommissioning liability is a growing problem requiring data solutions* — is **strongly validated** by both the academic literature in our database and supplementary industry sources. The evidence is overwhelming across four dimensions: cost scale, regulatory pressure, data/technology gaps, and market size. No significant countervailing evidence was found.

**Verdict: VALIDATED ✅**

---

## 1. Cost Data — The Liability is Real and Growing

### Global ARO Liability: $300–$362 Billion

The worldwide oil and gas decommissioning liability is estimated at **$300–$362 billion** (present value, c.2022), approximately evenly split between onshore and offshore requirements.

> *"We estimate that the liability currently stands at over $300 billion (real terms), and this will only increase as companies continue to make and develop new discoveries. Key offshore producers Brazil, the US Gulf of Mexico (US GoM), the UK, and Norway face the largest bill at almost $150 billion combined."*  
> — Welligence Energy Analytics (OE Digital, 2024)

> *"We extrapolate the group statistics and estimate the present value of worldwide oil and gas decommissioning liability between $311 and $362 billion c.2022, about evenly split between onshore and offshore requirements."*  
> — Kaiser, M.J., Louisiana State University, *Offshore Magazine*, Jul–Aug 2023

### US Gulf of Mexico: $40 Billion Offshore Alone

- US GoM accounts for ~US$40B of the global bill; ~80% attributable to deepwater projects
- Average well P&A cost: **$28M/well** (per BOEM reporting); deepwater average = **$17.2M/well**
- 838 wells in deepwater US GoM with probabilistic cost data as of c.2022
- Cost to P&A all 14,000 unplugged, non-producing GOM wells: **$30 billion** (UC Davis/LSU, *Nature Energy*, 2023)

### UK North Sea: £24.6 Billion Over 10 Years (2024–2033)

The OEUK Decommissioning Report 2024 provides the most granular public data available:

| Activity | Total Expenditure (£bn) |
|---|---|
| Well decommissioning | £11.727 |
| Facilities & pipeline cleaning | £2.477 |
| Topsides removals | £1.463 |
| Substructure removal | £1.480 |
| Subsea infrastructure | £2.928 |
| Post-decommissioning monitoring | £0.112 |
| **Total UKCS 2024–2033** | **£24.592** |

Key findings:
- Actual 2023 spend: **£1.7 billion** (+6% YoY, despite doing *less* work)
- Forecast 2024 spend: **£2.3 billion** (+32%)
- Average annual spend 2024–2033: **£2.4 billion/year**
- Decommissioning was 12% of total O&G expenditure in 2023; forecast to reach **33% by 2030** — exceeding capital investment
- 10-year forecast is **19% higher** than estimated just one year prior — costs consistently surprised to the upside

Average well decommissioning costs (UKCS, rising trend):

| Platform type | 2021 | 2022 | 2023 | 2024 (forecast) |
|---|---|---|---|---|
| Platform wells (£M) | 2.70 | 2.56 | 2.98 | 3.47 |
| Subsea wells (£M) | 7.81 | 7.89 | 7.92 | 8.57 |
| Exploration/appraisal (£M) | 4.36 | 4.42 | 5.33 | 7.04 |

> *"The predicted upsurge in decommissioning activity has landed... the UK has spent more money doing less work in 2023. Cost inflation, political risk and competition for resources have all made it harder to do business."*  
> — OEUK Decommissioning Report 2024

**Papers from our database directly addressing cost:**
- OTC-27672-MS: "Baseline for Planning and Cost Estimation of Brazilian Decommissioning" (2017)
- OTC-32601-MS: "Cost Reduction Challenges in Subsea Decommissioning Operations" (2023)
- OTC-27646-MS: "Transforming Decommissioning Planning" (2017)
- OTC-32466-MS: "Update on the Challenges to Offshore Facility Decommissioning on the California Coast" (2023)
- OTC-28844-MS: "The Challenges Facing the Industry in Offshore Facility Decommissioning on the California Coast" (2018)

---

## 2. Regulatory Drivers — The Pressure Is Intensifying

### US Regulatory Escalation

The US government has significantly escalated its enforcement posture:

**BOEM Final Rule (April 2024)** — Most significant update in 20 years:
- Requires **$6.9 billion in new supplemental financial assurance** from OCS operators
- Updates 20-year-old regulations that the GAO found inadequate
- Codifies federal government's process for estimating decommissioning costs
- Secretary Haaland: *"For far too long, the federal government has failed to follow through on measures to ensure accountability."*

**GAO Finding (2023):** As of June 2023, the federal waters of the Gulf of Mexico contained **~2,700 wells and 500 platforms overdue for decommissioning** (delinquent).

**Ocean Conservancy (Sept 2024):** Number of delinquent offshore wells could **double by 2030** if no action taken. Hurricane Beryl (2024) caused an oil spill from an abandoned, improperly decommissioned platform — real-world consequence.

**BOEM Proposed Amendments (March 2026):** Further modifications to the 2024 rule, indicating ongoing regulatory evolution and uncertainty — which itself drives demand for better compliance data tools.

**US BLM (2024):** Increased bonding amounts for first time in 50+ years; ~8,500 idled wells on federal lands.

**Bankruptcy/Taxpayer Risk (documented cases):**
- Australia: E&P bankruptcy left **$215M** in decommissioning costs to taxpayers
- New Zealand: **$192M** left to taxpayers
- US GOM: Fieldwood Energy bankruptcy (2021) caused "boomerang assets" — legacy liabilities reverting to predecessors

> *"Based on the size and magnitude of recent bankruptcies in the Gulf of Mexico and the number of assets being returned to predecessors in title, I sense that the North American market is greatly understated."*  
> — Ryan Lamothe, Director of Decommissioning, Hess (OTC 2023 Keynote, per JPT)

**Papers from our database on regulation:**
- OTC-30805-MS: "Risk-Based Offshore Decommissioning Standards and Regulations" (2020)
- OTC-27948-MS: "Well Abandonment and Decommissioning Challenges – Outline of the U.S. Federal Offshore Legal Framework" (2017)
- OTC-36005-MS: "Regulatory Aspects of Offshore Decommissioning: Bijupirá and Salema Project Case in Brazil" (2025)
- OTC-30824-MS: "A Comparison of Stakeholder Engagement Strategies for Offshore Decommissioning Projects" (2020)

---

## 3. Technology & Data Gaps — The Market Need for Solutions

### The Data Problem is Explicitly Named in the Literature

The MODS whitepaper "From Sinkhole to Sustainability: The Data-Driven Future of Decommissioning" (2025) — directly aligned with our thesis — identifies the core problem:

> *"Assets slated for decommissioning are inherently complicated because they are old. Many North Sea platforms were installed decades ago, long before current decommissioning regulations were conceived."*

> *"Poorly organized data, including information that isn't easily accessible or standardized, impacts coordination, leading to project delays and unexpected cost overruns. Without reliable data, it becomes harder to plan safe decommissioning workflows and to monitor the status and weights of infrastructure."*

> *"Operational data [is] often fragmented, siloed, or poorly structured. Legacy systems, inconsistent record-keeping, and limited accessibility hinder the ability to make informed decisions."*

**The Boomerang Asset Data Problem (confirmed by Hess at OTC 2023):**

> *"We were stepping back into these projects with little to no knowledge of the state of the facilities or the condition of the wellbores. We've often found that the wells in the facilities were left in an unacceptable and noncompliant state. So that has resulted in significant spending upfront."*  
> — Ryan Lamothe, Hess (JPT, 2023)

Hess's response: Built a **decommissioning review into all new well designs** — effectively creating an ARO-data-at-inception approach.

### Cost Estimation Accuracy is a Known Problem

BSEE (the US regulator) itself acknowledges the limitations of its cost estimation methods:

> *"Statistical methods are based on cost data provided by operators and apply statistical and empirical techniques... If data sets are too small or diverse, or if asset attribute data is unreliable, the results of statistical methods will be invalid."*  
> — ICF/BSEE: Decommissioning Methodology and Cost Evaluation

BSEE uses TWO different methods (statistical vs. work decomposition) precisely because no single approach is reliable without adequate data.

From 2016–2021, only **500 wells were permanently abandoned** in deepwater GOM — a pace far too slow relative to the backlog. The regulatory requirement to submit cost data within 120 days (NTL No. 2016-N03) was itself triggered by Deepwater Horizon scrutiny, showing how reactive (vs. proactive) the data collection regime has been.

### Digital Solutions Are Emerging But Fragmented

SPE-199218-MS (2019): "Digitally Transforming Front End Decommissioning Planning" (Ajero Pty Ltd) — directly addresses digital transformation as the solution.

OTC-31903-MS (2022): "Applied Methodology as a Model Based Digital Solution for Analysis of Brownfield Decommissioning Alternatives" — our database has this exact paper; confirms the gap is recognized and solutions are early-stage.

OTC-32435-MS (2023): "Improving the Efficiency and Economics of Offshore Well Abandonment with Remote Well Monitoring Solutions" — data-driven operational efficiency.

The UK's Oil and Gas Technology Centre (OGTC) explicitly funded calls for ideas on:
- Machine learning to organize and assess well data
- Digital analytics to extend field life and support decommissioning planning

> *"Decommissioning: A Digital Opportunity"* — MODS LinkedIn, 2023

**Papers from our database on digital/data solutions:**
- OTC-31903-MS: "Applied Methodology as a Model Based Digital Solution for Analysis of Brownfield Decommissioning Alternatives" (2022)
- OTC-27646-MS: "Transforming Decommissioning Planning" (2017)
- OTC-30539-MS: "Design and Analysis of Stakeholder Oriented Critical Paths for Offshore Decommissioning Projects" (2020)
- OTC-32435-MS: "Improving the Efficiency and Economics of Offshore Well Abandonment with Remote Well Monitoring Solutions" (2023)
- SPE-199218-MS: "Digitally Transforming Front End Decommissioning Planning" (2019)

---

## 4. Market Size — The Opportunity

### Active Market: $7.8–10 Billion/Year Today, $11.2 Billion by 2030

| Source | 2024 Market Value | 2030 Projection | CAGR |
|---|---|---|---|
| Strategic Market Research | $7.8 billion | $11.2 billion | 6.1% |
| Polaris Market Research | $5.25B (2021 base) | ~$9.9 billion | 7.6% |
| IHS Markit (via JPT) | — | ~$100B cumulative (2021–2030) | — |

These are *services market* figures. The **liability stock** ($300–$362B global) is the underlying obligation that drives demand.

### The Largest Markets

1. **UK North Sea** (33% of global spend) — most mature regulatory framework
2. **Asia Pacific** (23% of global spend)
3. **North America/GOM** (17% of global spend — likely understated per Hess)

The North Sea alone: **£24.6 billion** in decommissioning spend forecast 2024–2033 with 2,500+ wells needing decommissioning.

### The Unfunded Gap — Insurance/Surety Opportunity

BOEM identified **$6.9 billion in NEW supplemental financial assurance** required from OCS operators under the 2024 rule. This is the *gap* between what operators have bonded and what BOEM now requires. The DaaS angle: accurate ARO data is the foundation for setting bond requirements, negotiating terms, and pricing risk.

---

## 5. Key Papers from Our Database — Top 20 by Relevance

| # | Year | Conference | Title | DOI | Relevance to Thesis |
|---|---|---|---|---|---|
| 1 | 2023 | 23OTC | Cost Reduction Challenges in Subsea Decommissioning Operations | 10.4043/32601-ms | Direct cost challenge framing |
| 2 | 2022 | 22OTC | Applied Methodology as a Model Based Digital Solution for Analysis of Brownfield Decommissioning Alternatives | 10.4043/31903-ms | Digital/data solution approach |
| 3 | 2023 | 23OTC | Update on the Challenges to Offshore Facility Decommissioning on the California Coast | 10.4043/32466-ms | Regulatory/operational challenges |
| 4 | 2025 | 25OTC | Regulatory Aspects of Offshore Decommissioning: Bijupirá and Salema Project Case in Brazil | 10.4043/36005-ms | Regulatory drivers (Brazil) |
| 5 | 2024 | 24OTC | Decommissioning of Offshore Oil and Gas Installations in Campos Basin - Brazil: State of the Art | 10.4043/35397-ms | Current state/complexity |
| 6 | 2020 | 20OTC | Risk-Based Offshore Decommissioning Standards and Regulations | 10.4043/30805-ms | Regulatory framework gaps |
| 7 | 2017 | 17OTC | Baseline for Planning and Cost Estimation of Brazilian Decommissioning | 10.4043/27672-ms | Cost estimation methodology |
| 8 | 2017 | 17OTC | Well Abandonment and Decommissioning Challenges – Outline of the U.S. Federal Offshore Legal Framework | 10.4043/27948-ms | US legal/regulatory landscape |
| 9 | 2017 | 17OTC | Transforming Decommissioning Planning | 10.4043/27646-ms | Planning transformation (data) |
| 10 | 2023 | 23OTC | Improving the Efficiency and Economics of Offshore Well Abandonment with Remote Well Monitoring Solutions | 10.4043/32435-ms | Technology/data solutions |
| 11 | 2025 | 25OTC | EPRD: A New Frontier in Subsea Decommissioning in Brazil | 10.4043/35796-ms | Emerging regulatory requirements |
| 12 | 2025 | 25OTC | Repurposing Offshore Infrastructure for Clean Energy (ROICE) vs. Decommissioning – Commercial Considerations | 10.4043/35788-ms | Market alternatives/complexity |
| 13 | 2024 | 24OTC | Challenges and Approaches to Solutions for Green Decommissioning and Recycling of Offshore Facilities | 10.4043/35393-ms | Evolving regulatory expectations |
| 14 | 2025 | 25OTC | Impact of Planning and Experience in Well Abandonment - Lessons Learned from Complex Operations | 10.4043/35520-ms | Planning data importance |
| 15 | 2020 | 20OTC | Design and Analysis of Stakeholder Oriented Critical Paths for Offshore Decommissioning Projects | 10.4043/30539-ms | Project complexity/data needs |
| 16 | 2018 | 18OTC | The Challenges Facing the Industry in Offshore Facility Decommissioning on the California Coast | 10.4043/28844-ms | Aging infrastructure/cost data |
| 17 | 2025 | 25OTC | Amended Reefing Legislation is Needed to Spur Decommissioning of California Offshore Platforms | 10.4043/35596-ms | Regulatory stagnation |
| 18 | 2024 | 24OTC | Improving Safety, Risk Mitigation, and Efficiency for Installation, Repair, Maintenance, and Decommissioning of Subsea Equipment | 10.4043/35251-ms | Operational data gap |
| 19 | 2016 | 16OTC | Decommissioning Process Optimization Methodology | 10.4043/26867-ms | Methodology/data foundation |
| 20 | 2025 | 25ATCE | Decommissioning a Gas Processing Offshore Hub Pipeline and Installing a Subsea Bypass | 10.2118/228238-ms | Technical complexity driving data needs |

---

## 6. Thesis Validation Summary

| Thesis Component | Evidence | Strength |
|---|---|---|
| ARO liability is large and growing | $300–362B global; UK +19% YoY forecast; GOM $40B | ✅ STRONG |
| Liability is underfunded / at taxpayer risk | $6.9B BOEM gap; bankruptcies in AU/NZ/US; 2,700 delinquent assets | ✅ STRONG |
| Regulatory pressure is intensifying | BOEM 2024 Rule; BSEE reporting requirements; EU/Brazil frameworks | ✅ STRONG |
| Data/information is inadequate | Legacy systems, siloed data, boomerang asset data voids explicitly documented | ✅ STRONG |
| Technology gap exists | BSEE cost estimation challenges; digital tools early-stage; market fragmented | ✅ STRONG |
| Market size justifies a solution | $7.8–11.2B/yr services market; £24.6B UK pipeline alone | ✅ STRONG |
| Data-driven approach is the recognized solution | MODS, OGTC, SPE, OTC papers converge on data management as key lever | ✅ STRONG |

### Counterarguments to Monitor

1. **BOEM Rule Rollback (March 2026):** The Trump administration has proposed amendments to loosen the 2024 financial assurance requirements — this could reduce near-term regulatory pressure in the US market. However, (a) the liability itself doesn't disappear, (b) it increases rather than eliminates the data need for operators to demonstrate financial viability, and (c) global markets (UK, Brazil, Norway) continue to tighten.

2. **Repurposing vs. Decommissioning:** Growing literature on ROICE (Repurposing Offshore Infrastructure for Clean Energy) — could defer some decommissioning. However, this creates *more* data complexity, not less, as operators must track and manage hybrid infrastructure states.

3. **Cost Reductions in Technology:** Some papers suggest decommissioning costs could fall with better technology. This is actually *pro-thesis* — it requires data-driven decision support to capture those savings.

---

## 7. Key Citations

1. Kaiser, M.J. (2023). "New cost metrics can be used to help predict decommissioning liabilities." *Offshore Magazine*, Jul–Aug 2023. Worldwide ARO estimated $311–362B.

2. Welligence Energy Analytics / Burr, A. (2024). "The Mounting Offshore Oil & Gas Decommissioning Cost." *OE Digital*. Global liability >$300B; US GOM $40B; $28M/well average.

3. Offshore Energies UK (OEUK). *Decommissioning Report 2024*. UK 10-year forecast £24.592B; £2.4B/year average; costs 19% above prior year forecast.

4. US Department of Interior / BOEM (April 15, 2024). "Interior Department Takes Action to Protect Taxpayers from Offshore Oil and Gas Decommissioning Costs." Final rule requiring $6.9B in new supplemental financial assurance.

5. Ocean Conservancy (September 20, 2024). "Number of Delinquent Offshore Oil Wells Could Double by 2030." 2,700 wells and 500 platforms delinquent in US GOM as of June 2023 (per GAO).

6. MODS / De Vellis, L. (March 21, 2025). "From Sinkhole to Sustainability: The Data-Driven Future of Decommissioning." *illuminem*. Data fragmentation as core decommissioning problem.

7. Presley, J. (June 2023). "Offshore Decommissioning: Planning for the End at the Beginning." *Journal of Petroleum Technology*. $100B global spend 2021–2030; boomerang asset data voids; legacy information lacking.

8. Kaiser, M.J. (Nov–Dec 2023). "Review of analytical models can help operators properly determine well P&A costs." *Offshore Magazine*. BSEE data accuracy challenges; 500 deepwater wells 2016–2021; $17.2M average P&A cost.

9. ICF/BSEE. "Decommissioning Methodology and Cost Evaluation" (TAP 738AA). Statistical methods unreliable without quality data; dual-method approach required.

10. BOEM Jones Walker / Dicharry, S. (March 9, 2026). "BOEM Proposes Amendments to Its 2024 Offshore Financial Assurance Rule." Regulatory uncertainty continuing into 2026.

11. Strategic Market Research (2024). "Offshore Decommissioning Market Report 2024–2030." Market $7.8B (2024) → $11.2B (2030), CAGR 6.1%.

12. Smith, J.B. et al. (2018). "California Offshore Oil and Gas Decommissioning Outlook and Challenges." OTC-28844-MS. 23 federal OCS platforms; age 28–50 years; 5 in early decommissioning.

---

## 8. Recommended Next Steps

1. **Deeper dive on OTC-31903-MS** (Digital Solution for Brownfield Decommissioning Alternatives, 2022) — this paper most directly describes the product category we're building. Need full PDF.

2. **BOEM Decommissioning Cost Estimates database** (data.boem.gov) — publicly accessible data source that should be integrated into the DaaS product.

3. **UK NSTA Cessation of Production data** — NSTA requires operators to submit decommissioning programmes 6 years in advance of COP. This is a structured regulatory data requirement that our platform could serve.

4. **Brazil Campos Basin** — Multiple 2024–2025 OTC papers (35397, 36005, 35796) indicate Brazil is entering a major decommissioning wave. EPRD (Special Regulation for Decommissioning) represents regulatory escalation similar to BOEM 2024.

5. **Carbon Tracker ARO report** — Deep dive on investor disclosure angle; ARO transparency is becoming an ESG requirement per Climate Action 100+.

---

*Report prepared by Hunter (Research Lead) — Varro Project, March 2026*  
*Sources: Supabase papers database (3,133 papers), Exa neural search, OEUK 2024 report, BOEM/BSEE public data, OE Digital, JPT/SPE, Offshore Magazine, Ocean Conservancy, DOI press releases*
