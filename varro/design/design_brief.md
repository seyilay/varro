# Varro — Design Brief
## Landing Page, Brand Identity & Product Screens
### Version 1.0 | April 2026

---

## Brand Identity

**Company name:** Varro
**Category:** ARO Intelligence Platform
**Positioning:** The independent, market-calibrated intelligence layer for Asset Retirement Obligations in oil & gas.

**What Varro is:**
Varro tells you whether an oil & gas company's decommissioning provision is defensible — before the auditor, regulator, or acquirer asks. We model 4.5M+ wells across 22 jurisdictions, each with an independent P10/P50/P90 cost estimate, and compare that against what companies report on their balance sheets.

**What Varro is not:**
- Not an accounting system (ENFOS does that — stores what you give it)
- Not an ESG rating (Sustainable Fitch does that — scores governance)
- Not a compliance tool

**The gap Varro fills:** Nobody independently verifies whether an ARO provision is accurate. S&P takes it at face value. Moody's and Fitch ignore it entirely. Varro quantifies the delta.

---

## Target Customer (ICP)

**Primary:**
- M&A analysts at PE firms acquiring upstream E&P assets
- Finance teams at independent oil & gas operators (150–5,000 employees)
- Regulatory teams (state oil & gas commissions, BSEE, NSTA, AER)

**Secondary:**
- Credit analysts at banks lending to E&P companies
- ESG/sustainability teams at institutional investors
- Academic researchers on decommissioning policy

**Decision maker:** VP Finance, CFO, Head of M&A, Senior Financial Analyst
**Job to be done:** "I need to know if this company's ARO is realistic before I sign"

---

## Tone & Voice

- **Authoritative, not alarmist.** We're a data company, not an activist.
- **Precise, not jargon-heavy.** Finance professionals, not petroleum engineers.
- **Direct.** Short sentences. No filler.
- **Confident.** We have 4.5M wells. We know the cost.

Examples of the right tone:
> "Apache's balance sheet shows $2.4B in ARO. Our model, covering 83% of their well portfolio, estimates $26B."
> "Know what you owe. Before anyone else does."
> "Every well has a liability. We've modelled all of them."

---

## Visual Design Direction

**Aesthetic:** Dark, data-dense, finance-authoritative. Think Bloomberg Terminal meets modern SaaS. NOT an environmental company. NOT a compliance tool.

**Primary palette:**
- Background: Deep navy `#0A0F1E`
- Surface: Slightly lighter `#111827`
- Border/divider: `#1F2937`
- Accent: Warm amber `#F59E0B`
- Text primary: `#F9FAFB`
- Text secondary: `#9CA3AF`
- Positive signal: `#10B981` (green — correctly reserved)
- Warning signal: `#EF4444` (red — under-reserved)

**Typography:**
- Font: Inter (or Geist)
- Hero headline: 56–72px, weight 700, tight tracking
- Body: 16px, weight 400, generous line height
- Data labels: 11–13px, monospace, uppercase

**Design inspiration references:**
- Linear (dark, precise, developer-first energy but applied to finance)
- Vercel dashboard (clean dark UI, minimal chrome)
- Bloomberg (data density + authority)
- NOT: environmental dashboards, green-washed ESG tools

**Logo direction:**
- Clean wordmark "VARRO" in caps or sentence case
- Geometric mark: a simple V-shape or stacked horizontal lines suggesting geological strata / depth
- Amber accent mark with dark wordmark OR all-white wordmark on dark
- Avoid: oil rigs, flames, leaf/plant motifs

---

## Landing Page Structure

### 1. Navigation
- Logo (left)
- Links: Platform, Data, Pricing, Company
- CTA: "Request access" (amber button)

### 2. Hero Section
**Headline (choose one):**
- "The ARO your balance sheet doesn't see."
- "Every well has a liability. We've modelled all of them."
- "Independent decommissioning intelligence. At asset level."

**Subheadline:**
"Varro gives finance teams, M&A analysts, and regulators an independent, market-calibrated ARO estimate for every operator in their portfolio — built from 4.5 million wells across 22 jurisdictions."

**Primary CTA:** Request early access
**Secondary CTA:** See how it works

**Hero visual:** A dark dashboard card showing the variance signal for a real operator — e.g.:
```
Apache Corporation (APA)
EDGAR provision:    $2.4B   ↓
Varro P50 estimate: $26.4B  
Coverage:           83% of portfolio
Signal:             🔴 UNDER_RESERVED  +309% (PV-adjusted)
```

### 3. Stats Bar (social proof)
- 4.5M+ wells modelled
- 22 jurisdictions
- $300B+ in estimated ARO liability
- 27 public companies tracked

### 4. Product Sections

**Section 1 — ARO Intelligence**
"Stop guessing. Start knowing."
We model every well, platform, and pipeline in your portfolio using regional cost benchmarks calibrated to actual decommissioning data. P10/P50/P90 for every asset. Independent of what the operator reports.

**Section 2 — Variance Engine**
"Is the provision defensible?"
Varro's variance engine compares our market-calibrated P50 against EDGAR/balance sheet ARO disclosures — adjusted for present value discounting — and flags operators whose provisions sit outside normal range. High-confidence signals. No false positives.

**Section 3 — Living Bayesian Model**
"Your intelligence. Our model. Combined."
Inject proprietary data — actual well costs, regulatory penalties, campaign effects — and Varro recalculates your P5–P95 in real time. Every injection is provenance-logged. Your edge stays yours.

**Section 4 — Asset Coverage**
"22 jurisdictions. 4.5M wells. All of it."
US onshore basins. UK North Sea. Norwegian NCS. Canadian oil sands. Australian offshore. Brazil. Norway. We cover where the liability lives — and we're adding more every week.

### 5. How It Works (3 steps)
1. **Connect your portfolio** — Upload your asset list or connect via API
2. **Get your estimate** — Varro returns P10/P50/P90 for every asset within minutes
3. **See the variance** — Compare your provision against market rates. Know where you stand.

### 6. Quote / Testimonial placeholder
"[Design partner quote here]"
— [Name], [Title], [Company]

### 7. Footer CTA
"Ready to see your numbers?"
[Request early access]

---

## Key Screens to Generate (Stitch prompts)

### Screen 1: Landing Page — Hero + Nav + Stats
Full landing page above the fold. Dark navy background. Amber accents. The variance signal card as hero visual.

### Screen 2: ARO Variance Dashboard
A product demo screen showing:
- Left sidebar: operator list with signal badges (🔴 UNDER, ✅ OVER)
- Main panel: selected operator (Apache/APA) with:
  - EDGAR provision vs Varro P50 bar chart
  - P10/P50/P90 range
  - Coverage % ring chart
  - Well map (small, US-focused)
  - Signal summary card

### Screen 3: Well-level ARO Estimate Card
A single asset card showing:
- Well ID, location, operator
- Depth, vintage, well type
- P10 / P50 / P90 cost estimates
- Cost region, confidence level
- Comparable operators

---

## Logo Brief

**Name:** VARRO
**Direction:** 
- Option A: Clean caps wordmark "VARRO" with a small amber circle or V-mark before it
- Option B: Stacked lines mark (like geological strata, suggesting depth) + wordmark
- Option C: Simple V-mark made of two diagonal strokes, clean and geometric

**Don't want:** Anything that reads as oil company, environmental NGO, or generic fintech

---

## What This Is For

Design partner outreach. We're sharing screens — not shipping code. This needs to look like a real product from a serious company. Competitors (ENFOS, Sustainable Fitch) have established visual identities. Varro needs to show up at that level.

Stage: Pre-product. Generating conversation with potential design partners for feedback before building the full platform.
