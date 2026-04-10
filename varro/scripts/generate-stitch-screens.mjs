import { Stitch, StitchToolClient } from "@google/stitch-sdk";

const API_KEY = "process.env.STITCH_API_KEY";

const prompts = [
  {
    name: "Dashboard",
    prompt: `Design a dark-mode industrial dashboard screen for an oil and gas ARO (Asset Retirement Obligation) financial intelligence platform called Varro.

Visual style: "Control Room Intelligence" — think Vercel Geist meets Bloomberg terminal. Near-black background (#0a0a0a), monospace font (Geist Mono or JetBrains Mono) for all numbers and data, cyan accent color (#06b6d4), minimal borders (#222222), and a tight information-dense layout.

The screen shows:
1. A top header bar with "Varro" logo (cyan text), breadcrumb "Dashboard", a search icon, a notification bell, and a user avatar.
2. A left sidebar with vertical navigation icons and labels: Home (active), Search, ARO Estimator, Portfolio, Regulatory, Reports, Settings. Active state has a cyan left border and subtle cyan background.
3. Four KPI stat cards in a horizontal row: "TOTAL ARO EXPOSURE (P50)" → "$142.3M" in large cyan monospace, "TOTAL ARO EXPOSURE (P90)" → "$198.7M" in amber monospace, "ASSETS TRACKED" → "247" in white monospace, "DELINQUENT / AT RISK" → "12" in red monospace.
4. An area chart labeled "ARO EXPOSURE OVER TIME (P50 / P90)" — dark blue-tinted background (#0f1720), two overlapping area fills in cyan at different opacities.
5. A right column "REGULATORY ALERTS" showing 3-4 alert items with colored left-border dots.
6. A "TOP RISK ASSETS" table below the chart with columns API #, Well Name, Basin, ARO P50, ARO P90, Confidence, Status.

Use Tailwind CSS. Full-page layout, sidebar + content area.`
  },
  {
    name: "ARO_Estimator",
    prompt: `Design a dark-mode data screen for an ARO (Asset Retirement Obligation) cost estimator tool. Industrial aesthetic: near-black background, cyan accents (#06b6d4), monospace fonts for all data values.

Screen title: "ARO Estimator" with a subheader showing:
- "API: 177-054-12345-0000" in large cyan monospace
- "Murphy Oil Corp. — Well 42-A — US GOM / BOEM Region 3"
- A red warning badge: "⚠ DELINQUENT — BOEM LIST SINCE 2021"

Layout sections:
1. WELL ATTRIBUTES panel — a 3-column grid of label/value pairs in a dark elevated card.
2. ESTIMATE OUTPUT — split into two panels: LEFT shows P50 "$2,140,000" in 3xl cyan monospace with P90 and P10 below. RIGHT shows a confidence band visualization.
3. COMPARABLE WELLS table with columns: API #, Basin, Water Depth, Total Depth, Vintage, Actual P&A Cost, Year P&A'd, Source.
4. Export action bar with "[Export FASB ASC 410 Memo (PDF)]" primary cyan button.

Use Tailwind CSS, dark theme.`
  },
  {
    name: "Portfolio_View", 
    prompt: `Create a dark-mode portfolio management screen for an enterprise oil and gas liability platform. Aesthetic: Bloomberg terminal + Vercel Geist. Black background (#0a0a0a), cyan accents (#06b6d4), monospace data.

The screen "Portfolio View" has:
- Top action bar with "[Upload CSV]" (primary cyan) and "[Export Portfolio Report]" (outlined) buttons.
- Horizontal summary strip: TOTAL P50 $142.3M | TOTAL P90 $198.7M | ASSETS 247 | HIGH UNCERTAINTY 31 | DELINQUENT 12
- Split layout: LEFT (60%) shows "ARO EXPOSURE BY ASSET" horizontal bar chart with wells colored by confidence. RIGHT (40%) shows a "BY CONFIDENCE" donut chart.
- Full-width sortable "ALL ASSETS" table with columns: API #, Well Name, Basin, ARO P50, ARO P90, Confidence, Status, ∆ QoQ.

Use Tailwind CSS. Full desktop layout.`
  },
  {
    name: "Regulatory_Tracker",
    prompt: `Design a dark-mode regulatory monitoring screen for an oil and gas compliance platform. Industrial, data-dense aesthetic. Near-black background, cyan accents.

Screen title: "Regulatory Tracker" with subtext "Monitoring BOEM · BSEE · EPA · RRC TX · IOGCC"

Layout:
- TOP: Watchlist chips row: [GOM ×] [BOEM ×] [Fieldwood-successor ×] [RRC TX ×] and "+ Add Watch" button.
- LEFT COLUMN (65%): "RECENT ALERTS" with filter tabs and alert feed cards. Each card has colored left-border, agency badge, date, title, summary, and action links.
- RIGHT COLUMN (35%): "UPCOMING DEADLINES" list with dates, API numbers, requirements, and status badges.
- BOTTOM: "REGULATORY SOURCE LIBRARY" search bar and document chips.

Tailwind CSS. Dark mode.`
  },
  {
    name: "Asset_Search",
    prompt: `Design a dark-mode asset search screen for an oil and gas data platform. Bloomberg terminal aesthetic. Black background, cyan accents (#06b6d4), all data in monospace font.

Layout:
- Full-width search bar at top (48px height, placeholder "Enter API number, operator name, or lease ID...")
- Two-panel layout: filter sidebar (left 240px) + results area
- FILTER SIDEBAR with sections: BASIN checkboxes, STATUS checkboxes, WATER DEPTH range slider, VINTAGE range slider
- RESULTS AREA with sortable table: API # (cyan), Operator, Well Name, Basin, Water Depth, Status (badges), ARO P50, Actions
- DETAIL DRAWER (slides in from right, 420px) with well attributes and estimate display

Tailwind CSS, full desktop layout.`
  }
];

async function main() {
  console.log("Connecting to Stitch API...");
  
  const client = new StitchToolClient({ apiKey: API_KEY });
  const sdk = new Stitch(client);
  
  // Create project
  console.log("Creating Varro project...");
  const createResult = await client.callTool("create_project", { title: "Varro ARO Platform" });
  console.log("Project created:", JSON.stringify(createResult, null, 2));
  
  // Extract project ID from name field (e.g., "projects/4049459872698689573")
  let projectId = createResult.projectId || createResult.project_id;
  if (!projectId && createResult.name) {
    projectId = createResult.name.replace("projects/", "");
  }
  if (!projectId) {
    console.error("Failed to get project ID from:", createResult);
    process.exit(1);
  }
  console.log("Using project ID:", projectId);
  
  const project = sdk.project(projectId);
  
  const results = [];
  
  for (const p of prompts) {
    console.log(`\nGenerating: ${p.name}...`);
    try {
      const screen = await project.generate(p.prompt, "DESKTOP");
      const html = await screen.getHtml();
      const image = await screen.getImage();
      
      results.push({
        name: p.name,
        screenId: screen.screenId,
        html: html,
        image: image
      });
      
      console.log(`✓ ${p.name}: ${image}`);
    } catch (err) {
      console.error(`✗ ${p.name}: ${err.message}`);
    }
  }
  
  await client.close();
  
  console.log("\n=== RESULTS ===");
  for (const r of results) {
    console.log(`\n${r.name}:`);
    console.log(`  Preview: ${r.image}`);
    console.log(`  HTML: ${r.html}`);
  }
  
  // Save results to file
  const fs = await import("fs");
  fs.writeFileSync(
    "/home/openclaw/.openclaw/workspace/varro/designs/stitch-results.json",
    JSON.stringify(results, null, 2)
  );
  console.log("\nResults saved to designs/stitch-results.json");
}

main().catch(console.error);
