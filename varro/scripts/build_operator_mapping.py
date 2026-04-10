#!/usr/bin/env python3
"""
Build enriched operator-parent-mapping-v2.json
================================================
Matches Supabase operators to EDGAR parent companies using:
  1. GEM subsidiary CSV (exact, substring, token overlap)
  2. GOGET legacy mapping (raw_name list from v1)
  3. Text pattern fallbacks (Devon, Pioneer)

Output: /varro/data/operator-parent-mapping-v2.json
  {operator_uuid: {operator_name, edgar_parent, match_method, confidence}}
"""

import csv
import json
import re
import time
import urllib.request
import urllib.parse
import os
from urllib.request import Request

# ── Config ────────────────────────────────────────────────────────────────
SUPABASE_URL = "https://temtptsfiksixxhbigkg.supabase.co"
SUPABASE_KEY = "os.environ.get('SUPABASE_KEY')"
H = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "..", "data")

GEM_CSV_PATH = os.path.join(DATA_DIR, "raw", "gem-ownership", "subsidiary_name_lookup.csv")
V1_MAPPING_PATH = os.path.join(DATA_DIR, "operator-parent-mapping.json")
OUTPUT_PATH = os.path.join(DATA_DIR, "operator-parent-mapping-v2.json")

# Stop-words — too short or too generic to be significant tokens
STOP_WORDS = {
    "the", "and", "of", "in", "a", "an", "for", "de", "van",
    "inc", "ltd", "llc", "lp", "plc", "sa", "bv", "co", "corp",
    "company", "group", "holding", "holdings", "resources", "energy",
    "oil", "gas", "petroleum", "exploration", "production",
    "services", "international",
}

# ── Load GEM CSV ──────────────────────────────────────────────────────────

def load_gem_csv(path):
    """Return list of {name_lower, ultimate_parent}."""
    entries = []
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            name = row.get("name", "").strip()
            parent = row.get("ultimate_parent", "").strip()
            if name and parent and len(name) >= 5:
                entries.append({
                    "name": name,
                    "name_lower": name.lower(),
                    "ultimate_parent": parent,
                })
    print(f"  Loaded {len(entries)} GEM subsidiary names from CSV")
    return entries

# ── Load GOGET v1 legacy mapping ──────────────────────────────────────────

def load_v1_mapping(path):
    """Return dict of {raw_name_lower → edgar_parent}."""
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        data = json.load(f)
    m = {}
    for entry in data.get("mapping", []):
        raw = entry.get("raw_name", "").strip()
        parent = entry.get("edgar_parent", "").strip()
        if raw and parent:
            m[raw.lower()] = parent
    print(f"  Loaded {len(m)} v1 legacy mapping entries")
    return m

# ── Supabase operator fetch ───────────────────────────────────────────────

def fetch_all_operators():
    """Paginate through all operators in Supabase. Returns list of {id, name}."""
    all_ops = []
    page_size = 1000
    offset = 0
    while True:
        url = (
            f"{SUPABASE_URL}/rest/v1/operators"
            f"?select=id,name&limit={page_size}&offset={offset}"
        )
        req = Request(url, headers=H)
        with urllib.request.urlopen(req, timeout=30) as resp:
            batch = json.loads(resp.read())
        all_ops.extend(batch)
        print(f"    Fetched {len(all_ops)} operators so far (batch {len(batch)})...")
        if len(batch) < page_size:
            break
        offset += page_size
        time.sleep(0.3)
    print(f"  Total operators fetched: {len(all_ops)}")
    return all_ops

# ── Tokenizer ─────────────────────────────────────────────────────────────

def tokenize(text):
    """Split on whitespace/punctuation, lowercase, remove stop-words, min 4 chars."""
    tokens = re.split(r"[\s\-\&\(\)\/\.\,]+", text.lower())
    return {t for t in tokens if len(t) >= 4 and t not in STOP_WORDS}

# ── Matching logic ────────────────────────────────────────────────────────

def match_operator(op_name, gem_entries, v1_map):
    """
    Try to match operator name to an EDGAR parent.
    Returns (edgar_parent, match_method, confidence) or None.
    """
    op_lower = op_name.lower().strip()

    # ── 1. GOGET v1 exact match ───────────────────────────────────────────
    if op_lower in v1_map:
        return v1_map[op_lower], "goget_parent", "high"

    # ── 2. GEM exact match ────────────────────────────────────────────────
    for gem in gem_entries:
        if gem["name_lower"] == op_lower:
            return gem["ultimate_parent"], "gem_exact", "high"

    # ── 3. GOGET v1 substring match ───────────────────────────────────────
    for raw_lower, parent in v1_map.items():
        if len(raw_lower) >= 5:
            if raw_lower in op_lower or (len(op_lower) >= 5 and op_lower in raw_lower):
                return parent, "goget_parent", "medium"

    # ── 4. GEM substring match ────────────────────────────────────────────
    for gem in gem_entries:
        gem_low = gem["name_lower"]
        if len(gem_low) >= 5:
            if gem_low in op_lower or (len(op_lower) >= 5 and op_lower in gem_low):
                return gem["ultimate_parent"], "gem_substring", "medium"

    # ── 5. Token overlap (2+ significant tokens) ──────────────────────────
    op_tokens = tokenize(op_name)
    if len(op_tokens) >= 2:
        # Check v1 map
        for raw_lower, parent in v1_map.items():
            raw_tokens = tokenize(raw_lower)
            if len(op_tokens & raw_tokens) >= 2:
                return parent, "goget_parent", "low"
        # Check GEM
        for gem in gem_entries:
            gem_tokens = tokenize(gem["name"])
            if len(op_tokens & gem_tokens) >= 2:
                return gem["ultimate_parent"], "gem_token", "low"

    # ── 6. Text pattern fallbacks ─────────────────────────────────────────
    if "devon" in op_lower:
        return "Devon", "text_pattern", "medium"
    if "pioneer natural" in op_lower or "pioneer petroleum" in op_lower:
        return "Pioneer", "text_pattern", "medium"

    return None

# ── Main ──────────────────────────────────────────────────────────────────

def main():
    print("=== Building operator-parent-mapping-v2.json ===\n")

    print("[1] Loading reference data...")
    gem_entries = load_gem_csv(GEM_CSV_PATH)
    v1_map = load_v1_mapping(V1_MAPPING_PATH)

    print("\n[2] Fetching operators from Supabase...")
    operators = fetch_all_operators()

    print("\n[3] Matching operators...")
    mapping = {}
    matched = 0
    method_counts = {}

    for op in operators:
        op_id = op["id"]
        op_name = op.get("name") or ""
        if not op_name.strip():
            continue

        result = match_operator(op_name, gem_entries, v1_map)
        if result:
            parent, method, confidence = result
            mapping[op_id] = {
                "operator_name": op_name,
                "edgar_parent": parent,
                "match_method": method,
                "confidence": confidence,
            }
            matched += 1
            method_counts[method] = method_counts.get(method, 0) + 1

    total = len(operators)
    coverage = round((matched / total * 100), 2) if total else 0

    print(f"\n  Matched: {matched}/{total} operators ({coverage}%)")
    print("  By method:")
    for method, count in sorted(method_counts.items(), key=lambda x: -x[1]):
        print(f"    {method}: {count}")

    # Parent distribution
    parent_counts = {}
    for v in mapping.values():
        p = v["edgar_parent"]
        parent_counts[p] = parent_counts.get(p, 0) + 1
    print("\n  By parent:")
    for p, c in sorted(parent_counts.items(), key=lambda x: -x[1]):
        print(f"    {p}: {c}")

    output = {
        "version": "2.0",
        "source": "GEM Ownership API + GOGET parent field",
        "total_operators": total,
        "matched": matched,
        "coverage_pct": coverage,
        "mapping": mapping,
    }

    print(f"\n[4] Writing {OUTPUT_PATH}...")
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)
    print("  Done.")


if __name__ == "__main__":
    main()
