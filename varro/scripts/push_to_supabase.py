#!/usr/bin/env python3
"""
Push weather windows and steel prices to Supabase.
Run AFTER creating tables with SCHEMA_AND_INSERT.sql
"""
import urllib.request, json

SUPABASE_URL = "https://temtptsfiksixxhbigkg.supabase.co"
SUPABASE_KEY = "os.environ.get('SUPABASE_KEY')"

def upsert(table, records, conflict_cols=None):
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    body = json.dumps(records).encode()
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.status

# Weather windows
with open("/home/openclaw/.openclaw/workspace/varro/data/raw/weather/basin_weather_windows.json") as f:
    weather = json.load(f)
clean_weather = [{k: v for k, v in r.items() if k != "years_of_data"} for r in weather]
status = upsert("basin_weather_windows", clean_weather)
print(f"basin_weather_windows: HTTP {status} — {len(clean_weather)} records upserted")

# Steel prices
with open("/home/openclaw/.openclaw/workspace/varro/data/raw/commodities/steel_scrap_prices.json") as f:
    steel = json.load(f)
status = upsert("commodity_prices", steel)
print(f"commodity_prices: HTTP {status} — {len(steel)} records upserted")
