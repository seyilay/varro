#!/usr/bin/env python3
"""
Download RRC Texas dBase API files using requests session management.
"""
import requests, re, os, sys, time

BASE_URL = "https://mft.rrc.texas.gov"
LINK_ID = "1eb94d66-461d-4114-93f7-b4bc04a70674"  # dBase API format
OUTPUT_DIR = "/home/openclaw/.openclaw/workspace/varro/data/raw/rrc_texas/"

session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
})

def get_page(url, data=None):
    if data:
        r = session.post(url, data=data, timeout=60)
    else:
        r = session.get(url, timeout=60)
    return r

def extract_viewstate(html):
    vs = re.findall(r'<input[^>]*name="javax\.faces\.ViewState"[^>]*value="([^"]+)"', html)
    return vs[0] if vs else None

def extract_rks_and_names(html):
    """Extract file RKs and names from page."""
    items = []
    # Look for CDATA sections
    cdatas = re.findall(r'<!\[CDATA\[(.*?)\]\]>', html, re.DOTALL)
    target_html = '\n'.join(cdatas) if cdatas else html
    
    # Find rows
    rows = re.findall(r'data-rk="(\d+)"[^>]*>.*?class="NameColumn">(.*?)</td>', target_html, re.DOTALL)
    for rk, name_html in rows:
        name = re.sub(r'<[^>]+>', '', name_html).strip()
        items.append((rk, name))
    return items

print("Step 1: Loading main page...")
r = get_page(f"{BASE_URL}/link/{LINK_ID}")
print(f"  Status: {r.status_code}, Size: {len(r.text)}")

vs = extract_viewstate(r.text)
print(f"  ViewState: {vs[:50] if vs else 'NONE'}...")

items = extract_rks_and_names(r.text)
print(f"  Found {len(items)} files")
for rk, name in items[:5]:
    print(f"    rk={rk}: {name}")

if not items:
    print("No files found!")
    sys.exit(1)

# Try to download the first dBase file 
first_rk, first_name = items[0]
print(f"\nStep 2: Attempting to download first file: {first_name} (rk={first_rk})")

# Get fresh ViewState from page
download_data = {
    'j_id_3e_SUBMIT': '1',
    'j_id_3f:j_id_3f': '',
    'fileTable_selection': first_rk,
    'javax.faces.ViewState': vs,
}

print(f"  Posting to download form...")
r2 = session.post(
    f"{BASE_URL}/webclient/godrive/PublicGoDrive.xhtml",
    data=download_data,
    timeout=120,
    allow_redirects=True
)
print(f"  Status: {r2.status_code}")
print(f"  Content-Type: {r2.headers.get('Content-Type', 'unknown')}")
print(f"  Content-Disposition: {r2.headers.get('Content-Disposition', 'none')}")
print(f"  Size: {len(r2.content)} bytes")

if 'application' in r2.headers.get('Content-Type', '') or 'octet-stream' in r2.headers.get('Content-Type', ''):
    # Success! This is a file
    out_path = os.path.join(OUTPUT_DIR, first_name or 'download.dbf')
    with open(out_path, 'wb') as f:
        f.write(r2.content)
    print(f"  DOWNLOADED to {out_path}")
else:
    print(f"  Got HTML response - checking for download token...")
    # Look for any download token or redirect
    tokens = re.findall(r'token["\s=:]+([a-zA-Z0-9_\-]+)', r2.text[:2000])
    print(f"  Tokens found: {tokens[:3]}")
    print(f"  HTML preview: {r2.text[:500]}")
