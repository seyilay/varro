#!/usr/bin/env python3
"""
Download RRC Texas OG_WELLBORE_EWA_Report.csv using session management.
Uses 2-step process: select file via AJAX, then download.
"""
import requests, re, os, sys, time, json

BASE_URL = "https://mft.rrc.texas.gov"
LINK_ID = "650649b7-e019-4d77-a8e0-d118d6455381"  # 3-month wellbore
OUTPUT_DIR = "/home/openclaw/.openclaw/workspace/varro/data/raw/rrc_texas/"

session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
})

def extract_viewstate(html):
    vs = re.findall(r'<input[^>]*name="javax\.faces\.ViewState"[^>]*value="([^"]+)"', html)
    return vs[0] if vs else None

print("Step 1: Loading page...")
r = session.get(f"{BASE_URL}/link/{LINK_ID}", timeout=60)
print(f"  Status: {r.status_code}, Size: {len(r.text)}")

vs = extract_viewstate(r.text)
print(f"  ViewState: {vs[:60] if vs else 'NONE'}...")

# Find the first CSV file (OG_WELLBORE_EWA_Report.csv)
rks = re.findall(r'data-rk="(\d+)"', r.text)
names = re.findall(r'class="NameColumn">(.*?)</td>', r.text, re.DOTALL)
target_rk = rks[0] if rks else None
target_name = re.sub(r'<[^>]+>', '', names[0]).strip() if names else 'unknown'
print(f"  Target file: {target_name}, rk={target_rk}")

if not vs or not target_rk:
    print("Failed to extract ViewState or file RK")
    sys.exit(1)

# Step 2: AJAX row select to tell server we're selecting this file
print(f"\nStep 2: Selecting file via AJAX (rowSelectCheckbox event)...")
ajax_data = {
    'javax.faces.partial.ajax': 'true',
    'javax.faces.source': 'fileTable',
    'javax.faces.partial.execute': 'fileTable',
    'javax.faces.partial.render': 'multiRowButtons',
    'javax.faces.behavior.event': 'rowSelectCheckbox',
    'javax.faces.partial.event': 'rowSelectCheckbox',
    'fileTable': 'fileTable',
    'fileTable_encodeFeature': 'true',
    'fileTable_instantSelectedRowKey': target_rk,
    'fileTable_selection': target_rk,
    'fileList_SUBMIT': '1',
    'javax.faces.ViewState': vs,
}
ajax_headers = {
    'Faces-Request': 'partial/ajax',
    'Content-Type': 'application/x-www-form-urlencoded',
    'X-Requested-With': 'XMLHttpRequest',
}
r2 = session.post(
    f"{BASE_URL}/webclient/godrive/PublicGoDrive.xhtml",
    data=ajax_data,
    headers=ajax_headers,
    timeout=60,
)
print(f"  AJAX status: {r2.status_code}, size: {len(r2.text)}")
if 'error' in r2.text.lower() and len(r2.text) < 1000:
    print(f"  Error response: {r2.text[:500]}")

# Extract new ViewState from AJAX response
vs2 = extract_viewstate(r2.text) or vs
print(f"  New ViewState: {vs2[:60] if vs2 else 'same'}...")

# Step 3: Submit download form
print(f"\nStep 3: Submitting download form...")
download_data = {
    'j_id_3e_SUBMIT': '1',
    'j_id_3f:j_id_3f': '',  # Download button
    'fileTable_selection': target_rk,
    'javax.faces.ViewState': vs2,
}
r3 = session.post(
    f"{BASE_URL}/webclient/godrive/PublicGoDrive.xhtml",
    data=download_data,
    timeout=120,
    stream=True,
)
print(f"  Status: {r3.status_code}")
print(f"  Content-Type: {r3.headers.get('Content-Type', 'unknown')}")
print(f"  Content-Disposition: {r3.headers.get('Content-Disposition', 'none')}")
print(f"  Content-Length: {r3.headers.get('Content-Length', 'unknown')}")

ct = r3.headers.get('Content-Type', '')
if 'text/html' not in ct:
    # Actual file download!
    cd = r3.headers.get('Content-Disposition', '')
    fname = re.findall(r'filename="?([^";\n]+)"?', cd)
    out_name = fname[0] if fname else target_name
    out_path = os.path.join(OUTPUT_DIR, out_name)
    print(f"  Downloading to {out_path}...")
    
    downloaded = 0
    with open(out_path, 'wb') as f:
        for chunk in r3.iter_content(chunk_size=1024*1024):
            if chunk:
                f.write(chunk)
                downloaded += len(chunk)
                if downloaded % (10*1024*1024) == 0:
                    print(f"  {downloaded/(1024*1024):.0f} MB downloaded...")
    print(f"  SUCCESS! {downloaded/(1024*1024):.1f} MB saved to {out_path}")
else:
    print(f"  Got HTML - download didn't work")
    # Check if it redirected somewhere
    print(f"  Final URL: {r3.url}")
    print(f"  HTML preview: {r3.text[:300]}")
    
    # Try alternative: look for download URL in HTML
    dl_urls = re.findall(r'href="([^"]*[Dd]ownload[^"]*)"', r3.text[:5000])
    print(f"  Download URLs found: {dl_urls[:3]}")
