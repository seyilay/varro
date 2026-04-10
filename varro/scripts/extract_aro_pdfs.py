#!/usr/bin/env python3
"""
ARO/Decommissioning Provision Extractor
Extracts Asset Retirement Obligation data from NOC annual report PDFs.

Companies: CNOOC, ONGC, Rosneft
Sources: SEC EDGAR 20-F, HKEX filings, ONGC NSE archives, Rosneft IFRS statements

ACTUAL DATA ALREADY COLLECTED (hardcoded from successful extractions):
See ARO_DATA_COLLECTED dict below.

Usage:
  python3 extract_aro_pdfs.py [--download]   # --download fetches PDFs afresh
  python3 extract_aro_pdfs.py --output-csv   # write collected data to CSV

Dependencies: pdfplumber, PyMuPDF (fitz), requests
"""

import os
import re
import csv
import time
import requests
import sys
from pathlib import Path
from typing import Optional

try:
    import pdfplumber
except ImportError:
    pdfplumber = None

try:
    import fitz  # PyMuPDF
except ImportError:
    fitz = None

# ── Directory setup ───────────────────────────────────────────────────────────
BASE_DIR = Path("/home/openclaw/.openclaw/workspace/varro/data")
BASE_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_CSV = BASE_DIR / "aro_provisions.csv"

# ── COLLECTED DATA ────────────────────────────────────────────────────────────
# Verified from actual PDF extractions (see README_SOURCES.md)
# All values are closing year-end balances unless noted.

ARO_DATA_COLLECTED = {
    "CNOOC": {
        # Source: SEC 20-F filings (2007-2020) and HKEX annual reports (2021-2023)
        # Terminology: "Provision for Dismantlement"
        # Currency: RMB (Renminbi), Unit: million
        # Functional currency: RMB (CNY)
        "currency": "RMB",
        "unit": "million",
        "note": "Provision for Dismantlement (Note 29 in recent ARs). Pre-2013 figures from SEC 20-F 'as of Dec 31' disclosure in notes.",
        "series": [
            # year, closing_balance, source
            (2007, 6737.0,  "SEC 20-F FY2008 (EDGAR CIK 1095595), balance at Dec 31, 2007"),
            (2008, 8340.0,  "SEC 20-F FY2008 (EDGAR CIK 1095595), balance at Dec 31, 2008"),
            (2009, 11281.0, "SEC 20-F FY2009, balance at Dec 31, 2009"),
            (2010, 15825.0, "SEC 20-F FY2010 (dp17202_20f.htm), balance at Dec 31, 2010"),
            (2011, 24964.0, "SEC 20-F FY2011, balance at Dec 31, 2011"),
            (2012, 29406.0, "SEC 20-F FY2012, balance at Dec 31, 2012"),
            (2013, 42351.0, "SEC 20-F FY2013, balance at Dec 31, 2013 (includes Nexen acquisition Feb 2013)"),
            (2014, 52889.0, "SEC 20-F FY2015 (dp64914_20f.htm), balance at Dec 31, 2014"),
            (2015, 50063.0, "SEC 20-F FY2015 (dp64914_20f.htm), balance at Dec 31, 2015"),
            (2016, 50888.0, "SEC 20-F FY2017 (dp89178_20f.htm), balance at Dec 31, 2016"),
            (2017, 54073.0, "SEC 20-F FY2017 (dp89178_20f.htm), balance at Dec 31, 2017"),
            (2018, 54878.0, "SEC 20-F FY2019 (dp124679_20f.htm), balance at Dec 31, 2018"),
            (2019, 65602.0, "SEC 20-F FY2019 (dp124679_20f.htm), balance at Dec 31, 2019"),
            (2020, 70360.0, "HKEX AR 2021 (2022041200025.pdf), balance at Dec 31, 2020"),
            (2021, 76189.0, "HKEX AR 2021 (2022041200025.pdf), balance at Dec 31, 2021"),
            (2022, 88104.0, "HKEX AR 2023 (2024040202010.pdf Note 29), balance at Dec 31, 2022"),
            (2023, 98643.0, "HKEX AR 2023 (2024040202010.pdf Note 29), balance at Dec 31, 2023"),
        ],
        "pdf_sources": [
            "https://www.sec.gov/Archives/edgar/data/1095595/000095010309001057/dp13197_20f.htm",
            "https://www.sec.gov/Archives/edgar/data/1095595/000095010310001157/dp17202_20f.htm",
            "https://www.sec.gov/Archives/edgar/data/1095595/000095010316012730/dp64914_20f.htm",
            "https://www.sec.gov/Archives/edgar/data/1095595/000095010318004942/dp89178_20f.htm",
            "https://www.sec.gov/Archives/edgar/data/1095595/000095010320007907/dp124679_20f.htm",
            "https://www.hkexnews.hk/listedco/listconews/sehk/2022/0412/2022041200025.pdf",
            "https://www.hkexnews.hk/listedco/listconews/sehk/2024/0402/2024040202010.pdf",
        ],
    },

    "ONGC": {
        # Source: ONGC AR 2023-24 from NSE archives (ONGC_07082024233803_IntimationAR24.pdf)
        # Terminology: "Provision for Decommissioning" under Ind AS 37
        # Currency: INR (Indian Rupees), Unit: million
        # FY = April-March; year label = year of March 31 closing
        "currency": "INR",
        "unit": "million",
        "note": (
            "Consolidated: Note 32.4 of AR2023-24. "
            "FY22 = opening balance per Note 32.4; FY23 = comparative year; FY24 = current year. "
            "Non-current portion only shown for FY23 and FY24; total provision shown here. "
            "Standalone figures: Note 24.1 of same report."
        ),
        "series_consolidated": [
            # year (March 31), total_provision_mn, source
            (2022, 327158.51, "ONGC AR2023-24 Note 32.4 opening balance (= Mar 31 2022 closing)"),
            (2023, 373481.22, "ONGC AR2023-24 Note 32.4 closing FY23 (March 31, 2023)"),
            (2024, 480389.77, "ONGC AR2023-24 Note 32.4 closing FY24 (March 31, 2024)"),
        ],
        "series_standalone": [
            (2022, 268788.65, "ONGC AR2023-24 Note 24.1 opening balance (= Mar 31 2022 closing)"),
            (2023, 347634.17, "ONGC AR2023-24 Note 24.1 closing FY23 (March 31, 2023)"),
            (2024, 454911.76, "ONGC AR2023-24 Note 24.1 closing FY24 (March 31, 2024)"),
        ],
        "series": [  # Default: use consolidated for main series
            (2022, 327158.51, "Consolidated (see series_consolidated)"),
            (2023, 373481.22, "Consolidated"),
            (2024, 480389.77, "Consolidated"),
        ],
        "pdf_sources": [
            "https://nsearchives.nseindia.com/corporate/ONGC_07082024233803_IntimationAR24.pdf",
        ],
    },

    "Rosneft": {
        # Source: Rosneft IFRS Consolidated Financial Statements (English)
        # Published separately for 2011-2021 on rosneft.com/Investors/...
        # 2022 and 2023: Only condensed financial statements published (no full notes).
        # Terminology: "Asset Retirement (Decommissioning) Obligations" (Note 33/34)
        # Currency: RUB (Russian Rubles), Unit: billion
        "currency": "RUB",
        "unit": "billion",
        "note": (
            "IAS 37 provisions for decommissioning. "
            "2011-2021 from standalone IFRS CFS PDFs. "
            "2012 restated in 2013 report (original 64, restated 58); using restated. "
            "2022 and 2023 full IFRS not separately published; summary only. "
        ),
        "series": [
            (2010, 44.0,  "Rosneft IFRS 2012 (pjKJ0hS1IX.pdf, Note 33), Dec 31 2010"),
            (2011, 54.0,  "Rosneft IFRS 2012 (pjKJ0hS1IX.pdf), Dec 31 2011 (unrestated)"),
            (2012, 58.0,  "Rosneft IFRS 2013 (Yhjz8ODbaW.pdf, Note 33 restated), Dec 31 2012"),
            (2013, 94.0,  "Rosneft IFRS 2013 (Yhjz8ODbaW.pdf), Dec 31 2013"),
            (2014, 83.0,  "Rosneft IFRS 2015 (2015_Rosneft_FS_IFRS_ENG_SIGNED.pdf), Dec 31 2014"),
            (2015, 123.0, "Rosneft IFRS 2015, Dec 31 2015"),
            (2016, 178.0, "Rosneft IFRS 2017 (Rosneft_FS_12m2017_ENG.pdf), Dec 31 2016"),
            (2017, 218.0, "Rosneft IFRS 2017, Dec 31 2017"),
            (2018, 213.0, "Rosneft IFRS 2019 (Rosneft_FS_12m2019_ENG.pdf), Dec 31 2018"),
            (2019, 315.0, "Rosneft IFRS 2019, Dec 31 2019"),
            (2020, 406.0, "Rosneft IFRS 2021 (Rosnseft_IFRS_12m2021_en.pdf), Dec 31 2020"),
            (2021, 290.0, "Rosneft IFRS 2021, Dec 31 2021"),
            # 2022: Not published as standalone IFRS; condensed AR only
            # 2023: Not published as standalone IFRS; condensed AR only
        ],
        "pdf_sources": [
            "https://www.rosneft.com/upload/site2/document_cons_report/pjKJ0hS1IX.pdf",       # 2012
            "https://www.rosneft.com/upload/site2/document_cons_report/Yhjz8ODbaW.pdf",       # 2013
            "https://www.rosneft.com/upload/site2/document_cons_report/2015_Rosneft_FS_IFRS_ENG_SIGNED.pdf",
            "https://www.rosneft.com/upload/site2/document_cons_report/Rosneft_FS_12m2017_ENG.pdf",
            "https://www.rosneft.com/upload/site2/document_cons_report/Rosneft_FS_12m2019_ENG.pdf",
            "https://www.rosneft.com/upload/site2/document_cons_report/Rosnseft_IFRS_12m2021_en.pdf",
        ],
    },
}

# ── PDF Sources for download ──────────────────────────────────────────────────

PDF_DOWNLOAD_SOURCES = {
    "CNOOC": [
        {
            "year": 2023,
            "url": "https://www.hkexnews.hk/listedco/listconews/sehk/2024/0402/2024040202010.pdf",
            "currency": "RMB", "unit": "million",
        },
        {
            "year": 2021,
            "url": "https://www.hkexnews.hk/listedco/listconews/sehk/2022/0412/2022041200025.pdf",
            "currency": "RMB", "unit": "million",
        },
        # SEC 20-F for older years (HTML format)
        {
            "year": 2019, "format": "htm",
            "url": "https://www.sec.gov/Archives/edgar/data/1095595/000095010320007907/dp124679_20f.htm",
            "currency": "RMB", "unit": "million",
        },
        {
            "year": 2015, "format": "htm",
            "url": "https://www.sec.gov/Archives/edgar/data/1095595/000095010316012730/dp64914_20f.htm",
            "currency": "RMB", "unit": "million",
        },
    ],
    "ONGC": [
        {
            "year": 2024,
            "url": "https://nsearchives.nseindia.com/corporate/ONGC_07082024233803_IntimationAR24.pdf",
            "currency": "INR", "unit": "million",
        },
    ],
    "Rosneft": [
        {
            "year": 2021,
            "url": "https://www.rosneft.com/upload/site2/document_cons_report/Rosnseft_IFRS_12m2021_en.pdf",
            "currency": "RUB", "unit": "billion",
        },
        {
            "year": 2019,
            "url": "https://www.rosneft.com/upload/site2/document_cons_report/Rosneft_FS_12m2019_ENG.pdf",
            "currency": "RUB", "unit": "billion",
        },
        {
            "year": 2017,
            "url": "https://www.rosneft.com/upload/site2/document_cons_report/Rosneft_FS_12m2017_ENG.pdf",
            "currency": "RUB", "unit": "billion",
        },
        {
            "year": 2015,
            "url": "https://www.rosneft.com/upload/site2/document_cons_report/2015_Rosneft_FS_IFRS_ENG_SIGNED.pdf",
            "currency": "RUB", "unit": "billion",
        },
        {
            "year": 2013,
            "url": "https://www.rosneft.com/upload/site2/document_cons_report/Yhjz8ODbaW.pdf",
            "currency": "RUB", "unit": "billion",
        },
        {
            "year": 2012,
            "url": "https://www.rosneft.com/upload/site2/document_cons_report/pjKJ0hS1IX.pdf",
            "currency": "RUB", "unit": "billion",
        },
    ],
}

# ── ARO keyword patterns ──────────────────────────────────────────────────────

ARO_KEYWORDS = [
    r"abandonment",
    r"decommission",
    r"site restoration",
    r"well plugging",
    r"asset retirement",
    r"dismantlement",
]

MOVEMENT_PATTERNS = {
    "opening_balance": r"(?:opening|beginning|start|1 january|april 1)[^\d]*([\d,\.]+)",
    "additions": r"(?:additions|new provisions|increases|accrued)[^\d]*([\d,\.]+)",
    "accretion": r"(?:accretion|unwinding of discount|interest expense)[^\d]*([\d,\.]+)",
    "utilised": r"(?:utilised|paid|settled|reversed)[^\d]*([\d,\.]+)",
    "closing_balance": r"(?:closing|ending|at end|31 december|31 march|march 31)[^\d]*([\d,\.]+)",
}


def parse_number(s: str) -> Optional[float]:
    """Parse a number string like '12,345.6' or '12 345' to float."""
    if not s:
        return None
    clean = re.sub(r"[\s,]", "", str(s).strip())
    try:
        return float(clean)
    except ValueError:
        return None


def download_pdf(url: str, dest_path: Path, timeout: int = 120) -> bool:
    """Download a PDF/HTM from URL. Returns True on success."""
    if dest_path.exists() and dest_path.stat().st_size > 10000:
        print(f"  [cached] {dest_path.name}")
        return True

    headers = {"User-Agent": "Mozilla/5.0 (compatible; AcademicResearch/1.0)"}
    try:
        print(f"  [download] {url}")
        resp = requests.get(url, headers=headers, timeout=timeout, stream=True)
        resp.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=65536):
                f.write(chunk)
        print(f"  [saved] {dest_path.name} ({dest_path.stat().st_size // 1024} KB)")
        return True
    except Exception as e:
        print(f"  [ERROR] download failed for {url}: {e}")
        return False


def find_aro_pages_pdf(pdf_path: Path) -> list:
    """Find pages with ARO content using pdfplumber."""
    if pdfplumber is None:
        return []
    aro_pages = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for i, page in enumerate(pdf.pages):
                text = (page.extract_text() or "").lower()
                if any(re.search(kw, text) for kw in ARO_KEYWORDS):
                    aro_pages.append(i)
    except Exception as e:
        print(f"  [warn] pdfplumber error: {e}")
    return aro_pages


def find_aro_pages_fitz(pdf_path: Path) -> list:
    """Find pages with ARO content using PyMuPDF."""
    if fitz is None:
        return []
    aro_pages = []
    try:
        doc = fitz.open(str(pdf_path))
        for i, page in enumerate(doc.pages()):
            text = page.get_text().lower()
            if any(re.search(kw, text) for kw in ARO_KEYWORDS):
                aro_pages.append(i)
    except Exception as e:
        print(f"  [warn] fitz error: {e}")
    return aro_pages


def extract_text_from_pdf(pdf_path: Path) -> str:
    """Extract text from ARO-relevant pages."""
    if fitz:
        doc = fitz.open(str(pdf_path))
        aro_pages = find_aro_pages_fitz(pdf_path)
        texts = []
        pages_to_read = set()
        for p in aro_pages:
            for off in range(-1, 3):
                pages_to_read.add(p + off)
        for p in sorted(pages_to_read):
            if 0 <= p < doc.page_count:
                texts.append(f"\n--- PAGE {p+1} ---\n{doc[p].get_text()}")
        return "\n".join(texts)
    elif pdfplumber:
        aro_pages = find_aro_pages_pdf(pdf_path)
        texts = []
        with pdfplumber.open(pdf_path) as pdf:
            pages_to_read = set()
            for p in aro_pages:
                for off in range(-1, 3):
                    pages_to_read.add(p + off)
            for p in sorted(pages_to_read):
                if 0 <= p < len(pdf.pages):
                    t = pdf.pages[p].extract_text() or ""
                    texts.append(f"\n--- PAGE {p+1} ---\n{t}")
        return "\n".join(texts)
    return ""


def extract_from_htm(htm_path: Path, company: str) -> dict:
    """Extract ARO data from SEC 20-F HTML files."""
    text = htm_path.read_text(encoding="utf-8", errors="ignore")

    # CNOOC: search for "as of December 31" context with dismantlement
    if company == "CNOOC":
        # Find the "dismantlement" note with balance
        for pattern in [
            r'as of december 31.*?approximately rmb[^\d]*([\d,\.]+)[^\d]*million',
            r'dismantlement[^\d]*approximately rmb[^\d]*([\d,\.]+)[^\d]*million',
        ]:
            m = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            if m:
                amount = parse_number(re.sub(r'<[^>]+>|&[a-z#0-9]+;', '', m.group(1)))
                if amount:
                    return {"closing_balance": amount, "currency": "RMB", "unit": "million"}

    return {}


def extract_provision_from_text(text: str, company: str) -> dict:
    """Generic extraction of provision data from text."""
    result = {}

    # Company-specific closing balance patterns
    if company == "CNOOC":
        # Look for "At 31 December YYYY" amounts
        m = re.search(r'at 31 december[^\d]*([\d,\.]+)', text, re.IGNORECASE)
        if m:
            result["closing_balance"] = parse_number(m.group(1))

    elif company == "ONGC":
        # Look for "Balance at end of year" with March 31
        m = re.search(r'balance at (?:end of year|31 march)[^\d]*([\d,\.]+)', text, re.IGNORECASE)
        if m:
            result["closing_balance"] = parse_number(m.group(1))

    elif company == "Rosneft":
        m = re.search(r'(?:as of|at)[^:]*(?:31 december|december 31)[^\d]*([\d,\.]+)', text, re.IGNORECASE)
        if m:
            result["closing_balance"] = parse_number(m.group(1))

    # Generic movement table
    for field, pattern in MOVEMENT_PATTERNS.items():
        if field not in result:
            m = re.search(pattern, text, re.IGNORECASE)
            if m:
                result[field] = parse_number(m.group(1))

    return result


def output_collected_to_csv():
    """Write the pre-collected ARO data to CSV."""
    rows = []

    for company, data in ARO_DATA_COLLECTED.items():
        currency = data["currency"]
        unit = data["unit"]
        sources = data.get("pdf_sources", [])
        series = data.get("series", [])

        for year, balance, source in series:
            rows.append({
                "company": company,
                "year": year,
                "closing_balance": balance,
                "currency": currency,
                "unit": unit,
                "source": source,
                "note": data.get("note", ""),
            })

        # ONGC: also write consolidated and standalone separately
        if company == "ONGC":
            for suffix, key in [("_consolidated", "series_consolidated"), ("_standalone", "series_standalone")]:
                for year, balance, source in data.get(key, []):
                    rows.append({
                        "company": f"ONGC{suffix}",
                        "year": year,
                        "closing_balance": balance,
                        "currency": currency,
                        "unit": unit,
                        "source": source,
                        "note": key,
                    })

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["company", "year", "closing_balance", "currency", "unit", "source", "note"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n[saved] CSV: {OUTPUT_CSV} ({len(rows)} rows)")
    return rows


def print_summary():
    """Print a formatted summary of all collected ARO data."""
    print("\n" + "=" * 70)
    print("ARO/DECOMMISSIONING PROVISION DATA COLLECTED")
    print("=" * 70)

    for company, data in ARO_DATA_COLLECTED.items():
        series = data.get("series", [])
        currency = data["currency"]
        unit = data["unit"]

        print(f"\n{'─' * 70}")
        print(f"  {company}")
        print(f"  Currency: {currency} {unit}")
        print(f"  Note: {data.get('note','')[:120]}")
        print(f"  Years: {min(s[0] for s in series)} – {max(s[0] for s in series)}")
        print()
        print(f"  {'Year':>6} | {'Closing Balance':>22} | Notes")
        print(f"  {'-'*6}+{'-'*24}+{'-'*35}")

        for year, balance, source in sorted(series, key=lambda x: x[0]):
            bal_str = f"{balance:>20,.1f}"
            src_short = source.split(",")[0][:35]
            print(f"  {year:>6} | {bal_str} | {src_short}")

    # ONGC also show consolidated vs standalone
    ongc = ARO_DATA_COLLECTED["ONGC"]
    print(f"\n  ONGC Standalone vs Consolidated (INR million):")
    print(f"  {'Year':>6} | {'Consolidated':>22} | {'Standalone':>22}")
    print(f"  {'-'*6}+{'-'*24}+{'-'*24}")
    cons = {y: b for y, b, _ in ongc.get("series_consolidated", [])}
    stand = {y: b for y, b, _ in ongc.get("series_standalone", [])}
    for yr in sorted(set(list(cons.keys()) + list(stand.keys()))):
        c = f"{cons.get(yr, 0):,.1f}" if yr in cons else "N/A"
        s = f"{stand.get(yr, 0):,.1f}" if yr in stand else "N/A"
        print(f"  {yr:>6} | {c:>22} | {s:>22}")

    print(f"\n{'─' * 70}")
    print(f"\n  PDF Sources Used:")
    for company, data in ARO_DATA_COLLECTED.items():
        for url in data.get("pdf_sources", []):
            print(f"  [{company}] {url}")

    print("\n" + "=" * 70)


def run_extraction(download_mode: bool = False):
    """
    Main extraction runner.
    If download_mode=True, attempts to download PDFs and re-extract.
    Otherwise, just outputs collected data.
    """
    if download_mode:
        print("Download mode: fetching PDFs...")
        for company, sources in PDF_DOWNLOAD_SOURCES.items():
            company_dir = BASE_DIR / company.lower()
            company_dir.mkdir(exist_ok=True)

            for source in sources:
                year = source["year"]
                url = source["url"]
                fmt = source.get("format", "pdf")
                ext = ".htm" if fmt == "htm" else ".pdf"
                dest = company_dir / f"{company.lower()}_{year}{ext}"

                print(f"\n  {company} {year}: {url.split('/')[-1]}")
                if download_pdf(url, dest):
                    # Try extraction
                    if fmt == "htm":
                        result = extract_from_htm(dest, company)
                        if result.get("closing_balance"):
                            print(f"  -> Extracted: {result}")
                    else:
                        text = extract_text_from_pdf(dest)
                        if text:
                            result = extract_provision_from_text(text, company)
                            if result.get("closing_balance"):
                                print(f"  -> Extracted: {result}")
                            else:
                                debug_path = company_dir / f"{company.lower()}_{year}_aro_debug.txt"
                                with open(debug_path, "w") as df:
                                    df.write(text[:20000])
                                print(f"  -> No data parsed; debug saved to {debug_path.name}")
                        else:
                            print(f"  -> No ARO pages found in PDF")
                time.sleep(1)

    print_summary()
    rows = output_collected_to_csv()
    return rows


if __name__ == "__main__":
    download = "--download" in sys.argv
    run_extraction(download_mode=download)
