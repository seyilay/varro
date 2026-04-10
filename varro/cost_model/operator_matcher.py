"""
Canonical operator name normalizer + parent company hierarchy.
Replaces the naive fuzzy string matching in the variance engine.

Match tiers (in priority order):
1. CERTAIN: CIK exact match
2. HIGH: Ticker exact match
3. HIGH: Canonical name exact match (after normalization)
4. MEDIUM: Known parent/acquisition mapping (curated)
5. LOW: Embedding cosine similarity (not implemented yet; placeholder)
6. NONE: No match → excluded from variance engine
"""
import re

# Legal suffixes to strip for canonicalization
LEGAL_SUFFIXES = re.compile(
    r'\b(inc|corp|corporation|llc|ltd|limited|lp|l\.p\.|plc|sa|ag|nv|bv|'
    r'co|company|companies|holdings|group|international|national|'
    r'resources|energy|petroleum|oil|gas|exploration|production|'
    r'operating|operations|usa|us|americas)\b\.?$',
    re.IGNORECASE
)

def canonicalize(name: str) -> str:
    """Normalize company name for comparison."""
    if not name:
        return ''
    n = name.lower().strip()
    n = re.sub(r'[^\w\s]', ' ', n)   # punctuation → space
    n = re.sub(r'\s+', ' ', n)        # collapse spaces
    # Strip legal suffixes iteratively
    prev = None
    while prev != n:
        prev = n
        n = LEGAL_SUFFIXES.sub('', n).strip()
    return n.strip()


# Curated parent→subsidiary mappings
# Key: canonical subsidiary name → (parent ticker, parent canonical name, confidence)
PARENT_MAP = {
    # ExxonMobil acquisitions
    'xto':                       ('XOM', 'exxonmobil', 'HIGH'),
    'xto offshore':              ('XOM', 'exxonmobil', 'HIGH'),
    'pioneer natural':           ('XOM', 'exxonmobil', 'HIGH'),  # acquired May 2024
    'mobil':                     ('XOM', 'exxonmobil', 'HIGH'),
    'esso':                      ('XOM', 'exxonmobil', 'HIGH'),
    
    # Chevron acquisitions
    'noble':                     ('CVX', 'chevron', 'HIGH'),
    'noble energy':              ('CVX', 'chevron', 'HIGH'),
    'pdo':                       ('CVX', 'chevron', 'MEDIUM'),  # Petroleum Dev. Oman partial
    
    # ConocoPhillips acquisitions
    'conoco':                    ('COP', 'conocophillips', 'HIGH'),
    'phillips petroleum':        ('COP', 'conocophillips', 'HIGH'),
    'burlington resources':      ('COP', 'conocophillips', 'HIGH'),
    
    # Shell
    'shell':                     ('SHEL', 'shell', 'HIGH'),
    'bpz':                       ('SHEL', 'shell', 'MEDIUM'),
    
    # BP acquisitions
    'bp':                        ('BP', 'bp', 'HIGH'),
    'amoco':                     ('BP', 'bp', 'HIGH'),
    'arco':                      ('BP', 'bp', 'HIGH'),
    
    # TotalEnergies
    'total':                     ('TTE', 'totalenergies', 'HIGH'),
    'totalenergies':             ('TTE', 'totalenergies', 'HIGH'),
    'elf':                       ('TTE', 'totalenergies', 'HIGH'),
    
    # Coterra (Cimarex + Cabot merger 2021)
    'cimarex':                   ('CTRA', 'coterra', 'HIGH'),
    'cabot':                     ('CTRA', 'coterra', 'HIGH'),
    
    # Oxy
    'occidental':                ('OXY', 'occidental', 'HIGH'),
    'anadarko':                  ('OXY', 'occidental', 'HIGH'),  # acquired 2019
    
    # Equinor
    'statoil':                   ('EQNR', 'equinor', 'HIGH'),
    'statoilex':                 ('EQNR', 'equinor', 'HIGH'),
    
    # Harbour Energy (UK)
    'chrysaor':                  ('HBR', 'harbour', 'HIGH'),
    'premier oil':               ('HBR', 'harbour', 'HIGH'),
    
    # Canadian companies
    'imperial':                  ('IMO', 'imperial', 'HIGH'),    # ExxonMobil 69.6%
    'husky':                     ('CVE', 'cenovus', 'HIGH'),     # acquired 2021
    'meg energy':                ('MEG', 'meg', 'HIGH'),
    
    # Ithaca (UK)
    'ithaca':                    ('ITH', 'ithaca', 'HIGH'),
    'hurricane':                 ('ITH', 'ithaca', 'MEDIUM'),    # acquired Nov 2024
    
    # Talos Energy
    'talos':                     ('TALO', 'talos', 'HIGH'),
    'stone energy':              ('TALO', 'talos', 'HIGH'),       # merger 2021
    
    # Hilcorp (private — no ticker but large US operator)
    'hilcorp':                   (None, 'hilcorp', 'HIGH'),
    
    # Devon Energy
    'devon':                     ('DVN', 'devon', 'HIGH'),
    'wpx':                       ('DVN', 'devon', 'HIGH'),        # acquired 2021
    
    # EOG
    'eog':                       ('EOG', 'eog', 'HIGH'),
    'eog resources':             ('EOG', 'eog', 'HIGH'),
    
    # Diamondback
    'diamondback':               ('FANG', 'diamondback', 'HIGH'),
    'endeavor':                  ('FANG', 'diamondback', 'HIGH'), # merger 2024
    
    # Marathon
    'marathon':                  ('MRO', 'marathon', 'HIGH'),
    
    # Hess
    'hess':                      ('HES', 'hess', 'HIGH'),
    
    # Fieldwood (private)
    'fieldwood':                 (None, 'fieldwood', 'HIGH'),
    'apache':                    ('APA', 'apa', 'HIGH'),
}

def match_operator_to_edgar(
    operator_name: str,
    operator_ticker: str = None,
    edgar_by_ticker: dict = {},
    edgar_by_canonical: dict = {},
) -> tuple:
    """
    Returns (edgar_entry, match_method, confidence) or (None, None, None).
    """
    # Tier 2: Ticker exact match
    if operator_ticker:
        m = edgar_by_ticker.get(operator_ticker.upper().strip())
        if m:
            return m, 'ticker', 'HIGH'

    # Tier 3: Canonical name match
    canon = canonicalize(operator_name)
    m = edgar_by_canonical.get(canon)
    if m:
        return m, 'canonical_exact', 'HIGH'

    # Tier 4: Parent map
    for key, (parent_ticker, parent_canon, confidence) in PARENT_MAP.items():
        if key in canon or canon.startswith(key):
            # Try to find parent in EDGAR
            if parent_ticker:
                m = edgar_by_ticker.get(parent_ticker)
                if m:
                    return m, f'parent_map:{key}', confidence
            m = edgar_by_canonical.get(parent_canon)
            if m:
                return m, f'parent_map:{key}', confidence

    return None, None, None


if __name__ == '__main__':
    # Test canonicalization
    tests = [
        "CANADIAN NATURAL RESOURCES LIMITED",
        "Cenovus Energy Inc.",
        "XTO Energy LLC",
        "Pioneer Natural Resources USA Inc.",
        "Shell Exploration & Production Company",
        "Talos Energy Operating Company, LLC",
        "EOG Resources, Inc.",
    ]
    print("Canonicalization tests:")
    for t in tests:
        print(f"  '{t}' → '{canonicalize(t)}'")
    
    print("\nParent map lookups:")
    for name in ['XTO ENERGY INC', 'PIONEER NATURAL RESOURCES', 'ANADARKO PETROLEUM']:
        canon = canonicalize(name)
        match = None
        for key, (ticker, parent, conf) in PARENT_MAP.items():
            if key in canon:
                match = (ticker, parent, conf); break
        print(f"  '{name}' → {match}")
