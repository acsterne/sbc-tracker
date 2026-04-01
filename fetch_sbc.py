"""
fetch_sbc.py — 3-layer EDGAR fetcher for SBC and related financials.

Layer 1: XBRL Company Facts API     (fast, structured — covers ~85% of cases)
Layer 2: XBRL Instance Document     (fills gaps via filing-level XML parsing)
Layer 3: Filing HTML Parse          (last resort via BeautifulSoup)

Usage:
    DATABASE_URL=postgresql://... python3 fetch_sbc.py
    DATABASE_URL=postgresql://... python3 fetch_sbc.py --ticker SNAP
"""

import os
import re
import time
import json
import argparse
import requests
import psycopg2
import psycopg2.extras
from datetime import date
from xml.etree import ElementTree
from companies import COMPANIES

try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False
    print("[WARN] beautifulsoup4 not installed — Layer 3 HTML parse disabled")

DATABASE_URL = os.environ["DATABASE_URL"]
EDGAR_BASE = "https://data.sec.gov"
START_YEAR = 2009

HEADERS = {
    "User-Agent": "SBC Tracker research@sbctracker.io",
    "Accept": "application/json",
}

# ── XBRL concept lists (priority order, all tried and merged) ────────────────

SBC_CONCEPTS = [
    "ShareBasedCompensation",
    "AllocatedShareBasedCompensationExpense",
    "ShareBasedCompensationExpense",
    "EmployeeBenefitsAndShareBasedCompensation",
    "StockBasedCompensation",
]
REVENUE_CONCEPTS = [
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "Revenues",
    "RevenueFromContractWithCustomerIncludingAssessedTax",
    "SalesRevenueNet",
    "SalesRevenueGoodsNet",
    "SalesRevenueServicesNet",
]
GROSS_PROFIT_CONCEPTS = ["GrossProfit"]
NET_INCOME_CONCEPTS   = ["NetIncomeLoss", "ProfitLoss"]
SHARES_CONCEPTS       = ["CommonStockSharesOutstanding", "EntityCommonStockSharesOutstanding"]
BUYBACK_SPEND_CONCEPTS = [
    "PaymentsForRepurchaseOfCommonStock",
    "TreasuryStockValueAcquiredCostMethod",
]
SHARES_REPURCHASED_CONCEPTS = [
    "TreasuryStockSharesAcquired",
    "StockRepurchasedAndRetiredDuringPeriodShares",
    "StockRepurchasedDuringPeriodShares",
]
UNRECOGNIZED_SBC_CONCEPTS = [
    "EmployeeServiceShareBasedCompensationNonvestedAwardsTotalCompensationCostNotYetRecognized",
    "ShareBasedCompensationArrangementByShareBasedPaymentAwardEquityInstrumentsOtherThanOptionsNonvestedNumber",
]
OPERATING_INCOME_CONCEPTS = [
    "OperatingIncomeLoss",
    "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
    "IncomeLossFromContinuingOperationsBeforeIncomeTaxesDomestic",
]
DA_CONCEPTS = [
    "DepreciationDepletionAndAmortization",
    "DepreciationAndAmortization",
    "Depreciation",
]

# Maps concept name → hardcoded fallback list (used as safety net after dynamic discovery)
HARDCODED_FALLBACKS = {
    "sbc":              SBC_CONCEPTS,
    "revenue":          REVENUE_CONCEPTS,
    "gross_profit":     GROSS_PROFIT_CONCEPTS,
    "net_income":       NET_INCOME_CONCEPTS,
    "buybacks":         BUYBACK_SPEND_CONCEPTS,
    "operating_income": OPERATING_INCOME_CONCEPTS,
    "da":               DA_CONCEPTS,
}


# ── Request utilities ────────────────────────────────────────────────────────

def _sleep():
    time.sleep(0.11)  # Respect EDGAR ~10 req/sec limit


def fetch_url(url, max_retries=3):
    """GET with exponential backoff on 429."""
    for attempt in range(max_retries):
        try:
            _sleep()
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code == 429:
                wait = 5 * (2 ** attempt)
                print(f"    [RATE LIMIT] sleeping {wait}s...")
                time.sleep(wait)
                continue
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            if attempt == max_retries - 1:
                print(f"    [ERROR] fetch failed for {url}: {e}")
                return None
            time.sleep(2 ** attempt)
    return None


# ── DB ───────────────────────────────────────────────────────────────────────

def get_db():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor, connect_timeout=10)


def upsert_companies(cur):
    for c in COMPANIES:
        cur.execute("""
            INSERT INTO companies (ticker, name, cik, sector, ipo_year)
            VALUES (%(ticker)s, %(name)s, %(cik)s, %(sector)s, %(ipo_year)s)
            ON CONFLICT (ticker) DO UPDATE SET
                name=EXCLUDED.name, cik=EXCLUDED.cik,
                sector=EXCLUDED.sector, ipo_year=EXCLUDED.ipo_year
        """, c)


# ── Layer 1: XBRL Company Facts API ─────────────────────────────────────────

def layer1_fetch_facts(cik):
    url = f"{EDGAR_BASE}/api/xbrl/companyfacts/CIK{cik}.json"
    r = fetch_url(url)
    return r.json() if r else None


def _extract_merged(facts, concepts, unit="USD"):
    """
    Merge data across ALL concept names so every period_end gets the best value.
    Priority: earlier concepts win for same period, later concepts fill gaps.
    Returns (annual_dict, quarterly_dict) both keyed by period_end date string.
    """
    us_gaap = facts.get("facts", {}).get("us-gaap", {})
    annual, quarterly, matched = {}, {}, []

    for concept in concepts:
        if concept not in us_gaap:
            continue
        units = us_gaap[concept].get("units", {})
        if unit not in units:
            continue
        had_data = False
        for e in units[unit]:
            form = e.get("form", "")
            end  = e.get("end", "")
            val  = e.get("val")
            accn = e.get("accn", "")
            if val is None or not end or int(end[:4]) < START_YEAR:
                continue
            if form == "10-K":
                if end not in annual or accn > annual[end]["accn"]:
                    annual[end] = {"val": val, "accn": accn}
                had_data = True
            elif form == "10-Q":
                if end not in quarterly or accn > quarterly[end]["accn"]:
                    quarterly[end] = {"val": val, "accn": accn}
                had_data = True
        if had_data:
            matched.append(concept)

    if annual or quarterly:
        print(f"    [L1] matched {matched}: {len(annual)} annual, {len(quarterly)} quarterly")
        return {k: v["val"] for k, v in annual.items()}, {k: v["val"] for k, v in quarterly.items()}
    print(f"    [L1] no match for: {concepts}")
    return {}, {}


def _extract_shares_merged(facts, concepts):
    """Same as _extract_merged but auto-detects share unit name."""
    us_gaap = facts.get("facts", {}).get("us-gaap", {})
    dei     = facts.get("facts", {}).get("dei", {})
    all_ns  = {**us_gaap, **dei}
    annual, quarterly, matched = {}, {}, []

    for concept in concepts:
        if concept not in all_ns:
            continue
        units = all_ns[concept].get("units", {})
        share_unit = next((u for u in units if "share" in u.lower()), None)
        if not share_unit:
            continue
        had_data = False
        for e in units[share_unit]:
            form = e.get("form", "")
            end  = e.get("end", "")
            val  = e.get("val")
            accn = e.get("accn", "")
            if val is None or not end or int(end[:4]) < START_YEAR:
                continue
            if form == "10-K":
                if end not in annual or accn > annual[end]["accn"]:
                    annual[end] = {"val": val, "accn": accn}
                had_data = True
            elif form == "10-Q":
                if end not in quarterly or accn > quarterly[end]["accn"]:
                    quarterly[end] = {"val": val, "accn": accn}
                had_data = True
        if had_data:
            matched.append(concept)

    if annual or quarterly:
        return {k: v["val"] for k, v in annual.items()}, {k: v["val"] for k, v in quarterly.items()}
    return {}, {}


# ── Dynamic tag discovery ─────────────────────────────────────────────────────

def _count_10k_periods(entries):
    """Count distinct fiscal years with 10-K data at or after START_YEAR."""
    years_seen = set()
    for e in entries:
        if e.get("form") == "10-K" and e.get("val") is not None and e.get("end"):
            yr = int(e["end"][:4])
            if yr >= START_YEAR:
                years_seen.add(yr)
    return len(years_seen)


def _tag_matches_concept(tag, concept):
    """
    Check if a tag name satisfies inclusion/exclusion rules for a concept.
    Returns (matches: bool, bonus_score: int).
    """
    t = tag.lower()

    if concept == "sbc":
        if not any(kw in t for kw in ["sharebased", "stockbased", "compensation"]):
            return False, 0
        if any(kw in t for kw in ["nonvested", "unrecognized", "fairvalue", "weighted", "deferred"]):
            return False, 0
        return True, 0

    elif concept == "revenue":
        if not any(kw in t for kw in ["revenue", "sales", "turnover"]):
            return False, 0
        for excl in ["costof", "deferred", "unearned", "backlog", "remaining"]:
            if excl in t:
                return False, 0
        # "contract" is only ok when the tag also contains "revenue"
        if "contract" in t and "revenue" not in t:
            return False, 0
        return True, 0

    elif concept == "operating_income":
        if any(kw in t for kw in ["operatingincome", "operatingloss", "operatingprofit"]):
            return True, 0
        if "incomeloss" in t and "continuingoperation" in t and "beforeincometax" in t:
            return True, 0
        return False, 0

    elif concept == "da":
        if "depreciation" not in t:
            return False, 0
        # Exclude balance-sheet accumulated D&A, right-of-use, lease items
        if any(kw in t for kw in ["accumulated", "propertyplant", "rightofuse", "financelease"]):
            return False, 0
        bonus = 2 if "amortization" in t else 0
        return True, bonus

    elif concept == "net_income":
        if any(kw in t for kw in ["netincome", "profitloss"]):
            return True, 0
        return False, 0

    elif concept == "buybacks":
        has_repurchase = "repurchase" in t
        has_treasury_stock = "treasury" in t and "stock" in t
        return has_repurchase or has_treasury_stock, 0

    elif concept == "gross_profit":
        return "grossprofit" in t, 0

    return False, 0


def discover_tags(facts):
    """
    Dynamically score every XBRL tag in the companyfacts JSON and pick the
    best tag per concept.  Dynamic discovery runs first; hardcoded lists serve
    as the safety net when results are ambiguous.

    Scoring (higher = better):
      + annual 10-K period count   (primary signal)
      + 2  if tag is in us-gaap namespace (vs extension)
      + 5  if tag already appears in our hardcoded fallback list
      + 0-2 bonus per concept-specific rules (e.g. D&A with Amortization)

    Returns:
      dict keyed by concept name: {"tag", "namespace", "periods", "score"} | None
    """
    CONCEPTS = ["sbc", "revenue", "gross_profit", "net_income",
                "operating_income", "da", "buybacks"]

    candidates_by_concept = {c: [] for c in CONCEPTS}

    for ns, tags in facts.get("facts", {}).items():
        for tag, tag_data in tags.items():
            for unit, entries in tag_data.get("units", {}).items():
                if unit != "USD":
                    continue  # shares-unit concepts handled separately
                t_lower = tag.lower()
                for concept in CONCEPTS:
                    matches, bonus = _tag_matches_concept(t_lower, concept)
                    if not matches:
                        continue
                    n_periods = _count_10k_periods(entries)
                    if n_periods == 0:
                        continue
                    score = n_periods + bonus
                    if ns == "us-gaap":
                        score += 2
                    if tag in HARDCODED_FALLBACKS.get(concept, []):
                        score += 5
                    candidates_by_concept[concept].append((score, n_periods, ns, tag))

    results = {}
    for concept, candidates in candidates_by_concept.items():
        if not candidates:
            results[concept] = None
            continue
        candidates.sort(reverse=True)
        best_score, best_periods, best_ns, best_tag = candidates[0]
        results[concept] = {
            "tag": best_tag, "namespace": best_ns,
            "periods": best_periods, "score": best_score,
        }
        # Log when discovery picks something different from the hardcoded top choice
        hardcoded_top = HARDCODED_FALLBACKS.get(concept, [None])[0]
        if best_tag != hardcoded_top:
            print(f"      [DISC] {concept}: '{best_tag}' ({best_periods}p) "
                  f"over hardcoded '{hardcoded_top}'")

    return results


def save_discovered_tags(cur, company_id, discovered, actual_periods=None):
    """Upsert the discovered tag info for each concept into company_tags."""
    for concept, info in discovered.items():
        if info is None:
            source, tag, ns, periods = "needs_html_parse", None, None, 0
        else:
            source = "dynamic"
            tag    = info["tag"]
            ns     = info["namespace"]
            periods = (actual_periods or {}).get(concept, info["periods"])
        cur.execute("""
            INSERT INTO company_tags
                (company_id, concept, tag_used, namespace, periods_found, source)
            VALUES
                (%(cid)s, %(concept)s, %(tag)s, %(ns)s, %(periods)s, %(source)s)
            ON CONFLICT (company_id, concept) DO UPDATE SET
                tag_used      = EXCLUDED.tag_used,
                namespace     = EXCLUDED.namespace,
                periods_found = EXCLUDED.periods_found,
                source        = EXCLUDED.source,
                discovered_at = NOW()
        """, {"cid": company_id, "concept": concept, "tag": tag,
              "ns": ns, "periods": periods, "source": source})


def layer1_save_filings(cur, company_id, facts):
    """Parse Layer 1 facts and upsert filing rows. Returns set of fiscal years with SBC."""
    # Dynamic tag discovery — prepend best discovered tag to each hardcoded list
    discovered = discover_tags(facts)

    def _concepts(concept, hardcoded):
        info = discovered.get(concept)
        if info:
            tag = info["tag"]
            return [tag] + [c for c in hardcoded if c != tag]
        return hardcoded

    sbc_ann, sbc_qtd     = _extract_merged(facts, _concepts("sbc",              SBC_CONCEPTS))
    rev_ann, rev_qtd     = _extract_merged(facts, _concepts("revenue",          REVENUE_CONCEPTS))
    gp_ann, _            = _extract_merged(facts, _concepts("gross_profit",     GROSS_PROFIT_CONCEPTS))
    ni_ann, _            = _extract_merged(facts, _concepts("net_income",       NET_INCOME_CONCEPTS))
    bb_ann, _            = _extract_merged(facts, _concepts("buybacks",         BUYBACK_SPEND_CONCEPTS))
    shr_ann, _           = _extract_shares_merged(facts, SHARES_CONCEPTS)
    shrep_ann, _         = _extract_shares_merged(facts, SHARES_REPURCHASED_CONCEPTS)
    unrec_ann, _         = _extract_merged(facts, UNRECOGNIZED_SBC_CONCEPTS)
    oi_ann, _            = _extract_merged(facts, _concepts("operating_income", OPERATING_INCOME_CONCEPTS))
    da_ann, _            = _extract_merged(facts, _concepts("da",               DA_CONCEPTS))

    # Capture actual extracted period counts and persist to company_tags
    save_discovered_tags(cur, company_id, discovered, {
        "sbc": len(sbc_ann), "revenue": len(rev_ann), "gross_profit": len(gp_ann),
        "net_income": len(ni_ann), "buybacks": len(bb_ann),
        "operating_income": len(oi_ann), "da": len(da_ann),
    })

    all_annual = set(sbc_ann) | set(rev_ann) | set(gp_ann) | set(ni_ann) | set(bb_ann) | set(shr_ann)
    years_with_sbc = set()

    for d in sorted(all_annual):
        period_end = date.fromisoformat(d)
        fiscal_year = period_end.year
        sbc_val = sbc_ann.get(d)
        oi_val  = oi_ann.get(d)
        da_val  = da_ann.get(d)
        ebitda_val = (oi_val + da_val) if (oi_val is not None and da_val is not None) else None
        ebitda_src = "xbrl_derived" if ebitda_val is not None else None
        cur.execute("""
            INSERT INTO filings (
                company_id, period_end, fiscal_year, fiscal_quarter, form_type,
                sbc_expense, revenue, gross_profit, net_income,
                shares_outstanding, shares_repurchased, buyback_spend,
                unrecognized_sbc, operating_income, depreciation_amortization,
                ebitda, ebitda_source, data_source, confidence
            ) VALUES (
                %(cid)s, %(pe)s, %(fy)s, NULL, '10-K',
                %(sbc)s, %(rev)s, %(gp)s, %(ni)s,
                %(shr)s, %(shrep)s, %(bb)s,
                %(unrec)s, %(oi)s, %(da)s,
                %(ebitda)s, %(ebitda_src)s, %(src)s, %(conf)s
            )
            ON CONFLICT (company_id, period_end, form_type) DO UPDATE SET
                sbc_expense               = COALESCE(EXCLUDED.sbc_expense, filings.sbc_expense),
                revenue                   = COALESCE(EXCLUDED.revenue, filings.revenue),
                gross_profit              = COALESCE(EXCLUDED.gross_profit, filings.gross_profit),
                net_income                = COALESCE(EXCLUDED.net_income, filings.net_income),
                shares_outstanding        = COALESCE(EXCLUDED.shares_outstanding, filings.shares_outstanding),
                shares_repurchased        = COALESCE(EXCLUDED.shares_repurchased, filings.shares_repurchased),
                buyback_spend             = COALESCE(EXCLUDED.buyback_spend, filings.buyback_spend),
                unrecognized_sbc          = COALESCE(EXCLUDED.unrecognized_sbc, filings.unrecognized_sbc),
                operating_income          = COALESCE(EXCLUDED.operating_income, filings.operating_income),
                depreciation_amortization = COALESCE(EXCLUDED.depreciation_amortization, filings.depreciation_amortization),
                ebitda                    = COALESCE(EXCLUDED.ebitda, filings.ebitda),
                ebitda_source             = COALESCE(EXCLUDED.ebitda_source, filings.ebitda_source),
                data_source               = COALESCE(EXCLUDED.data_source, filings.data_source),
                confidence                = COALESCE(EXCLUDED.confidence, filings.confidence),
                fetched_at                = NOW()
        """, {
            "cid": company_id, "pe": period_end, "fy": fiscal_year,
            "sbc": sbc_val,
            "rev": rev_ann.get(d), "gp": gp_ann.get(d), "ni": ni_ann.get(d),
            "shr": shr_ann.get(d), "shrep": shrep_ann.get(d), "bb": bb_ann.get(d),
            "unrec": unrec_ann.get(d),
            "oi": oi_val, "da": da_val, "ebitda": ebitda_val, "ebitda_src": ebitda_src,
            "src": "layer1", "conf": "high" if sbc_val else None,
        })
        if sbc_val:
            years_with_sbc.add(fiscal_year)

    return years_with_sbc


# ── Layer 2: XBRL Instance Document ─────────────────────────────────────────

def get_submissions(cik):
    """Get list of 10-K filings from EDGAR submissions API."""
    url = f"{EDGAR_BASE}/submissions/CIK{cik}.json"
    r = fetch_url(url)
    if not r:
        return []
    data = r.json()
    recent = data.get("filings", {}).get("recent", {})
    forms   = recent.get("form", [])
    accns   = recent.get("accessionNumber", [])
    periods = recent.get("reportDate", [])
    results = []
    for i, form in enumerate(forms):
        if form in ("10-K", "10-K/A") and i < len(periods) and periods[i]:
            yr = int(periods[i][:4])
            if yr >= START_YEAR:
                results.append({
                    "form": form,
                    "accession_dashed": accns[i],
                    "accession_nodash": accns[i].replace("-", ""),
                    "period": periods[i],
                    "fiscal_year": yr,
                })
    return results


def _get_xbrl_instance_url(cik, accession_nodash, accession_dashed):
    """Find the XBRL instance document URL from the filing index."""
    cik_int = int(cik)
    index_url = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession_nodash}/{accession_dashed}-index.json"
    r = fetch_url(index_url)
    if not r:
        return None
    try:
        items = r.json().get("directory", {}).get("item", [])
    except Exception:
        return None
    skip_suffixes = ("_cal.xml", "_lab.xml", "_pre.xml", "_def.xml", "_ref.xml")
    for item in items:
        name = item.get("name", "")
        if name.endswith(".xml") and not any(name.endswith(s) for s in skip_suffixes):
            return f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession_nodash}/{name}"
    return None


SBC_XML_PATTERNS = ["sharebasedcompensation", "stockbasedcompensation", "sharebasedpayment", "stockcompensation"]

def layer2_parse_instance(xml_bytes):
    """
    Parse XBRL instance XML for SBC concepts (catches company extension tags).
    Returns {concept_local_name: value} for plausible SBC expense entries.
    """
    try:
        root = ElementTree.fromstring(xml_bytes)
    except Exception as e:
        print(f"    [L2] XML parse error: {e}")
        return {}

    candidates = {}
    for elem in root.iter():
        local = (elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag).lower()
        if not any(p in local for p in SBC_XML_PATTERNS):
            continue
        # Skip share-count tags, tax benefit, and non-cash items that aren't P&L expense
        if any(x in local for x in ("shares", "tax", "unrecognized", "numberof", "weighted")):
            continue
        text = (elem.text or "").strip()
        if not text.lstrip("-").isdigit():
            continue
        val = int(text)
        # Must be a plausible dollar figure (>$100K, <$500B)
        if abs(val) < 100_000 or abs(val) > 500_000_000_000:
            continue
        if local not in candidates or abs(val) > abs(candidates[local]):
            candidates[local] = val
    return candidates


def layer2_fetch_and_save(cur, company_id, cik, filing, covered_years):
    """For a specific 10-K filing missing from Layer 1, try instance doc."""
    fiscal_year = filing["fiscal_year"]
    if fiscal_year in covered_years:
        return False

    print(f"    [L2] Trying instance doc for FY{fiscal_year}...")
    xbrl_url = _get_xbrl_instance_url(cik, filing["accession_nodash"], filing["accession_dashed"])
    if not xbrl_url:
        print(f"    [L2] No XBRL instance found for FY{fiscal_year}")
        return False

    r = fetch_url(xbrl_url)
    if not r:
        return False

    candidates = layer2_parse_instance(r.content)
    if not candidates:
        print(f"    [L2] No SBC concepts found in instance doc for FY{fiscal_year}")
        return False

    # Pick the candidate with the largest absolute value (most likely to be total SBC expense)
    best_tag, best_val = max(candidates.items(), key=lambda x: abs(x[1]))
    is_standard = any(best_tag == c.lower() for c in SBC_CONCEPTS)
    confidence  = "high" if is_standard else "medium"
    print(f"    [L2] FY{fiscal_year}: found '{best_tag}' = ${best_val:,} (conf={confidence})")

    period_end = date.fromisoformat(filing["period"])
    cur.execute("""
        INSERT INTO filings (
            company_id, period_end, fiscal_year, fiscal_quarter, form_type,
            sbc_expense, data_source, confidence
        ) VALUES (%(cid)s, %(pe)s, %(fy)s, NULL, '10-K', %(sbc)s, %(src)s, %(conf)s)
        ON CONFLICT (company_id, period_end, form_type) DO UPDATE SET
            sbc_expense = COALESCE(EXCLUDED.sbc_expense, filings.sbc_expense),
            data_source = COALESCE(filings.data_source, EXCLUDED.data_source),
            confidence  = COALESCE(filings.confidence, EXCLUDED.confidence),
            fetched_at  = NOW()
    """, {
        "cid": company_id, "pe": period_end, "fy": fiscal_year,
        "sbc": best_val,
        "src": f"layer2|{best_tag}", "conf": confidence,
    })
    return True


# ── Layer 3: Filing HTML Parse ───────────────────────────────────────────────

def layer3_fetch_and_save(cur, company_id, cik, filing, covered_years):
    """Last resort: parse 10-K HTML for SBC figures using BeautifulSoup."""
    if not BS4_AVAILABLE:
        return False
    fiscal_year = filing["fiscal_year"]
    if fiscal_year in covered_years:
        return False

    print(f"    [L3] Trying HTML parse for FY{fiscal_year}...")
    cik_int = int(cik)
    index_url = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{filing['accession_nodash']}/{filing['accession_dashed']}-index.json"
    r = fetch_url(index_url)
    if not r:
        return False

    # Find main .htm document
    try:
        items = r.json().get("directory", {}).get("item", [])
    except Exception:
        return False
    htm_url = None
    for item in items:
        name = item.get("name", "")
        if name.endswith(".htm") and not re.search(r"ex[-_]|exhibit", name, re.I):
            htm_url = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{filing['accession_nodash']}/{name}"
            break
    if not htm_url:
        return False

    r = fetch_url(htm_url)
    if not r:
        return False

    try:
        soup = BeautifulSoup(r.text, "html.parser")
        text = soup.get_text(" ", strip=True)
    except Exception as e:
        print(f"    [L3] Parse error: {e}")
        return False

    # Search for SBC dollar amount near keywords
    patterns = [
        r"stock[- ]based compensation(?:\s+expense)?\s+[\$\(]?\s*([\d,]+)",
        r"share[- ]based compensation(?:\s+expense)?\s+[\$\(]?\s*([\d,]+)",
    ]
    for pat in patterns:
        match = re.search(pat, text, re.IGNORECASE)
        if match:
            val_str = match.group(1).replace(",", "")
            try:
                val = int(val_str)
                # EDGAR tables typically in thousands — scale up if suspiciously small
                if val < 10_000:
                    val *= 1_000_000
                elif val < 100_000_000:
                    val *= 1_000
                snippet = text[max(0, match.start()-50):match.end()+50]
                print(f"    [L3] FY{fiscal_year}: found ${val:,} via HTML parse")
                period_end = date.fromisoformat(filing["period"])
                cur.execute("""
                    INSERT INTO filings (
                        company_id, period_end, fiscal_year, fiscal_quarter, form_type,
                        sbc_expense, data_source, confidence
                    ) VALUES (%(cid)s, %(pe)s, %(fy)s, NULL, '10-K', %(sbc)s, 'layer3|html_parse', 'low')
                    ON CONFLICT (company_id, period_end, form_type) DO UPDATE SET
                        sbc_expense = COALESCE(EXCLUDED.sbc_expense, filings.sbc_expense),
                        data_source = COALESCE(filings.data_source, EXCLUDED.data_source),
                        confidence  = COALESCE(filings.confidence, EXCLUDED.confidence),
                        fetched_at  = NOW()
                """, {"cid": company_id, "pe": period_end, "fy": fiscal_year, "sbc": val})
                return True
            except (ValueError, TypeError):
                continue
    print(f"    [L3] FY{fiscal_year}: no SBC found in HTML")
    return False


# ── Metrics computation ──────────────────────────────────────────────────────

def refresh_metrics(cur, company_id):
    """Compute annual metrics from 10-K filings only (10-Q is YTD cumulative)."""
    cur.execute("DELETE FROM metrics WHERE company_id = %s", (company_id,))
    cur.execute("""
        SELECT fiscal_year,
               sbc_expense, revenue, gross_profit, net_income,
               buyback_spend, shares_repurchased, shares_outstanding,
               unrecognized_sbc, operating_income, depreciation_amortization, ebitda
        FROM filings
        WHERE company_id = %s AND form_type = '10-K'
        ORDER BY fiscal_year
    """, (company_id,))
    rows = {r["fiscal_year"]: r for r in cur.fetchall()}
    prev_rev = None

    for yr in sorted(rows):
        r = rows[yr]
        sbc   = r["sbc_expense"]
        rev   = r["revenue"]
        gp    = r["gross_profit"]
        shr   = r["shares_outstanding"]
        bb    = r["buyback_spend"]
        shrep = r["shares_repurchased"]
        oi    = r["operating_income"]
        da    = r["depreciation_amortization"]
        # Use stored ebitda if available, else derive from components
        ebitda = r["ebitda"] if r["ebitda"] is not None else (
            (oi + da) if (oi is not None and da is not None) else None
        )

        sbc_pct_rev    = (sbc / rev * 100)      if sbc and rev    else None
        sbc_pct_gp     = (sbc / gp * 100)       if sbc and gp     else None
        sbc_per_shr    = (sbc / shr)             if sbc and shr    else None
        rev_growth     = ((rev - prev_rev) / prev_rev * 100) if rev and prev_rev else None
        net_dil        = (shrep / shr * 100)     if shrep and shr  else None
        sbc_pct_ebitda = (sbc / ebitda * 100)   if sbc and ebitda else None
        ebitda_negative = (ebitda < 0)           if ebitda is not None else None

        cur.execute("""
            INSERT INTO metrics (
                company_id, fiscal_year,
                sbc_annual, revenue_annual, gross_profit_annual, net_income_annual,
                buyback_spend_annual, shares_repurchased_annual, shares_outstanding_eoy,
                sbc_pct_revenue, sbc_pct_gross_profit, sbc_per_share,
                net_dilution_pct, revenue_growth_yoy, unrecognized_sbc_annual,
                ebitda_annual, sbc_pct_ebitda, ebitda_negative, computed_at
            ) VALUES (
                %(cid)s, %(fy)s,
                %(sbc)s, %(rev)s, %(gp)s, %(ni)s,
                %(bb)s, %(shrep)s, %(shr)s,
                %(sbc_pct_rev)s, %(sbc_pct_gp)s, %(sbc_per_shr)s,
                %(net_dil)s, %(rev_growth)s, %(unrec)s,
                %(ebitda)s, %(sbc_pct_ebitda)s, %(ebitda_negative)s, NOW()
            )
            ON CONFLICT (company_id, fiscal_year) DO UPDATE SET
                sbc_annual              = EXCLUDED.sbc_annual,
                revenue_annual          = EXCLUDED.revenue_annual,
                gross_profit_annual     = EXCLUDED.gross_profit_annual,
                net_income_annual       = EXCLUDED.net_income_annual,
                buyback_spend_annual    = EXCLUDED.buyback_spend_annual,
                shares_repurchased_annual = EXCLUDED.shares_repurchased_annual,
                shares_outstanding_eoy  = EXCLUDED.shares_outstanding_eoy,
                sbc_pct_revenue         = EXCLUDED.sbc_pct_revenue,
                sbc_pct_gross_profit    = EXCLUDED.sbc_pct_gross_profit,
                sbc_per_share           = EXCLUDED.sbc_per_share,
                net_dilution_pct        = EXCLUDED.net_dilution_pct,
                revenue_growth_yoy      = EXCLUDED.revenue_growth_yoy,
                unrecognized_sbc_annual = EXCLUDED.unrecognized_sbc_annual,
                ebitda_annual           = EXCLUDED.ebitda_annual,
                sbc_pct_ebitda          = EXCLUDED.sbc_pct_ebitda,
                ebitda_negative         = EXCLUDED.ebitda_negative,
                computed_at             = NOW()
        """, {
            "cid": company_id, "fy": yr,
            "sbc": sbc, "rev": rev, "gp": gp, "ni": r["net_income"],
            "bb": bb, "shrep": shrep, "shr": shr,
            "sbc_pct_rev": sbc_pct_rev, "sbc_pct_gp": sbc_pct_gp,
            "sbc_per_shr": sbc_per_shr, "net_dil": net_dil, "rev_growth": rev_growth,
            "unrec": r["unrecognized_sbc"],
            "ebitda": ebitda, "sbc_pct_ebitda": sbc_pct_ebitda,
            "ebitda_negative": ebitda_negative,
        })
        if rev:
            prev_rev = rev


# ── Coverage report ──────────────────────────────────────────────────────────

def print_coverage_report(coverage):
    """Print a summary of which layer resolved each company's data."""
    total = sum(v["total_filings"] for v in coverage.values())
    l1 = sum(v["layer1"] for v in coverage.values())
    l2 = sum(v["layer2"] for v in coverage.values())
    l3 = sum(v["layer3"] for v in coverage.values())
    missing = sum(v["missing"] for v in coverage.values())

    print("\n" + "="*60)
    print("COVERAGE REPORT")
    print("="*60)
    print(f"  Total 10-K filings processed : {total}")
    print(f"  Layer 1 (XBRL facts)         : {l1} ({l1/total*100:.0f}%)" if total else "")
    print(f"  Layer 2 (instance doc)       : {l2} ({l2/total*100:.0f}%)" if total else "")
    print(f"  Layer 3 (HTML parse)         : {l3} ({l3/total*100:.0f}%)" if total else "")
    print(f"  Still missing SBC            : {missing} ({missing/total*100:.0f}%)" if total else "")
    if missing > 0:
        print("\n  Companies with gaps:")
        for ticker, v in coverage.items():
            if v["missing"] > 0:
                print(f"    {ticker}: {v['missing']} filings still missing SBC")
    print("="*60)


def print_coverage_matrix(cur):
    """
    Print a matrix: companies × concepts showing % of expected annual periods filled.
    GREEN ≥90%  YELLOW 70-89%  RED <70%
    Expected = years from max(START_YEAR, IPO year) to the latest year in metrics.
    """
    # Gracefully skip if company_tags table doesn't exist yet
    try:
        cur.execute("SELECT 1 FROM company_tags LIMIT 1")
    except Exception:
        print("\n[INFO] company_tags table not found — run schema_migrations.sql first")
        return

    cur.execute("""
        SELECT c.ticker, c.ipo_year,
               ct.concept, ct.periods_found, ct.tag_used, ct.source
        FROM company_tags ct
        JOIN companies c ON c.id = ct.company_id
        ORDER BY c.ticker, ct.concept
    """)
    tag_rows = cur.fetchall()

    # Latest fiscal year across all metrics (used as upper bound for expected)
    cur.execute("SELECT COALESCE(MAX(fiscal_year), %s) AS max_yr FROM metrics", (START_YEAR,))
    max_yr = cur.fetchone()["max_yr"]

    # Organise by ticker
    by_ticker = {}
    ipo_by_ticker = {}
    for r in tag_rows:
        t = r["ticker"]
        if t not in by_ticker:
            by_ticker[t] = {}
            ipo_by_ticker[t] = r["ipo_year"] or START_YEAR
        by_ticker[t][r["concept"]] = {
            "periods": r["periods_found"] or 0,
            "tag":     r["tag_used"],
            "source":  r["source"],
        }

    CONCEPTS = ["sbc", "revenue", "operating_income", "da", "net_income", "buybacks"]
    LABELS   = ["SBC", "Revenue", "OpIncome", "D&A", "NetIncome", "Buybacks"]

    RED, YEL, GRN, RST = "\033[91m", "\033[93m", "\033[92m", "\033[0m"

    width = 10
    print("\n" + "=" * (8 + width * len(CONCEPTS)))
    print("COVERAGE MATRIX  (% of expected annual 10-K periods filled)")
    print("GREEN ≥90%  YELLOW 70-89%  RED <70%")
    print("-" * (8 + width * len(CONCEPTS)))
    print(f"{'Ticker':<8}" + "".join(f"{lbl:>{width}}" for lbl in LABELS))
    print("-" * (8 + width * len(CONCEPTS)))

    flagged = []
    for ticker in sorted(by_ticker):
        ipo  = max(ipo_by_ticker[ticker] or START_YEAR, START_YEAR)
        expected = max(1, max_yr - ipo + 1)
        data = by_ticker[ticker]

        row_str = f"{ticker:<8}"
        for concept, lbl in zip(CONCEPTS, LABELS):
            info    = data.get(concept, {"periods": 0, "source": "missing"})
            periods = info["periods"]
            pct     = min(100, periods / expected * 100)
            val     = f"{pct:.0f}%"
            if pct >= 90:
                row_str += f"{GRN}{val:>{width}}{RST}"
            elif pct >= 70:
                row_str += f"{YEL}{val:>{width}}{RST}"
            else:
                row_str += f"{RED}{val:>{width}}{RST}"
                flagged.append((ticker, concept, periods, expected, info.get("tag")))
        print(row_str)

    print("=" * (8 + width * len(CONCEPTS)))

    if flagged:
        print("\nFLAGGED — <70% coverage (ticker / concept / periods / expected / tag):")
        for ticker, concept, periods, expected, tag in flagged:
            print(f"  {ticker:<6} {concept:<18} {periods:>3}/{expected:<3}  tag={tag or 'none'}")

    print()


# ── Main orchestrator ────────────────────────────────────────────────────────

def fetch_company(cur, company, ticker_filter=None):
    ticker = company["ticker"]
    if ticker_filter and ticker.upper() != ticker_filter.upper():
        return None

    print(f"\n  [{ticker}] {company['name']}")
    cik = company["cik"]

    # Get company_id from DB
    cur.execute("SELECT id FROM companies WHERE ticker = %s", (ticker,))
    row = cur.fetchone()
    if not row:
        print(f"    [WARN] not in DB — skipping")
        return None
    company_id = row["id"]

    coverage = {"total_filings": 0, "layer1": 0, "layer2": 0, "layer3": 0, "missing": 0}

    # ── Layer 1 ──────────────────────────────────────────────────────────────
    facts = layer1_fetch_facts(cik)
    years_with_sbc = set()
    if facts:
        years_with_sbc = layer1_save_filings(cur, company_id, facts)
        coverage["layer1"] = len(years_with_sbc)
    else:
        print(f"    [L1] No EDGAR facts found")

    # ── Layers 2 & 3: fill gaps ───────────────────────────────────────────────
    submissions = get_submissions(cik)
    coverage["total_filings"] = max(len(submissions), len(years_with_sbc))

    for filing in submissions:
        fy = filing["fiscal_year"]
        if fy in years_with_sbc:
            continue  # Already have good Layer 1 data

        # Try Layer 2
        if layer2_fetch_and_save(cur, company_id, cik, filing, years_with_sbc):
            years_with_sbc.add(fy)
            coverage["layer2"] += 1
            continue

        # Try Layer 3
        if layer3_fetch_and_save(cur, company_id, cik, filing, years_with_sbc):
            years_with_sbc.add(fy)
            coverage["layer3"] += 1
            continue

        coverage["missing"] += 1

    # ── Compute metrics ───────────────────────────────────────────────────────
    refresh_metrics(cur, company_id)
    print(f"    done — {len(years_with_sbc)} years with SBC data")
    return coverage


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticker", help="Fetch a single company by ticker")
    args = parser.parse_args()

    conn = get_db()
    conn.autocommit = False
    cur = conn.cursor()

    print("Upserting companies...")
    upsert_companies(cur)
    conn.commit()

    print(f"\nFetching EDGAR data (from {START_YEAR})...")
    all_coverage = {}
    for company in COMPANIES:
        try:
            result = fetch_company(cur, company, ticker_filter=args.ticker)
            conn.commit()
            if result and company["ticker"] in (args.ticker or company["ticker"]):
                all_coverage[company["ticker"]] = result
        except Exception as e:
            print(f"    [ERROR] {company['ticker']}: {e}")
            conn.rollback()

    if all_coverage:
        print_coverage_report(all_coverage)

    # Coverage matrix — reads from company_tags table populated during ingestion
    print_coverage_matrix(cur)

    cur.close()
    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
