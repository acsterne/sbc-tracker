"""
fetch_historical.py — Brute-force historical ingestion for all companies.

For each company, enumerates every 10-K filing ever made via the EDGAR
submissions API, then extracts financial data from the XBRL instance
document (with label linkbase for human-readable labels) and falls back
to HTML table parsing if XBRL fails.

Speed does not matter. Correctness does.

Usage:
    DATABASE_URL=... python3 fetch_historical.py               # all companies
    DATABASE_URL=... python3 fetch_historical.py --ticker SNAP  # one company
    DATABASE_URL=... python3 fetch_historical.py --force        # re-fetch even if data exists
    DATABASE_URL=... python3 fetch_historical.py --ticker SNAP --force
"""

import os
import re
import time
import json
import argparse
import requests
import psycopg2
import psycopg2.extras
from datetime import date, datetime
from xml.etree import ElementTree

try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False
    print("[WARN] beautifulsoup4 not installed — HTML fallback disabled")

DATABASE_URL      = os.environ["DATABASE_URL"]
EDGAR_BASE        = "https://data.sec.gov"
EDGAR_ARCHIVES    = "https://www.sec.gov/Archives/edgar/data"
START_YEAR        = 2009
CHECKPOINT_FILE   = os.path.join(os.path.dirname(__file__), "historical_checkpoint.json")
INGESTION_LOG     = os.path.join(os.path.dirname(__file__), "historical_ingestion.log")

HEADERS = {
    "User-Agent": "SBC Tracker historical@sbctracker.io",
    "Accept":     "application/json",
}


# ── Concept fuzzy-matching rules ──────────────────────────────────────────────
#
# Matching runs on the human-readable label from the label linkbase (preferred)
# or on the camelCase tag name converted to space-separated words (fallback).
# All comparisons are lowercased.
#
# require_any   — at least one phrase must appear in the label
# require_all   — all phrases must appear (applied after require_any)
# exclude       — if any phrase appears, reject
# bonus         — extra score per matching phrase (used for tie-breaking)

CONCEPT_MATCHERS = {
    "sbc": {
        "require_any": ["stock-based compensation", "share-based compensation",
                        "stock based compensation", "share based compensation"],
        "require_all": [],
        "exclude":     ["nonvested", "unrecognized", "fair value", "weighted",
                        "deferred", "future"],
        "bonus":       ["expense"],
    },
    "revenue": {
        "require_any": ["revenue", "net sales", "net revenue", "total revenue",
                        "revenues", "sales"],
        "require_all": [],
        "exclude":     ["cost of", "deferred", "unearned", "backlog",
                        "remaining performance", "contract liability",
                        "other sales", "other revenue", "other operating"],
        "bonus":       ["total", "net"],
    },
    "gross_profit": {
        "require_any": ["gross profit", "gross margin"],
        "require_all": [],
        "exclude":     [],
        "bonus":       [],
    },
    "net_income": {
        "require_any": ["net income", "net loss", "net earnings",
                        "profit or loss", "net profit"],
        "require_all": [],
        "exclude":     ["comprehensive", "per share", "per diluted", "attributable to",
                        "noncontrolling"],
        "bonus":       [],
    },
    "operating_income": {
        "require_any": ["operating income", "operating loss",
                        "income from operations", "loss from operations",
                        "operating profit"],
        "require_all": [],
        "exclude":     [],
        "bonus":       [],
    },
    "da": {
        "require_any": ["depreciation", "depreciation and amortization",
                        "amortization and depreciation"],
        "require_all": [],
        "exclude":     ["accumulated", "right-of-use", "finance lease",
                        "property and equipment"],
        "bonus":       ["amortization"],
    },
    "buybacks": {
        "require_any": ["repurchase of common stock", "repurchases of common stock",
                        "treasury stock repurchase", "payments for repurchase"],
        "require_all": [],
        "exclude":     ["shares repurchased", "number of shares"],
        "bonus":       [],
    },
}

ALL_CONCEPTS = list(CONCEPT_MATCHERS.keys()) + ["shares_outstanding"]

# Concepts where consolidated total > segment subtotal — always pick largest value
PREFER_LARGER_VALUE = {"sbc", "revenue", "operating_income", "net_income"}

# Tags that look like a concept match but are wrong (segment, "other", fair value, etc.)
TAG_BLACKLIST = {
    "revenue": {
        "OtherSalesRevenueNet", "OtherRevenue", "RevenueOther",
        "OtherSalesRevenue", "RevenuesOther", "OtherOperatingIncome",
        "OtherNonoperatingIncomeExpense",
    },
    "sbc": {
        "ShareBasedCompensationArrangementByShareBasedPaymentAwardEquityInstrumentsOtherThanOptionsVestedInPeriodTotalFairValue",
    },
}

# Direct tag-name priority lists (bypass fuzzy matching for reliability)
SHARES_TAG_PRIORITY = [
    "CommonStockSharesOutstanding",
    "EntityCommonStockSharesOutstanding",
    "CommonStockSharesIssuedAndOutstanding",
]
BUYBACK_TAG_PRIORITY = [
    "PaymentsForRepurchaseOfCommonStock",
    "TreasuryStockValueAcquiredCostMethod",
    "StockRepurchasedDuringPeriodValue",
]


# ── Label scoring ─────────────────────────────────────────────────────────────

def _camel_to_words(tag):
    """'ShareBasedCompensationExpense' → 'share based compensation expense'"""
    return re.sub(r"([A-Z][a-z]+|[A-Z]+(?=[A-Z]|$))", r" \1", tag).strip().lower()


def _score_label(label_text, concept):
    """
    Score how well label_text matches a concept.
    Returns (score: int, matched: bool).  score=0 means no match.
    """
    text = label_text.lower().replace("-", " ")
    rules = CONCEPT_MATCHERS[concept]

    if not any(phrase in text for phrase in rules["require_any"]):
        return 0, False
    if any(phrase in text for phrase in rules["exclude"]):
        return 0, False
    for phrase in rules["require_all"]:
        if phrase not in text:
            return 0, False

    score = 10
    for phrase in rules["bonus"]:
        if phrase in text:
            score += 2
    return score, True


# ── HTTP utilities ────────────────────────────────────────────────────────────

def _sleep():
    time.sleep(0.12)  # EDGAR rate limit: max 10 req/sec; 0.12s keeps us at ~8/sec


# ── Checkpoint helpers ────────────────────────────────────────────────────────

def load_checkpoint():
    """Return set of tickers that completed successfully."""
    if not os.path.exists(CHECKPOINT_FILE):
        return set()
    try:
        with open(CHECKPOINT_FILE) as f:
            data = json.load(f)
        return set(data.get("completed", []))
    except Exception:
        return set()


def save_checkpoint(completed_tickers):
    """Persist the set of completed tickers to disk."""
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({
            "completed":    sorted(completed_tickers),
            "last_updated": datetime.utcnow().isoformat() + "Z",
            "count":        len(completed_tickers),
        }, f, indent=2)


# ── Per-filing logger ─────────────────────────────────────────────────────────

def log_filing(ticker, fiscal_year, accession, source, found_concepts, all_concepts):
    """Append one structured line to the ingestion log file."""
    missing = [c for c in all_concepts if c not in found_concepts]
    found   = [c for c in all_concepts if c in found_concepts]
    ts      = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    line    = (
        f"{ts} | {ticker:<6} | FY{fiscal_year} | {accession} | "
        f"src={source or 'none':<16} | "
        f"found=[{','.join(found)}] | "
        f"missing=[{','.join(missing)}]\n"
    )
    with open(INGESTION_LOG, "a") as f:
        f.write(line)
    # Also echo gaps to stdout for visibility
    if missing:
        print(f"        missing: {', '.join(missing)}")


def fetch_url(url, max_retries=4, accept="application/json"):
    hdrs = {**HEADERS, "Accept": accept}
    for attempt in range(max_retries):
        try:
            _sleep()
            r = requests.get(url, headers=hdrs, timeout=45)
            if r.status_code == 429:
                wait = 15 * (2 ** attempt)
                print(f"        [RATE LIMIT] sleeping {wait}s…")
                time.sleep(wait)
                continue
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            if attempt == max_retries - 1:
                print(f"        [ERROR] fetch failed: {url} — {e}")
                return None
            time.sleep(2 ** attempt)
    return None


def get_db():
    return psycopg2.connect(DATABASE_URL,
                            cursor_factory=psycopg2.extras.RealDictCursor,
                            connect_timeout=10)


# ── Filing enumeration ────────────────────────────────────────────────────────

def get_all_10k_filings(cik):
    """
    Return every 10-K/10-K/A filing for a company since START_YEAR,
    handling EDGAR's pagination for companies with long histories.
    Deduplicates by period — keeps latest amendment for each period.
    """
    url = f"{EDGAR_BASE}/submissions/CIK{cik}.json"
    r = fetch_url(url)
    if not r:
        return []

    data = r.json()

    def _extract(recent):
        out = []
        forms   = recent.get("form", [])
        accns   = recent.get("accessionNumber", [])
        periods = recent.get("reportDate", [])
        for i, form in enumerate(forms):
            if form not in ("10-K", "10-K/A"):
                continue
            if i >= len(periods) or not periods[i]:
                continue
            yr = int(periods[i][:4])
            if yr >= START_YEAR:
                out.append({
                    "form":             form,
                    "accession_dashed": accns[i],
                    "accession_nodash": accns[i].replace("-", ""),
                    "period":           periods[i],
                    "fiscal_year":      yr,
                })
        return out

    filings = _extract(data.get("filings", {}).get("recent", {}))

    for older in data.get("filings", {}).get("files", []):
        r2 = fetch_url(f"{EDGAR_BASE}/submissions/{older['name']}")
        if r2:
            filings.extend(_extract(r2.json()))

    # Deduplicate: latest amendment per period wins
    seen = {}
    for f in sorted(filings, key=lambda x: x["period"]):
        seen[f["period"]] = f
    return sorted(seen.values(), key=lambda x: x["period"])


def get_filing_index(cik, accession_nodash, accession_dashed):
    """Return the list of documents in a filing, or [] on failure."""
    cik_int = int(cik)
    # Correct EDGAR format:  .../edgar/data/{cik}/{accn_no_dashes}/{accn_with_dashes}-index.json
    url = f"{EDGAR_ARCHIVES}/{cik_int}/{accession_nodash}/{accession_dashed}-index.json"
    print(f"        [INDEX] {url}")
    r = fetch_url(url)
    if r:
        try:
            return r.json().get("directory", {}).get("item", [])
        except Exception:
            pass
    # Fallback: try .htm index for older filings
    url_htm = f"{EDGAR_ARCHIVES}/{cik_int}/{accession_nodash}/{accession_dashed}-index.htm"
    print(f"        [INDEX] JSON failed, trying .htm: {url_htm}")
    r = fetch_url(url_htm, accept="text/html")
    if not r:
        return []
    # Parse the .htm index page to extract document names
    try:
        from bs4 import BeautifulSoup as BS
        soup = BS(r.text, "html.parser")
        items = []
        for row in soup.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) >= 4:
                link = cells[2].find("a") if len(cells) > 2 else None
                name = link.get_text(strip=True) if link else cells[2].get_text(strip=True)
                if name and "." in name:
                    items.append({"name": name})
        if items:
            print(f"        [INDEX] .htm parsed: {len(items)} docs")
        return items
    except Exception:
        return []


# ── Label linkbase parsing ────────────────────────────────────────────────────

def parse_label_linkbase(xml_bytes):
    """
    Parse *_lab.xml to build {concept_local_name → human_readable_label}.
    Returns {} if parsing fails — callers fall back to camelCase conversion.
    """
    try:
        root = ElementTree.fromstring(xml_bytes)
    except Exception:
        return {}

    locs   = {}   # xlink:label → concept local name (from href #fragment)
    labels = {}   # xlink:label → label text  (preferring 'label' role)
    arcs   = []   # (loc_label, lab_label)

    for elem in root.iter():
        local = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
        # Strip namespace from attribute names for portability
        attrs = {(k.split("}")[-1] if "}" in k else k): v for k, v in elem.attrib.items()}

        if local == "loc":
            href  = attrs.get("href", "")
            label = attrs.get("label", "")
            if "#" in href and label:
                locs[label] = href.split("#")[-1]

        elif local == "label":
            xl_label = attrs.get("label", "")
            role     = attrs.get("role", "")
            if xl_label and elem.text and "label" in role.lower():
                # Prefer the standard (non-verbose, non-terse) label
                if xl_label not in labels or "verboseLabel" in role or "terseLabel" in role:
                    labels[xl_label] = elem.text.strip()

        elif local == "labelArc":
            frm = attrs.get("from", "")
            to  = attrs.get("to", "")
            if frm and to:
                arcs.append((frm, to))

    label_map = {}
    for frm, to in arcs:
        concept = locs.get(frm)
        text    = labels.get(to)
        if concept and text and concept not in label_map:
            label_map[concept] = text

    return label_map


# ── XBRL instance document parsing ───────────────────────────────────────────

def _normalize(raw_text, decimals_attr, debug_tag=None):
    """
    Convert a raw XBRL value to an integer in base dollars.

    The decimals attribute is ambiguous in practice:
    - Some filers use it as a scale indicator (decimals=-6 → value in millions)
    - Some filers report full dollar values with decimals=-6 as precision hint

    Strategy: if the raw value already looks like a plausible dollar amount
    (>= 1 million), assume it's in base dollars. Only scale up small values.
    """
    try:
        val = float(raw_text.strip())
    except (ValueError, TypeError):
        return None
    if val == 0:
        return 0

    dec_str = (decimals_attr or "").strip()
    if not dec_str or dec_str in ("INF", "inf"):
        if debug_tag:
            print(f"          [DEBUG] {debug_tag}: raw={raw_text.strip()} dec=INF → {int(val)}")
        return int(val)

    try:
        dec = int(dec_str)
    except (ValueError, TypeError):
        return int(val)

    result = int(val)

    if dec < 0 and abs(val) < 1_000_000:
        # Small value + negative decimals → value is likely in millions/thousands
        result = int(val * (10 ** abs(dec)))
    # else: value is already large enough to be in base dollars — no scaling

    if debug_tag:
        print(f"          [DEBUG] {debug_tag}: raw={raw_text.strip()} dec={dec} "
              f"abs_val={abs(val):,.0f} scaled={'YES' if result != int(val) else 'NO'} → ${result:,}")

    return result


_XBRL_SKIP = ("_cal.xml", "_def.xml", "_pre.xml", "_ref.xml", "_lab.xml")


def parse_xbrl_instance(xml_bytes, label_map):
    """
    Extract every USD and shares-unit numeric fact from an XBRL instance doc.
    Uses BeautifulSoup XML parser (lxml-xml) which is more tolerant of
    malformed XBRL than stdlib ElementTree.
    Returns list of {tag, label, value, period_start, period_end, unit_type}.
    """
    try:
        soup = BeautifulSoup(xml_bytes, features="xml")
    except Exception as e:
        print(f"        [XBRL] XML parse error: {e}")
        return []

    # ── Parse contexts ────────────────────────────────────────────────────────
    contexts = {}
    for ctx_tag in soup.find_all("context"):
        ctx_id = ctx_tag.get("id", "")
        start = end = None
        sd = ctx_tag.find("startDate")
        ed = ctx_tag.find("endDate")
        inst = ctx_tag.find("instant")
        try:
            if sd and sd.string:
                start = date.fromisoformat(sd.string.strip())
            if ed and ed.string:
                end = date.fromisoformat(ed.string.strip())
            if inst and inst.string:
                start = end = date.fromisoformat(inst.string.strip())
        except ValueError:
            pass
        if end:
            contexts[ctx_id] = {"start": start, "end": end}

    # ── Extract facts ─────────────────────────────────────────────────────────
    facts = []
    for elem in soup.find_all(True):
        # Skip structural elements
        tag_name = elem.name
        if tag_name in ("context", "unit", "schemaRef", "linkbaseRef",
                        "roleRef", "arcroleRef", "[document]"):
            continue
        # Skip container elements (no direct text value)
        raw = elem.string
        if raw is None:
            continue
        raw = raw.strip()
        if not raw or not re.fullmatch(r"-?\d+(\.\d+)?", raw):
            continue

        unit_ref = elem.get("unitRef", "")
        if not unit_ref:
            continue
        if "pure" in unit_ref.lower():
            continue

        if "share" in unit_ref.lower():
            unit_type = "shares"
        elif any(x in unit_ref.upper() for x in ("USD", "US_DOLLAR")):
            unit_type = "USD"
        else:
            continue

        decimals_str = elem.get("decimals", "0")
        if unit_type == "USD":
            val = _normalize(raw, decimals_str)
            if val is None or abs(val) < 1_000:
                continue
        else:
            try:
                val = int(float(raw))
            except (ValueError, TypeError):
                continue
            if val <= 0:
                continue

        ctx = contexts.get(elem.get("contextRef", ""))
        if not ctx or not ctx["end"] or ctx["end"].year < START_YEAR:
            continue

        # Local tag name (strip namespace prefix if present)
        local = tag_name.split(":")[-1] if ":" in tag_name else tag_name
        label = label_map.get(local) or _camel_to_words(local)
        facts.append({
            "tag":          local,
            "label":        label,
            "value":        val,
            "raw_text":     raw,
            "decimals":     decimals_str,
            "unit_type":    unit_type,
            "period_start": ctx["start"],
            "period_end":   ctx["end"],
        })

    return facts


# ── Concept mapping ───────────────────────────────────────────────────────────

def map_to_concepts(facts, fiscal_year):
    """
    Given raw facts from one filing, find the best value per concept.

    Filters to full-year duration facts (270–380 day window) whose period
    ends in the expected fiscal year (including Jan–Mar of the next calendar
    year for Jan/Feb fiscal year-end companies like Snowflake).

    Returns {concept: {value, tag, label, confidence}}
    """
    # Acceptable period_end range for this fiscal year
    fy_end_min = date(fiscal_year, 1, 1)
    fy_end_max = date(fiscal_year + 1, 3, 31)   # covers Jan/Feb FY-end companies

    annual = []
    for f in facts:
        if f.get("unit_type") != "USD":
            continue  # shares handled by extract_shares_outstanding()
        end   = f["period_end"]
        start = f["period_start"]
        if not (fy_end_min <= end <= fy_end_max):
            continue
        if start is None or start == end:
            continue   # instant / balance-sheet-point facts
        duration = (end - start).days
        if not (270 <= duration <= 395):   # 9–13 months
            continue
        annual.append(f)

    results = {}
    for concept in CONCEPT_MATCHERS:  # USD concepts only
        blacklist = TAG_BLACKLIST.get(concept, set())
        candidates = []
        for f in annual:
            if f["tag"] in blacklist:
                continue
            s1, m1 = _score_label(f["label"], concept)
            s2, m2 = _score_label(_camel_to_words(f["tag"]), concept)
            score  = max(s1, s2)
            if score >= 10:
                candidates.append((score, f))

        if not candidates:
            continue

        if concept in PREFER_LARGER_VALUE and len(candidates) > 1:
            # Consolidated totals are always larger than segment subtotals
            best_fact = max(candidates, key=lambda x: abs(x[1]["value"]))[1]
            best_score = max(s for s, _ in candidates)
        else:
            best_score, best_fact = max(candidates, key=lambda x: x[0])

        # Debug: show raw→normalized + how many candidates existed
        extra = f" ({len(candidates)} candidates)" if len(candidates) > 1 else ""
        print(f"        [MAP] {concept}: tag={best_fact['tag']} "
              f"raw={best_fact.get('raw_text','')} dec={best_fact.get('decimals','')} "
              f"→ ${best_fact['value']:,}{extra}")
        results[concept] = {
            "value":      best_fact["value"],
            "tag":        best_fact["tag"],
            "label":      best_fact["label"],
            "confidence": "high" if best_score >= 14 else "medium",
        }

    return results


def extract_shares_outstanding(facts, fiscal_year):
    """
    Find shares outstanding at fiscal year-end from instant (point-in-time) facts.
    Uses direct tag-name matching in priority order.
    For multi-class companies (META, GOOGL), sums Class A + Class B if no
    single total tag is found.
    """
    fy_end_min = date(fiscal_year, 1, 1)
    fy_end_max = date(fiscal_year + 1, 3, 31)

    candidates = []
    for f in facts:
        if f.get("unit_type") != "shares":
            continue
        end = f["period_end"]
        if not (fy_end_min <= end <= fy_end_max):
            continue
        start = f["period_start"]
        if start is not None and start != end:
            continue  # want instant / point-in-time only
        candidates.append(f)

    # 1. Try priority tags (total shares outstanding)
    for tag in SHARES_TAG_PRIORITY:
        for f in candidates:
            if f["tag"] == tag:
                return {"value": f["value"], "tag": tag, "label": f["label"],
                        "confidence": "high"}

    # 2. Fallback: any tag containing "sharesoutstanding"
    for f in candidates:
        if "sharesoutstanding" in f["tag"].lower():
            return {"value": f["value"], "tag": f["tag"], "label": f["label"],
                    "confidence": "medium"}

    # 3. Multi-class fallback: sum Class A + Class B + Class C shares
    #    (META has ClassACommonStockSharesOutstanding + ClassBCommonStockSharesOutstanding,
    #     GOOGL has Class A + Class B + Class C)
    class_shares = {}
    for f in candidates:
        tag_lower = f["tag"].lower()
        if "sharesoutstanding" not in tag_lower:
            continue
        for cls in ("classa", "classb", "classc", "class_a", "class_b", "class_c"):
            if cls in tag_lower:
                # Use the class letter as key to avoid double-counting
                key = cls[-1]  # 'a', 'b', or 'c'
                if key not in class_shares or f["value"] > class_shares[key]["value"]:
                    class_shares[key] = f
                break

    if class_shares:
        total = sum(f["value"] for f in class_shares.values())
        tags_used = "+".join(f["tag"] for f in class_shares.values())
        labels    = " + ".join(f["label"] for f in class_shares.values())
        print(f"        [SHARES] multi-class sum: {total:,} from {tags_used}")
        return {"value": total, "tag": tags_used, "label": labels,
                "confidence": "medium"}

    return None


def extract_buybacks_direct(facts, fiscal_year):
    """
    Direct tag-name matching for buyback spend (USD duration facts).
    Falls back after fuzzy matching fails.
    """
    fy_end_min = date(fiscal_year, 1, 1)
    fy_end_max = date(fiscal_year + 1, 3, 31)

    annual_usd = []
    for f in facts:
        if f.get("unit_type") != "USD":
            continue
        end   = f["period_end"]
        start = f["period_start"]
        if not (fy_end_min <= end <= fy_end_max):
            continue
        if start is None or start == end:
            continue
        duration = (end - start).days
        if not (270 <= duration <= 395):
            continue
        annual_usd.append(f)

    for tag in BUYBACK_TAG_PRIORITY:
        for f in annual_usd:
            if f["tag"] == tag:
                return {"value": f["value"], "tag": tag, "label": f["label"],
                        "confidence": "high"}

    return None


# ── HTML table fallback ───────────────────────────────────────────────────────

def parse_html_financial_tables(html_bytes, fiscal_year):
    """
    Last resort: parse income statement / cash flow tables from 10-K HTML.
    Returns {concept: {value, label, confidence='low'}} for any concepts found.
    """
    if not BS4_AVAILABLE:
        return {}

    try:
        soup = BeautifulSoup(html_bytes, "lxml")
    except Exception:
        return {}

    results = {}
    # Values in HTML tables are often in thousands — we scale up
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue
            label_text = cells[0].get_text(separator=" ", strip=True)
            if not label_text or len(label_text) > 120:
                continue

            # First numeric cell after the label
            val = None
            for cell in cells[1:]:
                raw = re.sub(r"[$,\s]", "", cell.get_text(strip=True))
                negative = raw.startswith("(") and raw.endswith(")")
                raw = raw.strip("()")
                if not raw or not raw.lstrip("-").isdigit():
                    continue
                try:
                    v = int(raw)
                    if negative:
                        v = -v
                    # Scale: HTML tables typically in thousands
                    if 0 < abs(v) < 100_000:
                        v *= 1_000
                    if abs(v) >= 100_000:
                        val = v
                        break
                except ValueError:
                    continue

            if val is None:
                continue

            for concept in ALL_CONCEPTS:
                if concept in results:
                    continue
                score, matched = _score_label(label_text, concept)
                if matched:
                    results[concept] = {
                        "value": val, "label": label_text, "confidence": "low"
                    }

    return results


# ── Single-filing processor ───────────────────────────────────────────────────

def process_filing(cur, company_id, ticker, cik, filing, force=False):
    """
    Extract financial data from one 10-K filing.
    Returns True if SBC data was found, False otherwise.
    """
    fiscal_year      = filing["fiscal_year"]
    acc_dashed       = filing["accession_dashed"]
    acc_nodash       = filing["accession_nodash"]
    period_end_str   = filing["period"]
    period_end       = date.fromisoformat(period_end_str)
    cik_int          = int(cik)

    # Skip only if ALL key fields are already populated
    if not force:
        cur.execute("""
            SELECT sbc_expense, shares_outstanding, buyback_spend,
                   revenue, net_income, operating_income, depreciation_amortization
            FROM filings
            WHERE company_id = %s AND fiscal_year = %s AND form_type = '10-K'
        """, (company_id, fiscal_year))
        existing = cur.fetchone()
        if existing and all(existing[f] is not None for f in
                           ("sbc_expense", "shares_outstanding", "revenue",
                            "net_income", "operating_income", "depreciation_amortization")):
            print(f"      FY{fiscal_year}: complete — skip")
            return True
        if existing:
            missing = [f for f in ("sbc_expense", "shares_outstanding", "buyback_spend",
                                   "revenue", "net_income", "operating_income",
                                   "depreciation_amortization")
                       if existing[f] is None]
            print(f"      FY{fiscal_year}: filling gaps: {', '.join(missing)}")

    print(f"      FY{fiscal_year}  ({acc_dashed})")

    # Get filing document list
    items = get_filing_index(cik, acc_nodash, acc_dashed)
    if not items:
        print(f"        [WARN] could not retrieve filing index")
        return False

    # ── 1. Load label linkbase (*_lab.xml) ─────────────────────────────────────
    label_map = {}
    lab_item = next(
        (it for it in items if it.get("name", "").lower().endswith("_lab.xml")), None
    )
    if lab_item:
        r = fetch_url(
            f"{EDGAR_ARCHIVES}/{cik_int}/{acc_nodash}/{lab_item['name']}",
            accept="application/xml",
        )
        if r:
            label_map = parse_label_linkbase(r.content)
            print(f"        labels: {len(label_map)}")

    # ── 2. Parse XBRL instance document ────────────────────────────────────────
    mapped = {}
    source = None

    xbrl_item = next(
        (it for it in items
         if it.get("name", "").lower().endswith(".xml")
         and not any(it["name"].lower().endswith(s) for s in _XBRL_SKIP)),
        None,
    )
    all_facts = []  # preserve for shares/buyback extraction below
    if xbrl_item:
        r = fetch_url(
            f"{EDGAR_ARCHIVES}/{cik_int}/{acc_nodash}/{xbrl_item['name']}",
            accept="application/xml",
        )
        if r:
            all_facts = parse_xbrl_instance(r.content, label_map)
            usd_count   = sum(1 for f in all_facts if f.get("unit_type") == "USD")
            share_count = sum(1 for f in all_facts if f.get("unit_type") == "shares")
            print(f"        facts: {usd_count} USD, {share_count} shares")
            if all_facts:
                mapped = map_to_concepts(all_facts, fiscal_year)
                # Direct tag extraction for shares outstanding
                shr = extract_shares_outstanding(all_facts, fiscal_year)
                if shr:
                    mapped["shares_outstanding"] = shr
                # Direct tag fallback for buybacks
                if "buybacks" not in mapped:
                    bb = extract_buybacks_direct(all_facts, fiscal_year)
                    if bb:
                        mapped["buybacks"] = bb
                source = "xbrl_instance"
                if mapped:
                    found = ", ".join(
                        f"{c}=${v['value']:,}({v['confidence'][0]})"
                        for c, v in mapped.items()
                    )
                    print(f"        mapped: {found}")

    # ── 3. HTML fallback if no SBC found ───────────────────────────────────────
    if not mapped.get("sbc") and BS4_AVAILABLE:
        htm_item = next(
            (it for it in items
             if it.get("name", "").lower().endswith(".htm")
             and not re.search(r"ex[-_]|exhibit|r\d+\.", it.get("name", ""), re.I)),
            None,
        )
        if htm_item:
            r = fetch_url(
                f"{EDGAR_ARCHIVES}/{cik_int}/{acc_nodash}/{htm_item['name']}",
                accept="text/html",
            )
            if r:
                html_mapped = parse_html_financial_tables(r.content, fiscal_year)
                for concept, val in html_mapped.items():
                    if concept not in mapped:
                        mapped[concept] = val
                if "sbc" in html_mapped:
                    source = source or "html_parse"
                    print(f"        [HTML] SBC=${html_mapped['sbc']['value']:,}")

    if not mapped:
        print(f"        [WARN] no financial data extracted")
        log_filing(ticker, fiscal_year, acc_dashed, None, {}, ALL_CONCEPTS)
        return False

    # ── 4. Compute EBITDA from components ──────────────────────────────────────
    oi     = mapped.get("operating_income", {}).get("value")
    da     = mapped.get("da", {}).get("value")
    ebitda = (oi + da) if (oi is not None and da is not None) else None

    # ── 5. Upsert into filings ─────────────────────────────────────────────────
    cur.execute("""
        INSERT INTO filings (
            company_id, period_end, fiscal_year, fiscal_quarter, form_type,
            sbc_expense, revenue, gross_profit, net_income, buyback_spend,
            shares_outstanding,
            operating_income, depreciation_amortization, ebitda, ebitda_source,
            accession_number, data_source, confidence
        ) VALUES (
            %(cid)s, %(pe)s, %(fy)s, NULL, '10-K',
            %(sbc)s, %(rev)s, %(gp)s, %(ni)s, %(bb)s,
            %(shr)s,
            %(oi)s,  %(da)s, %(ebitda)s, %(ebitda_src)s,
            %(accn)s, %(src)s, %(conf)s
        )
        ON CONFLICT (company_id, fiscal_year, form_type) DO UPDATE SET
            sbc_expense               = COALESCE(filings.sbc_expense,
                                                 EXCLUDED.sbc_expense),
            revenue                   = COALESCE(filings.revenue,
                                                 EXCLUDED.revenue),
            gross_profit              = COALESCE(filings.gross_profit,
                                                 EXCLUDED.gross_profit),
            net_income                = COALESCE(filings.net_income,
                                                 EXCLUDED.net_income),
            buyback_spend             = COALESCE(filings.buyback_spend,
                                                 EXCLUDED.buyback_spend),
            shares_outstanding        = COALESCE(filings.shares_outstanding,
                                                 EXCLUDED.shares_outstanding),
            operating_income          = COALESCE(filings.operating_income,
                                                 EXCLUDED.operating_income),
            depreciation_amortization = COALESCE(filings.depreciation_amortization,
                                                 EXCLUDED.depreciation_amortization),
            ebitda                    = COALESCE(filings.ebitda,
                                                 EXCLUDED.ebitda),
            ebitda_source             = COALESCE(filings.ebitda_source,
                                                 EXCLUDED.ebitda_source),
            accession_number          = COALESCE(filings.accession_number,
                                                 EXCLUDED.accession_number),
            data_source               = COALESCE(filings.data_source,
                                                 EXCLUDED.data_source),
            confidence                = COALESCE(filings.confidence,
                                                 EXCLUDED.confidence),
            fetched_at                = NOW()
    """, {
        "cid":        company_id,
        "pe":         period_end,
        "fy":         fiscal_year,
        "sbc":        mapped.get("sbc",              {}).get("value"),
        "rev":        mapped.get("revenue",          {}).get("value"),
        "gp":         mapped.get("gross_profit",     {}).get("value"),
        "ni":         mapped.get("net_income",       {}).get("value"),
        "bb":         mapped.get("buybacks",         {}).get("value"),
        "shr":        mapped.get("shares_outstanding", {}).get("value"),
        "oi":         oi,
        "da":         da,
        "ebitda":     ebitda,
        "ebitda_src": "xbrl_derived" if ebitda is not None else None,
        "accn":       acc_dashed,
        "src":        f"historical|{source}",
        "conf":       mapped.get("sbc", {}).get("confidence"),
    })

    log_filing(ticker, fiscal_year, acc_dashed, source, mapped, ALL_CONCEPTS)
    return mapped.get("sbc") is not None


# ── Company processor ─────────────────────────────────────────────────────────

def process_company(cur, company_id, ticker, cik, force=False):
    """
    Enumerate and process every 10-K filing for one company.
    Commits after each filing. Recomputes metrics at the end.
    """
    print(f"\n  [{ticker}] enumerating 10-K filings…")
    filings = get_all_10k_filings(cik)
    if not filings:
        print(f"    [WARN] no filings found")
        return {"total": 0, "with_sbc": 0, "skipped": 0, "failed": 0}

    print(f"    {len(filings)} filings found")
    stats = {"total": len(filings), "with_sbc": 0, "skipped": 0, "failed": 0}

    for filing in filings:
        try:
            ok = process_filing(cur, company_id, ticker, cik, filing, force=force)
            if ok:
                stats["with_sbc"] += 1
            else:
                stats["failed"] += 1
        except Exception as e:
            print(f"        [ERROR] FY{filing['fiscal_year']}: {e}")
            log_filing(ticker, filing["fiscal_year"], filing["accession_dashed"],
                       "error", {}, ALL_CONCEPTS)
            stats["failed"] += 1
        cur.connection.commit()

    # Recompute metrics from freshly populated filings
    from fetch_sbc import refresh_metrics
    refresh_metrics(cur, company_id)
    cur.connection.commit()

    # Print year-by-year summary
    cur.execute("""
        SELECT fiscal_year, sbc_expense, shares_outstanding, buyback_spend
        FROM filings
        WHERE company_id = %s AND form_type = '10-K'
        ORDER BY fiscal_year
    """, (company_id,))
    filing_rows = cur.fetchall()
    if filing_rows:
        print(f"\n    [{ticker}] Year-by-year:")
        print(f"    {'Year':<6} {'SBC':>16} {'Shares Outstanding':>20} {'Buybacks':>16}")
        print(f"    {'-'*6} {'-'*16} {'-'*20} {'-'*16}")
        for r in filing_rows:
            sbc_str = f"${r['sbc_expense']:,}" if r['sbc_expense'] else "—"
            shr_str = f"{r['shares_outstanding']:,}" if r['shares_outstanding'] else "—"
            bb_str  = f"${r['buyback_spend']:,}" if r['buyback_spend'] else "—"
            print(f"    {r['fiscal_year']:<6} {sbc_str:>16} {shr_str:>20} {bb_str:>16}")

    # ── Validate this company ─────────────────────────────────────────────────
    from validate import run_benchmarks, run_sanity_rules, heal_suspect_values
    bench  = run_benchmarks(cur, ticker_filter=ticker)
    flags  = run_sanity_rules(cur, ticker_filter=ticker)
    healed = 0

    if flags:
        print(f"\n    [{ticker}] {len(flags)} suspect values — healing...")
        healed = heal_suspect_values(cur, flags)
        cur.connection.commit()

        # Re-validate after healing
        flags_after = run_sanity_rules(cur, ticker_filter=ticker)
        needs_review = len(flags_after)
        if needs_review:
            print(f"    [{ticker}] {needs_review} values still suspect after healing — needs_review")
    else:
        needs_review = 0

    bench_pass = bench["passed"]
    bench_fail = bench["failed"] + bench["missing"]
    if bench_fail:
        print(f"    [{ticker}] benchmarks: {bench_pass} passed, {bench_fail} failed")
    elif bench_pass:
        print(f"    [{ticker}] benchmarks: {bench_pass}/{bench_pass} passed")

    print(f"    [{ticker}] done — {stats['with_sbc']}/{stats['total']} filings with SBC")

    stats["validation"] = {
        "bench": bench,
        "flags": len(flags),
        "healed": healed,
        "needs_review": needs_review,
    }
    return stats


# ── Summary report ────────────────────────────────────────────────────────────

def print_summary(all_stats):
    total_companies = len(all_stats)
    total_filings   = sum(s["total"]    for s in all_stats.values())
    total_sbc       = sum(s["with_sbc"] for s in all_stats.values())
    total_failed    = sum(s["failed"]   for s in all_stats.values())

    # Aggregate validation results
    total_bench_pass = 0
    total_bench_fail = 0
    total_flags      = 0
    total_healed     = 0
    total_review     = 0
    all_bench_details = []
    for s in all_stats.values():
        v = s.get("validation", {})
        b = v.get("bench", {})
        total_bench_pass += b.get("passed", 0)
        total_bench_fail += b.get("failed", 0) + b.get("missing", 0)
        total_flags      += v.get("flags", 0)
        total_healed     += v.get("healed", 0)
        total_review     += v.get("needs_review", 0)
        all_bench_details.extend(b.get("details", []))

    RED = "\033[91m"
    GRN = "\033[92m"
    YEL = "\033[93m"
    RST = "\033[0m"
    BOLD = "\033[1m"

    print(f"\n{'='*65}")
    print(f"{BOLD}INGESTION + VALIDATION SUMMARY{RST}")
    print(f"{'='*65}")
    print(f"  Companies processed : {total_companies}")
    print(f"  Total 10-K filings  : {total_filings}")
    if total_filings:
        print(f"  Filings with SBC    : {total_sbc} ({total_sbc/total_filings*100:.0f}%)")
    print(f"  Failed/no data      : {total_failed}")
    print()
    total_bench = total_bench_pass + total_bench_fail
    if total_bench:
        print(f"  Benchmark checks    : {total_bench_pass}/{total_bench} passed")
    print(f"  Sanity flags        : {total_flags} values flagged")
    print(f"  Self-healed         : {total_healed} values nulled")
    print(f"  Needs review        : {total_review} values still suspect")

    # Failed benchmarks
    failed_bench = [d for d in all_bench_details if d["status"] != "PASS"]
    if failed_bench:
        print(f"\n  {RED}FAILED BENCHMARKS:{RST}")
        for d in failed_bench:
            actual = d.get("actual")
            if actual is None:
                actual_str = "MISSING"
            elif abs(actual) >= 1e9:
                actual_str = f"${actual/1e9:.1f}B"
            else:
                actual_str = f"${actual/1e6:.0f}M"
            exp = d["expected"]
            exp_str = f"${exp/1e9:.1f}B" if abs(exp) >= 1e9 else f"${exp/1e6:.0f}M"
            pct_str = f" ({d.get('pct_off',0)*100:.0f}% off)" if actual else ""
            print(f"    {RED}{d['ticker']:<6} FY{d['year']} {d['concept']:<10}: "
                  f"got {actual_str}, expected {exp_str}{pct_str}{RST}")

    # Coverage
    print(f"\n  Coverage (filings with SBC / total):")
    for ticker, s in sorted(all_stats.items()):
        pct = s["with_sbc"] / s["total"] * 100 if s["total"] else 0
        flag = f"  {RED}✗ <70%{RST}" if pct < 70 else ""
        print(f"    {ticker:<8} {s['with_sbc']:>3}/{s['total']:<3}  {pct:.0f}%{flag}")
    print(f"{'='*65}")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Brute-force historical 10-K ingestion from EDGAR"
    )
    parser.add_argument("--ticker", help="Process only this company (e.g. SNAP)")
    parser.add_argument("--force",  action="store_true",
                        help="Re-fetch filings even when SBC data already exists")
    parser.add_argument("--reset-checkpoint", action="store_true",
                        help="Clear the checkpoint file and start fresh")
    args = parser.parse_args()

    # Checkpoint for crash recovery
    if args.reset_checkpoint and os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        print("[INFO] Checkpoint file cleared")

    completed = set() if args.ticker else load_checkpoint()
    if completed:
        print(f"[INFO] Resuming — {len(completed)} companies already completed "
              f"(use --reset-checkpoint to start over)")

    conn = get_db()
    conn.autocommit = False
    cur  = conn.cursor()

    cur.execute("SELECT id, ticker, cik, ipo_year FROM companies ORDER BY ticker")
    companies = cur.fetchall()

    if args.ticker:
        companies = [c for c in companies
                     if c["ticker"].upper() == args.ticker.upper()]
        if not companies:
            print(f"[ERROR] {args.ticker} not found in companies table")
            return

    all_stats = {}
    for co in companies:
        ticker = co["ticker"]

        # Skip if already completed (checkpoint)
        if ticker in completed:
            print(f"\n  [{ticker}] already completed — skipping")
            continue

        try:
            stats = process_company(
                cur, co["id"], ticker, co["cik"], force=args.force
            )
            all_stats[ticker] = stats

            # Save checkpoint after each successful company
            if not args.ticker:
                completed.add(ticker)
                save_checkpoint(completed)

        except Exception as e:
            print(f"\n  [ERROR] {ticker}: {e}")
            try:
                cur.connection.rollback()
            except Exception:
                pass

    print_summary(all_stats)

    # Run consolidated validation report across ALL companies (not just processed ones)
    if all_stats:
        try:
            from validate import run_benchmarks, run_sanity_rules, print_report
            print("\n  Running final consolidated validation across all companies...")
            final_bench = run_benchmarks(cur)
            final_flags = run_sanity_rules(cur)
            print_report(final_bench, final_flags, healed_count=0)
        except Exception as e:
            print(f"[INFO] Consolidated validation skipped: {e}")

    cur.close()
    conn.close()

    if not args.ticker and all_stats:
        print(f"[INFO] Checkpoint saved to {CHECKPOINT_FILE}")
        print(f"[INFO] Detailed log at {INGESTION_LOG}")


if __name__ == "__main__":
    main()
