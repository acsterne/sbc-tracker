"""
debug_shares.py — Diagnose which share concepts have data for each company.

Prints every concept that returns data + the years available.
Run on Railway: python3 debug_shares.py

For each Priority 1 ticker, tries all known share concepts and shows coverage.
"""

from edgar import Company, set_identity
set_identity("research@sbctracker.io")

PRIORITY_1 = ["WDAY", "DDOG", "CRWD", "PINS", "SOUN", "AI", "CFLT", "LYFT"]
PRIORITY_2 = ["META", "GOOGL", "SNAP", "OKTA", "NET", "MDB", "ZM"]

ALL_TICKERS = PRIORITY_1 + PRIORITY_2

CONCEPTS = [
    "dei:EntityCommonStockSharesOutstanding",
    "EntityCommonStockSharesOutstanding",
    "us-gaap:CommonStockSharesOutstanding",
    "CommonStockSharesOutstanding",
    "us-gaap:CommonStockSharesIssued",
    "CommonStockSharesIssued",
    "us-gaap:WeightedAverageNumberOfSharesOutstandingBasic",
    "WeightedAverageNumberOfSharesOutstandingBasic",
    "us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding",
    "WeightedAverageNumberOfDilutedSharesOutstanding",
]


def debug_company(ticker):
    print(f"\n{'='*60}")
    print(f"  [{ticker}]")
    print(f"{'='*60}")

    company = Company(ticker)
    try:
        facts = company.get_facts()
    except Exception as e:
        print(f"  get_facts() FAILED: {e}")
        return

    # Method 1: time_series for each concept
    print(f"\n  time_series() results:")
    best_concept = None
    best_years = []

    for concept in CONCEPTS:
        try:
            df = facts.time_series(concept, periods=40)
            if df is None or df.empty:
                continue

            # Filter to FY
            if "fiscal_period" in df.columns:
                annual = df[df["fiscal_period"] == "FY"]
            else:
                annual = df

            years = set()
            for _, row in annual.iterrows():
                fy = None
                for col in ("fiscal_year", "fy"):
                    if col in row.index and row[col]:
                        try:
                            fy = int(row[col])
                        except (ValueError, TypeError):
                            pass
                        break
                if fy is None:
                    for col in ("period_end", "end"):
                        if col in row.index and row[col]:
                            try:
                                fy = int(str(row[col])[:4])
                            except (ValueError, TypeError):
                                pass
                            break
                if fy:
                    years.add(fy)

            if years:
                sorted_years = sorted(years)
                print(f"    {concept}")
                print(f"      {len(years)} years: {sorted_years[0]}–{sorted_years[-1]}: {sorted_years}")
                if len(years) > len(best_years):
                    best_years = sorted_years
                    best_concept = concept

        except Exception as e:
            # Don't print every failure — too noisy
            continue

    # Method 2: to_dataframe broad search
    print(f"\n  to_dataframe() search:")
    try:
        df = facts.to_dataframe()
        if df is not None and not df.empty:
            concept_col = next((c for c in ("concept", "tag", "name") if c in df.columns), None)
            if concept_col:
                # Find all share-related concepts
                mask = df[concept_col].str.contains("Share", case=False, na=False)
                share_concepts = df[mask][concept_col].unique()
                outstanding = [c for c in share_concepts
                               if any(kw in c.lower() for kw in
                                      ("outstanding", "issued", "weightedaverage"))]
                print(f"    Share-related concepts found: {len(share_concepts)}")
                print(f"    Outstanding/issued/weighted: {outstanding[:10]}")

                # For each, count annual data points
                for sc in outstanding[:8]:
                    sub = df[df[concept_col] == sc]
                    form_col = next((c for c in sub.columns if c.lower() in ("form", "form_type")), None)
                    if form_col:
                        sub_10k = sub[sub[form_col].isin(["10-K", "10-K/A"])]
                    else:
                        sub_10k = sub

                    fy_col = next((c for c in sub_10k.columns
                                   if c.lower() in ("fiscal_year", "fy")), None)
                    end_col = next((c for c in sub_10k.columns
                                    if c.lower() in ("period_end", "end")), None)

                    years = set()
                    for _, row in sub_10k.iterrows():
                        fy = None
                        if fy_col:
                            try: fy = int(row[fy_col])
                            except: pass
                        if fy is None and end_col:
                            try: fy = int(str(row[end_col])[:4])
                            except: pass
                        if fy:
                            years.add(fy)

                    if years:
                        sy = sorted(years)
                        print(f"      {sc}: {len(years)}yr {sy[0]}–{sy[-1]}")

    except Exception as e:
        print(f"    to_dataframe() failed: {e}")

    # Method 3: search_concepts
    print(f"\n  search_concepts():")
    try:
        results = facts.search_concepts("shares outstanding")
        if results:
            print(f"    {results[:5]}")
        else:
            print(f"    no results")
    except Exception as e:
        print(f"    {e}")

    if best_concept:
        print(f"\n  BEST: {best_concept} ({len(best_years)} years)")
    else:
        print(f"\n  NO SHARE DATA FOUND via time_series")


def main():
    for ticker in ALL_TICKERS:
        try:
            debug_company(ticker)
        except Exception as e:
            print(f"\n  [{ticker}] FATAL ERROR: {e}")


if __name__ == "__main__":
    main()
