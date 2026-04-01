"""
Quick test: does edgartools return clean financial data for META?
Run on Railway: python3 test_edgar.py
"""
from edgar import Company, set_identity
set_identity("research@sbctracker.io")

company = Company("META")
print(f"Company: {company.name} (CIK: {company.cik})")

filings = company.get_filings(form="10-K")
print(f"10-K filings found: {len(filings)}")

# ── Latest filing ─────────────────────────────────────────────────────────────
filing = filings.latest()
print(f"\nLatest: filed {filing.filing_date}, period {filing.period_of_report}")

try:
    xbrl = filing.xbrl()
    print(f"xbrl() returned: {type(xbrl)}")
except Exception as e:
    print(f"xbrl() ERROR: {e}")
    xbrl = None

if xbrl:
    print("\n" + "="*70)
    print("INCOME STATEMENT (latest)")
    print("="*70)
    try:
        inc = xbrl.income_statement()
        df = inc.to_dataframe()
        print(f"Columns: {list(df.columns)}")
        print(df.head(30).to_string())
    except Exception as e:
        print(f"ERROR: {e}")

    print("\n" + "="*70)
    print("CASH FLOW STATEMENT (latest)")
    print("="*70)
    try:
        cf = xbrl.cash_flow_statement()
        df = cf.to_dataframe()
        print(f"Columns: {list(df.columns)}")
        print(df.head(30).to_string())
    except Exception as e:
        print(f"ERROR: {e}")

    print("\n" + "="*70)
    print("BALANCE SHEET (latest)")
    print("="*70)
    try:
        bs = xbrl.balance_sheet()
        df = bs.to_dataframe()
        print(f"Columns: {list(df.columns)}")
        print(df.head(30).to_string())
    except Exception as e:
        print(f"ERROR: {e}")

# ── FY2019 filing ─────────────────────────────────────────────────────────────
print("\n\n" + "="*70)
print("FY2019 CASH FLOW (looking for SBC ~$4.8B)")
print("="*70)
try:
    filings_list = list(filings)
    print(f"Total filings to scan: {len(filings_list)}")
    for f in filings_list:
        por = str(f.period_of_report or "")
        if "2019" in por:
            print(f"Found: filed {f.filing_date}, period {f.period_of_report}")
            xbrl2 = f.xbrl()
            if xbrl2:
                cf2 = xbrl2.cash_flow_statement()
                df2 = cf2.to_dataframe()
                print(f"Columns: {list(df2.columns)}")
                print(df2.to_string())
            break
    else:
        print("No 2019 filing found — printing all periods:")
        for f in filings_list[:15]:
            print(f"  {f.filing_date} | {f.period_of_report}")
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()
